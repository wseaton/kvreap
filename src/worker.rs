//! Sampled-LRU eviction (the Redis approach) instead of crawling the tree.
//!
//! ```text
//!  loop while evicting:
//!    bucket <- next <rank>/<hhh> of this worker's shard, each once per shuffled pass
//!    statx every *.bin under bucket/*/  ──►  pool (oldest POOL_CAP cold files)
//!    evict (sampled / SAMPLED_PER_EVICTION) from the pool (remainders carry over)
//! ```
//!
//! Work scales with the bytes to free, not with the size of the tree, and every
//! metadata op goes through the shared `Budget`.
//!
//! | module | |
//! |---|---|
//! | `buckets` | listing and sampling the shard's buckets, reaping empty dirs |
//! | `pool` | the oldest cold candidates |
//! | `unlink` | unlinking one candidate or many in parallel |
//! | `radix` | eviction by the prefix-chain index when KV events are configured |
//! | `subtree` | deleting a dead block with everything below it |

use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::budget::{Budget, OpKind};
use crate::capacity::Samples;
use crate::chains::Chains;
use crate::config::Config;
use crate::controller::SharedState;
use crate::fsops;
use crate::layout::Shard;
use crate::shutdown::Shutdown;
use crate::stats::Stats;
use crate::worker::buckets::BucketIndex;
use crate::worker::pool::Pool;

mod buckets;
mod pool;
mod radix;
mod subtree;
#[cfg(test)]
mod testing;
mod unlink;

const POOL_CAP: usize = 4096;

/// A round may evict one file for every this many it sampled.
const SAMPLED_PER_EVICTION: usize = 2;
const RECHECK_AFTER: Duration = Duration::from_secs(5);
const INDEX_REFRESH: Duration = Duration::from_secs(300);
const IDLE_POLL: Duration = Duration::from_millis(250);
const EMPTY_INDEX_RETRY: Duration = Duration::from_secs(5);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const CHAIN_WINDOW: usize = 8;

pub struct Context {
    pub config: Arc<Config>,
    pub shared: Arc<SharedState>,
    pub budget: Arc<Budget>,
    pub stats: Arc<Stats>,
    pub samples: Arc<Samples>,
    pub shutdown: Arc<Shutdown>,
    pub chains: Option<Arc<Chains>>,
}

pub struct Worker {
    id: usize,
    shard: Shard,
    ctx: Context,
    pool: Pool,
    index: BucketIndex,
    /// Files evicted so far in the current pass over the buckets.
    pass_evicted: usize,
    backoff: Duration,
    /// Sampled files not yet matched by an eviction, below `SAMPLED_PER_EVICTION`.
    evict_carry: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct RoundResult {
    sampled: usize,
    evicted: usize,
}

impl Worker {
    pub fn new(id: usize, shard: Shard, ctx: Context) -> Self {
        Self {
            id,
            shard,
            ctx,
            pool: Pool::new(POOL_CAP),
            index: BucketIndex::default(),
            pass_evicted: 0,
            backoff: BACKOFF_MIN,
            evict_carry: 0,
        }
    }

    pub fn run(mut self) {
        tracing::info!(worker = self.id, shard = %self.shard, "worker started");
        while !self.ctx.shutdown.is_set() {
            if !self.ctx.shared.mode().is_evicting() {
                self.pool.clear();
                self.evict_carry = 0;
                self.index.refreshed_at = None;
                self.pass_evicted = 0;
                self.backoff = BACKOFF_MIN;
                self.ctx.shutdown.wait(IDLE_POLL);
                continue;
            }
            if !self.ctx.shared.should_delete() {
                self.ctx.shutdown.wait(IDLE_POLL);
                continue;
            }
            match self.round() {
                Ok(Some(r)) => {
                    if let Some(wait) = self.after_round(&r) {
                        tracing::debug!(
                            worker = self.id,
                            ?wait,
                            "no cold files found, backing off"
                        );
                        self.ctx.shutdown.wait(wait);
                    }
                }
                Ok(None) => {
                    self.ctx.shutdown.wait(EMPTY_INDEX_RETRY);
                }
                Err(e) => {
                    Stats::add(&self.ctx.stats.errors, 1);
                    tracing::debug!(worker = self.id, error = %e, "eviction round failed");
                }
            }
        }
        tracing::info!(worker = self.id, "worker stopped");
    }

    /// Updates backoff state; returns how long to back off, if at all. A
    /// worker backs off only after a whole pass over its buckets evicted
    /// nothing.
    fn after_round(&mut self, r: &RoundResult) -> Option<Duration> {
        self.pass_evicted += r.evicted;
        if r.evicted > 0 {
            self.backoff = BACKOFF_MIN;
        }
        if !self.index.pass_done() {
            return None;
        }
        let evicted = std::mem::take(&mut self.pass_evicted);
        if evicted > 0 {
            return None;
        }
        self.index.refreshed_at = None;
        let wait = self.backoff;
        self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
        Some(wait)
    }

    fn acquire(&self) -> bool {
        self.ctx.acquire()
    }

    fn timed<T>(&self, kind: OpKind, f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        self.ctx.timed(kind, f)
    }

    fn is_hot(&self, atime: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(atime)
            .map_or(true, |age| age < self.ctx.config.hot_threshold)
    }

    /// One sample-then-evict round. `None` when there is nothing to sample.
    fn round(&mut self) -> io::Result<Option<RoundResult>> {
        let stale = self
            .index
            .refreshed_at
            .is_none_or(|t| t.elapsed() >= INDEX_REFRESH || self.index.buckets.is_empty());
        if stale {
            self.refresh_index();
        }
        let Some((rank, bucket)) = self.index.next() else {
            return Ok(None);
        };
        let sampled = match self.sample_bucket(&rank, &bucket) {
            Ok(n) => n,
            Err(e) if fsops::is_not_found(&e) => {
                self.index.refreshed_at = None;
                0
            }
            Err(e) => return Err(e),
        };
        let owed = self.evict_carry + sampled;
        self.evict_carry = owed % SAMPLED_PER_EVICTION;
        let evicted = self.evict(owed / SAMPLED_PER_EVICTION);
        Ok(Some(RoundResult { sampled, evicted }))
    }

    fn evict(&mut self, quota: usize) -> usize {
        if let Some(chains) = self.ctx.chains.clone() {
            return self.evict_radix(quota, &chains);
        }
        let mut evicted = 0;
        while evicted < quota && self.ctx.shared.should_delete() && !self.ctx.shutdown.is_set() {
            let Some(c) = self.pool.pop_oldest() else {
                break;
            };
            if self.evict_one(c) {
                evicted += 1;
            }
        }
        evicted
    }

    fn evicting(&self) -> bool {
        self.ctx.shared.should_delete() && !self.ctx.shutdown.is_set()
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::controller::Mode;
    use crate::stats::Stats;
    use crate::worker::testing::{age, bins, config, fixture, harness, run_rounds};

    #[test]
    fn evicts_cold_files_oldest_first_and_spares_hot_ones() {
        let fx = fixture(6, 2);
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 200);

        let left = bins(&fx.cache);
        assert_eq!(left.len(), 2, "only the hot files survive: {left:?}");
        assert_eq!(left, vec!["1006000000000000.bin", "1007000000000000.bin"]);
        assert_eq!(Stats::get(&h.stats.files_deleted), 6);
        assert_eq!(Stats::get(&h.stats.bytes_freed), 600);
    }

    #[test]
    fn single_bucket_evicts_in_atime_order() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
        fs::create_dir_all(&leaf).expect("mkdir");
        for (i, secs) in [(1u64, 9000u64), (2, 7300), (3, 8000), (4, 7400)] {
            let f = leaf.join(format!("abcde0000000000{i}.bin"));
            fs::write(&f, b"x").expect("write");
            age(&f, secs);
        }
        let mut h = harness(config(&cache, &[]));
        h.shared.set_mode(Mode::Evicting);

        // One round samples 4 files and evicts 4 * 0.5 = 2: the two oldest.
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            vec!["abcde00000000002.bin", "abcde00000000004.bin"]
        );
    }

    #[test]
    fn eviction_stops_once_the_byte_budget_is_spent() {
        let fx = fixture(6, 0);
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_to_free(250);
        run_rounds(&mut h, 50);
        assert_eq!(
            Stats::get(&h.stats.files_deleted),
            3,
            "100-byte files: the third deletion crosses the 250-byte budget"
        );
        assert_eq!(bins(&fx.cache).len(), 3);
        assert!(!h.shared.should_delete());
        h.shared.set_to_free(100);
        run_rounds(&mut h, 50);
        assert_eq!(bins(&fx.cache).len(), 2, "a new budget resumes eviction");
    }

    #[test]
    fn idle_mode_touches_nothing() {
        let fx = fixture(3, 0);
        let h = harness(config(&fx.cache, &[]));
        let shutdown = Arc::clone(&h.worker.ctx.shutdown);
        let handle = std::thread::spawn(move || h.worker.run());
        std::thread::sleep(Duration::from_millis(800));
        shutdown.trigger();
        handle.join().expect("join");
        assert_eq!(bins(&fx.cache).len(), 3);
        assert_eq!(Stats::get(&h.stats.readdir_ops), 0);
        assert_eq!(Stats::get(&h.stats.stat_ops), 0);
    }

    #[test]
    fn stops_evicting_when_mode_returns_to_idle() {
        let fx = fixture(8, 0);
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        let after_first = bins(&fx.cache).len();
        h.shared.set_mode(Mode::Idle);
        assert_eq!(h.worker.evict(100), 0);
        assert_eq!(bins(&fx.cache).len(), after_first);
    }

    #[test]
    fn dry_run_deletes_nothing() {
        let fx = fixture(4, 0);
        let mut h = harness(config(&fx.cache, &[("DRY_RUN", "true")]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 50);
        assert_eq!(bins(&fx.cache).len(), 4);
        assert!(Stats::get(&h.stats.files_deleted) > 0);
    }

    #[test]
    fn worker_run_exits_on_shutdown_while_evicting() {
        let fx = fixture(50, 0);
        let h = harness(config(&fx.cache, &[("DELETION_MAX_FILES_PER_SECOND", "5")]));
        h.shared.set_mode(Mode::Evicting);
        let shutdown = Arc::clone(&h.worker.ctx.shutdown);
        let handle = std::thread::spawn(move || h.worker.run());
        std::thread::sleep(Duration::from_millis(300));
        let start = Instant::now();
        shutdown.trigger();
        handle.join().expect("join");
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
