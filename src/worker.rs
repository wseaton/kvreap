//! Sampled-LRU eviction (the Redis approach) instead of crawling the tree.
//!
//! ```text
//!  loop while evicting:
//!    bucket <- random <rank>/<hhh> owned by this worker's shard
//!    statx every *.bin under bucket/*/  ──►  pool (oldest POOL_CAP cold files)
//!    unlink the oldest sampled x EVICT_FRACTION from the pool (fractions carry over)
//! ```
//!
//! Work scales with the bytes to free, not with the size of the tree, and every
//! metadata op goes through the shared `Budget`.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant, SystemTime};

use crate::budget::{Budget, OpKind};
use crate::capacity::{BucketSample, Samples};
use crate::config::Config;
use crate::controller::{Mode, SharedState};
use crate::fsops::{self, EntryKind, Meta};
use crate::layout::{BlockHash, Shard, block_files, discover_rank_dirs, model_base_dir};
use crate::shutdown::Shutdown;
use crate::stats::Stats;

const POOL_CAP: usize = 4096;
const EVICT_FRACTION: f64 = 0.5;
const RECHECK_AFTER: Duration = Duration::from_secs(5);
const INDEX_REFRESH: Duration = Duration::from_secs(300);
const IDLE_POLL: Duration = Duration::from_millis(250);
const EMPTY_INDEX_RETRY: Duration = Duration::from_secs(5);
const FRUITLESS_ROUNDS_BEFORE_BACKOFF: u32 = 16;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// A deleted block, reported to the events publisher when the `events` feature is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    pub model_base: PathBuf,
    pub hash: BlockHash,
}

#[derive(Debug)]
struct RankDir {
    path: PathBuf,
    model_base: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct Candidate {
    path: PathBuf,
    rank: Arc<RankDir>,
    hash: BlockHash,
    size: u64,
    atime: SystemTime,
    sampled_at: Instant,
}

/// Cold files ordered by atime, capped at `cap` by evicting the youngest.
#[derive(Debug)]
struct Pool {
    cap: usize,
    seq: u64,
    by_age: BTreeMap<(SystemTime, u64), Candidate>,
    keys: HashMap<PathBuf, (SystemTime, u64)>,
}

impl Pool {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            seq: 0,
            by_age: BTreeMap::new(),
            keys: HashMap::new(),
        }
    }

    fn insert(&mut self, c: Candidate) {
        if let Some(old) = self.keys.remove(&c.path) {
            self.by_age.remove(&old);
        }
        self.seq += 1;
        let key = (c.atime, self.seq);
        self.keys.insert(c.path.clone(), key);
        self.by_age.insert(key, c);
        while self.by_age.len() > self.cap {
            if let Some((_, youngest)) = self.by_age.pop_last() {
                self.keys.remove(&youngest.path);
            }
        }
    }

    fn pop_oldest(&mut self) -> Option<Candidate> {
        let (_, c) = self.by_age.pop_first()?;
        self.keys.remove(&c.path);
        Some(c)
    }

    fn clear(&mut self) {
        self.by_age.clear();
        self.keys.clear();
    }
}

#[derive(Debug, Default)]
struct BucketIndex {
    buckets: Vec<(Arc<RankDir>, CString)>,
    refreshed_at: Option<Instant>,
}

pub struct Context {
    pub config: Arc<Config>,
    pub shared: Arc<SharedState>,
    pub budget: Arc<Budget>,
    pub stats: Arc<Stats>,
    pub samples: Arc<Samples>,
    pub shutdown: Arc<Shutdown>,
    pub events: Option<Sender<Removed>>,
}

pub struct Worker {
    id: usize,
    shard: Shard,
    ctx: Context,
    pool: Pool,
    index: BucketIndex,
    fruitless_rounds: u32,
    backoff: Duration,
    evict_credit: f64,
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
            fruitless_rounds: 0,
            backoff: BACKOFF_MIN,
            evict_credit: 0.0,
        }
    }

    pub fn run(mut self) {
        tracing::info!(worker = self.id, shard = %self.shard, "worker started");
        while !self.ctx.shutdown.is_set() {
            if !self.ctx.shared.mode().is_evicting() {
                self.pool.clear();
                self.evict_credit = 0.0;
                self.index.refreshed_at = None;
                self.fruitless_rounds = 0;
                self.backoff = BACKOFF_MIN;
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

    /// Updates backoff state; returns how long to back off, if at all.
    fn after_round(&mut self, r: &RoundResult) -> Option<Duration> {
        if r.evicted > 0 {
            self.fruitless_rounds = 0;
            self.backoff = BACKOFF_MIN;
            return None;
        }
        self.fruitless_rounds += 1;
        if self.fruitless_rounds < FRUITLESS_ROUNDS_BEFORE_BACKOFF {
            return None;
        }
        self.index.refreshed_at = None;
        let wait = self.backoff;
        self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
        Some(wait)
    }

    fn unpaced(&self) -> bool {
        self.ctx.shared.mode() == Mode::Emergency
    }

    fn acquire(&self) -> bool {
        self.ctx.budget.acquire(&self.ctx.shutdown, self.unpaced())
    }

    fn timed<T>(&self, kind: OpKind, f: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let start = Instant::now();
        let out = f();
        self.ctx.budget.observe(kind, start.elapsed());
        let counter = match kind {
            OpKind::Readdir => &self.ctx.stats.readdir_ops,
            OpKind::Stat => &self.ctx.stats.stat_ops,
            OpKind::Unlink => &self.ctx.stats.unlink_ops,
        };
        Stats::add(counter, 1);
        out
    }

    fn is_hot(&self, atime: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(atime)
            .map(|age| age < self.ctx.config.hot_threshold)
            .unwrap_or(true)
    }

    fn refresh_index(&mut self) {
        let cache = self.ctx.config.cache_path();
        let bucket_len = self.ctx.config.hex_bucket_len;
        let mut buckets = Vec::new();
        for path in discover_rank_dirs(&cache) {
            let rank = Arc::new(RankDir {
                model_base: model_base_dir(&path),
                path,
            });
            let listed = fsops::open_dir(&rank.path)
                .and_then(|fd| self.timed(OpKind::Readdir, || fsops::list(&fd)));
            let Ok(entries) = listed else {
                continue;
            };
            for e in entries {
                let owned = e.kind == EntryKind::Dir
                    && e.name
                        .to_str()
                        .is_ok_and(|n| self.shard.owns_bucket(n, bucket_len));
                if owned {
                    buckets.push((Arc::clone(&rank), e.name));
                }
            }
        }
        tracing::debug!(
            worker = self.id,
            buckets = buckets.len(),
            "bucket index refreshed"
        );
        self.index = BucketIndex {
            buckets,
            refreshed_at: Some(Instant::now()),
        };
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
        if self.index.buckets.is_empty() {
            return Ok(None);
        }
        let (rank, bucket) =
            self.index.buckets[fastrand::usize(..self.index.buckets.len())].clone();
        let sampled = match self.sample_bucket(&rank, &bucket) {
            Ok(n) => n,
            Err(e) if fsops::is_not_found(&e) => {
                self.index.refreshed_at = None;
                0
            }
            Err(e) => return Err(e),
        };
        self.evict_credit += sampled as f64 * EVICT_FRACTION;
        let quota = self.evict_credit.floor();
        self.evict_credit -= quota;
        let evicted = self.evict(quota as usize);
        Ok(Some(RoundResult { sampled, evicted }))
    }

    /// Stats every block in `bucket` into the pool and, if the walk finished,
    /// records the bucket's size for the usage estimate. Returns files sampled.
    fn sample_bucket(&mut self, rank: &Arc<RankDir>, bucket: &CStr) -> io::Result<usize> {
        let bucket_path = rank.path.join(bucket.to_string_lossy().as_ref());
        let bucket_fd = fsops::open_dir(&bucket_path)?;
        if !self.acquire() {
            return Ok(0);
        }
        let leaves = self.timed(OpKind::Readdir, || fsops::list(&bucket_fd))?;
        if leaves.is_empty() {
            self.ctx.samples.record(BucketSample::default());
            self.reap_if_stale(&bucket_path);
            return Ok(0);
        }
        let mut sample = BucketSample::default();
        for leaf in leaves.iter().filter(|e| e.kind == EntryKind::Dir) {
            if self.ctx.shutdown.is_set() {
                return Ok(0);
            }
            let leaf_path = bucket_path.join(leaf.name.to_string_lossy().as_ref());
            let Ok(leaf_fd) = fsops::open_dir_at(&bucket_fd, &leaf.name) else {
                continue;
            };
            if !self.acquire() {
                return Ok(0);
            }
            let Ok(files) = self.timed(OpKind::Readdir, || fsops::list(&leaf_fd)) else {
                continue;
            };
            if files.is_empty() {
                self.reap_if_stale(&leaf_path);
                continue;
            }
            for (name, hash) in block_files(&files) {
                if !self.acquire() {
                    return Ok(0);
                }
                let meta: Meta = match self.timed(OpKind::Stat, || fsops::stat_at(&leaf_fd, name)) {
                    Ok(m) => m,
                    Err(e) if fsops::is_not_found(&e) => continue,
                    Err(e) => {
                        Stats::add(&self.ctx.stats.errors, 1);
                        tracing::debug!(error = %e, "statx failed while sampling");
                        continue;
                    }
                };
                sample.files += 1;
                sample.bytes = sample.bytes.saturating_add(meta.size);
                Stats::add(&self.ctx.stats.files_sampled, 1);
                if self.is_hot(meta.atime) {
                    Stats::add(&self.ctx.stats.files_skipped_hot, 1);
                    continue;
                }
                self.pool.insert(Candidate {
                    path: leaf_path.join(name.to_string_lossy().as_ref()),
                    rank: Arc::clone(rank),
                    hash,
                    size: meta.size,
                    atime: meta.atime,
                    sampled_at: Instant::now(),
                });
            }
        }
        self.ctx.samples.record(sample);
        Ok(usize::try_from(sample.files).unwrap_or(usize::MAX))
    }

    fn evict(&mut self, quota: usize) -> usize {
        let mut evicted = 0;
        while evicted < quota && self.ctx.shared.mode().is_evicting() && !self.ctx.shutdown.is_set()
        {
            let Some(c) = self.pool.pop_oldest() else {
                break;
            };
            if self.evict_one(c) {
                evicted += 1;
            }
        }
        evicted
    }

    fn evict_one(&mut self, mut c: Candidate) -> bool {
        if c.sampled_at.elapsed() >= RECHECK_AFTER {
            if !self.acquire() {
                return false;
            }
            match self.timed(OpKind::Stat, || fsops::stat_path(&c.path)) {
                Ok(meta) => {
                    if self.is_hot(meta.atime) {
                        Stats::add(&self.ctx.stats.files_skipped_hot, 1);
                        return false;
                    }
                    c.size = meta.size;
                }
                Err(_) => return false,
            }
        }
        if self.ctx.config.dry_run {
            tracing::debug!(path = %c.path.display(), "[DRY RUN] would delete");
            Stats::add(&self.ctx.stats.files_deleted, 1);
            return true;
        }
        if !self.acquire() || !self.ctx.shared.mode().is_evicting() {
            return false;
        }
        match self.timed(OpKind::Unlink, || fsops::unlink_path(&c.path)) {
            Ok(()) => {}
            Err(e) if fsops::is_not_found(&e) => return false,
            Err(e) => {
                Stats::add(&self.ctx.stats.errors, 1);
                tracing::warn!(path = %c.path.display(), error = %e, "unlink failed");
                return false;
            }
        }
        Stats::add(&self.ctx.stats.files_deleted, 1);
        Stats::add(&self.ctx.stats.bytes_freed, c.size);
        if let (Some(tx), Some(base)) = (&self.ctx.events, &c.rank.model_base) {
            let _ = tx.send(Removed {
                model_base: base.clone(),
                hash: c.hash,
            });
        }
        true
    }

    /// Removes `dir` if it is empty and unchanged for `DIR_CLEANUP_TTL_SECONDS`.
    fn reap_if_stale(&self, dir: &Path) {
        if !self.ctx.config.enable_dir_cleanup || !self.acquire() {
            return;
        }
        let Ok(meta) = self.timed(OpKind::Stat, || fsops::stat_path(dir)) else {
            return;
        };
        let age = SystemTime::now()
            .duration_since(meta.mtime)
            .unwrap_or(Duration::ZERO);
        if age < self.ctx.config.dir_cleanup_ttl || !self.acquire() {
            return;
        }
        Stats::add(&self.ctx.stats.rmdir_ops, 1);
        match fsops::rmdir_path(dir) {
            Ok(()) => Stats::add(&self.ctx.stats.dirs_removed, 1),
            Err(e) if fsops::is_not_found(&e) || fsops::is_not_empty(&e) => {}
            Err(e) => tracing::debug!(dir = %dir.display(), error = %e, "rmdir failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::{Duration, Instant, SystemTime};

    use crate::budget::Budget;
    use crate::capacity::{BucketSample, Samples};
    use crate::config::Config;
    use crate::controller::{Mode, SharedState};
    use crate::layout::{BlockHash, Shard};
    use crate::shutdown::Shutdown;
    use crate::stats::Stats;
    use crate::worker::{Candidate, Context, Pool, RankDir, Removed, Worker};

    fn candidate(path: &str, age_secs: u64) -> Candidate {
        Candidate {
            path: PathBuf::from(path),
            rank: Arc::new(RankDir {
                path: PathBuf::from("/r"),
                model_base: None,
            }),
            hash: BlockHash(0),
            size: 1,
            atime: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000 - age_secs),
            sampled_at: Instant::now(),
        }
    }

    #[test]
    fn pool_pops_oldest_first_and_drops_youngest_over_cap() {
        let mut pool = Pool::new(3);
        pool.insert(candidate("/a", 10));
        pool.insert(candidate("/b", 50));
        pool.insert(candidate("/c", 30));
        pool.insert(candidate("/d", 40));
        assert_eq!(pool.by_age.len(), 3);
        let order: Vec<_> = std::iter::from_fn(|| pool.pop_oldest())
            .map(|c| c.path)
            .collect();
        assert_eq!(
            order,
            vec![
                PathBuf::from("/b"),
                PathBuf::from("/d"),
                PathBuf::from("/c")
            ]
        );
    }

    #[test]
    fn pool_reinsert_replaces_existing_entry() {
        let mut pool = Pool::new(10);
        pool.insert(candidate("/a", 10));
        pool.insert(candidate("/b", 20));
        pool.insert(candidate("/a", 99));
        assert_eq!(pool.by_age.len(), 2);
        assert_eq!(pool.pop_oldest().map(|c| c.path), Some(PathBuf::from("/a")));
    }

    struct Fixture {
        _tmp: tempfile::TempDir,
        cache: PathBuf,
    }

    fn age(path: &Path, secs: u64) {
        let t = SystemTime::now() - Duration::from_secs(secs);
        fs::File::options()
            .write(true)
            .open(path)
            .and_then(|f| f.set_times(fs::FileTimes::new().set_accessed(t).set_modified(t)))
            .expect("set times");
    }

    /// `cold` files aged `cold_age + i` seconds (so i=0 is youngest), `hot` files fresh.
    fn fixture(cold: usize, hot: usize) -> Fixture {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let base = cache.join("org-model_abcdef012345");
        fs::create_dir_all(&base).expect("mkdir");
        fs::write(base.join("config.json"), r#"{"model_name": "org/model"}"#).expect("write");
        for i in 0..cold + hot {
            let hash = 0x1000_0000_0000_0000u64 + i as u64 * 0x0001_0000_0000_0000;
            let hex = format!("{hash:016x}");
            let leaf = cache.join(format!(
                "org-model_abcdef012345_r0/{}/{}_g0",
                &hex[..3],
                &hex[3..5]
            ));
            fs::create_dir_all(&leaf).expect("mkdir");
            let f = leaf.join(format!("{hex}.bin"));
            fs::write(&f, vec![0u8; 100]).expect("write");
            if i < cold {
                age(&f, 7200 + i as u64);
            }
        }
        Fixture { _tmp: tmp, cache }
    }

    fn config(cache: &Path, extra: &[(&str, &str)]) -> Config {
        let mut env: HashMap<String, String> = [
            ("PVC_MOUNT_PATH", cache.to_str().expect("utf8")),
            ("CACHE_DIRECTORY", "."),
            ("NUM_CRAWLER_PROCESSES", "1"),
            ("DIR_CLEANUP_TTL_SECONDS", "0"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        Config::from_lookup(|k| env.get(k).cloned()).expect("config")
    }

    struct Harness {
        worker: Worker,
        shared: Arc<SharedState>,
        stats: Arc<Stats>,
        samples: Arc<Samples>,
        rx: mpsc::Receiver<Removed>,
    }

    fn harness(cfg: Config) -> Harness {
        let shared = Arc::new(SharedState::default());
        let stats = Arc::new(Stats::default());
        let samples = Arc::new(Samples::default());
        let (tx, rx) = mpsc::channel();
        let ctx = Context {
            budget: Arc::new(Budget::new(cfg.max_files_per_second)),
            config: Arc::new(cfg),
            shared: Arc::clone(&shared),
            stats: Arc::clone(&stats),
            samples: Arc::clone(&samples),
            shutdown: Arc::new(Shutdown::default()),
            events: Some(tx),
        };
        Harness {
            worker: Worker::new(0, Shard::split(1)[0], ctx),
            shared,
            stats,
            samples,
            rx,
        }
    }

    fn bins(cache: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![cache.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in fs::read_dir(&d).expect("read_dir").flatten() {
                let p = e.path();
                if p.is_dir() {
                    stack.push(p);
                } else if p.extension().is_some_and(|x| x == "bin") {
                    out.push(p.file_name().expect("name").to_string_lossy().into_owned());
                }
            }
        }
        out.sort();
        out
    }

    fn run_rounds(h: &mut Harness, n: usize) {
        for _ in 0..n {
            h.worker.round().expect("round");
        }
    }

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
        assert!(h.rx.try_recv().is_err());
    }

    #[test]
    fn emits_removed_events_with_model_base_and_hash() {
        let fx = fixture(2, 0);
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 50);
        let mut got: Vec<_> = h.rx.try_iter().collect();
        got.sort_by_key(|r| r.hash.0);
        let base = fx.cache.join("org-model_abcdef012345");
        assert_eq!(
            got,
            vec![
                Removed {
                    model_base: base.clone(),
                    hash: BlockHash(0x1000_0000_0000_0000)
                },
                Removed {
                    model_base: base,
                    hash: BlockHash(0x1001_0000_0000_0000)
                },
            ]
        );
    }

    #[test]
    fn emptied_leaf_is_left_for_ttl_reaping() {
        let fx = fixture(1, 0);
        let leaf = fx.cache.join("org-model_abcdef012345_r0/100/00_g0");
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        h.worker.evict(1);
        assert!(bins(&fx.cache).is_empty());
        assert!(
            leaf.is_dir(),
            "unlinking the last file must not rmdir its leaf"
        );
        assert_eq!(Stats::get(&h.stats.rmdir_ops), 0);

        run_rounds(&mut h, 5);
        assert!(
            !leaf.exists(),
            "empty leaf is reaped when sampling finds it"
        );
        assert!(
            !leaf.parent().expect("bucket").exists(),
            "emptied bucket is reaped on a later round"
        );
        assert_eq!(Stats::get(&h.stats.dirs_removed), 2);
    }

    #[test]
    fn emptied_leaf_younger_than_ttl_survives() {
        let fx = fixture(1, 0);
        let leaf = fx.cache.join("org-model_abcdef012345_r0/100/00_g0");
        let mut h = harness(config(&fx.cache, &[("DIR_CLEANUP_TTL_SECONDS", "3600")]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 10);
        assert!(bins(&fx.cache).is_empty());
        assert!(leaf.is_dir());
        assert_eq!(Stats::get(&h.stats.rmdir_ops), 0);
    }

    #[test]
    fn dir_cleanup_disabled_keeps_dirs() {
        let fx = fixture(1, 0);
        let mut h = harness(config(&fx.cache, &[("ENABLE_DIR_CLEANUP", "false")]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 5);
        assert!(
            fx.cache
                .join("org-model_abcdef012345_r0/100/00_g0")
                .is_dir()
        );
        assert_eq!(Stats::get(&h.stats.rmdir_ops), 0);
    }

    #[test]
    fn fresh_empty_leaf_survives_ttl() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let bucket = cache.join("m_abcdef012345_r0/abc");
        fs::create_dir_all(bucket.join("de_g0")).expect("mkdir");
        fs::create_dir_all(bucket.join("ff_g0")).expect("mkdir");
        let f = bucket.join("de_g0/abcde00000000001.bin");
        fs::write(&f, b"x").expect("write");
        age(&f, 7200);

        let mut h = harness(config(&cache, &[("DIR_CLEANUP_TTL_SECONDS", "3600")]));
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert!(
            bucket.join("ff_g0").is_dir(),
            "freshly created leaf must not be reaped"
        );

        let mut h = harness(config(&cache, &[("DIR_CLEANUP_TTL_SECONDS", "0")]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 3);
        assert!(!bucket.join("ff_g0").exists());
    }

    #[test]
    fn file_read_after_sampling_is_spared() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
        fs::create_dir_all(&leaf).expect("mkdir");
        let f = leaf.join("abcde00000000001.bin");
        fs::write(&f, b"x").expect("write");
        age(&f, 7200);

        let mut h = harness(config(&cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        let rank = Arc::new(RankDir {
            path: cache.join("m_abcdef012345_r0"),
            model_base: None,
        });
        h.worker.sample_bucket(&rank, c"abc").expect("sample");
        let mut c = h.worker.pool.pop_oldest().expect("candidate");
        c.sampled_at = Instant::now() - Duration::from_secs(60);
        age(&f, 0);
        assert!(!h.worker.evict_one(c));
        assert!(f.exists());
        assert_eq!(Stats::get(&h.stats.files_skipped_hot), 1);
    }

    #[test]
    fn non_bin_and_tmp_files_are_never_touched() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
        fs::create_dir_all(&leaf).expect("mkdir");
        for name in ["abcde00000000001.bin_123.tmp", "notes.txt", "short.bin"] {
            let f = leaf.join(name);
            fs::write(&f, b"x").expect("write");
            age(&f, 7200);
        }
        let mut h = harness(config(&cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 10);
        assert_eq!(fs::read_dir(&leaf).expect("read").count(), 3);
        assert_eq!(Stats::get(&h.stats.files_deleted), 0);
    }

    #[test]
    fn buckets_created_after_index_build_are_found() {
        let fx = fixture(1, 3);
        let mut h = harness(config(&fx.cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 5);
        assert_eq!(bins(&fx.cache).len(), 3, "the single cold block is gone");

        let late = fx.cache.join("org-model_abcdef012345_r0/fff/ff_g0");
        fs::create_dir_all(&late).expect("mkdir");
        let f = late.join("fffff00000000000.bin");
        fs::write(&f, b"x").expect("write");
        age(&f, 7200);

        for _ in 0..200 {
            let r = h.worker.round().expect("round").expect("buckets");
            let _ = h.worker.after_round(&r);
            if !f.exists() {
                return;
            }
        }
        panic!("block in a bucket created after the index was built was never evicted");
    }

    #[test]
    fn only_shard_buckets_are_sampled() {
        let fx = fixture(4, 0);
        let mut h = harness(config(&fx.cache, &[]));
        h.worker.shard = Shard::split(16)[5]; // fixture files all live in bucket 0x100 (% 16 == 0)
        h.shared.set_mode(Mode::Evicting);
        assert_eq!(h.worker.round().expect("round"), None);
        assert_eq!(bins(&fx.cache).len(), 4);
    }

    #[test]
    fn sampled_buckets_feed_the_usage_estimate() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
        fs::create_dir_all(&leaf).expect("mkdir");
        for (i, size) in [(1, 100usize), (2, 250), (3, 50)] {
            fs::write(
                leaf.join(format!("abcde0000000000{i}.bin")),
                vec![0u8; size],
            )
            .expect("write");
        }
        fs::write(leaf.join("notes.txt"), vec![0u8; 999]).expect("write");

        let mut h = harness(config(&cache, &[]));
        h.shared.set_mode(Mode::Evicting);
        h.samples.set_bucket_count(10);
        run_rounds(&mut h, 15);
        assert_eq!(h.samples.estimated_used_bytes(), None, "15 samples");
        h.worker.round().expect("round");
        assert_eq!(h.samples.estimated_used_bytes(), Some(4000));
        assert_eq!(bins(&cache).len(), 3, "all files are hot");

        h.samples.record(BucketSample::default());
        assert!(h.samples.estimated_used_bytes().is_some_and(|b| b < 4000));
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
