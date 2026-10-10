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
//!
//! With a prefix-chain index (`events` feature, `KV_EVENTS_ENDPOINTS`) and
//! `CHAIN_EVICTION=tail-first`, each round takes the oldest
//! `quota * CHAIN_WINDOW` candidates and deletes, best first: dead blocks
//! (parent gone), childless blocks, interiors deferred `CHAIN_MAX_DEFERRALS`
//! times, then the remaining interiors. The rest go back to the pool.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{CStr, CString};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant, SystemTime};

use crate::budget::{Budget, OpKind};
use crate::capacity::{BucketSample, Samples};
use crate::chains::{Chains, LeafEdge, Rank};
use crate::config::ChainPolicy;
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
const CHAIN_WINDOW: usize = 8;

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
    deferrals: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unlink {
    Deleted,
    /// The file was already gone.
    Missing,
    /// Not attempted (grant spent, shutdown) or failed.
    Skipped,
}

/// Outcome of unlinking one block's file in every rank dir.
#[derive(Debug, Default, Clone, Copy)]
struct Named {
    deleted: usize,
    missing: usize,
    skipped: usize,
}

impl Named {
    /// No rank had the file, so the block is gone without a deletion of ours.
    fn vanished(self) -> bool {
        self.deleted == 0 && self.skipped == 0 && self.missing > 0
    }
}

/// Radix eviction order of a candidate, first to go first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RadixKey {
    Dead,
    Untracked,
    /// On a leaf edge whose newest block was written at this instant.
    Edge(Option<Instant>),
}

impl RadixKey {
    fn of(chains: &Chains, c: &Candidate) -> Self {
        if chains.rank(c.hash, 0) == Rank::Dead {
            return Self::Dead;
        }
        match chains.leaf_edge(c.hash) {
            Some(edge) => Self::Edge(edge.newest_store),
            None => Self::Untracked,
        }
    }
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

    /// Inserts or replaces `c`; a replaced entry's deferral count carries over.
    fn insert(&mut self, mut c: Candidate) {
        if let Some(old) = self.keys.remove(&c.path)
            && let Some(prev) = self.by_age.remove(&old)
        {
            c.deferrals = c.deferrals.max(prev.deferrals);
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

    fn is_empty(&self) -> bool {
        self.by_age.is_empty()
    }
}

#[derive(Debug, Default)]
struct BucketIndex {
    buckets: Vec<(Arc<RankDir>, CString)>,
    /// Every rank dir found, including ones with no bucket in this shard.
    ranks: Vec<Arc<RankDir>>,
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
    pub chains: Option<Arc<Chains>>,
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
        let mut ranks = Vec::new();
        for path in discover_rank_dirs(&cache) {
            let rank = Arc::new(RankDir {
                model_base: model_base_dir(&path),
                path,
            });
            ranks.push(Arc::clone(&rank));
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
            ranks,
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
                    deferrals: 0,
                });
            }
        }
        self.ctx.samples.record(sample);
        Ok(usize::try_from(sample.files).unwrap_or(usize::MAX))
    }

    fn evict(&mut self, quota: usize) -> usize {
        if let Some(chains) = self.ordering_chains() {
            return match chains.policy {
                ChainPolicy::Subtree => self.evict_subtrees(quota, &chains),
                ChainPolicy::Radix => self.evict_radix(quota, &chains),
                _ => self.evict_tail_first(quota, &chains),
            };
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

    /// The chain index when it should reorder eviction. Emergency mode
    /// ignores chain order.
    fn ordering_chains(&self) -> Option<Arc<Chains>> {
        self.ctx
            .chains
            .as_ref()
            .filter(|c| match c.policy {
                ChainPolicy::Observe => false,
                ChainPolicy::Radix => true,
                ChainPolicy::TailFirst | ChainPolicy::Subtree => !self.unpaced(),
            })
            .cloned()
    }

    /// Deletes the best subtree root in the window, then every on-disk block
    /// below it: none of them is reachable by a prefix lookup once it is gone.
    fn evict_subtrees(&mut self, quota: usize, chains: &Chains) -> usize {
        let mut window: Vec<Candidate> = std::iter::from_fn(|| self.pool.pop_oldest())
            .take(quota.saturating_mul(CHAIN_WINDOW))
            .collect();
        let mut evicted = 0;
        while evicted < quota && self.ctx.shared.should_delete() && !self.ctx.shutdown.is_set() {
            let best = window
                .iter()
                .enumerate()
                .map(|(i, c)| (chains.subtree_key(c.hash), i))
                .min();
            let Some((_, i)) = best else {
                break;
            };
            evicted += self.delete_subtree(window.remove(i), chains);
        }
        for c in window {
            self.pool.insert(c);
        }
        evicted
    }

    /// Deletes `root` and every on-disk block below it, in every rank dir of
    /// its model. Returns files removed.
    fn delete_subtree(&mut self, root: Candidate, chains: &Chains) -> usize {
        let (hash, size, rank) = (root.hash, root.size, Arc::clone(&root.rank));
        let group = leaf_group(&root.path);
        let name = root
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_owned);
        if !self.evict_one(root) {
            return 0;
        }
        let siblings = self.sibling_ranks(&rank);
        let mut removed = 1;
        let (Some(group), Some(name)) = (group, name) else {
            return removed;
        };
        removed += self
            .remove_named(&siblings, &name, &group, hash, size)
            .deleted;
        for d in chains.descendants(hash) {
            if !self.ctx.shared.should_delete() || self.ctx.shutdown.is_set() {
                break;
            }
            let Some(name) = d.file_name else {
                continue;
            };
            let named = self.remove_named(
                &[std::slice::from_ref(&rank), siblings.as_slice()].concat(),
                &name,
                &group,
                d.hash,
                size,
            );
            if named.deleted > 0 {
                removed += named.deleted;
                chains.deleted(d.hash);
                Stats::add(&chains.stats.cascaded, 1);
            } else if named.vanished() {
                chains.vanished(d.hash);
            }
        }
        removed
    }

    /// Unlinks block file `name` from each of `ranks`.
    fn remove_named(
        &mut self,
        ranks: &[Arc<RankDir>],
        name: &str,
        group: &str,
        hash: BlockHash,
        size: u64,
    ) -> Named {
        let bucket_len = self.ctx.config.hex_bucket_len;
        let mut out = Named::default();
        for r in ranks {
            let Some(path) = block_path(&r.path, name, bucket_len, group) else {
                continue;
            };
            match self.remove_block(&path, hash, size, r, false) {
                Unlink::Deleted => out.deleted += 1,
                Unlink::Missing => out.missing += 1,
                Unlink::Skipped => out.skipped += 1,
            }
        }
        out
    }

    /// Dead candidates take their whole subtree with them. Any other
    /// candidate maps to the radix leaf edge it belongs to (or, inside a
    /// shared prefix, the oldest edge below it), deleted leaf first. Edges
    /// with a block younger than the hot threshold wait; if the round still
    /// falls short, the least recently written of them go anyway.
    /// Dead blocks first, then blocks the index does not know, then leaf
    /// edges by their newest write, oldest first. Edges written within the
    /// hot threshold are dropped from the pool so the sampler brings in
    /// others, and only go when the pool has nothing else or usage is in the
    /// emergency band.
    fn evict_radix(&mut self, quota: usize, chains: &Chains) -> usize {
        let mut window: Vec<(RadixKey, Candidate)> = std::iter::from_fn(|| self.pool.pop_oldest())
            .take(quota.saturating_mul(CHAIN_WINDOW))
            .map(|c| (RadixKey::of(chains, &c), c))
            .collect();
        window.sort_by_key(|(key, _)| *key);
        let mut window = window.into_iter();
        let mut held = Vec::new();
        let mut evicted = 0;
        while evicted < quota && self.evicting() {
            let Some((key, c)) = window.next() else {
                break;
            };
            match (key, RadixKey::of(chains, &c)) {
                (_, RadixKey::Dead) => evicted += self.delete_subtree(c, chains),
                (_, RadixKey::Untracked) => {
                    if self.evict_one(c) {
                        evicted += 1;
                    }
                }
                (RadixKey::Edge(planned), RadixKey::Edge(now)) if now > planned => held.push(c),
                (_, RadixKey::Edge(newest)) if self.holds_back(newest) => {
                    Stats::add(&chains.stats.young_edges, 1);
                }
                (_, RadixKey::Edge(newest)) => {
                    let Some(edge) = chains.leaf_edge(c.hash) else {
                        held.push(c);
                        continue;
                    };
                    let (files, candidate_gone) = self.delete_edge(&c, &edge, chains);
                    if files > 0 && self.young(newest) {
                        Stats::add(&chains.stats.young_fallbacks, 1);
                    }
                    evicted += files;
                    if !candidate_gone {
                        held.push(c);
                    }
                }
            }
        }
        for (key, c) in window {
            match key {
                RadixKey::Edge(newest) if self.holds_back(newest) => {
                    Stats::add(&chains.stats.young_edges, 1);
                }
                _ => self.pool.insert(c),
            }
        }
        for c in held {
            self.pool.insert(c);
        }
        evicted
    }

    fn young(&self, newest_store: Option<Instant>) -> bool {
        newest_store.is_some_and(|t| t.elapsed() < self.ctx.config.hot_threshold)
    }

    fn holds_back(&self, newest_store: Option<Instant>) -> bool {
        self.young(newest_store) && !self.unpaced() && !self.pool.is_empty()
    }

    fn evicting(&self) -> bool {
        self.ctx.shared.should_delete() && !self.ctx.shutdown.is_set()
    }

    /// Deletes `edge` leaf first in every rank dir of `c`'s model. Returns
    /// files removed and whether `c` itself was one of them.
    fn delete_edge(&mut self, c: &Candidate, edge: &LeafEdge, chains: &Chains) -> (usize, bool) {
        let Some(group) = leaf_group(&c.path) else {
            return (0, false);
        };
        let ranks = [
            std::slice::from_ref(&c.rank),
            self.sibling_ranks(&c.rank).as_slice(),
        ]
        .concat();
        let own_name = c
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .map(str::to_owned);
        let (mut files, mut candidate_gone) = (0, false);
        for b in edge.blocks.iter().rev() {
            let is_candidate = b.hash == c.hash;
            let Some(name) = b
                .file_name
                .clone()
                .or_else(|| own_name.clone().filter(|_| is_candidate))
            else {
                break;
            };
            let named = self.remove_named(&ranks, &name, &group, b.hash, c.size);
            if named.deleted > 0 {
                files += named.deleted;
                chains.deleted(b.hash);
                if is_candidate {
                    candidate_gone = true;
                } else {
                    Stats::add(&chains.stats.cascaded, 1);
                }
            } else if named.vanished() {
                chains.vanished(b.hash);
                candidate_gone |= is_candidate;
            } else if chains.on_disk(b.hash) {
                break;
            }
        }
        (files, candidate_gone)
    }

    /// Ranks are recomputed after every deletion: removing a leaf can turn
    /// its parent into one. Interiors with deferrals left are never deleted;
    /// the round evicts less instead.
    fn evict_tail_first(&mut self, quota: usize, chains: &Chains) -> usize {
        let mut window: Vec<Candidate> = std::iter::from_fn(|| self.pool.pop_oldest())
            .take(quota.saturating_mul(CHAIN_WINDOW))
            .collect();
        let mut evicted = 0;
        while evicted < quota && self.ctx.shared.should_delete() && !self.ctx.shutdown.is_set() {
            let best = window
                .iter()
                .enumerate()
                .map(|(i, c)| (chains.rank(c.hash, c.deferrals), i))
                .min();
            let Some((rank, i)) = best.filter(|(rank, _)| *rank != Rank::Interior) else {
                break;
            };
            tracing::trace!(?rank, path = %window[i].path.display(), "chain-ordered eviction");
            if self.evict_one(window.remove(i)) {
                evicted += 1;
            }
        }
        for mut c in window {
            if chains.rank(c.hash, c.deferrals) == Rank::Interior {
                c.deferrals += 1;
                Stats::add(&chains.stats.deferrals, 1);
            }
            self.pool.insert(c);
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
        self.remove_block(&c.path, c.hash, c.size, &c.rank, true) == Unlink::Deleted
    }

    /// Other rank dirs of `rank`'s model. Tensor-parallel ranks each hold a
    /// shard of every block, and a block is useless once any shard is gone.
    fn sibling_ranks(&self, rank: &RankDir) -> Vec<Arc<RankDir>> {
        let Some(base) = &rank.model_base else {
            return Vec::new();
        };
        self.index
            .ranks
            .iter()
            .filter(|r| r.model_base.as_ref() == Some(base) && r.path != rank.path)
            .cloned()
            .collect()
    }

    /// Unlinks one block file and records it everywhere a deletion is counted;
    /// `record_chain` also marks the block gone in the chain index.
    fn remove_block(
        &mut self,
        path: &Path,
        hash: BlockHash,
        size: u64,
        rank: &RankDir,
        record_chain: bool,
    ) -> Unlink {
        if self.ctx.config.dry_run {
            tracing::debug!(path = %path.display(), "[DRY RUN] would delete");
            Stats::add(&self.ctx.stats.files_deleted, 1);
            self.ctx.shared.freed(size);
            return Unlink::Deleted;
        }
        if !self.acquire() || !self.ctx.shared.should_delete() {
            return Unlink::Skipped;
        }
        match self.timed(OpKind::Unlink, || fsops::unlink_path(path)) {
            Ok(()) => {}
            Err(e) if fsops::is_not_found(&e) => {
                if let Some(chains) = self.ctx.chains.as_ref().filter(|_| record_chain) {
                    chains.vanished(hash);
                }
                return Unlink::Missing;
            }
            Err(e) => {
                Stats::add(&self.ctx.stats.errors, 1);
                tracing::warn!(path = %path.display(), error = %e, "unlink failed");
                return Unlink::Skipped;
            }
        }
        Stats::add(&self.ctx.stats.files_deleted, 1);
        Stats::add(&self.ctx.stats.bytes_freed, size);
        self.ctx.shared.freed(size);
        if let Some(chains) = self.ctx.chains.as_ref().filter(|_| record_chain) {
            chains.deleted(hash);
        }
        if let (Some(tx), Some(base)) = (&self.ctx.events, &rank.model_base) {
            let _ = tx.send(Removed {
                model_base: base.clone(),
                hash,
            });
        }
        Unlink::Deleted
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

/// `g0` from `.../<hh>_g0/<hash>.bin`.
fn leaf_group(block: &Path) -> Option<String> {
    let leaf = block.parent()?.file_name()?.to_str()?;
    leaf.rsplit_once("_g").map(|(_, g)| g.to_owned())
}

/// `<rank>/<hhh>/<hh>_g<group>/<name>` for a block file `name`.
fn block_path(rank: &Path, name: &str, bucket_len: usize, group: &str) -> Option<PathBuf> {
    let bucket = name.get(..bucket_len)?;
    let leaf = name.get(bucket_len..bucket_len + 2)?;
    Some(
        rank.join(bucket)
            .join(format!("{leaf}_g{group}"))
            .join(name),
    )
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
            deferrals: 0,
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
            chains: None,
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

    mod chain_fixtures {
        use std::fs;
        use std::path::{Path, PathBuf};
        use std::sync::Arc;

        use crate::chains::{Chains, KvEvent};
        use crate::config::ChainPolicy;
        use crate::layout::BlockHash;
        use crate::worker::tests::{Harness, age, config, harness};

        pub const BASE: u64 = 0xabcd_e000_0000_0000;

        pub fn hash(n: u64) -> BlockHash {
            BlockHash(BASE + n)
        }

        /// One leaf dir of blocks `BASE + n`; `Some(secs)` ages a block, `None` keeps it hot.
        pub fn bucket(blocks: &[(u64, Option<u64>)]) -> (tempfile::TempDir, PathBuf) {
            let tmp = tempfile::tempdir().expect("tempdir");
            let cache = tmp.path().join("cache");
            let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
            fs::create_dir_all(&leaf).expect("mkdir");
            for &(n, secs) in blocks {
                let f = leaf.join(format!("{:016x}.bin", BASE + n));
                fs::write(&f, b"x").expect("write");
                if let Some(secs) = secs {
                    age(&f, secs);
                }
            }
            (tmp, cache)
        }

        pub fn names(ns: &[u64]) -> Vec<String> {
            ns.iter()
                .map(|n| format!("{:016x}.bin", BASE + n))
                .collect()
        }

        pub fn chain_harness(cache: &Path, policy: ChainPolicy, chain: &[u64]) -> Harness {
            let mut h = harness(config(cache, &[]));
            let chains = Arc::new(Chains::new(1000, policy, 2, None));
            chains.apply(&[stored(None, chain)]);
            h.worker.ctx.chains = Some(chains);
            h
        }

        /// A GPU `BlockStored` whose digests are the 8-byte hashes, so the
        /// digest names the fixture's 16-hex-digit files.
        pub fn stored(parent: Option<u64>, chain: &[u64]) -> KvEvent {
            KvEvent::Stored {
                parent: parent.map(hash),
                hashes: chain.iter().map(|&n| hash(n)).collect(),
                digests: chain
                    .iter()
                    .map(|&n| hash(n).0.to_be_bytes().to_vec())
                    .collect(),
                medium: Some("GPU".into()),
            }
        }

        pub fn chains(h: &Harness) -> &Chains {
            h.worker.ctx.chains.as_deref().expect("chains")
        }
    }

    #[test]
    fn tail_first_deletes_a_chain_from_the_leaf_back() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains, names};

        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::TailFirst, &[1, 2, 3, 4]);
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2]),
            "4 then 3 go first though 1 is oldest"
        );
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[1]));
        run_rounds(&mut h, 3);
        assert!(bins(&cache).is_empty());
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_leaf), 3);
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_orphan), 0);
        assert_eq!(
            Stats::get(&s.deferrals),
            1,
            "only 1 still had a child on disk when round one ended"
        );
    }

    #[test]
    fn observe_policy_evicts_oldest_first_and_counts_dead_tails() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains, names};

        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::Observe, &[1, 2, 3, 4]);
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[3, 4]), "plain oldest-first");
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_orphan), 1, "2 lost its parent first");
        assert_eq!(Stats::get(&s.deferrals), 0);
    }

    #[test]
    fn tail_first_prefers_childless_blocks_over_older_interiors() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, names};

        // Chain 1 -> 2 with 2 hot; 9 is an unrelated, younger single block.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, None), (9, Some(7300))]);
        let mut h = chain_harness(&cache, ChainPolicy::TailFirst, &[1, 2]);
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[1, 2]));
        let deferred = h.worker.pool.pop_oldest().expect("1 is back in the pool");
        assert_eq!(deferred.deferrals, 1);
        h.worker.pool.insert(deferred);
        run_rounds(&mut h, 5);
        assert_eq!(
            bins(&cache),
            names(&[2]),
            "an interior is evicted once its deferrals run out"
        );
    }

    #[test]
    fn interior_alone_is_deferred_a_bounded_number_of_rounds() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, names};

        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, None)]);
        let mut h = chain_harness(&cache, ChainPolicy::TailFirst, &[1, 2]);
        h.shared.set_mode(Mode::Evicting);
        let mut evicted_in = None;
        for round in 1..=5 {
            let r = h.worker.round().expect("round").expect("buckets");
            if r.evicted > 0 {
                evicted_in = Some(round);
                break;
            }
        }
        assert_eq!(evicted_in, Some(3), "max_deferrals is 2");
        assert_eq!(bins(&cache), names(&[2]));
    }

    #[test]
    fn tail_first_deletes_dead_blocks_before_older_live_ones() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains, hash, names};

        // 1 -> 2 -> 3 where 1 is already gone; 9 is an older unrelated block.
        let (_tmp, cache) = bucket(&[
            (2, Some(8000)),
            (3, Some(7999)),
            (9, Some(9000)),
            (10, None),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::TailFirst, &[1, 2, 3]);
        chains(&h).deleted(hash(1));
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[9, 10]));
        assert_eq!(
            Stats::get(&chains(&h).stats.deleted_orphan),
            2,
            "3 is orphaned once 2 goes"
        );
        assert_eq!(Stats::get(&chains(&h).stats.deleted_leaf), 0);
    }

    #[test]
    fn subtree_policy_deletes_the_unshared_chain_whole_and_spares_the_shared_prefix() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains, names, stored};

        // Shared prefix 1-2 continued by 3 and 4 (older); unshared chain 10-11-12.
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
            (10, Some(8000)),
            (11, Some(7999)),
            (12, Some(7998)),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::Subtree, &[1, 2, 3]);
        chains(&h).apply(&[stored(Some(2), &[4]), stored(None, &[10, 11, 12])]);
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[1, 2, 3, 4]));
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.cascaded), 2);
        assert_eq!(
            Stats::get(&s.deleted_orphan),
            2,
            "11 and 12 were dead once 10 went"
        );
        assert_eq!(Stats::get(&h.stats.files_deleted), 3);
    }

    #[test]
    fn subtree_cascade_skips_blocks_without_a_digest_and_missing_files() {
        use crate::chains::KvEvent;
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains, hash, names};

        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, None)]);
        let mut h = chain_harness(&cache, ChainPolicy::Subtree, &[1, 2]);
        chains(&h).apply(&[KvEvent::Stored {
            parent: Some(hash(2)),
            hashes: vec![hash(3), hash(4)],
            digests: Vec::new(),
            medium: Some("GPU".into()),
        }]);
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[3]),
            "3 has no known file name; 4 has no file"
        );
        assert_eq!(Stats::get(&chains(&h).stats.cascaded), 1);
        assert_eq!(Stats::get(&h.stats.errors), 0);
    }

    #[test]
    fn subtree_policy_deletes_every_tensor_parallel_shard_once() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{chain_harness, chains, names};

        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let shards = ["m_abcdef012345_r0", "m_abcdef012345_r1"];
        for rank in shards {
            let leaf = cache.join(rank).join("abc/de_g0");
            fs::create_dir_all(&leaf).expect("mkdir");
            for (n, secs) in [(1u64, 9000u64), (2, 8999), (3, 8998)] {
                let f = leaf.join(&names(&[n])[0]);
                fs::write(&f, vec![0u8; 10]).expect("write");
                age(&f, secs);
            }
        }
        let mut h = chain_harness(&cache, ChainPolicy::Subtree, &[1, 2, 3]);
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 20);
        assert!(bins(&cache).is_empty(), "left: {:?}", bins(&cache));
        assert_eq!(Stats::get(&h.stats.files_deleted), 6);
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.cascaded), 2, "blocks, not files");
        assert_eq!(Stats::get(&s.deleted_orphan), 2);
        assert_eq!(
            Stats::get(&s.deleted_untracked),
            0,
            "each block recorded once"
        );
    }

    fn radix_harness(cache: &Path, hot_minutes: &str) -> Harness {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{chain_harness, chains, stored};

        // System prompt 1-2 shared by three one-block conversations 3, 4, 5.
        let mut h = chain_harness(cache, ChainPolicy::Radix, &[1, 2, 3]);
        h.worker.ctx.config = Arc::new(config(
            cache,
            &[("FILE_ACCESS_TIME_THRESHOLD_MINUTES", hot_minutes)],
        ));
        chains(&h).apply(&[stored(Some(2), &[4]), stored(Some(2), &[5])]);
        h.shared.set_mode(Mode::Evicting);
        h
    }

    #[test]
    fn radix_steps_over_a_leaf_whose_file_is_already_gone() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, chains};

        // Block 4's file is gone though the index still has it on disk.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, Some(8998))]);
        let mut h = chain_harness(&cache, ChainPolicy::Radix, &[1, 2, 3, 4]);
        h.worker.ctx.config = Arc::new(config(
            &cache,
            &[("FILE_ACCESS_TIME_THRESHOLD_MINUTES", "0")],
        ));
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 4);
        assert!(bins(&cache).is_empty(), "left: {:?}", bins(&cache));
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.vanished), 1);
        assert_eq!(Stats::get(&s.deleted_internal), 0);
        assert_eq!(Stats::get(&s.deleted_orphan), 0);
    }

    #[test]
    fn radix_keeps_a_shared_prompt_while_it_has_continuations() {
        use crate::worker::tests::chain_fixtures::{bucket, chains, names};

        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "0");
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "the oldest blocks are the prompt, but its oldest conversations go instead"
        );
        assert_eq!(Stats::get(&chains(&h).stats.deleted_leaf), 2);
        assert_eq!(Stats::get(&chains(&h).stats.deleted_root), 0);
        run_rounds(&mut h, 10);
        assert!(
            bins(&cache).is_empty(),
            "with one continuation left the prompt is part of its edge"
        );
    }

    #[test]
    fn radix_falls_back_to_the_least_recently_written_young_edges() {
        use crate::worker::tests::chain_fixtures::{bucket, chains, names};

        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "60");
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "all edges are young, so the least recently written go first"
        );
        let s = &chains(&h).stats;
        assert_eq!(
            Stats::get(&s.young_edges),
            0,
            "nothing else was sampled, so none is held back"
        );
        assert_eq!(Stats::get(&s.young_fallbacks), 2);
    }

    #[test]
    fn radix_deletes_a_dead_candidate_with_its_subtree() {
        use crate::worker::tests::chain_fixtures::{bucket, chains, hash, names};

        let (_tmp, cache) = bucket(&[
            (2, Some(8000)),
            (3, Some(7999)),
            (9, Some(9000)),
            (10, None),
        ]);
        let mut h = radix_harness(&cache, "0");
        chains(&h).deleted(hash(1));
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[9, 10]), "2 and 3 were dead");
        assert_eq!(Stats::get(&chains(&h).stats.cascaded), 1);
    }

    #[test]
    fn subtree_cascade_stops_at_the_byte_budget() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, names};

        // Fixture files are 1 byte: a budget of 3 bytes allows the root and two below it.
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
            (5, Some(8996)),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::Subtree, &[1, 2, 3, 4, 5]);
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_to_free(3);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache).len(), 2, "left: {:?}", bins(&cache));
        assert!(
            !bins(&cache).contains(&names(&[1])[0]),
            "the root goes first"
        );
        assert!(!h.shared.should_delete());
    }

    #[test]
    fn block_path_rebuilds_the_fs_tier_layout() {
        use std::path::Path;

        use crate::worker::{block_path, leaf_group};

        let block = Path::new("/c/m_abc_r0/abc/de_g3/abcdef.bin");
        assert_eq!(leaf_group(block).as_deref(), Some("3"));
        assert_eq!(leaf_group(Path::new("/c/m/abc/de/x.bin")), None);
        assert_eq!(
            block_path(Path::new("/c/m_abc_r0"), "0123ff.bin", 3, "3"),
            Some(Path::new("/c/m_abc_r0/012/3f_g3/0123ff.bin").to_path_buf())
        );
        assert_eq!(block_path(Path::new("/r"), "ab", 3, "0"), None);
    }

    #[test]
    fn emergency_mode_ignores_chain_order() {
        use crate::config::ChainPolicy;
        use crate::worker::tests::chain_fixtures::{bucket, chain_harness, names};

        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
        ]);
        let mut h = chain_harness(&cache, ChainPolicy::TailFirst, &[1, 2, 3, 4]);
        h.shared.set_mode(Mode::Emergency);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[3, 4]));
    }

    #[test]
    fn pool_reinsert_keeps_the_higher_deferral_count() {
        let mut pool = Pool::new(10);
        let mut c = candidate("/a", 10);
        c.deferrals = 3;
        pool.insert(c);
        pool.insert(candidate("/a", 10));
        assert_eq!(pool.pop_oldest().map(|c| c.deferrals), Some(3));
    }
}
