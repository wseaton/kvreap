//! Prefix-chain index built from vLLM's KV cache events, shared by the
//! subscriber that fills it and the workers that evict by it.
//!
//! A prefix-cache lookup stops at the first missing block, so deleting block k
//! of a chain makes every later block useless. vLLM publishes `BlockStored`
//! events with each block's parent; [`index::ChainIndex`] keeps
//! `block -> parent` and a per-block count of children still on disk, so the
//! workers can delete whole unshared tails instead of cutting chains.
//!
//! ```text
//!  root ──► b1 ──► b2 ──► b3        evict b3 (leaf) first: b2 becomes a leaf
//!                    └──► b2'       evict b1 first: b2, b3, b2' are dead weight
//! ```

use std::collections::HashSet;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use crate::chains::decode::KvEvent;
use crate::chains::index::{ChainIndex, Descendant, LeafEdge, Position, Rank, Store, SubtreeKey};
use crate::config::ChainPolicy;
use crate::layout::BlockHash;
use crate::stats::Stats;

pub mod decode;
pub mod index;
pub mod subscriber;
#[cfg(test)]
mod testing;

pub const INDEX_CAP: usize = 4 << 20;
const SUBTREE_BUDGET: usize = 4096;
const MEDIUM_STORAGE: &str = "STORAGE";

/// Where radix eviction ranks a block, first to go first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RadixKey {
    Dead,
    /// Not on disk in the index, or its edge is longer than the walk budget.
    Untracked,
    /// On a leaf edge whose newest block was marked on disk at this instant.
    Edge(Instant),
}

/// A block one worker is deleting from; released on drop.
#[derive(Debug)]
pub struct Claim<'a> {
    chains: &'a Chains,
    hash: BlockHash,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.chains
            .claims
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.hash);
    }
}

/// What became of a block kvreap went to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gone {
    Deleted,
    /// Its file was already gone.
    Vanished,
}

#[derive(Debug, Default)]
pub struct ChainStats {
    pub batches: AtomicU64,
    pub decode_errors: AtomicU64,
    pub blocks_stored: AtomicU64,
    pub deleted_untracked: AtomicU64,
    pub deleted_root: AtomicU64,
    pub deleted_orphan: AtomicU64,
    pub deleted_internal: AtomicU64,
    pub deleted_leaf: AtomicU64,
    pub deferrals: AtomicU64,
    pub cascaded: AtomicU64,
    /// Stored blocks announced as int hashes, whose files cannot be named.
    pub undigested: AtomicU64,
    /// Leaf edges left alone because a block in them is younger than the hot threshold.
    pub young_edges: AtomicU64,
    /// Young edges deleted because nothing older was left to free.
    pub young_fallbacks: AtomicU64,
    /// Live batches whose sequence number skipped ahead of the last applied.
    pub gaps: AtomicU64,
    /// Publishers whose sequence numbers went backwards (restarted).
    pub resets: AtomicU64,
    pub replays: AtomicU64,
    pub replayed_batches: AtomicU64,
    pub replay_failures: AtomicU64,
    /// Batches never applied: skipped by the SUB socket and gone from the replay buffer.
    pub events_lost: AtomicU64,
    /// Blocks the index had on disk whose files were already gone when kvreap went to delete them.
    pub vanished: AtomicU64,
    /// Edges or subtrees skipped because another worker was deleting them.
    pub claim_conflicts: AtomicU64,
    pub events_lock: LockStats,
    pub worker_lock: LockStats,
    /// Most batches waiting in the subscriber's queue since the last status line.
    pub queue_depth_max: AtomicU64,
}

/// Time spent waiting for and holding the index lock, in microseconds.
#[derive(Debug, Default)]
pub struct LockStats {
    pub acquisitions: AtomicU64,
    pub wait_us: AtomicU64,
    pub max_wait_us: AtomicU64,
    pub hold_us: AtomicU64,
    pub max_hold_us: AtomicU64,
}

impl LockStats {
    fn waited(&self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        Stats::add(&self.acquisitions, 1);
        Stats::add(&self.wait_us, us);
        self.max_wait_us.fetch_max(us, Ordering::Relaxed);
    }

    fn held(&self, d: Duration) {
        let us = u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
        Stats::add(&self.hold_us, us);
        self.max_hold_us.fetch_max(us, Ordering::Relaxed);
    }
}

/// An index lock guard, timed from acquisition to drop.
struct Held<'a, G> {
    guard: G,
    since: Instant,
    stats: &'a LockStats,
}

fn timed<G>(stats: &LockStats, lock: impl FnOnce() -> G) -> Held<'_, G> {
    let asked = Instant::now();
    let guard = lock();
    let since = Instant::now();
    stats.waited(since.duration_since(asked));
    Held {
        guard,
        since,
        stats,
    }
}

impl<G: Deref<Target = ChainIndex>> Deref for Held<'_, G> {
    type Target = ChainIndex;

    fn deref(&self) -> &ChainIndex {
        &self.guard
    }
}

impl<G: DerefMut<Target = ChainIndex>> DerefMut for Held<'_, G> {
    fn deref_mut(&mut self) -> &mut ChainIndex {
        &mut self.guard
    }
}

impl<G> Drop for Held<'_, G> {
    fn drop(&mut self) {
        self.stats.held(self.since.elapsed());
    }
}

/// The index shared by the subscriber and every worker.
#[derive(Debug)]
pub struct Chains {
    index: RwLock<ChainIndex>,
    /// Leaves of the edges and roots of the subtrees workers are deleting.
    claims: Mutex<HashSet<BlockHash>>,
    pub policy: ChainPolicy,
    pub max_deferrals: u32,
    /// When set, only `BlockStored` events of this medium mark blocks on
    /// disk; the rest only teach parent links.
    pub disk_medium: Option<String>,
    pub stats: ChainStats,
}

impl Chains {
    pub fn new(
        cap: usize,
        policy: ChainPolicy,
        max_deferrals: u32,
        disk_medium: Option<String>,
    ) -> Self {
        Self {
            index: RwLock::new(ChainIndex::new(cap)),
            claims: Mutex::default(),
            policy,
            max_deferrals,
            disk_medium,
            stats: ChainStats::default(),
        }
    }

    /// Shares the index with other readers, for eviction workers.
    fn index(&self) -> Held<'_, impl Deref<Target = ChainIndex>> {
        timed(&self.stats.worker_lock, || {
            self.index.read().unwrap_or_else(PoisonError::into_inner)
        })
    }

    fn write<'a>(&'a self, stats: &'a LockStats) -> Held<'a, impl DerefMut<Target = ChainIndex>> {
        timed(stats, || {
            self.index.write().unwrap_or_else(PoisonError::into_inner)
        })
    }

    pub fn rank(&self, hash: BlockHash, deferrals: u32) -> Rank {
        self.index().rank(hash, deferrals, self.max_deferrals)
    }

    pub fn subtree_key(&self, hash: BlockHash) -> SubtreeKey {
        self.index().subtree_key(hash, SUBTREE_BUDGET)
    }

    pub fn descendants(&self, hash: BlockHash) -> Vec<Descendant> {
        self.index().descendants(hash, SUBTREE_BUDGET)
    }

    pub fn leaf_edge(&self, hash: BlockHash) -> Option<LeafEdge> {
        self.index().leaf_edge(hash, SUBTREE_BUDGET)
    }

    /// Reserves `hash` for one worker until the claim drops; `None` while
    /// another worker holds it.
    pub fn claim(&self, hash: BlockHash) -> Option<Claim<'_>> {
        let mut claims = self.claims.lock().unwrap_or_else(PoisonError::into_inner);
        if claims.insert(hash) {
            Some(Claim { chains: self, hash })
        } else {
            Stats::add(&self.stats.claim_conflicts, 1);
            None
        }
    }

    pub fn radix_key(&self, hash: BlockHash) -> RadixKey {
        let index = self.index();
        if index.rank(hash, 0, self.max_deferrals) == Rank::Dead {
            return RadixKey::Dead;
        }
        match index.edge_newest(hash, SUBTREE_BUDGET) {
            Some(newest) => RadixKey::Edge(newest),
            None => RadixKey::Untracked,
        }
    }

    /// Drops a block whose file is gone although kvreap did not delete it.
    pub fn vanished(&self, hash: BlockHash) {
        self.record(&[(hash, Gone::Vanished)]);
    }

    /// Records a deletion by kvreap and counts its chain position.
    pub fn deleted(&self, hash: BlockHash) -> Position {
        self.note(
            &mut self.write(&self.stats.worker_lock),
            hash,
            Gone::Deleted,
        )
    }

    /// Records blocks kvreap deleted or found gone, in order, under one lock.
    pub fn record(&self, gone: &[(BlockHash, Gone)]) {
        if gone.is_empty() {
            return;
        }
        let mut index = self.write(&self.stats.worker_lock);
        for &(hash, what) in gone {
            self.note(&mut index, hash, what);
        }
    }

    fn note(&self, index: &mut ChainIndex, hash: BlockHash, what: Gone) -> Position {
        let position = index.remove(hash);
        let counter = match (what, position) {
            (Gone::Vanished, Position::Untracked) => return position,
            (Gone::Vanished, _) => &self.stats.vanished,
            (Gone::Deleted, Position::Untracked) => &self.stats.deleted_untracked,
            (Gone::Deleted, Position::Root) => &self.stats.deleted_root,
            (Gone::Deleted, Position::Orphan) => &self.stats.deleted_orphan,
            (Gone::Deleted, Position::Internal) => &self.stats.deleted_internal,
            (Gone::Deleted, Position::Leaf) => &self.stats.deleted_leaf,
        };
        Stats::add(counter, 1);
        position
    }

    pub fn apply(&self, events: &[KvEvent]) {
        let mut index = self.write(&self.stats.events_lock);
        for event in events {
            match event {
                KvEvent::Stored {
                    parent,
                    hashes,
                    digests,
                    medium,
                } => {
                    let store = if self
                        .disk_medium
                        .as_deref()
                        .is_none_or(|m| medium.as_deref() == Some(m))
                    {
                        Store::OnDisk
                    } else {
                        Store::LinksOnly
                    };
                    index.store(*parent, hashes, store);
                    if digests.is_empty() {
                        Stats::add(&self.stats.undigested, hashes.len() as u64);
                    }
                    for (h, d) in hashes.iter().zip(digests) {
                        index.set_digest(*h, d);
                    }
                    if store == Store::OnDisk {
                        Stats::add(&self.stats.blocks_stored, hashes.len() as u64);
                    }
                }
                KvEvent::Removed { hashes, medium }
                    if medium.as_deref()
                        == Some(self.disk_medium.as_deref().unwrap_or(MEDIUM_STORAGE)) =>
                {
                    for &h in hashes {
                        index.remove(h);
                    }
                }
                KvEvent::Removed { .. } | KvEvent::Other => {}
            }
        }
    }

    pub fn log_status(&self) {
        let s = &self.stats;
        let (root, orphan) = (Stats::get(&s.deleted_root), Stats::get(&s.deleted_orphan));
        tracing::info!(
            policy = ?self.policy,
            disk_medium = self.disk_medium.as_deref().unwrap_or("any"),
            index_blocks = self.index().len(),
            event_batches = Stats::get(&s.batches),
            decode_errors = Stats::get(&s.decode_errors),
            blocks_stored = Stats::get(&s.blocks_stored),
            deleted_heads = root + orphan,
            deleted_root = root,
            deleted_orphan = orphan,
            deleted_internal = Stats::get(&s.deleted_internal),
            deleted_leaf = Stats::get(&s.deleted_leaf),
            deleted_untracked = Stats::get(&s.deleted_untracked),
            deferrals = Stats::get(&s.deferrals),
            cascaded = Stats::get(&s.cascaded),
            undigested = Stats::get(&s.undigested),
            young_edges = Stats::get(&s.young_edges),
            young_fallbacks = Stats::get(&s.young_fallbacks),
            gaps = Stats::get(&s.gaps),
            resets = Stats::get(&s.resets),
            replays = Stats::get(&s.replays),
            replayed_batches = Stats::get(&s.replayed_batches),
            replay_failures = Stats::get(&s.replay_failures),
            events_lost = Stats::get(&s.events_lost),
            vanished = Stats::get(&s.vanished),
            claim_conflicts = Stats::get(&s.claim_conflicts),
            events_lock_wait_ms = Stats::get(&s.events_lock.wait_us) / 1000,
            events_lock_max_wait_us = s.events_lock.max_wait_us.swap(0, Ordering::Relaxed),
            events_lock_hold_ms = Stats::get(&s.events_lock.hold_us) / 1000,
            events_lock_max_hold_us = s.events_lock.max_hold_us.swap(0, Ordering::Relaxed),
            worker_lock_acquisitions = Stats::get(&s.worker_lock.acquisitions),
            worker_lock_wait_ms = Stats::get(&s.worker_lock.wait_us) / 1000,
            worker_lock_max_wait_us = s.worker_lock.max_wait_us.swap(0, Ordering::Relaxed),
            worker_lock_hold_ms = Stats::get(&s.worker_lock.hold_us) / 1000,
            worker_lock_max_hold_us = s.worker_lock.max_hold_us.swap(0, Ordering::Relaxed),
            queue_depth_max = s.queue_depth_max.swap(0, Ordering::Relaxed),
            "chains"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::chains::Chains;
    use crate::chains::index::{Position, Rank};
    use crate::chains::testing::{h, removed, stored};
    use crate::config::ChainPolicy;
    use crate::stats::Stats;

    #[test]
    fn chains_counts_deletions_and_ignores_non_storage_removals() {
        let chains = Chains::new(100, ChainPolicy::TailFirst, 2, None);
        chains.apply(&[stored(None, &[1, 2, 3], "GPU"), removed(&[3], "GPU")]);
        assert_eq!(
            chains.rank(h(2), 0),
            Rank::Interior,
            "GPU removal is not a disk removal"
        );
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.rank(h(2), 0), Rank::Childless);
        assert_eq!(chains.deleted(h(1)), Position::Root);
        assert_eq!(chains.deleted(h(2)), Position::Orphan);
        assert_eq!(chains.deleted(h(9)), Position::Untracked);
        let s = &chains.stats;
        assert_eq!(Stats::get(&s.blocks_stored), 3);
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_orphan), 1);
        assert_eq!(Stats::get(&s.deleted_untracked), 1);
        assert_eq!(Stats::get(&s.deleted_leaf), 0);
        assert_eq!(Stats::get(&s.undigested), 3, "test events carry int hashes");
    }

    #[test]
    fn disk_medium_limits_which_stores_mark_disk() {
        let chains = Chains::new(100, ChainPolicy::TailFirst, 2, Some("STORAGE".into()));
        chains.apply(&[stored(None, &[1, 2, 3], "GPU")]);
        assert_eq!(
            chains.rank(h(1), 0),
            Rank::Childless,
            "GPU only: not on disk"
        );
        assert_eq!(Stats::get(&chains.stats.blocks_stored), 0);
        // The fs tier's own events carry only the hash.
        chains.apply(&[
            stored(None, &[1], "STORAGE"),
            stored(None, &[2], "STORAGE"),
            stored(None, &[3], "STORAGE"),
        ]);
        assert_eq!(Stats::get(&chains.stats.blocks_stored), 3);
        assert_eq!(chains.rank(h(1), 0), Rank::Interior);
        assert_eq!(chains.rank(h(3), 0), Rank::Childless);
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.rank(h(2), 0), Rank::Childless);
        assert_eq!(chains.deleted(h(2)), Position::Leaf);
        assert_eq!(chains.deleted(h(1)), Position::Root);
    }

    #[test]
    fn subtree_key_prefers_dead_then_most_blocks_per_leaf() {
        // Shared prefix 1-2 with four continuations; unshared chain 10-11-12.
        let chains = Chains::new(100, ChainPolicy::Subtree, 2, None);
        chains.apply(&[
            stored(None, &[1, 2], "GPU"),
            stored(Some(2), &[3], "GPU"),
            stored(Some(2), &[4], "GPU"),
            stored(Some(2), &[5], "GPU"),
            stored(Some(2), &[6], "GPU"),
            stored(None, &[10, 11, 12], "GPU"),
        ]);
        let (shared, unshared, leaf) = (
            chains.subtree_key(h(1)),
            chains.subtree_key(h(10)),
            chains.subtree_key(h(12)),
        );
        assert!(unshared < shared, "3 blocks per leaf beats 6 / 4");
        assert!(unshared < leaf, "a whole chain beats its last block");
        assert!(
            shared < leaf,
            "6 blocks for 4 continuations beats 1 block for 1"
        );
        chains.deleted(h(10));
        let dead = chains.subtree_key(h(11));
        assert!(dead < unshared.min(shared).min(leaf), "dead blocks first");
    }

    #[test]
    fn index_lock_records_wait_and_hold_per_side() {
        let chains = Arc::new(Chains::new(100, ChainPolicy::Radix, 2, None));
        let held = {
            let chains = Arc::clone(&chains);
            let (tx, rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                let _events = chains.write(&chains.stats.events_lock);
                tx.send(()).expect("locked");
                std::thread::sleep(Duration::from_millis(60));
            });
            rx.recv().expect("locked");
            handle
        };
        chains.rank(h(1), 0);
        held.join().expect("join");
        let (ev, wk) = (&chains.stats.events_lock, &chains.stats.worker_lock);
        assert_eq!(Stats::get(&ev.acquisitions), 1);
        assert!(
            Stats::get(&ev.hold_us) >= 50_000,
            "{}",
            Stats::get(&ev.hold_us)
        );
        assert_eq!(Stats::get(&wk.acquisitions), 1);
        assert!(
            Stats::get(&wk.wait_us) >= 40_000,
            "{}",
            Stats::get(&wk.wait_us)
        );
        assert_eq!(Stats::get(&wk.max_wait_us), Stats::get(&wk.wait_us));
    }

    #[test]
    fn workers_read_the_index_together_and_writers_wait() {
        let chains = Arc::new(Chains::new(100, ChainPolicy::Radix, 2, None));
        let reader = {
            let chains = Arc::clone(&chains);
            let (tx, rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                let _shared = chains.index();
                tx.send(()).expect("locked");
                std::thread::sleep(Duration::from_millis(60));
            });
            rx.recv().expect("locked");
            handle
        };
        chains.leaf_edge(h(1));
        let read_wait = Stats::get(&chains.stats.worker_lock.max_wait_us);
        chains.apply(&[stored(None, &[1], "STORAGE")]);
        reader.join().expect("join");
        assert!(read_wait < 20_000, "a second reader waited {read_wait}us");
        let write_wait = Stats::get(&chains.stats.events_lock.wait_us);
        assert!(
            write_wait >= 30_000,
            "the writer waited only {write_wait}us"
        );
    }

    #[test]
    fn radix_key_orders_dead_untracked_then_edges_by_newest_write() {
        use crate::chains::RadixKey;

        let chains = Chains::new(100, ChainPolicy::Radix, 2, None);
        chains.apply(&[stored(None, &[1, 2], "STORAGE")]);
        std::thread::sleep(Duration::from_millis(5));
        chains.apply(&[stored(None, &[3, 4], "STORAGE")]);
        let (old, new) = (chains.radix_key(h(1)), chains.radix_key(h(3)));
        assert!(matches!(old, RadixKey::Edge(_)));
        assert!(old < new, "the edge written first goes first");
        assert_eq!(chains.radix_key(h(99)), RadixKey::Untracked);
        chains.deleted(h(1));
        assert_eq!(chains.radix_key(h(2)), RadixKey::Dead);
        assert!(RadixKey::Dead < RadixKey::Untracked && RadixKey::Untracked < old);
    }

    #[test]
    fn record_applies_a_batch_in_order_under_one_lock() {
        use crate::chains::Gone;

        let chains = Chains::new(100, ChainPolicy::Radix, 2, None);
        chains.apply(&[stored(None, &[1, 2, 3], "STORAGE")]);
        let before = Stats::get(&chains.stats.worker_lock.acquisitions);
        chains.record(&[
            (h(3), Gone::Deleted),
            (h(2), Gone::Vanished),
            (h(1), Gone::Deleted),
            (h(9), Gone::Vanished),
        ]);
        assert_eq!(
            Stats::get(&chains.stats.worker_lock.acquisitions),
            before + 1
        );
        let s = &chains.stats;
        assert_eq!(Stats::get(&s.deleted_leaf), 1, "3 went while 2 was on disk");
        assert_eq!(Stats::get(&s.vanished), 1, "9 was never tracked");
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_internal), 0);
        let index = chains.index();
        assert!(!index.on_disk(h(1)) && !index.on_disk(h(2)) && !index.on_disk(h(3)));
    }

    #[test]
    fn one_worker_at_a_time_claims_an_edge() {
        let chains = Chains::new(100, ChainPolicy::Radix, 2, None);
        let first = chains.claim(h(7)).expect("free");
        assert!(chains.claim(h(7)).is_none(), "held by the first worker");
        assert!(chains.claim(h(8)).is_some(), "other edges are independent");
        drop(first);
        assert!(chains.claim(h(7)).is_some(), "released on drop");
        assert_eq!(Stats::get(&chains.stats.claim_conflicts), 1);
    }
}
