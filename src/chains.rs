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
use std::sync::atomic::AtomicU64;
use std::sync::{Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use crate::chains::decode::KvEvent;
use crate::chains::index::{ChainIndex, Descendant, LeafEdge, Position, Store};
use crate::layout::BlockHash;
use crate::stats::Stats;

pub mod decode;
pub mod index;
pub mod subscriber;
#[cfg(test)]
pub mod testing;

pub const INDEX_CAP: usize = 4 << 20;
const WALK_BUDGET: usize = 4096;
const MEDIUM_STORAGE: &str = "STORAGE";

/// Where radix eviction ranks a block, first to go first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RadixKey {
    Dead,
    /// Not on disk in the index, or its edge is longer than the walk budget.
    Untracked,
    /// On a leaf edge whose newest block was marked on disk at this instant.
    Edge(SystemTime),
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
    pub cascaded: AtomicU64,
    /// Stored blocks announced as int hashes, whose files cannot be named.
    pub undigested: AtomicU64,
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
}

/// The index shared by the subscriber and every worker.
#[derive(Debug)]
pub struct Chains {
    index: RwLock<ChainIndex>,
    /// Leaves of the edges and roots of the subtrees workers are deleting.
    claims: Mutex<HashSet<BlockHash>>,
    /// When set, only `BlockStored` events of this medium mark blocks on
    /// disk; the rest only teach parent links.
    pub disk_medium: Option<String>,
    pub stats: ChainStats,
}

impl Chains {
    pub fn new(cap: usize, disk_medium: Option<String>) -> Self {
        Self {
            index: RwLock::new(ChainIndex::new(cap)),
            claims: Mutex::default(),
            disk_medium,
            stats: ChainStats::default(),
        }
    }

    /// Shares the index with other readers, for eviction workers.
    fn index(&self) -> RwLockReadGuard<'_, ChainIndex> {
        self.index.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, ChainIndex> {
        self.index.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn index_blocks(&self) -> usize {
        self.index().len()
    }

    pub fn descendants(&self, hash: BlockHash) -> Vec<Descendant> {
        self.index().descendants(hash, WALK_BUDGET)
    }

    pub fn leaf_edge(&self, hash: BlockHash) -> Option<LeafEdge> {
        self.index().leaf_edge(hash, WALK_BUDGET)
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
        if index.position(hash) == Position::Orphan {
            return RadixKey::Dead;
        }
        match index.edge_newest(hash, WALK_BUDGET) {
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
        self.note(&mut self.write(), hash, Gone::Deleted)
    }

    /// Records blocks kvreap deleted or found gone, in order, under one lock.
    pub fn record(&self, gone: &[(BlockHash, Gone)]) {
        if gone.is_empty() {
            return;
        }
        let mut index = self.write();
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
        let mut index = self.write();
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
        tracing::info!(
            disk_medium = self.disk_medium.as_deref().unwrap_or("any"),
            index_blocks = self.index_blocks(),
            event_batches = Stats::get(&s.batches),
            decode_errors = Stats::get(&s.decode_errors),
            blocks_stored = Stats::get(&s.blocks_stored),
            deleted_root = Stats::get(&s.deleted_root),
            deleted_orphan = Stats::get(&s.deleted_orphan),
            deleted_internal = Stats::get(&s.deleted_internal),
            deleted_leaf = Stats::get(&s.deleted_leaf),
            deleted_untracked = Stats::get(&s.deleted_untracked),
            cascaded = Stats::get(&s.cascaded),
            undigested = Stats::get(&s.undigested),
            gaps = Stats::get(&s.gaps),
            resets = Stats::get(&s.resets),
            replays = Stats::get(&s.replays),
            replayed_batches = Stats::get(&s.replayed_batches),
            replay_failures = Stats::get(&s.replay_failures),
            events_lost = Stats::get(&s.events_lost),
            vanished = Stats::get(&s.vanished),
            claim_conflicts = Stats::get(&s.claim_conflicts),
            "chains"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::chains::Chains;
    use crate::chains::index::Position;
    use crate::chains::testing::{h, removed, stored};
    use crate::stats::Stats;

    #[test]
    fn chains_counts_deletions_and_ignores_non_storage_removals() {
        let chains = Chains::new(100, None);
        chains.apply(&[stored(None, &[1, 2, 3], "GPU"), removed(&[3], "GPU")]);
        assert_eq!(
            chains.index().position(h(2)),
            Position::Internal,
            "GPU removal is not a disk removal"
        );
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.index().position(h(2)), Position::Leaf);
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
        let chains = Chains::new(100, Some("STORAGE".into()));
        chains.apply(&[stored(None, &[1, 2, 3], "GPU")]);
        assert_eq!(
            chains.index().position(h(1)),
            Position::Untracked,
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
        assert_eq!(chains.index().position(h(2)), Position::Internal);
        assert_eq!(chains.index().position(h(3)), Position::Leaf);
        chains.apply(&[removed(&[3], "STORAGE")]);
        assert_eq!(chains.index().position(h(2)), Position::Leaf);
        assert_eq!(chains.deleted(h(2)), Position::Leaf);
        assert_eq!(chains.deleted(h(1)), Position::Root);
    }

    #[test]
    fn radix_key_orders_dead_untracked_then_edges_by_newest_write() {
        use crate::chains::RadixKey;

        let chains = Chains::new(100, None);
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
    fn record_applies_a_batch_in_order() {
        use crate::chains::Gone;

        let chains = Chains::new(100, None);
        chains.apply(&[stored(None, &[1, 2, 3], "STORAGE")]);
        chains.record(&[
            (h(3), Gone::Deleted),
            (h(2), Gone::Vanished),
            (h(1), Gone::Deleted),
            (h(9), Gone::Vanished),
        ]);
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
        let chains = Chains::new(100, None);
        let first = chains.claim(h(7)).expect("free");
        assert!(chains.claim(h(7)).is_none(), "held by the first worker");
        assert!(chains.claim(h(8)).is_some(), "other edges are independent");
        drop(first);
        assert!(chains.claim(h(7)).is_some(), "released on drop");
        assert_eq!(Stats::get(&chains.stats.claim_conflicts), 1);
    }
}
