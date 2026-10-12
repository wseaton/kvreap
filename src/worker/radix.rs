//! Chain-aware eviction: delete whole radix-tree leaf edges (unshared tails),
//! least recently used first, never a prefix that still has two
//! continuations on disk.

use std::time::SystemTime;

use crate::chains::index::LeafEdge;
use crate::chains::{Chains, Claim, Gone, RadixKey};
use crate::layout::BlockHash;
use crate::stats::Stats;
use crate::worker::pool::Candidate;
use crate::worker::unlink::leaf_group;
use crate::worker::{CHAIN_WINDOW, Worker};

/// `hash`'s leaf edge, reserved for this worker: the edge is read again
/// after claiming its leaf, so it is not one another worker just finished.
pub(crate) fn claim_edge(chains: &Chains, hash: BlockHash) -> Option<(LeafEdge, Claim<'_>)> {
    let leaf = chains.leaf_edge(hash)?.blocks.last()?.hash;
    let claim = chains.claim(leaf)?;
    let edge = chains.leaf_edge(hash)?;
    (edge.blocks.last()?.hash == leaf).then_some((edge, claim))
}

impl Worker {
    /// When `hash` was last useful, for ordering the pool: never for a dead
    /// block, the newest write or use on its leaf edge for one the index knows, and
    /// `atime` otherwise. Without the chain index, `atime`.
    pub(crate) fn last_used(&self, hash: BlockHash, atime: SystemTime) -> SystemTime {
        match self.ctx.chains.as_ref().map(|c| c.radix_key(hash)) {
            Some(RadixKey::Dead) => SystemTime::UNIX_EPOCH,
            Some(RadixKey::Edge(newest)) => newest,
            Some(RadixKey::Untracked) | None => atime,
        }
    }

    /// Evicts from the pool in `last_used` order: a dead block with its whole
    /// subtree, a block the index does not know by itself, and any other block
    /// as the whole leaf edge it belongs to (or, inside a shared prefix, the
    /// oldest edge below it), leaf first. A candidate whose edge was written
    /// since it was sampled goes back with its new time. Below the pressure
    /// line, the first edge written within the hot threshold ends the round,
    /// so the sampler looks for older ones first.
    pub(crate) fn evict_radix(&mut self, quota: usize, chains: &Chains) -> usize {
        let mut window: Vec<Candidate> = std::iter::from_fn(|| self.pool.pop_oldest())
            .take(quota.saturating_mul(CHAIN_WINDOW))
            .collect();
        window.reverse();
        let mut evicted = 0;
        while evicted < quota && self.evicting() {
            let Some(mut c) = window.pop() else {
                break;
            };
            match chains.radix_key(c.hash) {
                RadixKey::Dead => evicted += self.delete_subtree(c, chains),
                RadixKey::Untracked => {
                    if self.evict_one(c) {
                        evicted += 1;
                    }
                }
                RadixKey::Edge(newest) if newest > c.last_used => {
                    c.last_used = newest;
                    self.pool.insert(c);
                }
                RadixKey::Edge(newest) if self.young(newest) && !self.pressed() => {
                    Stats::add(&chains.stats.young_edges, 1);
                    self.pool.insert(c);
                    break;
                }
                RadixKey::Edge(_) => {
                    let Some((edge, _claim)) = claim_edge(chains, c.hash) else {
                        self.pool.insert(c);
                        continue;
                    };
                    let (files, candidate_gone) = self.delete_edge(&c, &edge, chains);
                    evicted += files;
                    if !candidate_gone {
                        self.pool.insert(c);
                    }
                }
            }
        }
        for c in window {
            self.pool.insert(c);
        }
        evicted
    }

    fn young(&self, newest_use: SystemTime) -> bool {
        SystemTime::now()
            .duration_since(newest_use)
            .map_or(true, |age| age < self.ctx.config.hot_threshold)
    }

    /// Deletes `edge` leaf first in every rank dir of `c`'s model. Returns
    /// files removed and whether `c` itself was one of them.
    pub(crate) fn delete_edge(
        &mut self,
        c: &Candidate,
        edge: &LeafEdge,
        chains: &Chains,
    ) -> (usize, bool) {
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
        let blocks: Vec<(BlockHash, String)> = edge
            .blocks
            .iter()
            .rev()
            .map_while(|b| {
                let name = b
                    .file_name()
                    .or_else(|| own_name.clone().filter(|_| b.hash == c.hash))?;
                Some((b.hash, name))
            })
            .collect();
        let (mut files, mut candidate_gone) = (0, false);
        let mut gone = Vec::new();
        for (hash, named) in self.unlink_blocks(&ranks, &group, c.size, &blocks) {
            files += named.deleted;
            let Some(what) = named.gone() else {
                continue;
            };
            candidate_gone |= hash == c.hash;
            if what == Gone::Deleted && hash != c.hash {
                Stats::add(&chains.stats.cascaded, 1);
            }
            gone.push((hash, what));
        }
        chains.record(&gone);
        (files, candidate_gone)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use crate::controller::Mode;
    use crate::stats::Stats;
    use crate::worker::testing::{
        Harness, bins, bucket, chain_harness, chains, config, hash, names, run_rounds, stored,
    };

    fn radix_harness(cache: &Path, hot_minutes: &str) -> Harness {
        // System prompt 1-2 shared by three one-block conversations 3, 4, 5.
        let mut h = chain_harness(cache, &[1, 2, 3]);
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
        // Block 4's file is gone though the index still has it on disk.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, Some(8998))]);
        let mut h = chain_harness(&cache, &[1, 2, 3, 4]);
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
    fn radix_deletes_edges_least_recently_written_first() {
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "60");
        h.shared.set_usage(95.0);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "above the pressure line the conversations stored first go first, \
             young or not, whatever their atimes"
        );
        assert_eq!(Stats::get(&chains(&h).stats.deleted_leaf), 2);
    }

    #[test]
    fn young_edges_wait_below_the_pressure_line() {
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "60");
        h.shared.set_usage(88.0);
        run_rounds(&mut h, 4);
        assert_eq!(bins(&cache), names(&[1, 2, 3, 4, 5]));
        assert!(Stats::get(&chains(&h).stats.young_edges) >= 1);
        h.shared.set_usage(91.0);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "91% is the line for cleanup at 85%"
        );
    }

    #[test]
    fn a_live_session_with_old_files_waits_behind_a_finished_one() {
        // Live session 1-2-3 has the oldest files but was written last; the
        // finished conversation 10-11 has newer files written long ago.
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (10, Some(8000)),
            (11, Some(7999)),
        ]);
        let mut h = chain_harness(&cache, &[10, 11]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        chains(&h).apply(&[stored(None, &[1, 2, 3])]);
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_usage(95.0);
        h.shared.set_to_free(2);
        run_rounds(&mut h, 4);
        assert_eq!(bins(&cache), names(&[1, 2, 3]));
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.cascaded), 1, "10-11 went as one edge");
        assert_eq!(Stats::get(&s.deleted_internal), 0);
    }

    #[test]
    fn radix_deletes_a_dead_candidate_with_its_subtree() {
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
}
