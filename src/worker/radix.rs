//! Chain-aware eviction: delete whole radix-tree leaf edges (unshared tails),
//! least recently written first, never a prefix that still has two
//! continuations on disk.

use std::time::Instant;

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
    /// Evicts dead blocks (with their whole subtree) first, then blocks the
    /// index does not know, then leaf edges by their newest write, oldest
    /// first. A candidate maps to the leaf edge it belongs to (or, inside a
    /// shared prefix, the oldest edge below it), deleted leaf first. Edges
    /// written within the hot threshold are dropped from the pool so the
    /// sampler brings in others, and only go in the emergency band.
    pub(crate) fn evict_radix(&mut self, quota: usize, chains: &Chains) -> usize {
        let mut window: Vec<(RadixKey, Candidate)> = std::iter::from_fn(|| self.pool.pop_oldest())
            .take(quota.saturating_mul(CHAIN_WINDOW))
            .map(|c| (chains.radix_key(c.hash), c))
            .collect();
        window.sort_by_key(|(key, _)| *key);
        let mut window = window.into_iter();
        let mut held = Vec::new();
        let mut evicted = 0;
        while evicted < quota && self.evicting() {
            let Some((key, c)) = window.next() else {
                break;
            };
            match (key, chains.radix_key(c.hash)) {
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
                    let Some((edge, _claim)) = claim_edge(chains, c.hash) else {
                        held.push(c);
                        continue;
                    };
                    let (files, candidate_gone) = self.delete_edge(&c, &edge, chains);
                    if files > 0 && self.young(newest) {
                        Stats::add(&chains.stats.young_emergency, 1);
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

    pub(crate) fn young(&self, newest_store: Instant) -> bool {
        newest_store.elapsed() < self.ctx.config.hot_threshold
    }

    pub(crate) fn holds_back(&self, newest_store: Instant) -> bool {
        self.young(newest_store) && !self.unpaced()
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
    fn radix_never_deletes_a_young_edge_outside_the_emergency_band() {
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "60");
        run_rounds(&mut h, 4);
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 3, 4, 5]),
            "every edge is young and the pool has nothing else"
        );
        let s = &chains(&h).stats;
        assert!(Stats::get(&s.young_edges) >= 3);
        assert_eq!(Stats::get(&s.young_emergency), 0);
    }

    #[test]
    fn radix_deletes_young_edges_least_recently_written_first_in_emergency() {
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ]);
        let mut h = radix_harness(&cache, "60");
        h.shared.set_mode(Mode::Emergency);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "all edges are young, so the least recently written go first"
        );
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.young_edges), 0);
        assert_eq!(Stats::get(&s.young_emergency), 2);
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
