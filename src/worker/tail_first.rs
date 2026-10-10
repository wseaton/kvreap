//! `CHAIN_EVICTION=tail-first`: among the oldest candidates, delete dead
//! blocks, then childless ones, deferring chain interiors.

use crate::chains::Chains;
use crate::chains::index::Rank;
use crate::stats::Stats;
use crate::worker::pool::Candidate;
use crate::worker::{CHAIN_WINDOW, Worker};

impl Worker {
    /// Ranks are recomputed after every deletion: removing a leaf can turn
    /// its parent into one. Interiors with deferrals left are never deleted;
    /// the round evicts less instead.
    pub(crate) fn evict_tail_first(&mut self, quota: usize, chains: &Chains) -> usize {
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
}

#[cfg(test)]
mod tests {

    use crate::controller::Mode;

    use crate::stats::Stats;

    use crate::worker::testing::{bins, bucket, chain_harness, chains, hash, names, run_rounds};

    #[test]
    fn tail_first_deletes_a_chain_from_the_leaf_back() {
        use crate::config::ChainPolicy;

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
    fn tail_first_prefers_childless_blocks_over_older_interiors() {
        use crate::config::ChainPolicy;

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
}
