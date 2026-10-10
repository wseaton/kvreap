//! `CHAIN_EVICTION=subtree`: delete a block with everything below it.

use std::sync::Arc;

use crate::chains::{Chains, Gone};
use crate::stats::Stats;
use crate::worker::pool::Candidate;
use crate::worker::unlink::{Named, leaf_group};
use crate::worker::{CHAIN_WINDOW, Worker};

impl Worker {
    /// Deletes the best subtree root in the window, then every on-disk block
    /// below it: none of them is reachable by a prefix lookup once it is gone.
    pub(crate) fn evict_subtrees(&mut self, quota: usize, chains: &Chains) -> usize {
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
    pub(crate) fn delete_subtree(&mut self, root: Candidate, chains: &Chains) -> usize {
        let (hash, size, rank) = (root.hash, root.size, Arc::clone(&root.rank));
        let Some(_claim) = chains.claim(hash) else {
            return 0;
        };
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
        let (Some(group), Some(name)) = (group, name) else {
            return 1;
        };
        let all_ranks = [std::slice::from_ref(&rank), siblings.as_slice()].concat();
        let mut targets = self.targets(&siblings, &name, &group, hash, size);
        let mut spans = Vec::new();
        for d in chains.descendants(hash) {
            let Some(name) = d.file_name() else {
                continue;
            };
            let start = targets.len();
            targets.extend(self.targets(&all_ranks, &name, &group, d.hash, size));
            spans.push((d.hash, start..targets.len()));
        }
        let results = self.ctx.unlink_all(&targets);
        let tally = |r: std::ops::Range<usize>| Named::of(results.get(r).unwrap_or_default());
        let first = spans.first().map_or(targets.len(), |(_, r)| r.start);
        let mut removed = 1 + tally(0..first).deleted;
        let mut gone = Vec::new();
        for (h, span) in spans {
            let named = tally(span);
            if named.deleted > 0 {
                removed += named.deleted;
                gone.push((h, Gone::Deleted));
                Stats::add(&chains.stats.cascaded, 1);
            } else if named.vanished() {
                gone.push((h, Gone::Vanished));
            }
        }
        chains.record(&gone);
        removed
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use crate::controller::Mode;

    use crate::stats::Stats;

    use crate::worker::testing::{
        age, bins, bucket, chain_harness, chains, hash, names, run_rounds, stored,
    };

    #[test]
    fn subtree_policy_deletes_the_unshared_chain_whole_and_spares_the_shared_prefix() {
        use crate::config::ChainPolicy;

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
        use crate::chains::decode::KvEvent;
        use crate::config::ChainPolicy;

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

    #[test]
    fn subtree_cascade_stops_at_the_byte_budget() {
        use crate::config::ChainPolicy;

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
}
