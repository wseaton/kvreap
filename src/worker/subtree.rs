//! Deleting a dead block (its parent is gone, so no prefix lookup reaches
//! it) together with every on-disk block below it.

use std::sync::Arc;

use crate::chains::{Chains, Gone};
use crate::layout::BlockHash;
use crate::stats::Stats;
use crate::worker::Worker;
use crate::worker::pool::Candidate;
use crate::worker::unlink::leaf_group;

impl Worker {
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
        let (Some(group), Some(name)) = (group, name) else {
            return 1;
        };
        let siblings = self.sibling_ranks(&rank);
        let mut removed = 1 + self
            .unlink_blocks(&siblings, &group, size, &[(hash, name)])
            .iter()
            .map(|(_, named)| named.deleted)
            .sum::<usize>();
        let all_ranks = [std::slice::from_ref(&rank), siblings.as_slice()].concat();
        let below: Vec<(BlockHash, String)> = chains
            .descendants(hash)
            .into_iter()
            .filter_map(|d| Some((d.hash, d.file_name()?)))
            .collect();
        let mut gone = Vec::new();
        for (h, named) in self.unlink_blocks(&all_ranks, &group, size, &below) {
            removed += named.deleted;
            let Some(what) = named.gone() else {
                continue;
            };
            if what == Gone::Deleted {
                Stats::add(&chains.stats.cascaded, 1);
            }
            gone.push((h, what));
        }
        chains.record(&gone);
        removed
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use crate::chains::decode::KvEvent;
    use crate::controller::Mode;
    use crate::stats::Stats;
    use crate::worker::testing::{
        Harness, age, bins, bucket, chain_harness, chains, hash, names, run_rounds, stored,
    };

    /// `chain` hanging off block 0, which kvreap saw on disk and then lost:
    /// every block of `chain` is dead.
    fn dead_chain(cache: &Path, chain: &[u64]) -> Harness {
        let h = chain_harness(cache, &[0]);
        chains(&h).apply(&[stored(Some(0), chain)]);
        chains(&h).deleted(hash(0));
        h.shared.set_mode(Mode::Evicting);
        h
    }

    #[test]
    fn a_dead_block_goes_with_everything_below_it() {
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (10, Some(8000)),
            (11, Some(7999)),
        ]);
        let mut h = dead_chain(&cache, &[1, 2, 3]);
        chains(&h).apply(&[stored(None, &[10, 11])]);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[10, 11]));
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_orphan), 3);
        assert_eq!(Stats::get(&s.cascaded), 2);
        assert_eq!(Stats::get(&h.stats.files_deleted), 3);
    }

    #[test]
    fn subtree_cascade_skips_blocks_without_a_digest_and_missing_files() {
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, None)]);
        let mut h = dead_chain(&cache, &[1, 2]);
        chains(&h).apply(&[KvEvent::Stored {
            parent: Some(hash(2)),
            hashes: vec![hash(3), hash(4)],
            digests: Vec::new(),
            medium: Some("GPU".into()),
            locality: None,
        }]);
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
    fn subtree_cascade_deletes_every_tensor_parallel_shard_once() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        for rank in ["m_abcdef012345_r0", "m_abcdef012345_r1"] {
            let leaf = cache.join(rank).join("abc/de_g0");
            fs::create_dir_all(&leaf).expect("mkdir");
            for (n, secs) in [(1u64, 9000u64), (2, 8999), (3, 8998)] {
                let f = leaf.join(&names(&[n])[0]);
                fs::write(&f, vec![0u8; 10]).expect("write");
                age(&f, secs);
            }
        }
        let mut h = dead_chain(&cache, &[1, 2, 3]);
        run_rounds(&mut h, 20);
        assert!(bins(&cache).is_empty(), "left: {:?}", bins(&cache));
        assert_eq!(Stats::get(&h.stats.files_deleted), 6);
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.deleted_orphan), 3);
        assert_eq!(Stats::get(&s.cascaded), 2, "blocks, not files");
        assert_eq!(
            Stats::get(&s.deleted_untracked),
            0,
            "each block recorded once"
        );
    }

    #[test]
    fn subtree_cascade_stops_at_the_byte_budget() {
        // Fixture files are 1 byte: a budget of 3 bytes allows the root and two below it.
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8998)),
            (4, Some(8997)),
            (5, Some(8996)),
        ]);
        let mut h = dead_chain(&cache, &[1, 2, 3, 4, 5]);
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
