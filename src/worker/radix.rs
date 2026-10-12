//! Chain-aware eviction: delete whole radix-tree leaf edges (unshared tails)
//! in the chain index's global least-recently-used order, never a prefix
//! that still has two continuations on disk. Dead subtrees go first; sampled
//! files the index does not order go by atime among the edges.

use std::time::Duration;

use crate::budget::OpKind;
use crate::chains::index::LeafEdge;
use crate::chains::{Chains, Gone};
use crate::fsops;
use crate::layout::BlockHash;
use crate::stats::Stats;
use crate::worker::Worker;
use crate::worker::unlink::{Place, block_path};

/// Files a radix round may delete before the worker samples its next bucket.
const ROUND_FILES: usize = 1024;
/// A block found nowhere is taken for gone only if the rank dirs were listed
/// this recently; otherwise they are listed again first.
const LAYOUT_FRESH: Duration = Duration::from_secs(10);

/// Where a named block's files are.
#[derive(Debug)]
pub(crate) enum Located {
    At(Vec<Place>),
    /// No file of it in any known model and group.
    Missing,
    /// No rank dir or group learned yet, or the probe failed.
    Unknown,
}

impl Worker {
    /// Evicts dead subtrees first, then the least recently used leaf edge or
    /// the oldest pooled file the index does not order, whichever was last
    /// used earlier, until the grant is spent or `ROUND_FILES` are gone.
    /// Pooled files count against `quota`.
    pub(crate) fn evict_radix(&mut self, quota: usize, chains: &Chains) -> usize {
        let (mut evicted, mut unordered) = (0, 0);
        while evicted < ROUND_FILES && self.evicting() {
            if let Some((root, _claim)) = chains.dead_root() {
                match self.delete_subtree(&root, chains) {
                    Some(files) => evicted += files,
                    None => break,
                }
                continue;
            }
            let edge = chains.oldest_edge();
            let file_first = unordered < quota
                && self
                    .pool
                    .oldest()
                    .is_some_and(|atime| edge.as_ref().is_none_or(|(e, _)| atime < e.newest_use));
            if file_first {
                drop(edge);
                if let Some(c) = self.pool.pop_oldest()
                    && !chains.orders(c.hash)
                    && self.evict_one(c)
                {
                    unordered += 1;
                    evicted += 1;
                }
                continue;
            }
            let Some((edge, _claim)) = edge else {
                break;
            };
            match self.delete_edge(&edge, chains) {
                Some(files) => evicted += files,
                None => break,
            }
        }
        evicted
    }

    /// Deletes `edge` leaf first, as far as its blocks are named. `None` when
    /// no block of it changed.
    pub(crate) fn delete_edge(&mut self, edge: &LeafEdge, chains: &Chains) -> Option<usize> {
        let blocks: Vec<(BlockHash, String)> = edge
            .blocks
            .iter()
            .rev()
            .map_while(|b| Some((b.hash, b.file_name()?)))
            .collect();
        self.delete_blocks(&blocks, chains)
    }

    /// Unlinks `blocks` in order wherever the first one's files are, and
    /// records what went. A first block with no file anywhere is recorded
    /// gone by itself. `None` when no block changed.
    pub(crate) fn delete_blocks(
        &mut self,
        blocks: &[(BlockHash, String)],
        chains: &Chains,
    ) -> Option<usize> {
        let (first, name) = blocks.first()?;
        let places = match self.locate(name) {
            Located::At(places) => places,
            Located::Missing if self.layout_fresh() => {
                chains.record(&[(*first, Gone::Vanished)]);
                return Some(0);
            }
            Located::Missing => {
                self.index.refreshed_at = None;
                return None;
            }
            Located::Unknown => return None,
        };
        let mut files = 0;
        let mut gone = Vec::new();
        for (hash, named) in self.unlink_blocks(&places, blocks) {
            files += named.deleted;
            let Some(what) = named.gone() else {
                continue;
            };
            if what == Gone::Deleted && hash != *first {
                Stats::add(&chains.stats.cascaded, 1);
            }
            gone.push((hash, what));
        }
        if gone.is_empty() {
            return None;
        }
        chains.record(&gone);
        Some(files)
    }

    fn layout_fresh(&self) -> bool {
        self.index
            .refreshed_at
            .is_some_and(|t| t.elapsed() < LAYOUT_FRESH)
    }

    /// Finds the block file `name` by a `statx` in the first rank dir of each
    /// model under each group sampling has seen.
    pub(crate) fn locate(&self, name: &str) -> Located {
        if self.groups.is_empty() || self.index.models.is_empty() {
            return Located::Unknown;
        }
        let bucket_len = self.ctx.config.hex_bucket_len;
        let mut places = Vec::new();
        for model in &self.index.models {
            let Some(first) = model.first() else {
                continue;
            };
            for group in &self.groups {
                let Some(path) = block_path(&first.path, name, bucket_len, group) else {
                    continue;
                };
                if !self.acquire() {
                    return Located::Unknown;
                }
                match self.timed(OpKind::Stat, || fsops::stat_path(&path)) {
                    Ok(meta) => places.push(Place {
                        ranks: model.clone(),
                        group: group.clone(),
                        size: meta.size,
                    }),
                    Err(e) if fsops::is_not_found(&e) => {}
                    Err(e) => {
                        Stats::add(&self.ctx.stats.errors, 1);
                        tracing::debug!(path = %path.display(), error = %e, "statx failed while locating a block");
                        return Located::Unknown;
                    }
                }
            }
        }
        if places.is_empty() {
            Located::Missing
        } else {
            Located::At(places)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::chains::decode::KvEvent;
    use crate::controller::{Mode, UNGRANTED};
    use crate::layout::{BlockHash, Shard};
    use crate::stats::Stats;
    use crate::worker::radix::Located;
    use crate::worker::testing::{
        Harness, age, bins, bucket, chain_harness, chains, config, names, run_rounds, stored,
    };

    fn hot_minutes(h: &mut Harness, cache: &Path, minutes: &str) {
        h.worker.ctx.config = Arc::new(config(
            cache,
            &[("FILE_ACCESS_TIME_THRESHOLD_MINUTES", minutes)],
        ));
    }

    fn radix_harness(cache: &Path, minutes: &str) -> Harness {
        // System prompt 1-2 shared by three one-block conversations 3, 4, 5.
        let mut h = chain_harness(cache, &[1, 2, 3]);
        hot_minutes(&mut h, cache, minutes);
        std::thread::sleep(Duration::from_millis(5));
        chains(&h).apply(&[stored(Some(2), &[4])]);
        std::thread::sleep(Duration::from_millis(5));
        chains(&h).apply(&[stored(Some(2), &[5])]);
        h.shared.set_mode(Mode::Evicting);
        h
    }

    fn conversations() -> (tempfile::TempDir, PathBuf) {
        bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(8500)),
            (4, Some(8400)),
            (5, Some(8300)),
        ])
    }

    /// A block file named by the 8-byte hash `raw` in `rank`, aged `secs`.
    fn block_file(cache: &Path, rank: &str, raw: u64, secs: u64) -> PathBuf {
        let name = format!("{raw:016x}");
        let leaf = cache
            .join(rank)
            .join(&name[..3])
            .join(format!("{}_g0", &name[3..5]));
        fs::create_dir_all(&leaf).expect("mkdir");
        let f = leaf.join(format!("{name}.bin"));
        fs::write(&f, b"x").expect("write");
        age(&f, secs);
        f
    }

    fn raw_stored(parent: Option<u64>, chain: &[u64]) -> KvEvent {
        KvEvent::Stored {
            parent: parent.map(BlockHash),
            hashes: chain.iter().copied().map(BlockHash).collect(),
            digests: chain.iter().map(|n| n.to_be_bytes().to_vec()).collect(),
            medium: Some("GPU".into()),
            locality: None,
        }
    }

    #[test]
    fn radix_steps_over_a_leaf_whose_file_is_already_gone() {
        // Block 4's file is gone though the index still has it on disk.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, Some(8998))]);
        let mut h = chain_harness(&cache, &[1, 2, 3, 4]);
        hot_minutes(&mut h, &cache, "0");
        h.shared.set_mode(Mode::Evicting);
        h.worker.round().expect("round");
        assert!(bins(&cache).is_empty(), "left: {:?}", bins(&cache));
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.vanished), 1);
        assert_eq!(Stats::get(&s.deleted_leaf), 2);
        assert_eq!(Stats::get(&s.deleted_root), 1);
        assert_eq!(Stats::get(&s.deleted_internal), 0);
        assert_eq!(Stats::get(&s.deleted_orphan), 0);
    }

    #[test]
    fn radix_keeps_a_shared_prompt_while_it_has_continuations() {
        let (_tmp, cache) = conversations();
        let mut h = radix_harness(&cache, "0");
        h.shared.set_to_free(2);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "the oldest blocks are the prompt, but its oldest conversations go instead"
        );
        assert_eq!(Stats::get(&chains(&h).stats.deleted_leaf), 2);
        assert_eq!(Stats::get(&chains(&h).stats.deleted_root), 0);
        h.shared.set_to_free(UNGRANTED);
        h.worker.round().expect("round");
        assert!(
            bins(&cache).is_empty(),
            "with one continuation left the prompt is part of its edge"
        );
        assert_eq!(Stats::get(&chains(&h).stats.deleted_internal), 0);
    }

    #[test]
    fn radix_deletes_edges_least_recently_used_first_whatever_the_atimes() {
        // 5 was stored last but has the oldest file; 3 the newest file.
        let (_tmp, cache) = bucket(&[
            (1, Some(9000)),
            (2, Some(8999)),
            (3, Some(7000)),
            (4, Some(8000)),
            (5, Some(8900)),
        ]);
        let mut h = radix_harness(&cache, "60");
        h.shared.set_to_free(2);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2, 5]),
            "the conversations stored first go first, young or not"
        );
        std::thread::sleep(Duration::from_millis(5));
        chains(&h).apply(&[stored(None, &[1])]);
        h.shared.set_to_free(1);
        h.worker.round().expect("round");
        assert_eq!(
            bins(&cache),
            names(&[1, 2]),
            "a use of the prompt is a use of its one remaining conversation"
        );
    }

    #[test]
    fn a_finished_session_goes_before_a_live_one_although_it_was_never_sampled() {
        // Live session 1-2-3 is in the worker's one bucket (abc) with the
        // oldest files; finished session f1-f2 was stored before the live
        // one and sits in a bucket (fed) this worker never samples.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (3, Some(8998))]);
        let finished = [0xfedc_b000_0000_0001, 0xfedc_b000_0000_0002];
        let files: Vec<PathBuf> = finished
            .iter()
            .map(|&raw| block_file(&cache, "m_abcdef012345_r0", raw, 100))
            .collect();
        let mut h = chain_harness(&cache, &[]);
        chains(&h).apply(&[raw_stored(None, &finished)]);
        std::thread::sleep(Duration::from_millis(5));
        chains(&h).apply(&[stored(None, &[1, 2, 3])]);
        h.worker.shard = Shard::split(16)[0xabc % 16];
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_to_free(2);
        h.worker.round().expect("round");
        assert!(
            files.iter().all(|f| !f.exists()),
            "the finished session went"
        );
        assert_eq!(bins(&cache), names(&[1, 2, 3]), "the live session is whole");
        assert_eq!(
            Stats::get(&h.stats.files_sampled),
            3,
            "only the live session's bucket was sampled"
        );
        let s = &chains(&h).stats;
        assert_eq!(Stats::get(&s.cascaded), 1, "f1-f2 went as one edge");
        assert_eq!(Stats::get(&s.deleted_internal), 0);
    }

    #[test]
    fn unordered_files_go_by_atime_among_the_edges() {
        // 1-2 is an edge stored now; 7 is a file the index never heard of,
        // older than the edge; 8 is one written after the edge.
        let (_tmp, cache) = bucket(&[(1, Some(9000)), (2, Some(8999)), (7, Some(9500))]);
        let mut h = chain_harness(&cache, &[1, 2]);
        hot_minutes(&mut h, &cache, "0");
        std::thread::sleep(Duration::from_millis(20));
        let late = cache
            .join("m_abcdef012345_r0/abc/de_g0")
            .join(&names(&[8])[0]);
        fs::write(&late, b"x").expect("write");
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_to_free(1);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[1, 2, 8]), "7 is older than the edge");
        h.shared.set_to_free(2);
        h.worker.round().expect("round");
        assert_eq!(bins(&cache), names(&[8]), "the edge is older than 8");
        h.shared.set_to_free(1);
        h.worker.round().expect("round");
        assert!(bins(&cache).is_empty());
        assert_eq!(Stats::get(&chains(&h).stats.deleted_untracked), 2);
    }

    #[test]
    fn an_edge_goes_from_every_rank_of_its_model_and_no_other_model() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        let chain = [0xabcd_e000_0000_0001u64, 0xabcd_e000_0000_0002];
        for rank in ["b_222_r0", "b_222_r1"] {
            for raw in chain {
                block_file(&cache, rank, raw, 9000);
            }
        }
        let other = block_file(&cache, "a_111_r0", 0x1234_5000_0000_0000, 60);
        let mut h = chain_harness(&cache, &[]);
        hot_minutes(&mut h, &cache, "60");
        chains(&h).apply(&[raw_stored(None, &chain)]);
        h.shared.set_mode(Mode::Evicting);
        run_rounds(&mut h, 3);
        assert_eq!(bins(&cache), vec!["1234500000000000.bin"]);
        assert!(other.exists());
        assert_eq!(Stats::get(&h.stats.files_deleted), 4);
        assert_eq!(Stats::get(&chains(&h).stats.deleted_leaf), 1);
        assert_eq!(Stats::get(&chains(&h).stats.deleted_root), 1);
        assert!(matches!(
            h.worker.locate("ffff000000000000.bin"),
            Located::Missing
        ));
    }

    #[test]
    fn a_block_found_nowhere_waits_for_a_fresh_listing_of_the_rank_dirs() {
        // 4 has no file under the rank dirs listed a while ago: maybe its
        // model's rank dir was created since.
        let (_tmp, cache) = bucket(&[(3, Some(9000))]);
        let mut h = chain_harness(&cache, &[3, 4]);
        hot_minutes(&mut h, &cache, "0");
        h.shared.set_mode(Mode::Evicting);
        h.shared.set_to_free(0);
        h.worker.round().expect("round");
        h.worker.index.refreshed_at = h
            .worker
            .index
            .refreshed_at
            .and_then(|t| t.checked_sub(Duration::from_secs(60)));
        h.shared.set_to_free(UNGRANTED);
        assert_eq!(h.worker.evict(100), 0);
        assert_eq!(Stats::get(&chains(&h).stats.vanished), 0);
        assert!(
            h.worker.index.refreshed_at.is_none(),
            "listed again next round"
        );
        h.worker.round().expect("round");
        assert_eq!(Stats::get(&chains(&h).stats.vanished), 1);
        assert!(bins(&cache).is_empty());
    }

    #[test]
    fn nothing_is_deleted_before_sampling_has_seen_the_layout() {
        let (_tmp, cache) = conversations();
        let mut h = radix_harness(&cache, "0");
        assert!(matches!(h.worker.locate(&names(&[3])[0]), Located::Unknown));
        assert_eq!(h.worker.evict(100), 0);
        assert_eq!(bins(&cache).len(), 5);
        assert_eq!(Stats::get(&chains(&h).stats.vanished), 0);
        h.worker.round().expect("round");
        assert!(
            bins(&cache).is_empty(),
            "the round's sample taught the group"
        );
    }
}
