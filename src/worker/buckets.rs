//! A worker's share of the hex buckets: listing them, sampling their block
//! files into the pool, and removing directories left empty.

use std::ffi::{CStr, CString};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use crate::budget::OpKind;
use crate::capacity::BucketSample;
use crate::fsops::{self, EntryKind, Meta};
use crate::layout::{block_files, discover_rank_dirs, model_base_dir};
use crate::stats::Stats;
use crate::worker::Worker;
use crate::worker::pool::Candidate;

#[derive(Debug)]
pub(crate) struct RankDir {
    pub(crate) path: PathBuf,
    pub(crate) model_base: Option<PathBuf>,
}

/// The shard's buckets, sampled in passes: each pass visits every bucket once
/// in a fresh random order.
#[derive(Debug, Default)]
pub(crate) struct BucketIndex {
    pub(crate) buckets: Vec<(Arc<RankDir>, CString)>,
    /// Every rank dir found, including ones with no bucket in this shard.
    pub(crate) ranks: Vec<Arc<RankDir>>,
    pub(crate) refreshed_at: Option<Instant>,
    /// Indices into `buckets` for the current pass, and how many are visited.
    pub(crate) pass: Vec<usize>,
    pub(crate) visited: usize,
}

impl BucketIndex {
    pub(crate) fn next(&mut self) -> Option<(Arc<RankDir>, CString)> {
        if self.visited >= self.pass.len() {
            self.pass = (0..self.buckets.len()).collect();
            fastrand::shuffle(&mut self.pass);
            self.visited = 0;
        }
        let i = *self.pass.get(self.visited)?;
        self.visited += 1;
        self.buckets.get(i).cloned()
    }

    pub(crate) fn pass_done(&self) -> bool {
        self.visited >= self.pass.len()
    }
}

impl Worker {
    pub(crate) fn refresh_index(&mut self) {
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
            ..BucketIndex::default()
        };
    }

    /// Stats every block in `bucket` into the pool and, if the walk finished,
    /// records the bucket's size for the usage estimate. Returns files sampled.
    pub(crate) fn sample_bucket(
        &mut self,
        rank: &Arc<RankDir>,
        bucket: &CStr,
    ) -> io::Result<usize> {
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

    /// Other rank dirs of `rank`'s model. Tensor-parallel ranks each hold a
    /// shard of every block, and a block is useless once any shard is gone.
    pub(crate) fn sibling_ranks(&self, rank: &RankDir) -> Vec<Arc<RankDir>> {
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

    /// Removes `dir` if it is empty and unchanged for `DIR_CLEANUP_TTL_SECONDS`.
    pub(crate) fn reap_if_stale(&self, dir: &Path) {
        let Some(ttl) = self.ctx.config.dir_cleanup else {
            return;
        };
        if !self.acquire() {
            return;
        }
        let Ok(meta) = self.timed(OpKind::Stat, || fsops::stat_path(dir)) else {
            return;
        };
        let age = SystemTime::now()
            .duration_since(meta.mtime)
            .unwrap_or(Duration::ZERO);
        if age < ttl || !self.acquire() {
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

    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    use std::time::{Duration, Instant};

    use crate::capacity::BucketSample;

    use crate::controller::Mode;
    use crate::layout::Shard;

    use crate::stats::Stats;
    use crate::worker::buckets::{BucketIndex, RankDir};

    use crate::worker::testing::{age, bins, config, fixture, harness, run_rounds};

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
    fn a_pass_visits_every_bucket_once_before_repeating() {
        use std::collections::HashSet;
        use std::ffi::CString;

        let rank = Arc::new(RankDir {
            path: PathBuf::from("/r"),
            model_base: None,
        });
        let mut index = BucketIndex {
            buckets: (0..9)
                .map(|i| {
                    (
                        Arc::clone(&rank),
                        CString::new(format!("{i:03x}")).expect("name"),
                    )
                })
                .collect(),
            ..BucketIndex::default()
        };
        for _ in 0..3 {
            let pass: HashSet<CString> = (0..9).map(|_| index.next().expect("bucket").1).collect();
            assert_eq!(pass.len(), 9, "each bucket once per pass");
            assert!(index.pass_done());
        }
        assert!(BucketIndex::default().next().is_none());
    }
}
