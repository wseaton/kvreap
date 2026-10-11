//! Unlinking block files: one sampled candidate, or many with a bounded
//! number in flight, all paced by the shared op budget.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crate::budget::OpKind;
use crate::chains::Gone;
use crate::config::DeleteMode;
use crate::fsops;
use crate::layout::BlockHash;
use crate::stats::Stats;
use crate::worker::buckets::RankDir;
use crate::worker::pool::Candidate;
use crate::worker::{Context, RECHECK_AFTER, Removed, Worker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Unlink {
    Deleted,
    /// The file was already gone.
    Missing,
    /// Not attempted (grant spent, shutdown) or failed.
    Skipped,
}

/// Outcome of unlinking one block's file in every rank dir.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Named {
    pub(crate) deleted: usize,
    pub(crate) missing: usize,
    pub(crate) skipped: usize,
}

impl Named {
    pub(crate) fn of(results: &[Unlink]) -> Self {
        let mut out = Self::default();
        for r in results {
            match r {
                Unlink::Deleted => out.deleted += 1,
                Unlink::Missing => out.missing += 1,
                Unlink::Skipped => out.skipped += 1,
            }
        }
        out
    }

    /// What became of the block: deleted from some rank, already gone from
    /// every rank, or `None` while a file of it may remain.
    pub(crate) fn gone(self) -> Option<Gone> {
        if self.deleted > 0 {
            Some(Gone::Deleted)
        } else if self.skipped == 0 && self.missing > 0 {
            Some(Gone::Vanished)
        } else {
            None
        }
    }
}

/// One block file to unlink.
#[derive(Debug)]
pub(crate) struct Target {
    pub(crate) path: PathBuf,
    pub(crate) hash: BlockHash,
    pub(crate) size: u64,
    pub(crate) rank: Arc<RankDir>,
}

/// `g0` from `.../<hh>_g0/<hash>.bin`.
pub(crate) fn leaf_group(block: &Path) -> Option<String> {
    let leaf = block.parent()?.file_name()?.to_str()?;
    leaf.rsplit_once("_g").map(|(_, g)| g.to_owned())
}

/// `<rank>/<hhh>/<hh>_g<group>/<name>` for a block file `name`.
pub(crate) fn block_path(
    rank: &Path,
    name: &str,
    bucket_len: usize,
    group: &str,
) -> Option<PathBuf> {
    let bucket = name.get(..bucket_len)?;
    let leaf = name.get(bucket_len..bucket_len + 2)?;
    Some(
        rank.join(bucket)
            .join(format!("{leaf}_g{group}"))
            .join(name),
    )
}

impl Context {
    pub(crate) fn acquire(&self) -> bool {
        self.budget
            .acquire(&self.shutdown, self.shared.mode().pacing())
    }

    pub(crate) fn timed<T>(
        &self,
        kind: OpKind,
        f: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let start = Instant::now();
        let out = f();
        let elapsed = start.elapsed();
        self.budget.observe(kind, elapsed);
        self.stats.timings.op(kind).observe(elapsed.as_secs_f64());
        let counter = match kind {
            OpKind::Readdir => &self.stats.readdir_ops,
            OpKind::Stat => &self.stats.stat_ops,
            OpKind::Unlink => &self.stats.unlink_ops,
        };
        Stats::add(counter, 1);
        out
    }

    /// Unlinks one block file and counts it as freed and as a removal event.
    pub(crate) fn unlink(&self, t: &Target) -> Unlink {
        if self.config.delete_mode == DeleteMode::DryRun {
            tracing::debug!(path = %t.path.display(), "[DRY RUN] would delete");
            Stats::add(&self.stats.files_deleted, 1);
            self.shared.freed(t.size);
            return Unlink::Deleted;
        }
        if !self.acquire() || !self.shared.should_delete() {
            return Unlink::Skipped;
        }
        match self.timed(OpKind::Unlink, || fsops::unlink_path(&t.path)) {
            Ok(()) => {}
            Err(e) if fsops::is_not_found(&e) => return Unlink::Missing,
            Err(e) => {
                Stats::add(&self.stats.errors, 1);
                tracing::warn!(path = %t.path.display(), error = %e, "unlink failed");
                return Unlink::Skipped;
            }
        }
        Stats::add(&self.stats.files_deleted, 1);
        Stats::add(&self.stats.bytes_freed, t.size);
        self.shared.freed(t.size);
        if let (Some(tx), Some(base)) = (&self.events, &t.rank.model_base) {
            let _ = tx.send(Removed {
                model_base: base.clone(),
                hash: t.hash,
            });
        }
        Unlink::Deleted
    }

    /// Unlinks `targets` in order with up to `DELETE_FANOUT` in flight, and
    /// issues no new unlink once one is skipped. Unissued targets come back
    /// `Skipped`.
    pub(crate) fn unlink_all(&self, targets: &[Target]) -> Vec<Unlink> {
        let lanes = self.config.delete_fanout.get().min(targets.len());
        let next = AtomicUsize::new(0);
        let stop = AtomicBool::new(false);
        let results: Vec<OnceLock<Unlink>> = targets.iter().map(|_| OnceLock::new()).collect();
        std::thread::scope(|scope| {
            for _ in 0..lanes {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let (Some(t), Some(slot)) = (targets.get(i), results.get(i)) else {
                            break;
                        };
                        let r = self.unlink(t);
                        if r == Unlink::Skipped {
                            stop.store(true, Ordering::Relaxed);
                        }
                        let _ = slot.set(r);
                    }
                });
            }
        });
        results
            .into_iter()
            .map(|r| r.into_inner().unwrap_or(Unlink::Skipped))
            .collect()
    }
}

impl Worker {
    /// Unlinks each block's file `name` in every one of `ranks`, in order with
    /// up to `DELETE_FANOUT` in flight, and returns what became of each block.
    pub(crate) fn unlink_blocks(
        &self,
        ranks: &[Arc<RankDir>],
        group: &str,
        size: u64,
        blocks: &[(BlockHash, String)],
    ) -> Vec<(BlockHash, Named)> {
        let mut targets = Vec::new();
        let mut spans = Vec::new();
        for (hash, name) in blocks {
            let start = targets.len();
            targets.extend(self.targets(ranks, name, group, *hash, size));
            spans.push((*hash, start..targets.len()));
        }
        let results = self.ctx.unlink_all(&targets);
        spans
            .into_iter()
            .map(|(hash, span)| (hash, Named::of(results.get(span).unwrap_or_default())))
            .collect()
    }

    /// The paths of block file `name` in each of `ranks`.
    pub(crate) fn targets(
        &self,
        ranks: &[Arc<RankDir>],
        name: &str,
        group: &str,
        hash: BlockHash,
        size: u64,
    ) -> Vec<Target> {
        let bucket_len = self.ctx.config.hex_bucket_len;
        ranks
            .iter()
            .filter_map(|r| {
                Some(Target {
                    path: block_path(&r.path, name, bucket_len, group)?,
                    hash,
                    size,
                    rank: Arc::clone(r),
                })
            })
            .collect()
    }

    pub(crate) fn evict_one(&mut self, mut c: Candidate) -> bool {
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
        self.remove_block(&c) == Unlink::Deleted
    }

    /// Unlinks one sampled block file and marks it gone in the chain index.
    pub(crate) fn remove_block(&self, c: &Candidate) -> Unlink {
        let target = Target {
            path: c.path.clone(),
            hash: c.hash,
            size: c.size,
            rank: Arc::clone(&c.rank),
        };
        let r = self.ctx.unlink(&target);
        if let Some(chains) = &self.ctx.chains {
            match r {
                Unlink::Deleted => {
                    chains.deleted(c.hash);
                }
                Unlink::Missing => chains.vanished(c.hash),
                Unlink::Skipped => {}
            }
        }
        r
    }
}

#[cfg(test)]
mod tests {

    use std::fs;
    use std::path::Path;
    use std::sync::Arc;

    use crate::controller::Mode;
    use crate::layout::BlockHash;

    use crate::stats::Stats;
    use crate::worker::buckets::RankDir;

    use crate::worker::testing::{config, harness};
    use crate::worker::unlink::{Target, Unlink};

    #[test]
    fn block_path_rebuilds_the_fs_tier_layout() {
        use std::path::Path;

        use crate::worker::unlink::{block_path, leaf_group};

        let block = Path::new("/c/m_abc_r0/abc/de_g3/abcdef.bin");
        assert_eq!(leaf_group(block).as_deref(), Some("3"));
        assert_eq!(leaf_group(Path::new("/c/m/abc/de/x.bin")), None);
        assert_eq!(
            block_path(Path::new("/c/m_abc_r0"), "0123ff.bin", 3, "3"),
            Some(Path::new("/c/m_abc_r0/012/3f_g3/0123ff.bin").to_path_buf())
        );
        assert_eq!(block_path(Path::new("/r"), "ab", 3, "0"), None);
    }

    fn fanout_targets(dir: &Path, n: usize, missing: &[usize]) -> Vec<Target> {
        let rank = Arc::new(RankDir {
            path: dir.to_path_buf(),
            model_base: None,
        });
        (0..n)
            .map(|i| {
                let path = dir.join(format!("{i:04}.bin"));
                if !missing.contains(&i) {
                    fs::write(&path, b"x").expect("write");
                }
                Target {
                    path,
                    hash: BlockHash(i as u64),
                    size: 100,
                    rank: Arc::clone(&rank),
                }
            })
            .collect()
    }

    #[test]
    fn unlink_all_deletes_in_parallel_and_reports_each_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let h = harness(config(tmp.path(), &[("DELETE_FANOUT", "8")]));
        h.shared.set_mode(Mode::Evicting);
        let targets = fanout_targets(tmp.path(), 40, &[3, 17]);
        let results = h.worker.ctx.unlink_all(&targets);
        for (i, r) in results.iter().enumerate() {
            let want = if [3, 17].contains(&i) {
                Unlink::Missing
            } else {
                Unlink::Deleted
            };
            assert_eq!(*r, want, "target {i}");
        }
        assert!(targets.iter().all(|t| !t.path.exists()));
        assert_eq!(Stats::get(&h.stats.files_deleted), 38);
    }

    #[test]
    fn unlink_all_stops_issuing_once_the_grant_is_spent() {
        for (fanout, extra) in [("1", 0), ("4", 3)] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let h = harness(config(tmp.path(), &[("DELETE_FANOUT", fanout)]));
            h.shared.set_mode(Mode::Evicting);
            h.shared.set_to_free(300);
            let targets = fanout_targets(tmp.path(), 40, &[]);
            let results = h.worker.ctx.unlink_all(&targets);
            let deleted = results.iter().filter(|r| **r == Unlink::Deleted).count();
            assert!(
                (3..=3 + extra).contains(&deleted),
                "fanout {fanout}: {deleted} deleted for a 3-file grant"
            );
            if fanout == "1" {
                assert_eq!(&results[..3], &[Unlink::Deleted; 3]);
                assert!(results[3..].iter().all(|r| *r == Unlink::Skipped));
            }
            let left = targets.iter().filter(|t| t.path.exists()).count();
            assert_eq!(left, 40 - deleted, "fanout {fanout}");
        }
    }
}
