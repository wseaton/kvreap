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
use crate::worker::{Context, RECHECK_AFTER, Worker};

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
    pub(crate) size: u64,
}

/// `0` from the leaf dir name `<hh>_g0`.
pub(crate) fn group_of(leaf_dir: &str) -> Option<&str> {
    leaf_dir.rsplit_once("_g").map(|(_, g)| g)
}

/// Where a block's files are: one file per rank dir of a model, in one group.
#[derive(Debug, Clone)]
pub(crate) struct Place {
    pub(crate) ranks: Vec<Arc<RankDir>>,
    pub(crate) group: String,
    pub(crate) size: u64,
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

    /// Unlinks one block file against the delete grant and counts it as freed.
    pub(crate) fn unlink(&self, t: &Target) -> Unlink {
        if !self.shared.reserve(t.size) {
            return Unlink::Skipped;
        }
        self.unlink_reserved(t)
    }

    /// Unlinks a file whose bytes are already reserved against the grant,
    /// and returns them when nothing was deleted.
    fn unlink_reserved(&self, t: &Target) -> Unlink {
        if self.config.delete_mode == DeleteMode::DryRun {
            tracing::debug!(path = %t.path.display(), "[DRY RUN] would delete");
            Stats::add(&self.stats.files_deleted, 1);
            self.shared.freed(t.size);
            return Unlink::Deleted;
        }
        if !self.acquire() {
            self.shared.release(t.size);
            return Unlink::Skipped;
        }
        match self.timed(OpKind::Unlink, || fsops::unlink_path(&t.path)) {
            Ok(()) => {}
            Err(e) if fsops::is_not_found(&e) => {
                self.shared.release(t.size);
                return Unlink::Missing;
            }
            Err(e) => {
                self.shared.release(t.size);
                Stats::add(&self.stats.errors, 1);
                tracing::warn!(path = %t.path.display(), error = %e, "unlink failed");
                return Unlink::Skipped;
            }
        }
        Stats::add(&self.stats.files_deleted, 1);
        Stats::add(&self.stats.bytes_freed, t.size);
        self.shared.freed(t.size);
        Unlink::Deleted
    }

    /// Reserves `targets`' bytes in order until the grant runs out, then
    /// unlinks the reserved ones with up to `DELETE_FANOUT` in flight, issuing
    /// no new unlink once one is skipped. So a chain passed leaf first loses
    /// a leaf-side run of blocks, never blocks behind one it kept. Targets
    /// not unlinked come back `Skipped`.
    pub(crate) fn unlink_all(&self, targets: &[Target]) -> Vec<Unlink> {
        let reserved = targets
            .iter()
            .take_while(|t| self.shared.reserve(t.size))
            .count();
        let lanes = self.config.delete_fanout.get().min(reserved);
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
                        if i >= reserved {
                            break;
                        }
                        let r = self.unlink_reserved(t);
                        if r == Unlink::Skipped {
                            stop.store(true, Ordering::Relaxed);
                        }
                        let _ = slot.set(r);
                    }
                });
            }
        });
        targets
            .iter()
            .zip(results)
            .enumerate()
            .map(|(i, (t, r))| {
                r.into_inner().unwrap_or_else(|| {
                    if i < reserved {
                        self.shared.release(t.size);
                    }
                    Unlink::Skipped
                })
            })
            .collect()
    }
}

impl Worker {
    /// Unlinks each block's file `name` in every rank dir of every place, in
    /// order with up to `DELETE_FANOUT` in flight, and returns what became of
    /// each block.
    pub(crate) fn unlink_blocks(
        &self,
        places: &[Place],
        blocks: &[(BlockHash, String)],
    ) -> Vec<(BlockHash, Named)> {
        let bucket_len = self.ctx.config.hex_bucket_len;
        let mut targets = Vec::new();
        let mut spans = Vec::new();
        for (hash, name) in blocks {
            let start = targets.len();
            for place in places {
                targets.extend(place.ranks.iter().filter_map(|r| {
                    Some(Target {
                        path: block_path(&r.path, name, bucket_len, &place.group)?,
                        size: place.size,
                    })
                }));
            }
            spans.push((*hash, start..targets.len()));
        }
        let results = self.ctx.unlink_all(&targets);
        spans
            .into_iter()
            .map(|(hash, span)| (hash, Named::of(results.get(span).unwrap_or_default())))
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
            size: c.size,
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

    use crate::controller::Mode;
    use crate::stats::Stats;
    use crate::worker::testing::{config, harness};
    use crate::worker::unlink::{Target, Unlink};

    #[test]
    fn block_path_rebuilds_the_fs_tier_layout() {
        use std::path::Path;

        use crate::worker::unlink::{block_path, group_of};

        assert_eq!(group_of("de_g3"), Some("3"));
        assert_eq!(group_of("de_g12"), Some("12"));
        assert_eq!(group_of("de"), None);
        assert_eq!(
            block_path(Path::new("/c/m_abc_r0"), "0123ff.bin", 3, "3"),
            Some(Path::new("/c/m_abc_r0/012/3f_g3/0123ff.bin").to_path_buf())
        );
        assert_eq!(block_path(Path::new("/r"), "ab", 3, "0"), None);
    }

    fn fanout_targets(dir: &Path, n: usize, missing: &[usize]) -> Vec<Target> {
        (0..n)
            .map(|i| {
                let path = dir.join(format!("{i:04}.bin"));
                if !missing.contains(&i) {
                    fs::write(&path, b"x").expect("write");
                }
                Target { path, size: 100 }
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
    fn unlink_all_deletes_exactly_the_leading_targets_the_grant_covers() {
        for fanout in ["1", "4", "16"] {
            let tmp = tempfile::tempdir().expect("tempdir");
            let h = harness(config(tmp.path(), &[("DELETE_FANOUT", fanout)]));
            h.shared.set_mode(Mode::Evicting);
            h.shared.set_to_free(300);
            let targets = fanout_targets(tmp.path(), 40, &[1]);
            let results = h.worker.ctx.unlink_all(&targets);
            assert_eq!(
                &results[..3],
                &[Unlink::Deleted, Unlink::Missing, Unlink::Deleted],
                "fanout {fanout}"
            );
            assert!(
                results[3..].iter().all(|r| *r == Unlink::Skipped),
                "fanout {fanout}: no block behind the grant's end goes: {results:?}"
            );
            assert_eq!(
                h.shared.to_free(),
                100,
                "fanout {fanout}: the missing file's bytes go back to the grant"
            );
            let left = targets.iter().filter(|t| t.path.exists()).count();
            assert_eq!(left, 37, "fanout {fanout}");
        }
    }
}
