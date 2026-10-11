use std::sync::atomic::{AtomicU64, Ordering};

use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};

use crate::budget::OpKind;

#[derive(Debug, Default)]
pub struct Stats {
    pub files_sampled: AtomicU64,
    pub files_skipped_hot: AtomicU64,
    pub files_deleted: AtomicU64,
    pub bytes_freed: AtomicU64,
    pub dirs_removed: AtomicU64,
    pub readdir_ops: AtomicU64,
    pub stat_ops: AtomicU64,
    pub unlink_ops: AtomicU64,
    pub rmdir_ops: AtomicU64,
    pub errors: AtomicU64,
    pub timings: Timings,
}

impl Stats {
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}

/// Latency distributions, in seconds.
#[derive(Debug)]
pub struct Timings {
    readdir: Histogram,
    stat: Histogram,
    unlink: Histogram,
    /// From entering the cleanup band to reaching the target.
    pub prune: Histogram,
}

impl Default for Timings {
    fn default() -> Self {
        let op = || Histogram::new(exponential_buckets(0.000_1, 2.0, 16));
        Self {
            readdir: op(),
            stat: op(),
            unlink: op(),
            prune: Histogram::new(exponential_buckets(1.0, 2.0, 12)),
        }
    }
}

impl Timings {
    pub fn op(&self, kind: OpKind) -> &Histogram {
        match kind {
            OpKind::Readdir => &self.readdir,
            OpKind::Stat => &self.stat,
            OpKind::Unlink => &self.unlink,
        }
    }
}
