use std::sync::atomic::{AtomicU64, Ordering};

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
    /// Ops by the `CAPACITY_BYTES` sampler, which bypass the budget.
    pub sampler_ops: AtomicU64,
    pub errors: AtomicU64,
}

impl Stats {
    /// Metadata ops issued through the budget.
    pub fn budgeted_ops(&self) -> u64 {
        [
            &self.readdir_ops,
            &self.stat_ops,
            &self.unlink_ops,
            &self.rmdir_ops,
        ]
        .into_iter()
        .map(Self::get)
        .sum()
    }

    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }
}
