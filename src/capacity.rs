//! Cache size estimated from bucket samples, for volumes whose `statvfs`
//! does not describe the volume (VAST before its first write, hostPath, emptyDir).
//!
//! ```text
//!   used ~= mean(block bytes per sampled bucket, last WINDOW buckets) x bucket count
//!
//!   workers ──(every bucket they sample while evicting)──┐
//!   sampler ──(one bucket per IDLE_SAMPLE_INTERVAL)──────┴─► Samples ──► controller
//! ```

use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::CapacityBytes;
use crate::fsops::{self, EntryKind};
use crate::layout::{block_files, discover_rank_dirs, is_bucket_name};
use crate::shutdown::Shutdown;
use crate::stats::Stats;

const WINDOW: usize = 64;
const MIN_SAMPLES: usize = 16;
const IDLE_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);
const BUCKET_LIST_REFRESH: Duration = Duration::from_secs(60);
const STATVFS_MISMATCH_FACTOR: u64 = 10;

/// Block files found in one bucket and their total size.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BucketSample {
    pub files: u64,
    pub bytes: u64,
}

#[derive(Debug, Default)]
struct Window {
    samples: VecDeque<BucketSample>,
    bucket_count: Option<u64>,
}

/// Recent bucket samples shared by the workers, the sampler and the controller.
#[derive(Debug, Default)]
pub struct Samples {
    inner: Mutex<Window>,
}

impl Samples {
    pub fn record(&self, sample: BucketSample) {
        if let Ok(mut w) = self.inner.lock() {
            w.samples.push_back(sample);
            if w.samples.len() > WINDOW {
                w.samples.pop_front();
            }
        }
    }

    pub fn set_bucket_count(&self, count: u64) {
        if let Ok(mut w) = self.inner.lock() {
            w.bucket_count = Some(count);
        }
    }

    fn len(&self) -> usize {
        self.inner.lock().map(|w| w.samples.len()).unwrap_or(0)
    }

    /// Estimated bytes of block files in the cache; `None` until the buckets
    /// have been counted and at least `MIN_SAMPLES` of them sampled.
    pub fn estimated_used_bytes(&self) -> Option<u64> {
        let w = self.inner.lock().ok()?;
        let count = w.bucket_count?;
        if count == 0 {
            return Some(0);
        }
        if w.samples.len() < MIN_SAMPLES {
            return None;
        }
        let total: u128 = w.samples.iter().map(|s| u128::from(s.bytes)).sum();
        let estimate = total.saturating_mul(u128::from(count)) / w.samples.len() as u128;
        Some(u64::try_from(estimate).unwrap_or(u64::MAX))
    }
}

/// True when `statvfs` reports more than 10x `CAPACITY_BYTES`, i.e. it is
/// describing something bigger than the volume.
pub fn statvfs_overreports(statvfs_total: u64, capacity: CapacityBytes) -> bool {
    statvfs_total > capacity.get().saturating_mul(STATVFS_MISMATCH_FACTOR)
}

/// Lists and stats one bucket without going through the op budget.
fn bucket_sample(bucket: &Path, stats: &Stats) -> io::Result<BucketSample> {
    let bucket_fd = fsops::open_dir(bucket)?;
    Stats::add(&stats.readdir_ops, 1);
    let leaves = fsops::list(&bucket_fd)?;
    let mut sample = BucketSample::default();
    for leaf in leaves.iter().filter(|e| e.kind == EntryKind::Dir) {
        let Ok(leaf_fd) = fsops::open_dir_at(&bucket_fd, &leaf.name) else {
            continue;
        };
        Stats::add(&stats.readdir_ops, 1);
        let Ok(files) = fsops::list(&leaf_fd) else {
            continue;
        };
        for (name, _) in block_files(&files) {
            Stats::add(&stats.stat_ops, 1);
            if let Ok(meta) = fsops::stat_at(&leaf_fd, name) {
                sample.files += 1;
                sample.bytes = sample.bytes.saturating_add(meta.size);
            }
        }
    }
    Ok(sample)
}

/// Keeps `Samples` fresh while workers are idle: counts buckets every
/// `BUCKET_LIST_REFRESH` and samples one random bucket per `IDLE_SAMPLE_INTERVAL`
/// (back to back until `MIN_SAMPLES` are in).
pub struct Sampler {
    pub cache: PathBuf,
    pub bucket_len: usize,
    pub samples: Arc<Samples>,
    pub stats: Arc<Stats>,
    pub shutdown: Arc<Shutdown>,
}

impl Sampler {
    fn list_buckets(&self) -> Vec<PathBuf> {
        let mut buckets = Vec::new();
        for rank in discover_rank_dirs(&self.cache) {
            Stats::add(&self.stats.readdir_ops, 1);
            let Ok(entries) = fsops::open_dir(&rank).and_then(|fd| fsops::list(&fd)) else {
                continue;
            };
            buckets.extend(
                entries
                    .iter()
                    .filter(|e| e.kind == EntryKind::Dir)
                    .filter_map(|e| e.name.to_str().ok())
                    .filter(|n| is_bucket_name(n, self.bucket_len))
                    .map(|n| rank.join(n)),
            );
        }
        buckets
    }

    pub fn run(self) {
        let mut buckets = Vec::new();
        let mut listed_at: Option<Instant> = None;
        while !self.shutdown.is_set() {
            if listed_at.is_none_or(|t| t.elapsed() >= BUCKET_LIST_REFRESH) {
                buckets = self.list_buckets();
                listed_at = Some(Instant::now());
                self.samples
                    .set_bucket_count(u64::try_from(buckets.len()).unwrap_or(u64::MAX));
            }
            let recorded = match fastrand::choice(&buckets) {
                Some(bucket) => match bucket_sample(bucket, &self.stats) {
                    Ok(sample) => {
                        self.samples.record(sample);
                        true
                    }
                    Err(e) => {
                        if fsops::is_not_found(&e) {
                            listed_at = None;
                        } else {
                            Stats::add(&self.stats.errors, 1);
                            tracing::debug!(bucket = %bucket.display(), error = %e, "bucket sample failed");
                        }
                        false
                    }
                },
                None => false,
            };
            let warming_up = recorded && self.samples.len() < MIN_SAMPLES;
            if !warming_up && self.shutdown.wait(IDLE_SAMPLE_INTERVAL) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::capacity::{
        BucketSample, MIN_SAMPLES, Sampler, Samples, WINDOW, bucket_sample, statvfs_overreports,
    };
    use crate::config::{CapacityBytes, Config};
    use crate::shutdown::Shutdown;
    use crate::stats::Stats;

    fn capacity(bytes: &str) -> CapacityBytes {
        let cfg = Config::from_lookup(|k| (k == "CAPACITY_BYTES").then(|| bytes.to_string()))
            .expect("config");
        cfg.capacity_bytes.expect("capacity set")
    }

    fn sample(files: u64, bytes: u64) -> BucketSample {
        BucketSample { files, bytes }
    }

    #[test]
    fn no_estimate_until_buckets_counted_and_enough_samples() {
        let s = Samples::default();
        for _ in 0..MIN_SAMPLES {
            s.record(sample(2, 200));
        }
        assert_eq!(s.estimated_used_bytes(), None, "bucket count unknown");

        let s = Samples::default();
        s.set_bucket_count(10);
        for _ in 0..MIN_SAMPLES - 1 {
            s.record(sample(2, 200));
        }
        assert_eq!(s.estimated_used_bytes(), None);
        s.record(sample(2, 200));
        assert_eq!(s.estimated_used_bytes(), Some(2000));
    }

    #[test]
    fn empty_cache_is_zero_without_samples() {
        let s = Samples::default();
        s.set_bucket_count(0);
        assert_eq!(s.estimated_used_bytes(), Some(0));
    }

    #[test]
    fn estimate_is_mean_bucket_bytes_times_bucket_count() {
        let s = Samples::default();
        s.set_bucket_count(4096);
        for i in 0..32u64 {
            s.record(sample(i % 3, (i % 4) * 1000));
        }
        // bytes cycle 0, 1000, 2000, 3000: mean 1500.
        assert_eq!(s.estimated_used_bytes(), Some(1500 * 4096));
    }

    #[test]
    fn window_forgets_old_samples() {
        let s = Samples::default();
        s.set_bucket_count(100);
        for _ in 0..WINDOW {
            s.record(sample(1, 1_000_000));
        }
        for _ in 0..WINDOW {
            s.record(sample(1, 10));
        }
        assert_eq!(s.estimated_used_bytes(), Some(1000));
    }

    #[test]
    fn huge_estimates_saturate() {
        let s = Samples::default();
        s.set_bucket_count(u64::MAX);
        for _ in 0..MIN_SAMPLES {
            s.record(sample(1, u64::MAX));
        }
        assert_eq!(s.estimated_used_bytes(), Some(u64::MAX));
    }

    #[test]
    fn statvfs_mismatch_needs_more_than_ten_times() {
        let cap = capacity("1000");
        assert!(!statvfs_overreports(10_000, cap));
        assert!(statvfs_overreports(10_001, cap));
        assert!(!statvfs_overreports(500, cap));
        assert!(!statvfs_overreports(
            u64::MAX,
            capacity(&u64::MAX.to_string())
        ));
    }

    fn write_block(cache: &Path, hex: &str, size: usize) {
        let leaf = cache.join(format!("m_abcdef012345_r0/{}/{}_g0", &hex[..3], &hex[3..5]));
        fs::create_dir_all(&leaf).expect("mkdir");
        fs::write(leaf.join(format!("{hex}.bin")), vec![0u8; size]).expect("write");
    }

    #[test]
    fn bucket_sample_counts_only_block_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path();
        write_block(cache, "abcde00000000001", 100);
        write_block(cache, "abcde00000000002", 250);
        write_block(cache, "abcff00000000003", 50);
        let leaf = cache.join("m_abcdef012345_r0/abc/de_g0");
        fs::write(leaf.join("abcde00000000004.bin_9.tmp"), vec![0u8; 999]).expect("write");
        fs::write(leaf.join("notes.txt"), vec![0u8; 999]).expect("write");
        fs::create_dir_all(cache.join("m_abcdef012345_r0/abc/00_g0")).expect("mkdir");

        let stats = Stats::default();
        let got = bucket_sample(&cache.join("m_abcdef012345_r0/abc"), &stats).expect("sample");
        assert_eq!(got, sample(3, 400));
        assert_eq!(Stats::get(&stats.stat_ops), 3);
        assert_eq!(Stats::get(&stats.readdir_ops), 4);
    }

    #[test]
    fn sampler_estimates_a_real_tree() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let cache = tmp.path().join("cache");
        // 64 buckets with 1, 2 or 3 blocks of 1000 bytes each: 128 blocks in total.
        for b in 0..64u64 {
            for f in 0..(1 + b % 3) {
                write_block(&cache, &format!("{:03x}{f:02x}{b:011x}", b * 7), 1000);
            }
        }
        let fs_files: usize = (0..64u64).map(|b| 1 + (b % 3) as usize).sum();
        let samples = Arc::new(Samples::default());
        let shutdown = Arc::new(Shutdown::default());
        let sampler = Sampler {
            cache,
            bucket_len: 3,
            samples: Arc::clone(&samples),
            stats: Arc::new(Stats::default()),
            shutdown: Arc::clone(&shutdown),
        };
        let handle = std::thread::spawn(move || sampler.run());
        let start = Instant::now();
        let estimate = loop {
            if let Some(e) = samples.estimated_used_bytes() {
                break e;
            }
            assert!(start.elapsed() < Duration::from_secs(5), "no estimate");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            start.elapsed() < Duration::from_millis(900),
            "warm-up samples back to back"
        );
        shutdown.trigger();
        handle.join().expect("join");
        let actual = fs_files as f64 * 1000.0;
        assert!(
            (estimate as f64 - actual).abs() < actual * 0.5,
            "estimate {estimate} vs actual {actual}"
        );
    }

    #[test]
    fn sampler_reports_empty_cache_as_zero_and_stops_on_shutdown() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let samples = Arc::new(Samples::default());
        let shutdown = Arc::new(Shutdown::default());
        let sampler = Sampler {
            cache: tmp.path().join("missing"),
            bucket_len: 3,
            samples: Arc::clone(&samples),
            stats: Arc::new(Stats::default()),
            shutdown: Arc::clone(&shutdown),
        };
        let handle = std::thread::spawn(move || sampler.run());
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(samples.estimated_used_bytes(), Some(0));
        let start = Instant::now();
        shutdown.trigger();
        handle.join().expect("join");
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
