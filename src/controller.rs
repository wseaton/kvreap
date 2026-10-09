use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use crate::capacity::Samples;
use crate::config::{CapacityBytes, Percent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Idle,
    Evicting,
    /// Usage is close enough to full that pacing is dropped.
    Emergency,
}

impl Mode {
    fn to_u8(self) -> u8 {
        match self {
            Self::Idle => 0,
            Self::Evicting => 1,
            Self::Emergency => 2,
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Evicting,
            2 => Self::Emergency,
            _ => Self::Idle,
        }
    }

    pub fn is_evicting(self) -> bool {
        self != Self::Idle
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskUsage {
    pub total_bytes: u64,
    pub used_bytes: u64,
}

impl DiskUsage {
    pub fn percent(self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 / self.total_bytes as f64 * 100.0
        }
    }
}

/// Used = total - free (not - available), matching the Python activator.
pub fn disk_usage(path: &Path) -> io::Result<DiskUsage> {
    let st = rustix::fs::statvfs(path)?;
    let total = st.f_blocks.saturating_mul(st.f_frsize);
    let free = st.f_bfree.saturating_mul(st.f_frsize);
    Ok(DiskUsage {
        total_bytes: total,
        used_bytes: total.saturating_sub(free),
    })
}

/// Where the controller gets used and total bytes from. `statvfs` runs every
/// poll either way: it is also the liveness signal for the mount.
#[derive(Debug, Clone)]
pub enum UsageSource {
    Statvfs,
    /// Sampled cache size against a configured capacity (`CAPACITY_BYTES`).
    Sampled {
        capacity: CapacityBytes,
        samples: Arc<Samples>,
    },
}

impl UsageSource {
    /// `None` while a sampled estimate is not available yet.
    pub fn read(&self, statvfs: DiskUsage) -> Option<DiskUsage> {
        match self {
            Self::Statvfs => Some(statvfs),
            Self::Sampled { capacity, samples } => {
                samples.estimated_used_bytes().map(|used| DiskUsage {
                    total_bytes: capacity.get(),
                    used_bytes: used,
                })
            }
        }
    }
}

/// Starts evicting at `cleanup`, stops at `target`, and drops pacing at or above `emergency`.
#[derive(Debug, Clone, Copy)]
pub struct Hysteresis {
    cleanup: f64,
    target: f64,
    emergency: f64,
}

impl Hysteresis {
    pub fn new(cleanup: Percent, target: Percent, emergency: Percent) -> Self {
        Self {
            cleanup: cleanup.get(),
            target: target.get(),
            emergency: emergency.get(),
        }
    }

    pub fn next(&self, current: Mode, usage: f64) -> Mode {
        if usage >= self.emergency {
            return Mode::Emergency;
        }
        match current {
            Mode::Idle if usage >= self.cleanup => Mode::Evicting,
            Mode::Idle => Mode::Idle,
            Mode::Evicting | Mode::Emergency if usage <= self.target => Mode::Idle,
            Mode::Evicting | Mode::Emergency => Mode::Evicting,
        }
    }
}

/// Time a prune should take to bring usage from where it started down to target.
pub const PRUNE_HORIZON: Duration = Duration::from_secs(60);
/// Two statx per delete (half of what is sampled is evicted) plus the unlink.
const DEFAULT_OPS_PER_DELETE: f64 = 3.0;
const MIN_DELETES_FOR_RATIO: u64 = 20;

/// Budgeted metadata ops and deletes so far, from `Stats`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpCounts {
    pub ops: u64,
    pub deletes: u64,
}

/// Delete pace for the current prune.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Plan {
    pub files_per_sec: f64,
    pub ops_per_sec: f64,
}

#[derive(Debug, Clone, Copy)]
struct Episode {
    excess_bytes: f64,
    counts: OpCounts,
}

/// Paces a prune to need: the delete rate that brings usage to target in
/// `PRUNE_HORIZON` at a steady pace.
///
/// ```text
///   bytes/s = max(excess now, excess at start) / PRUNE_HORIZON
///   files/s = bytes/s / mean sampled file size
///   ops/s   = files/s x ops per delete (measured this prune, 3 until 20 deletes)
/// ```
#[derive(Debug, Clone)]
pub struct Pacer {
    target: f64,
    episode: Option<Episode>,
}

impl Pacer {
    pub fn new(target: Percent) -> Self {
        Self {
            target: target.get(),
            episode: None,
        }
    }

    /// Ends the current prune; the next `plan` starts a new one.
    pub fn stop(&mut self) {
        self.episode = None;
    }

    /// `None` until a block file size has been sampled.
    pub fn plan(
        &mut self,
        usage: DiskUsage,
        mean_file_size: Option<f64>,
        counts: OpCounts,
    ) -> Option<Plan> {
        let target_bytes = usage.total_bytes as f64 * self.target / 100.0;
        let excess = (usage.used_bytes as f64 - target_bytes).max(0.0);
        let start = *self.episode.get_or_insert(Episode {
            excess_bytes: excess,
            counts,
        });
        let file_size = mean_file_size.filter(|s| *s > 0.0)?;
        let files_per_sec =
            excess.max(start.excess_bytes) / PRUNE_HORIZON.as_secs_f64() / file_size;
        let deletes = counts.deletes.saturating_sub(start.counts.deletes);
        let ops_per_delete = if deletes >= MIN_DELETES_FOR_RATIO {
            counts.ops.saturating_sub(start.counts.ops) as f64 / deletes as f64
        } else {
            DEFAULT_OPS_PER_DELETE
        };
        Some(Plan {
            files_per_sec,
            ops_per_sec: files_per_sec * ops_per_delete,
        })
    }
}

/// Mode and usage published by the controller thread for workers to read.
#[derive(Debug, Default)]
pub struct SharedState {
    mode: AtomicU8,
    usage_bits: AtomicU64,
}

impl SharedState {
    pub fn mode(&self) -> Mode {
        Mode::from_u8(self.mode.load(Ordering::Acquire))
    }

    pub fn set_mode(&self, mode: Mode) {
        self.mode.store(mode.to_u8(), Ordering::Release);
    }

    pub fn usage(&self) -> f64 {
        f64::from_bits(self.usage_bits.load(Ordering::Relaxed))
    }

    pub fn set_usage(&self, usage: f64) {
        self.usage_bits.store(usage.to_bits(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::capacity::{BucketSample, Samples};
    use crate::config::{Config, Percent};
    use crate::controller::{
        DiskUsage, Hysteresis, Mode, OpCounts, Pacer, Plan, SharedState, UsageSource, disk_usage,
    };

    fn h(cleanup: f64, target: f64) -> Hysteresis {
        let c = Config::from_lookup(|k| match k {
            "CLEANUP_THRESHOLD" => Some(cleanup.to_string()),
            "TARGET_THRESHOLD" => Some(target.to_string()),
            _ => None,
        })
        .expect("config");
        Hysteresis::new(
            c.cleanup_threshold,
            c.target_threshold,
            c.emergency_threshold,
        )
    }

    #[test]
    fn starts_at_cleanup_and_stops_at_target() {
        let h = h(85.0, 70.0);
        assert_eq!(h.next(Mode::Idle, 84.9), Mode::Idle);
        assert_eq!(h.next(Mode::Idle, 85.0), Mode::Evicting);
        assert_eq!(h.next(Mode::Evicting, 80.0), Mode::Evicting);
        assert_eq!(h.next(Mode::Evicting, 70.1), Mode::Evicting);
        assert_eq!(h.next(Mode::Evicting, 70.0), Mode::Idle);
        assert_eq!(h.next(Mode::Idle, 80.0), Mode::Idle);
    }

    #[test]
    fn emergency_band_and_recovery() {
        let h = h(85.0, 70.0);
        assert_eq!(h.next(Mode::Idle, 97.0), Mode::Emergency);
        assert_eq!(h.next(Mode::Evicting, 99.0), Mode::Emergency);
        assert_eq!(h.next(Mode::Emergency, 96.9), Mode::Evicting);
        assert_eq!(h.next(Mode::Emergency, 60.0), Mode::Idle);
    }

    #[test]
    fn configured_emergency_band() {
        let pct = |v: f64| Percent::new("p", v).expect("pct");
        let h = Hysteresis::new(pct(85.0), pct(70.0), pct(90.0));
        assert_eq!(h.next(Mode::Idle, 89.9), Mode::Evicting);
        assert_eq!(h.next(Mode::Idle, 90.0), Mode::Emergency);
        assert_eq!(h.next(Mode::Emergency, 89.0), Mode::Evicting);
        assert_eq!(h.next(Mode::Emergency, 70.0), Mode::Idle);

        let h = Hysteresis::new(pct(85.0), pct(70.0), pct(85.0));
        assert_eq!(h.next(Mode::Idle, 84.9), Mode::Idle);
        assert_eq!(h.next(Mode::Idle, 85.0), Mode::Emergency, "no paced band");
    }

    #[test]
    fn emergency_never_below_cleanup() {
        let h = h(98.0, 90.0);
        assert_eq!(h.next(Mode::Idle, 97.5), Mode::Idle);
        assert_eq!(h.next(Mode::Idle, 98.0), Mode::Emergency);
    }

    #[test]
    fn usage_percent() {
        let u = DiskUsage {
            total_bytes: 200,
            used_bytes: 170,
        };
        assert_eq!(u.percent(), 85.0);
        assert_eq!(
            DiskUsage {
                total_bytes: 0,
                used_bytes: 0
            }
            .percent(),
            0.0
        );
    }

    #[test]
    fn statvfs_on_real_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let u = disk_usage(tmp.path()).expect("statvfs");
        assert!(u.total_bytes > 0);
        assert!(u.used_bytes <= u.total_bytes);
    }

    #[test]
    fn statvfs_source_passes_statvfs_through() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let st = disk_usage(tmp.path()).expect("statvfs");
        assert_eq!(UsageSource::Statvfs.read(st), Some(st));
    }

    #[test]
    fn sampled_source_uses_capacity_and_estimate() {
        let capacity = Config::from_lookup(|k| (k == "CAPACITY_BYTES").then(|| "4000".into()))
            .expect("config")
            .capacity_bytes
            .expect("capacity");
        let samples = Arc::new(Samples::default());
        let source = UsageSource::Sampled {
            capacity,
            samples: Arc::clone(&samples),
        };
        let statvfs = DiskUsage {
            total_bytes: 1 << 50,
            used_bytes: 1 << 49,
        };
        assert_eq!(source.read(statvfs), None);
        samples.set_bucket_count(10);
        for _ in 0..64 {
            samples.record(BucketSample {
                files: 3,
                bytes: 300,
            });
        }
        let usage = source.read(statvfs).expect("estimate");
        assert_eq!(
            usage,
            DiskUsage {
                total_bytes: 4000,
                used_bytes: 3000
            }
        );
        assert_eq!(usage.percent(), 75.0);
    }

    fn usage(used: u64) -> DiskUsage {
        DiskUsage {
            total_bytes: 100_000_000,
            used_bytes: used,
        }
    }

    fn counts(ops: u64, deletes: u64) -> OpCounts {
        OpCounts { ops, deletes }
    }

    fn pacer(target: f64) -> Pacer {
        Pacer::new(Percent::new("t", target).expect("pct"))
    }

    #[test]
    fn plan_reaches_target_in_the_horizon() {
        let mut p = pacer(70.0);
        // 15 MB over target, 1 MB files: 15 files over 60 s.
        let plan = p
            .plan(usage(85_000_000), Some(1_000_000.0), counts(0, 0))
            .expect("plan");
        assert_eq!(
            plan,
            Plan {
                files_per_sec: 0.25,
                ops_per_sec: 0.75
            }
        );
    }

    #[test]
    fn plan_holds_its_pace_as_usage_falls_and_speeds_up_if_it_grows() {
        let mut p = pacer(70.0);
        let size = Some(1_000_000.0);
        let first = p.plan(usage(85_000_000), size, counts(0, 0)).expect("plan");
        let later = p.plan(usage(72_000_000), size, counts(0, 0)).expect("plan");
        assert_eq!(first, later, "steady pace, not proportional decay");
        let grown = p.plan(usage(91_000_000), size, counts(0, 0)).expect("plan");
        assert!((grown.files_per_sec - 0.35).abs() < 1e-12, "{grown:?}");
    }

    #[test]
    fn plan_needs_a_file_size_but_still_records_the_start() {
        let mut p = pacer(70.0);
        assert_eq!(p.plan(usage(85_000_000), None, counts(0, 0)), None);
        assert_eq!(p.plan(usage(85_000_000), Some(0.0), counts(0, 0)), None);
        let plan = p
            .plan(usage(75_000_000), Some(1_000_000.0), counts(0, 0))
            .expect("plan");
        assert_eq!(plan.files_per_sec, 0.25, "start excess was 15 MB");
    }

    #[test]
    fn plan_measures_ops_per_delete_after_enough_deletes() {
        let mut p = pacer(70.0);
        let size = Some(1_000_000.0);
        p.plan(usage(85_000_000), size, counts(1000, 50));
        let early = p
            .plan(usage(85_000_000), size, counts(1100, 69))
            .expect("plan");
        assert_eq!(early.ops_per_sec, 0.25 * 3.0, "19 deletes: default ratio");
        let measured = p
            .plan(usage(85_000_000), size, counts(1150, 70))
            .expect("plan");
        assert_eq!(measured.ops_per_sec, 0.25 * 150.0 / 20.0);
    }

    #[test]
    fn plan_is_zero_below_target_and_stop_starts_over() {
        let mut p = pacer(70.0);
        let size = Some(1_000_000.0);
        let plan = p.plan(usage(60_000_000), size, counts(0, 0)).expect("plan");
        assert_eq!(plan.files_per_sec, 0.0);
        p.stop();
        let plan = p.plan(usage(76_000_000), size, counts(0, 0)).expect("plan");
        assert_eq!(plan.files_per_sec, 0.1);
    }

    #[test]
    fn shared_state_round_trips() {
        let s = SharedState::default();
        assert_eq!(s.mode(), Mode::Idle);
        s.set_mode(Mode::Emergency);
        s.set_usage(91.25);
        assert_eq!(s.mode(), Mode::Emergency);
        assert_eq!(s.usage(), 91.25);
    }
}
