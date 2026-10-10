use std::collections::VecDeque;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::capacity::Samples;
use crate::config::{CapacityBytes, Percent};

pub const EMERGENCY_FLOOR: f64 = 97.0;
/// How long a deletion may take to show up in a usage reading.
pub const USAGE_LAG: Duration = Duration::from_secs(10);

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
    /// Bytes used above `target` percent of the volume.
    pub fn above(self, target: Percent) -> u64 {
        let target_bytes = (self.total_bytes as f64 * target.get() / 100.0) as u64;
        self.used_bytes.saturating_sub(target_bytes)
    }

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

/// Where the controller gets used and total bytes from.
#[derive(Debug, Clone)]
pub enum UsageSource {
    Statvfs(PathBuf),
    /// Sampled cache size against a configured capacity (`CAPACITY_BYTES`).
    Sampled {
        capacity: CapacityBytes,
        samples: Arc<Samples>,
    },
}

impl UsageSource {
    /// `Ok(None)` while a sampled estimate is not available yet.
    pub fn read(&self) -> io::Result<Option<DiskUsage>> {
        match self {
            Self::Statvfs(path) => disk_usage(path).map(Some),
            Self::Sampled { capacity, samples } => {
                Ok(samples.estimated_used_bytes().map(|used| DiskUsage {
                    total_bytes: capacity.get(),
                    used_bytes: used,
                }))
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
    pub fn new(cleanup: Percent, target: Percent) -> Self {
        Self {
            cleanup: cleanup.get(),
            target: target.get(),
            emergency: cleanup.get().max(EMERGENCY_FLOOR),
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

/// Share of the volume added to every grant, so the writes that land while a
/// reading settles do not leave usage just above the target.
pub const GRANT_MARGIN_PERCENT: u64 = 2;

/// `SharedState::to_free` before the controller has granted anything.
pub const UNGRANTED: u64 = u64::MAX;

/// What the controller knows when it sets the delete budget after a reading.
#[derive(Debug, Clone, Copy)]
pub struct GrantInputs {
    /// The mode the reading moves to.
    pub mode: Mode,
    /// Bytes left of the current grant.
    pub outstanding: u64,
    pub above_target: u64,
    /// Bytes deleted within the last `USAGE_LAG`.
    pub recently_freed: u64,
    pub margin: u64,
}

impl GrantInputs {
    /// The delete budget to publish.
    ///
    /// A prune is granted what is above the target plus `margin` and spends
    /// it. Once spent, a new grant comes only from a reading taken after no
    /// deletions for `USAGE_LAG`, so a reading that lags the deletions is
    /// never mistaken for space still to free: a fast statvfs ends the prune
    /// as soon as the budget is spent, a lagging one pauses it until the
    /// reading settles.
    pub fn next_grant(self) -> u64 {
        if !self.mode.is_evicting() {
            0
        } else if self.outstanding > 0 && self.outstanding != UNGRANTED {
            self.outstanding
        } else if self.recently_freed == 0 && self.above_target > 0 {
            self.above_target.saturating_add(self.margin)
        } else {
            0
        }
    }
}

/// The workers' freed-bytes counter over the last `lag`, so a usage reading
/// that does not show recent deletions yet is not taken as space still to free.
#[derive(Debug, Default)]
pub struct RecentFrees {
    marks: VecDeque<(Instant, u64)>,
}

impl RecentFrees {
    /// Records `freed_total` at `now`; returns bytes freed within the last `lag`.
    pub fn observe(&mut self, now: Instant, freed_total: u64, lag: Duration) -> u64 {
        self.marks.push_back((now, freed_total));
        while self
            .marks
            .get(1)
            .is_some_and(|(t, _)| now.saturating_duration_since(*t) >= lag)
        {
            self.marks.pop_front();
        }
        let base = self.marks.front().map_or(freed_total, |(_, f)| *f);
        freed_total.saturating_sub(base)
    }
}

/// Mode, usage and the remaining bytes to free, published by the controller
/// thread for workers to read.
#[derive(Debug)]
pub struct SharedState {
    mode: AtomicU8,
    usage_bits: AtomicU64,
    /// Bytes still to delete before usage reaches the target; `UNGRANTED` until the controller sets it.
    to_free: AtomicU64,
    freed_total: AtomicU64,
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            mode: AtomicU8::new(Mode::Idle.to_u8()),
            usage_bits: AtomicU64::new(0),
            to_free: AtomicU64::new(UNGRANTED),
            freed_total: AtomicU64::new(0),
        }
    }
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

    pub fn set_to_free(&self, bytes: u64) {
        self.to_free.store(bytes, Ordering::Release);
    }

    pub fn to_free(&self) -> u64 {
        self.to_free.load(Ordering::Acquire)
    }

    pub fn freed_total(&self) -> u64 {
        self.freed_total.load(Ordering::Relaxed)
    }

    /// Workers delete while evicting and the target is not yet met.
    pub fn should_delete(&self) -> bool {
        self.mode().is_evicting() && self.to_free() > 0
    }

    /// Records `bytes` deleted against the remaining budget.
    pub fn freed(&self, bytes: u64) {
        self.freed_total.fetch_add(bytes, Ordering::Relaxed);
        let mut left = self.to_free.load(Ordering::Acquire);
        while left != UNGRANTED {
            match self.to_free.compare_exchange_weak(
                left,
                left.saturating_sub(bytes),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(now) => left = now,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::capacity::{BucketSample, Samples};
    use crate::config::{Config, Percent};
    use std::time::{Duration, Instant};

    use crate::controller::{
        DiskUsage, Hysteresis, Mode, RecentFrees, SharedState, UNGRANTED, UsageSource, disk_usage,
    };

    fn h(cleanup: f64, target: f64) -> Hysteresis {
        Hysteresis::new(
            Percent::new("c", cleanup).expect("pct"),
            Percent::new("t", target).expect("pct"),
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
    fn bytes_above_target() {
        let u = DiskUsage {
            total_bytes: 1000,
            used_bytes: 850,
        };
        assert_eq!(u.above(Percent::new("t", 70.0).expect("pct")), 150);
        assert_eq!(u.above(Percent::new("t", 90.0).expect("pct")), 0);
    }

    #[test]
    fn recent_frees_cover_only_the_lag_window() {
        let lag = Duration::from_secs(2);
        let t0 = Instant::now();
        let at = |ms| t0 + Duration::from_millis(ms);
        let mut r = RecentFrees::default();
        assert_eq!(r.observe(at(0), 0, lag), 0);
        assert_eq!(r.observe(at(500), 100, lag), 100);
        assert_eq!(r.observe(at(1000), 300, lag), 300);
        assert_eq!(r.observe(at(2500), 300, lag), 200, "the t=0 mark aged out");
        assert_eq!(r.observe(at(5000), 300, lag), 0, "nothing freed for 2 s");
        assert_eq!(r.observe(at(5100), 350, lag), 50);
    }

    #[test]
    fn budget_is_spent_before_a_settled_reading_refills_it() {
        use crate::controller::GrantInputs;

        let grant = |mode, outstanding, above_target, recently_freed| {
            GrantInputs {
                mode,
                outstanding,
                above_target,
                recently_freed,
                margin: 20,
            }
            .next_grant()
        };
        let evicting = Mode::Evicting;
        assert_eq!(grant(Mode::Idle, 500, 900, 0), 0, "idle");
        assert_eq!(
            grant(evicting, UNGRANTED, 900, 0),
            920,
            "prune start: above target plus margin"
        );
        assert_eq!(
            grant(evicting, 400, 900, 500),
            400,
            "spending: readings ignored"
        );
        assert_eq!(
            grant(evicting, 400, 0, 0),
            400,
            "an outstanding grant is not withdrawn"
        );
        assert_eq!(
            grant(evicting, 0, 700, 300),
            0,
            "spent while the reading may still lag: wait"
        );
        assert_eq!(
            grant(Mode::Emergency, 0, 120, 0),
            140,
            "settled reading still above target: top up with margin"
        );
        assert_eq!(
            grant(evicting, 0, 0, 0),
            0,
            "at or below target: nothing to grant, margin or not"
        );
    }

    #[test]
    fn shared_budget_stops_deleting_at_zero() {
        let s = SharedState::default();
        assert!(!s.should_delete(), "idle");
        s.set_mode(Mode::Evicting);
        assert!(s.should_delete(), "no budget published yet: unbounded");
        s.freed(10);
        assert_eq!(s.to_free(), UNGRANTED, "an unbounded budget is not spent");
        s.set_to_free(250);
        s.freed(100);
        s.freed(100);
        assert!(s.should_delete());
        s.freed(100);
        assert_eq!(s.to_free(), 0);
        assert!(!s.should_delete());
        assert_eq!(s.freed_total(), 310);
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
    fn statvfs_source_reads_the_mount() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let got = UsageSource::Statvfs(tmp.path().to_path_buf())
            .read()
            .expect("statvfs")
            .expect("always available");
        assert_eq!(
            got.total_bytes,
            disk_usage(tmp.path()).expect("statvfs").total_bytes
        );
        assert!(got.used_bytes <= got.total_bytes);
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
        assert_eq!(source.read().expect("read"), None);
        samples.set_bucket_count(10);
        for _ in 0..64 {
            samples.record(BucketSample {
                files: 3,
                bytes: 300,
            });
        }
        let usage = source.read().expect("read").expect("estimate");
        assert_eq!(
            usage,
            DiskUsage {
                total_bytes: 4000,
                used_bytes: 3000
            }
        );
        assert_eq!(usage.percent(), 75.0);
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
