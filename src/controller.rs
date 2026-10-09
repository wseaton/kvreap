use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

use crate::config::Percent;

pub const EMERGENCY_FLOOR: f64 = 97.0;

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
    use crate::config::Percent;
    use crate::controller::{DiskUsage, Hysteresis, Mode, SharedState, disk_usage};

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
    fn shared_state_round_trips() {
        let s = SharedState::default();
        assert_eq!(s.mode(), Mode::Idle);
        s.set_mode(Mode::Emergency);
        s.set_usage(91.25);
        assert_eq!(s.mode(), Mode::Emergency);
        assert_eq!(s.usage(), 91.25);
    }
}
