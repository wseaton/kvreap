//! Metadata-operation budget shared by all workers.
//!
//! ```text
//!   ops/s = min(cap, aimd)            cap = 2 x DELETION_MAX_FILES_PER_SECOND (stat + unlink)
//!
//!   every tick (1s):  latency EWMA per op kind vs. its rolling-minimum baseline
//!     any kind > CONGESTION_RATIO x baseline (and > LATENCY_FLOOR)  -> aimd /= 2
//!     otherwise                                                     -> aimd += step
//! ```

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::shutdown::Shutdown;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Readdir,
    Stat,
    Unlink,
}

impl OpKind {
    const ALL: [Self; 3] = [Self::Readdir, Self::Stat, Self::Unlink];

    fn index(self) -> usize {
        match self {
            Self::Readdir => 0,
            Self::Stat => 1,
            Self::Unlink => 2,
        }
    }
}

const START_RATE: f64 = 500.0;
const MIN_RATE: f64 = 20.0;
const ADDITIVE_STEP: f64 = 50.0;
const EWMA_ALPHA: f64 = 0.2;
const CONGESTION_RATIO: f64 = 3.0;
const LATENCY_FLOOR: Duration = Duration::from_millis(2);
const BASELINE_WINDOW: usize = 120;
pub const TICK: Duration = Duration::from_secs(1);

#[derive(Debug, Default, Clone)]
struct LatencyTrack {
    ewma: Option<f64>,
    observed_since_tick: bool,
    history: VecDeque<f64>,
}

impl LatencyTrack {
    fn observe(&mut self, secs: f64) {
        self.ewma = Some(match self.ewma {
            Some(prev) => prev + EWMA_ALPHA * (secs - prev),
            None => secs,
        });
        self.observed_since_tick = true;
    }

    /// True if this tick's EWMA is well above the rolling minimum; ticks without samples are skipped.
    fn congested_and_roll(&mut self) -> bool {
        let Some(ewma) = self.ewma.filter(|_| self.observed_since_tick) else {
            return false;
        };
        self.observed_since_tick = false;
        let baseline = self.history.iter().copied().fold(f64::INFINITY, f64::min);
        self.history.push_back(ewma);
        if self.history.len() > BASELINE_WINDOW {
            self.history.pop_front();
        }
        baseline.is_finite()
            && ewma > LATENCY_FLOOR.as_secs_f64()
            && ewma > baseline * CONGESTION_RATIO
    }
}

#[derive(Debug, Clone)]
pub struct Aimd {
    rate: f64,
    cap: Option<f64>,
    tracks: [LatencyTrack; 3],
}

impl Aimd {
    pub fn new(cap: Option<f64>) -> Self {
        let mut aimd = Self {
            rate: START_RATE,
            cap,
            tracks: Default::default(),
        };
        aimd.clamp();
        aimd
    }

    fn clamp(&mut self) {
        let max = self.cap.unwrap_or(f64::INFINITY);
        self.rate = self.rate.clamp(MIN_RATE.min(max), max);
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    pub fn observe(&mut self, kind: OpKind, latency: Duration) {
        self.tracks[kind.index()].observe(latency.as_secs_f64());
    }

    /// Returns true if the rate was cut.
    pub fn tick(&mut self) -> bool {
        let congested = OpKind::ALL
            .iter()
            .map(|k| self.tracks[k.index()].congested_and_roll())
            .fold(false, |acc, c| acc | c);
        if congested {
            self.rate /= 2.0;
        } else {
            self.rate += ADDITIVE_STEP;
        }
        self.clamp();
        congested
    }
}

#[derive(Debug)]
struct Inner {
    aimd: Aimd,
    next_slot: Instant,
    last_tick: Instant,
}

/// Spaces operations evenly (no burst) at the current AIMD rate.
#[derive(Debug)]
pub struct Budget {
    inner: Mutex<Inner>,
}

impl Budget {
    /// `max_files_per_second` of 0 means no cap; AIMD still applies.
    pub fn new(max_files_per_second: f64) -> Self {
        let cap = (max_files_per_second > 0.0).then_some(max_files_per_second * 2.0);
        let now = Instant::now();
        Self {
            inner: Mutex::new(Inner {
                aimd: Aimd::new(cap),
                next_slot: now,
                last_tick: now,
            }),
        }
    }

    pub fn rate(&self) -> f64 {
        self.inner.lock().map(|g| g.aimd.rate()).unwrap_or(0.0)
    }

    pub fn observe(&self, kind: OpKind, latency: Duration) {
        if let Ok(mut g) = self.inner.lock() {
            g.aimd.observe(kind, latency);
        }
    }

    /// Waits for the next slot unless `unpaced`. Returns false if shutdown fired while waiting.
    pub fn acquire(&self, shutdown: &Shutdown, unpaced: bool) -> bool {
        let delay = {
            let Ok(mut g) = self.inner.lock() else {
                return !shutdown.is_set();
            };
            let now = Instant::now();
            if now.duration_since(g.last_tick) >= TICK {
                g.last_tick = now;
                if g.aimd.tick() {
                    tracing::info!(
                        rate = g.aimd.rate(),
                        "metadata latency rising, halving op rate"
                    );
                }
            }
            if unpaced {
                g.next_slot = now;
                Duration::ZERO
            } else {
                let interval = Duration::from_secs_f64(1.0 / g.aimd.rate());
                let slot = g.next_slot.max(now);
                g.next_slot = slot + interval;
                slot - now
            }
        };
        if delay.is_zero() {
            !shutdown.is_set()
        } else {
            !shutdown.wait(delay)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::budget::{ADDITIVE_STEP, Aimd, Budget, MIN_RATE, OpKind, START_RATE};
    use crate::shutdown::Shutdown;

    fn ms(v: u64) -> Duration {
        Duration::from_millis(v)
    }

    fn steady(aimd: &mut Aimd, kind: OpKind, latency: Duration, ticks: usize) {
        for _ in 0..ticks {
            for _ in 0..10 {
                aimd.observe(kind, latency);
            }
            aimd.tick();
        }
    }

    #[test]
    fn additive_increase_when_healthy() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 4);
        assert_eq!(a.rate(), START_RATE + 4.0 * ADDITIVE_STEP);
    }

    #[test]
    fn halves_when_latency_triples_over_baseline() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 5);
        let before = a.rate();
        for _ in 0..30 {
            a.observe(OpKind::Unlink, ms(20));
        }
        assert!(a.tick());
        assert_eq!(a.rate(), before / 2.0);
    }

    #[test]
    fn sub_floor_latency_never_congests() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Stat, Duration::from_micros(10), 5);
        for _ in 0..30 {
            a.observe(OpKind::Stat, ms(1));
        }
        assert!(!a.tick(), "1ms is below the floor even at 100x baseline");
    }

    #[test]
    fn any_op_kind_can_trigger_backoff() {
        let mut a = Aimd::new(None);
        for _ in 0..5 {
            a.observe(OpKind::Unlink, ms(3));
            a.observe(OpKind::Readdir, ms(4));
            a.tick();
        }
        for _ in 0..30 {
            a.observe(OpKind::Readdir, ms(50));
        }
        a.observe(OpKind::Unlink, ms(3));
        assert!(a.tick());
    }

    #[test]
    fn rate_respects_cap_and_floor() {
        let mut capped = Aimd::new(Some(100.0));
        assert_eq!(capped.rate(), 100.0);
        steady(&mut capped, OpKind::Unlink, ms(3), 3);
        assert_eq!(capped.rate(), 100.0);

        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 3);
        for i in 0..20 {
            for _ in 0..30 {
                a.observe(OpKind::Unlink, ms(3 * 4u64.pow((i % 6) + 1)));
            }
            a.tick();
        }
        assert!(a.rate() >= MIN_RATE);
    }

    #[test]
    fn tiny_cap_below_min_rate_is_honored() {
        assert_eq!(Aimd::new(Some(5.0)).rate(), 5.0);
    }

    #[test]
    fn ticks_without_samples_do_not_count_as_congestion() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 3);
        let before = a.rate();
        assert!(!a.tick());
        assert_eq!(a.rate(), before + ADDITIVE_STEP);
    }

    #[test]
    fn budget_paces_at_cap() {
        let budget = Budget::new(25.0); // cap 50 ops/s -> 20ms spacing
        let shutdown = Shutdown::default();
        let start = Instant::now();
        for _ in 0..11 {
            assert!(budget.acquire(&shutdown, false));
        }
        let elapsed = start.elapsed();
        assert!(elapsed >= ms(190), "{elapsed:?}");
        assert!(elapsed < ms(600), "{elapsed:?}");
    }

    #[test]
    fn unpaced_acquire_does_not_wait() {
        let budget = Budget::new(1.0);
        let shutdown = Shutdown::default();
        let start = Instant::now();
        for _ in 0..100 {
            assert!(budget.acquire(&shutdown, true));
        }
        assert!(start.elapsed() < ms(200));
    }

    #[test]
    fn acquire_returns_false_on_shutdown() {
        let budget = Budget::new(1.0);
        let shutdown = Shutdown::default();
        assert!(budget.acquire(&shutdown, false));
        shutdown.trigger();
        let start = Instant::now();
        assert!(!budget.acquire(&shutdown, false));
        assert!(start.elapsed() < ms(200));
    }
}
