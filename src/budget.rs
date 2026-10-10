//! Metadata-operation budget shared by all workers.
//!
//! ```text
//!   ops/s = min(cap, aimd)            cap = 2 x DELETION_MAX_FILES_PER_SECOND (stat + unlink)
//!
//!   every tick (1s), per op kind: mean latency of the tick's samples vs. the
//!   lowest tick mean of the last BASELINE_WINDOW ticks
//!
//!     fewer than MIN_SAMPLES samples         -> hold (no signal, no growth)
//!     settling after a cut                   -> hold
//!     > CONGESTION_RATIO x baseline, and > LATENCY_FLOOR:
//!         first time                         -> aimd /= 2, settle SETTLE_TICKS
//!         still >= RESPONSE_RATIO x the latency that caused the last cut
//!                                            -> not ours: undo the cut, baseline = now
//!     otherwise                              -> aimd += step
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
const CONGESTION_RATIO: f64 = 3.0;
const RESPONSE_RATIO: f64 = 0.8;
const LATENCY_FLOOR: Duration = Duration::from_millis(2);
const BASELINE_WINDOW: usize = 120;
const MIN_SAMPLES: u32 = 5;
const SETTLE_TICKS: u32 = 2;
pub const TICK: Duration = Duration::from_secs(1);

/// What a tick did to the rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Increased,
    Held,
    Halved,
    /// Latency stayed up after a cut, so the cut was undone and the baseline moved.
    Reanchored,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Signal {
    Quiet,
    Healthy,
    Congested(f64),
}

#[derive(Debug, Default, Clone)]
struct LatencyTrack {
    sum: f64,
    count: u32,
    history: VecDeque<f64>,
    /// Tick latency that triggered the last cut, until a healthy tick clears it.
    cut_at: Option<f64>,
}

impl LatencyTrack {
    fn observe(&mut self, secs: f64) {
        self.sum += secs;
        self.count += 1;
    }

    fn roll(&mut self) -> Signal {
        let (sum, count) = (self.sum, self.count);
        (self.sum, self.count) = (0.0, 0);
        if count < MIN_SAMPLES {
            return Signal::Quiet;
        }
        let mean = sum / f64::from(count);
        let baseline = self.history.iter().copied().fold(f64::INFINITY, f64::min);
        self.history.push_back(mean);
        if self.history.len() > BASELINE_WINDOW {
            self.history.pop_front();
        }
        if baseline.is_finite()
            && mean > LATENCY_FLOOR.as_secs_f64()
            && mean > baseline * CONGESTION_RATIO
        {
            Signal::Congested(mean)
        } else {
            self.cut_at = None;
            Signal::Healthy
        }
    }

    fn reanchor(&mut self, latency: f64) {
        self.history.clear();
        self.history.push_back(latency);
        self.cut_at = None;
    }
}

#[derive(Debug, Clone)]
pub struct Aimd {
    rate: f64,
    cap: Option<f64>,
    tracks: [LatencyTrack; 3],
    settling: u32,
    rate_before_cut: Option<f64>,
}

impl Aimd {
    pub fn new(cap: Option<f64>) -> Self {
        let mut aimd = Self {
            rate: START_RATE,
            cap,
            tracks: Default::default(),
            settling: 0,
            rate_before_cut: None,
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

    pub fn tick(&mut self) -> Verdict {
        let signals = OpKind::ALL.map(|k| self.tracks[k.index()].roll());
        if self.settling > 0 {
            self.settling -= 1;
            return Verdict::Held;
        }
        let mut ours = false;
        let mut external = false;
        for (track, signal) in self.tracks.iter_mut().zip(signals) {
            let Signal::Congested(latency) = signal else {
                continue;
            };
            match track.cut_at {
                Some(cut_at) if latency >= cut_at * RESPONSE_RATIO => {
                    track.reanchor(latency);
                    external = true;
                }
                _ => {
                    track.cut_at = Some(latency);
                    ours = true;
                }
            }
        }
        let verdict = if ours {
            self.rate_before_cut = Some(self.rate);
            self.rate /= 2.0;
            self.settling = SETTLE_TICKS;
            Verdict::Halved
        } else if external {
            self.rate = self.rate_before_cut.take().unwrap_or(self.rate);
            Verdict::Reanchored
        } else if signals.contains(&Signal::Healthy) {
            self.rate_before_cut = None;
            self.rate += ADDITIVE_STEP;
            Verdict::Increased
        } else {
            Verdict::Held
        };
        self.clamp();
        verdict
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
        self.inner.lock().map_or(0.0, |g| g.aimd.rate())
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
                match g.aimd.tick() {
                    Verdict::Halved => tracing::info!(
                        rate = g.aimd.rate(),
                        "metadata latency rising, halving op rate"
                    ),
                    Verdict::Reanchored => tracing::info!(
                        rate = g.aimd.rate(),
                        "metadata latency stayed up after slowing down, restoring op rate"
                    ),
                    Verdict::Increased | Verdict::Held => {}
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

    use crate::budget::{
        ADDITIVE_STEP, Aimd, Budget, MIN_RATE, OpKind, SETTLE_TICKS, START_RATE, Verdict,
    };
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
        assert_eq!(a.tick(), Verdict::Halved);
        assert_eq!(a.rate(), before / 2.0);
    }

    #[test]
    fn sub_floor_latency_never_congests() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Stat, Duration::from_micros(10), 5);
        for _ in 0..30 {
            a.observe(OpKind::Stat, ms(1));
        }
        assert_eq!(
            a.tick(),
            Verdict::Increased,
            "1ms is below the floor even at 100x baseline"
        );
    }

    #[test]
    fn any_op_kind_can_trigger_backoff() {
        let mut a = Aimd::new(None);
        for _ in 0..5 {
            for _ in 0..10 {
                a.observe(OpKind::Unlink, ms(3));
                a.observe(OpKind::Readdir, ms(4));
            }
            a.tick();
        }
        for _ in 0..30 {
            a.observe(OpKind::Readdir, ms(50));
            a.observe(OpKind::Unlink, ms(3));
        }
        assert_eq!(a.tick(), Verdict::Halved);
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
    fn ticks_without_enough_samples_neither_cut_nor_grow() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 3);
        let before = a.rate();
        assert_eq!(a.tick(), Verdict::Held, "idle");
        for _ in 0..4 {
            a.observe(OpKind::Unlink, ms(500));
        }
        assert_eq!(a.tick(), Verdict::Held, "4 slow samples are not a signal");
        assert_eq!(a.rate(), before);
    }

    #[test]
    fn halves_once_then_waits_for_samples_at_the_new_rate() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 5);
        let before = a.rate();
        steady(&mut a, OpKind::Unlink, ms(30), 1);
        assert_eq!(a.rate(), before / 2.0);
        for _ in 0..SETTLE_TICKS {
            for _ in 0..10 {
                a.observe(OpKind::Unlink, ms(30));
            }
            assert_eq!(a.tick(), Verdict::Held);
        }
        assert_eq!(a.rate(), before / 2.0, "one cut for one congestion event");
    }

    #[test]
    fn latency_that_does_not_respond_restores_the_rate_and_moves_the_baseline() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 5);
        let before = a.rate();
        // The filesystem slows down for everyone and stays slow.
        steady(&mut a, OpKind::Unlink, ms(20), 1 + SETTLE_TICKS as usize);
        for _ in 0..10 {
            a.observe(OpKind::Unlink, ms(20));
        }
        assert_eq!(a.tick(), Verdict::Reanchored);
        assert_eq!(a.rate(), before, "the cut did not help, so it is undone");
        for _ in 0..10 {
            a.observe(OpKind::Unlink, ms(21));
        }
        assert_eq!(a.tick(), Verdict::Increased, "20ms is the new normal");
    }

    #[test]
    fn latency_that_responds_keeps_backing_off() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 5);
        let before = a.rate();
        steady(&mut a, OpKind::Unlink, ms(40), 1 + SETTLE_TICKS as usize);
        for _ in 0..10 {
            a.observe(OpKind::Unlink, ms(20));
        }
        assert_eq!(
            a.tick(),
            Verdict::Halved,
            "halving the rate halved latency, so the load was ours"
        );
        assert_eq!(a.rate(), before / 4.0);
    }

    #[test]
    fn a_long_external_slowdown_costs_one_cut_at_most() {
        let mut a = Aimd::new(None);
        steady(&mut a, OpKind::Unlink, ms(3), 10);
        let before = a.rate();
        let mut lowest = before;
        for _ in 0..300 {
            steady(&mut a, OpKind::Unlink, ms(25), 1);
            lowest = lowest.min(a.rate());
        }
        assert_eq!(lowest, before / 2.0, "5 minutes at 25ms");
        assert!(a.rate() > before);
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
