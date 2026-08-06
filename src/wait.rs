//! Wait strategies. `BusySpin` and `Backoff` are self-waking; `Park` blocks
//! via the notify layer (`crate::notify`) and needs the Dekker wake protocol.

use std::time::Duration;

/// How a blocked side (consumer-on-empty, producer-on-full) waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// `spin_loop()` until progress: lowest latency, one core per blocked side.
    BusySpin,
    /// Aeron-style idle ladder: spins, then yields, then timed parks doubling
    /// 1 µs → 1 ms. Self-waking — the other side never needs to notify.
    Backoff,
    /// Fully blocking park/wake via the notify layer: zero idle CPU,
    /// ~1–5 µs wake latency.
    Park,
}

pub(crate) const SPINS: u32 = 10;
pub(crate) const YIELDS: u32 = 20;
const PARK_MIN: Duration = Duration::from_micros(1);
const PARK_MAX: Duration = Duration::from_millis(1);

/// Per-blocking-operation ladder state (the `backoff` bench cell's ladder).
#[derive(Debug)]
pub(crate) struct Idle {
    step: u32,
    park: Duration,
}

impl Idle {
    pub(crate) fn new() -> Self {
        Idle {
            step: 0,
            park: PARK_MIN,
        }
    }

    /// One rung of the ladder. Timed parks self-wake, so callers re-check
    /// their condition after every call.
    pub(crate) fn idle(&mut self) {
        if self.step < SPINS {
            std::hint::spin_loop();
        } else if self.step < SPINS + YIELDS {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(self.park);
            self.park = (self.park * 2).min(PARK_MAX);
        }
        self.step = self.step.saturating_add(1);
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn idle_ladder_spins_then_yields_then_parks_bounded() {
        let mut idle = Idle::new();
        // The first SPINS + YIELDS steps must not sleep (fast).
        let t = Instant::now();
        for _ in 0..(SPINS + YIELDS) {
            idle.idle();
        }
        assert!(t.elapsed().as_millis() < 100, "spin/yield rungs slept");
        // Subsequent steps park with doubling, capped at PARK_MAX.
        let t = Instant::now();
        idle.idle(); // first park: 1 µs floor
        assert!(t.elapsed().as_nanos() >= 1_000, "first park under 1 µs");
        for _ in 0..30 {
            idle.idle(); // doubling must cap, not overflow
        }
    }
}
