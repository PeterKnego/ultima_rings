//! Wait strategies. `BusySpin`, `Backoff` and `BackoffYield` are self-waking —
//! the productive side pays nothing for them; `Park` blocks via the notify
//! layer (`crate::notify`) and needs the Dekker wake protocol.
//!
//! Ordered by wake granularity, measured on a 4-core Linux VM:
//! `BusySpin` (~27 ns) → `BackoffYield` (~0.7 µs) → `Park` (~10 µs, costs the
//! peer a fence + wake) → `Backoff` (~64 µs floor, costs the peer nothing).
//!
//! Idle CPU is the other half of that trade, and it is measured rather than
//! asserted — `examples/cpu_cost.rs`, consumer thread only, one element every
//! 200 µs, as a fraction of one core:
//!
//! | strategy | idle CPU |
//! |---|---:|
//! | `BackoffYield` | 100.0% |
//! | `BusySpin` | 99.9% |
//! | `Backoff` | 10.2% |
//! | `Park` | 1.8% |
//!
//! Note that `Backoff` is a tenth of a core, not zero — earlier revisions of
//! this doc called both it and `Park` "no idle CPU", which overstated `Backoff`
//! by that tenth.
//!
//! Idle CPU is not the whole trade, and reading only the table above gets
//! `BackoffYield` exactly backwards. Under **oversubscription** the ranking
//! inverts — blocking `send`/`recv`, 4 cores, median of 3 (same example):
//!
//! | strategy | p2 | p8 (2x) | p32 (8x) |
//! |---|---:|---:|---:|
//! | `BusySpin` | 69.11 | 35.67 | 4.84 |
//! | `BackoffYield` | 71.45 | **62.77** | **35.65** |
//! | `Backoff` | 58.72 | 61.17 | 36.64 |
//! | `Park` | 10.92 | 11.13 | 11.76 |
//!
//! Melem/s. `BusySpin` collapses once threads outnumber cores, because a
//! spinner burns the core the thread it is waiting on needs; it also becomes
//! wildly unstable there (4.71–19.93 across three runs at p32). `Park` is the
//! slowest everywhere and the only strategy indifferent to the ratio.
//!
//! **Those figures are from a 4-CPU/2-core VM and do not generalize.** Re-run
//! across 2–16 physical cores (`2026-08-12-topology-sweep.md`), `BackoffYield`
//! leads `BusySpin` at 8x oversubscription by 12.3x on 2 cores, 4.6x on 4, and
//! only **1.2x on 16** — one wasted CPU is half a small machine and 6% of a
//! large one. The direction holds everywhere; the magnitude is a property of the
//! core count, so never quote it without one.
//!
//! The collapse threshold is **schedulable CPUs, not physical cores**: three
//! threads on two CPUs collapse `BusySpin` to 7.70 Melem/s, while the same three
//! threads on four CPUs that are still only two physical cores give 32.38. An
//! SMT sibling is a poor execution resource but a perfectly good runqueue slot,
//! which is all this mechanism needs.
//!
//! **That table oversubscribes with the channel's own producers. Doing it with
//! unrelated threads reverses the ranking.** With 2 producers + 1 consumer plus
//! four CPU-bound threads that never touch the channel, `Park` is fastest and
//! by far the most stable, while `BusySpin`, `BackoffYield` and `Backoff` land
//! within noise of one another. Yielding pays only when the thread you yield to
//! is the one you are waiting on; yielding to a stranger surrenders a slice and
//! returns nothing, whereas parking leaves the runqueue entirely.
//!
//! The same measurement gives the figure this crate is otherwise silent on —
//! what a wait strategy costs the code around it, as a fraction of the external
//! threads' throughput running alone:
//!
//! | strategy | external throughput kept |
//! |---|---:|
//! | `Backoff` | 98% |
//! | `BackoffYield` | 96% |
//! | `Park` | 86% |
//! | `BusySpin` | **77%** |
//!
//! That 77% is a 4-CPU figure. The cost scales with how large a share one CPU
//! is: `BusySpin` keeps 50% of external throughput on 2 cores, 77% on 4, and 94%
//! on 16, while `Backoff` and `BackoffYield` stay between 96% and 100% at every
//! size. Budget for the CPU a spinner holds, as a fraction of the machine you
//! actually have.
//!
//! **Under external load `Park` is the fastest strategy, not the slowest.** With
//! the machine already busy it leads by 5.0x on 4 cores, 24x on 8 and 14x on 16
//! (`2026-08-12-topology-sweep.md`), while keeping 70–95% of external throughput
//! — comparable to `BusySpin` or better. Every "`Park` is ~6x slower" figure in
//! this crate's documentation was measured on an *idle* machine. On a busy one
//! the ordering inverts, because a parked consumer is woken when work exists
//! instead of competing for slices it cannot use.

use std::time::Duration;

/// How a blocked side (consumer-on-empty, producer-on-full) waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// `spin_loop()` until progress: lowest latency, one core per blocked side
    /// — measured at 99.9% of a core while idle (`examples/cpu_cost.rs`).
    BusySpin,
    /// Aeron-style idle ladder: spins, then yields, then timed parks doubling
    /// 64 µs → 1 ms. Self-waking — the other side never needs to notify.
    /// Lowest CPU of the self-waking strategies at 10.2% of a core when idle
    /// (`examples/cpu_cost.rs`), at the cost of a wake latency
    /// floored by the OS timer: the 64 µs floor is deliberate, because
    /// `thread::park_timeout` cannot deliver sub-floor sleeps (a 1 µs request
    /// measured ~60 µs on a 4-core Linux VM).
    Backoff,
    /// Spins, then yields **indefinitely** — never parks. Sits between
    /// [`WaitStrategy::BusySpin`] and [`WaitStrategy::Backoff`]: wake
    /// granularity stays at the cost of one `yield_now` (~0.7 µs measured)
    /// rather than the OS timer floor (~60 µs), while still surrendering the
    /// core whenever another thread is runnable.
    ///
    /// **This does not reduce CPU use on an idle machine.** Measured at 100.0%
    /// of a core while idle, against `BusySpin`'s 99.9% (`examples/cpu_cost.rs`)
    /// — with nothing else runnable, `yield_now` returns immediately and this
    /// burns a core just like [`WaitStrategy::BusySpin`] — only less efficiently per iteration
    /// (~0.7 µs vs ~27 ns). It buys *politeness under contention*, not idle
    /// CPU. Callers wanting low CPU want [`WaitStrategy::Backoff`] or
    /// [`WaitStrategy::Park`].
    BackoffYield,
    /// Fully blocking park/wake via the notify layer: 1.8% of a core when
    /// idle, ~10 µs median wake latency. Unlike the self-waking strategies, this
    /// makes the *productive* side pay a `SeqCst` fence plus a wake on every
    /// operation.
    ///
    /// The 10 µs figure is measured publish-to-delivery on a 4-core Linux VM
    /// (`examples/wake_latency.rs`; 10.19–10.40 µs p50 across three runs).
    /// Earlier revisions of this doc claimed ~1–5 µs, which was never measured
    /// and is roughly 2x optimistic. Tail latency is deliberately not quoted:
    /// it was not reproducible on that machine.
    Park,
}

pub(crate) const SPINS: u32 = 10;
pub(crate) const YIELDS: u32 = 20;

/// Floor for the [`WaitStrategy::Backoff`] ladder's timed parks.
///
/// Deliberately **not** 1 µs. `thread::park_timeout` cannot deliver sub-floor
/// sleeps: measured on a 4-core Linux VM, `park_timeout(1µs)` actually sleeps
/// ~60 µs, and requests of 1/2/4/8 µs are indistinguishable from one another.
/// A `PARK_MIN` below that floor makes the first four rungs identical, so the
/// documented doubling would be fiction — the ladder would jump straight from
/// ~14 µs of yielding to ~60 µs of parking.
///
/// The exact floor is OS- and machine-dependent (kernel timer resolution); 64 µs
/// is chosen to be at or above it on typical Linux. On a tuned low-latency box
/// the real floor may be lower, making this slightly conservative — the
/// trade-off taken is an honest ladder over an optimal one.
const PARK_MIN: Duration = Duration::from_micros(64);
const PARK_MAX: Duration = Duration::from_millis(1);

/// Per-blocking-operation ladder state, shared by [`WaitStrategy::Backoff`]
/// and [`WaitStrategy::BackoffYield`].
#[derive(Debug)]
pub(crate) struct Idle {
    step: u32,
    park: Duration,
    /// `true` for [`WaitStrategy::BackoffYield`]: yield forever after the spin
    /// rungs instead of escalating to timed parks.
    yield_only: bool,
}

impl Idle {
    /// The parking ladder ([`WaitStrategy::Backoff`]).
    pub(crate) fn new() -> Self {
        Idle {
            step: 0,
            park: PARK_MIN,
            yield_only: false,
        }
    }

    /// The ladder appropriate to `strategy`. `BusySpin` and `Park` never call
    /// [`Idle::idle`], so they get the parking ladder and simply ignore it.
    pub(crate) fn for_strategy(strategy: WaitStrategy) -> Self {
        Idle {
            yield_only: strategy == WaitStrategy::BackoffYield,
            ..Idle::new()
        }
    }

    /// One rung of the ladder. Timed parks self-wake, so callers re-check
    /// their condition after every call.
    pub(crate) fn idle(&mut self) {
        if self.step < SPINS {
            std::hint::spin_loop();
        } else if self.yield_only || self.step < SPINS + YIELDS {
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
        idle.idle(); // first park
        assert!(
            t.elapsed() >= PARK_MIN,
            "first park returned under PARK_MIN"
        );
        for _ in 0..30 {
            idle.idle(); // doubling must cap, not overflow
        }
    }

    #[test]
    fn park_min_is_at_or_above_the_os_timer_floor() {
        // `park_timeout` cannot deliver sub-floor sleeps. Measured on a 4-core
        // Linux VM: park_timeout(1us) actually sleeps ~60us, and 1/2/4/8us are
        // indistinguishable. A PARK_MIN below that floor makes the ladder's
        // first rungs identical, so the documented doubling is fiction.
        assert!(
            PARK_MIN >= Duration::from_micros(64),
            "PARK_MIN {PARK_MIN:?} is below the OS timer floor; \
             the first ladder rungs would collapse into one another"
        );
        assert!(
            PARK_MAX > PARK_MIN,
            "PARK_MAX must leave room for at least one doubling"
        );
    }

    /// Deterministic (no wall-clock): `Idle::park` only ever advances on the
    /// parking branch, so an untouched interval proves that branch was never
    /// taken. Asserting elapsed time here instead would be both flaky and
    /// wrong under miri, whose interpreted execution does not track real time.
    #[test]
    fn yielding_ladder_never_escalates_to_parks() {
        let mut yielding = Idle::for_strategy(WaitStrategy::BackoffYield);
        let mut parking = Idle::for_strategy(WaitStrategy::Backoff);
        // Well past the rung where `Backoff` starts parking.
        for _ in 0..(SPINS + YIELDS + 5) {
            yielding.idle();
            parking.idle();
        }
        assert!(
            parking.park > PARK_MIN,
            "Backoff never reached a park rung, so this proves nothing"
        );
        assert_eq!(
            yielding.park, PARK_MIN,
            "yielding ladder escalated to parking"
        );
    }

    /// The wall-clock counterpart to the above. Skipped under miri, which runs
    /// orders of magnitude slower than real time and cannot honour the bound.
    #[test]
    #[cfg_attr(miri, ignore = "wall-clock bound is meaningless under miri")]
    fn yielding_ladder_does_not_sleep() {
        let mut idle = Idle::for_strategy(WaitStrategy::BackoffYield);
        let t = Instant::now();
        // 50 rungs past where `Backoff` would have started parking — which
        // would cost 64 µs + 128 µs + ... capped at PARK_MAX, tens of ms.
        for _ in 0..(SPINS + YIELDS + 50) {
            idle.idle();
        }
        assert!(
            t.elapsed() < Duration::from_millis(10),
            "yielding ladder slept (took {:?})",
            t.elapsed()
        );
    }
}
