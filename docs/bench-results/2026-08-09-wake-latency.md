# Single-wake latency per wait strategy

**Date:** 2026-08-09
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; box verified quiet (90% idle,
0 runnable) before measuring
**Probe:** `examples/wake_latency.rs` — `cargo run --release --example wake_latency`

## Purpose

To settle a claim the crate had documented but never measured: `Park`'s
"~1–5 µs wake latency".

`benches/throughput.rs`'s `wait_strategy_paced_handoff` could not settle it. That
bench measures a *round trip* containing two asymmetric wakes plus two handoffs —
the responder wakes from a ladder deepened by the full pacing gap while the
requester's is still in its spin rungs — so halving its result does not yield a
wake latency. `docs/bench-results/2026-08-08-wait-strategies.md` says so
explicitly and leaves the claim open. This probe closes it.

## Method

SPSC channel, payload is an `Instant` stamped by the producer at publish; the
consumer reads it on return from `recv()` and records the delta. Each sample is
therefore one publish-to-delivery path, including the wake. `Instant` is
monotonic and comparable across threads on Linux.

The producer spins to a 2000 µs deadline between sends — spun, not slept, since
`thread::sleep` inherits the same ~60 µs overshoot being measured. The gap is
long enough that a `Park` consumer is genuinely parked before each publish, and
that `Backoff` has climbed to its capped 1 ms rung.

`BusySpin` is the control: it never parks, so its figure is handoff plus
spin-detect, and another strategy's excess over it is the cost of its wait
mechanism. 1000 samples per strategy, three independent runs.

## Results

Median, three runs:

| Strategy | p50 run 1 | p50 run 2 | p50 run 3 | p50 spread | over control |
|---|---:|---:|---:|---:|---:|
| `BusySpin` | 0.17 µs | 0.17 µs | 0.16 µs | 6% | — |
| `BackoffYield` | 0.52 µs | 0.55 µs | 0.51 µs | 8% | +0.35 µs |
| **`Park`** | **10.21 µs** | **10.40 µs** | **10.19 µs** | **2%** | **+10.0 µs** |
| `Backoff` | 542 µs | 528 µs | 516 µs | 5% | +528 µs |

## Verdict on the claim

**`Park`'s documented "~1–5 µs wake latency" was too optimistic — the measured
median is ~10.2 µs, roughly 2x the top of the claimed range.** The figure is the
most reproducible in the table (2% spread across three runs), so this is not a
noise artifact. `src/wait.rs` and `README.md` now state ~10 µs and record that
the previous figure was never measured.

The ordering the docs claimed is otherwise correct: `BusySpin` <
`BackoffYield` < `Park` < `Backoff`, and `Backoff`'s ~528 µs is consistent with a
consumer sitting on the ladder's capped 1 ms rung when the publish lands.

## What this does NOT establish: tail latency

p99 and max were **not reproducible on this box** and are deliberately not
quoted as results. The same cell across three runs:

| Strategy | p99 run 1 | p99 run 2 | p99 run 3 |
|---|---:|---:|---:|
| `BusySpin` | 8.3 µs | 388.7 µs | 6.6 µs |
| `BackoffYield` | 1.4 µs | 21.5 µs | 5.5 µs |
| `Park` | 77.9 µs | 218.3 µs | 49.2 µs |

A 59x swing in `BusySpin`'s p99 between runs is co-tenant interference, not a
property of the strategy — the same machine has produced 2.4x swings on
throughput cells (`docs/bench-results/2026-08-07-sharded-mpsc.md`, "Follow-up").

This specifically retracts an observation the first run appeared to support:
that `BackoffYield` has a *better* tail than `BusySpin` because yielding
cooperates with the scheduler instead of being forcibly preempted. Run 1 of the
repeat looked consistent with it (388.7 µs vs 21.5 µs), run 2 did not (6.6 µs vs
5.5 µs). It is a plausible mechanism and it may well be real, but this box cannot
show it. Measuring tails needs a dedicated machine with isolated cores.
