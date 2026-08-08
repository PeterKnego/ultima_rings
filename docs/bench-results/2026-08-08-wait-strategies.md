# Wait strategies under a paced handoff

**Date:** 2026-08-08
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; box verified quiet (91–93% idle,
0 runnable) before measuring; built to completion first
**Bench:** `benches/throughput.rs`, group `wait_strategy_paced_handoff`

## Why paced ping-pong

A saturating throughput bench cannot measure wait strategies: the ring is rarely
empty long enough for `Idle` to climb past its spin rungs, so every strategy
reports the same number. Pacing the requester forces the responder's ladder to
deepen before each wake.

It also has to be a **ping-pong, not a one-way stream**. The first version of
this bench was one-way and measured nothing — the producer never waited on the
consumer, so the consumer's wake latency was off the critical path and all four
strategies reported the pacing time alone (100.11–100.35 ms, indistinguishable).
Round-tripping puts the responder's wake on the requester's critical path.

Shape: SPSC request channel + SPSC response channel, both cap 1024, both on the
strategy under test. The requester spins to a 200 µs deadline (spinning, not
sleeping — `thread::sleep` would inherit the same ~60 µs overshoot being
measured), sends, then blocks on the response. 500 rounds per iteration.

## Results

Reported time is `500 x (200 µs gap + RTT)`. The gap is identical across cells,
so 100.0 ms is pure pacing and the excess is round-trip cost.

| Strategy | Total (mid) | Range | Above pacing | Per round | vs `BusySpin` |
|---|---:|---|---:|---:|---:|
| `BusySpin` | 100.46 ms | 100.33 – 100.62 | 0.46 ms | **0.92 µs** | 1.0x |
| `BackoffYield` | 100.80 ms | 100.71 – 100.92 | 0.80 ms | **1.60 µs** | 1.7x |
| `Park` | 108.31 ms | 108.16 – 108.48 | 8.31 ms | **16.6 µs** | 18x |
| `Backoff` | 169.36 ms | 168.65 – 170.00 | 69.36 ms | **138.7 µs** | 151x |

## Reading the numbers

**`BackoffYield` fills the gap it was added for.** It costs 1.7x `BusySpin`'s
round trip while `Backoff` costs 151x. Each round trip contains roughly two
wakes, so `BackoffYield`'s ~0.8 µs per wake matches the independently measured
`yield_now` cost of 692 ns — the mechanism and the measurement agree.

**`Backoff`'s cost tracks `PARK_MIN` as designed.** ~69 µs per wake against a
64 µs floor. Before this round `PARK_MIN` was 1 µs, which `park_timeout` cannot
honour — a 1 µs request measured ~60 µs on this box, and 1/2/4/8 µs were
indistinguishable, so the ladder's documented doubling was fiction for its first
four rungs. Raising the floor to 64 µs did not make `Backoff` slower; it made the
existing cost visible and the rungs real.

**What this does NOT establish.** The per-wake figures above are derived by
halving a round trip, and the two wakes in a round trip are not symmetric — the
responder wakes from a ladder deepened by the full 200 µs gap, while the
requester's ladder is still in its spin rungs when the reply arrives. So these
are round-trip costs under one specific pacing, not isolated wake latencies.

In particular, this does **not** verify or refute `src/wait.rs`'s documented
"~1–5 µs wake latency" for `Park`. A 16.6 µs round trip is consistent with a
1–5 µs unpark plus two handoffs plus scheduler wakeup, and consistent with a
slower unpark. Isolating it needs a single-wake measurement, which was not done.
The claim stands as previously documented and unverified.

## Reproducing

```
cargo bench --bench throughput -- wait_strategy_paced_handoff
```

Check the box is quiet first (`vmstat 2 3` — want 85%+ idle, 0 runnable); this
crate's spin-based strategies are sensitive to co-tenant load, and this box has
produced 2.4x swings on other cells when contended (see
`docs/bench-results/2026-08-07-sharded-mpsc.md`, "Follow-up").
