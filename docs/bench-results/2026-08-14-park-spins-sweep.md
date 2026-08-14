# `PARK_SPINS`: throughput is a plateau, CPU is a slope — so 64 was wrong

**Date:** 2026-08-14
**Hardware:** the 4-vCPU VM (2 physical cores + SMT), chosen because
`park_block`'s minimum detectable effect is ~11% here against 25% on the 16-core
rig (`2026-08-12-resolution-budgets-rig.md`)
**Change:** `PARK_SPINS` 64 → **16**

`PARK_SPINS = 64` shipped in `6a35383` on a latency argument — 64 spin hints cost
~1.7 µs against a ~10 µs park/unpark pair — and was never measured. Two sweeps,
paired per (alignment, round) across 3 alignments × 3 rounds, with `spins = 0` as
the same-run reference arm and `spsc` as the control.

## Throughput: a plateau from 1 to 256

`backoff_isolation/park_block`, ratio against `spins = 0`, 9 pairs each:

| spins | Melem/s | vs 0 | 95% CI | pairs won |
|---:|---:|---:|---|---:|
| 0 | 7.8–8.0 | 1.000x | — | — |
| 1 | 11.55 | 1.465x | [1.347, 1.582] | 9/9 |
| 2 | 12.04 | 1.524x | [1.411, 1.636] | 9/9 |
| 4 | 12.01 | 1.525x | [1.360, 1.689] | 9/9 |
| 8 | 12.11 | 1.533x | [1.388, 1.678] | 9/9 |
| **16** | **12.72** | **1.610x** | [1.457, 1.764] | 9/9 |
| 32 | 12.38 | 1.605x | [1.468, 1.742] | 9/9 |
| 64 | 12.28 | 1.587x | [1.459, 1.715] | 9/9 |
| 128 | 12.26 | 1.581x | [1.455, 1.707] | 9/9 |
| 256 | 12.42 | 1.605x | [1.458, 1.752] | 9/9 |

Every value beats not spinning, 9 pairs out of 9, and **no two values are
distinguishable from each other** — the intervals overlap almost completely. The
16 row was measured twice, in two independent sweeps, at 1.610x and 1.642x.

Controls behaved: `spsc` is `ns` at every value in the low sweep, and `park_poll`
— which uses `try_recv` and never enters the `Park` arm — shows nothing.

## CPU: a slope

Consumer idle CPU from `examples/cpu_cost.rs` (paced section, one element per
200 µs), median of 3:

| spins | % of a core | cpu ns/elem | vs 0 |
|---:|---:|---:|---:|
| 0 | 2.00% | 5129 | 1.00x |
| **16** | **2.10%** | **5484** | **1.07x** |
| 32 | 2.30% | 5926 | 1.16x |
| 64 | 2.70% | 6940 | 1.35x |
| 128 | 3.40% | 8824 | 1.72x |
| 256 | 4.80% | 12404 | 2.42x |

Throughput cannot choose a value; cost can. **16 dominates 64: the same
throughput for 26% less idle CPU.**

## A regression the original gate could not see

`2026-08-13-park-prespin-gate.md` measured throughput only. Merging the spin at
64 moved `Park`'s idle CPU from 2.00% to 2.70% of a core — a **35% increase** in
the one number `Park` exists to keep small. That was a real cost of the merge and
it went unreported, because the gate had no CPU cell.

At 16 the cost is 7% instead of 35%, for throughput that is if anything slightly
better.

The general lesson is already in `README.md` §2 — wall-clock throughput hides
what spinning costs — and this is the first time the project has paid it on its
own change rather than observing it in a competitor.

## The mechanism in the source comment was wrong

The comment shipped with the constant explained the gain as absorbing an
in-flight publish inside ~1.7 µs of spinning. **A single spin already delivers
1.465x of the 1.610x that 16 reaches** — 91% of the effect at one iteration,
where there is no meaningful duration to absorb anything into.

So the benefit is dominated by *one extra re-check before committing to the
park*, not by spinning for a while. There is a small additional gradient from 1
to 16 (1.465 → 1.610) which is consistent with a genuine short-spin effect on
top, but it is inside the noise and should not be leaned on.

The source comment has been rewritten to say this. The old wording would have
sent the next person tuning this constant looking for a duration to match against
wake latency, which is the wrong model.

## Verification

| check | result |
|---|---|
| loom, 5 models | pass |
| miri, default + `--all-features` | clean |
| `cargo test --all-features` | 51 passed |
| clippy, all targets | clean |

## Limits

- **One machine.** `PARK_SPINS` interacts with wake latency and publish spacing,
  both machine-dependent. 16 is right for this VM; the plateau's flatness from 1
  to 256 suggests the choice is not delicate, which is the more useful finding.
- **2 producers, cap 1024, `BusySpin` producers.** The producer ladder was not
  re-run.
- **CPU measured only at 0/16/32/64/128/256**, not at 1/2/4/8. Those would cost
  less still, but they also give up throughput point-estimates, and the slope
  below 16 is small enough not to change the choice.
- **`Backoff` and `BackoffYield` still have no `backoff_isolation` cell**, so the
  constant is unmeasured against them. `Backoff` parks but already spins first,
  so it should be unaffected.
- **Multiple comparisons.** Each sweep runs 15 arm-by-cell tests at 95%, so about
  one false positive per sweep is expected. One appeared on `spsc` at 128 in the
  high sweep and one on `park_poll` at 16 in the low sweep; neither reproduced in
  the other sweep, and both are marginal. This is exactly what the control is
  for.
