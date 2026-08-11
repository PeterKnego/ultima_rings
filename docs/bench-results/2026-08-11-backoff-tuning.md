# Tuning the claim-CAS backoff ceiling: no change warranted

**Date:** 2026-08-11
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; box at 81–88% idle, 0–3 runnable
**Outcome:** **`CLAIM_BACKOFF_MAX` stays at 64.** No code change.

The backoff added in `70193fd` grows 1, 2, 4 … up to a ceiling of 64, resetting
per `try_send`. The 64 was inherited from `crossbeam-utils`' `Backoff` spin
ceiling and never swept. This is the sweep.

## Method

Values are interleaved in **rounds** — every ceiling measured once per round,
then the next round — rather than all runs of one value before the next. Box
conditions drift over the tens of minutes a sweep takes, and a block design
would let that drift masquerade as a parameter effect. That is exactly how the
padding round went wrong (`2026-08-09-mpsc-perf-v2.md`).

`mpsc_layout_probe`, single cell per sweep to keep runs short.

## Ceiling sweep at 2 producers (cap1024_p2), 3 rounds

| Ceiling | mean | range |
|---|---:|---|
| 1 | 35.19 | 34.14 – 35.85 |
| 4 | 57.91 | 57.06 – 59.14 |
| 16 | 73.12 | 72.48 – 73.76 |
| **64** | **74.39** | 74.15 – 74.55 |
| 256 | 74.77 | 74.43 – 75.08 |
| 1024 | 72.82 | 71.12 – 73.85 |

Four things the curve shows:

1. **The knob is worth more than 2×.** Ceiling 1 gives 35.19; ceiling 64 gives
   74.39.
2. **Growth matters, not merely spacing.** A ceiling of 1 means every retry waits
   exactly one `spin_loop` — a constant, not an escalation. That yields 35.19
   against the no-backoff baseline of ~31.4, so a fixed single pause recovers
   almost nothing. The escalation is doing the work.
3. **The plateau is broad.** 16 through 256 all sit within ~2% of each other, so
   the choice is not fragile.
4. **Too much backoff hurts.** 1024 falls to 72.82, below 64 and 256, with no
   overlap against either.

64 and 256 overlap (74.15–74.55 against 74.43–75.08) and are not separable here.

## Ceiling sweep at 4 producers (cap1024_p4), 3 rounds

| Ceiling | mean | range |
|---|---:|---|
| 16 | 60.46 | 59.37 – 61.36 |
| 64 | 62.10 | 61.70 – 62.89 |
| 256 | 63.91 | 62.56 – 64.95 |
| 1024 | 59.92 | 59.44 – 60.79 |

At higher contention 256 appeared to lead 64 by **+2.9%**, winning 2 of 3
rounds. That is mechanically plausible — more producers means more collisions,
which should favour a longer maximum wait — so it was worth resolving rather
than dismissing.

## The 2.9% did not survive more rounds

A focused head-to-head, 5 interleaved rounds, same cell:

| Ceiling | runs | mean |
|---|---|---:|
| **64** | 65.44, 62.54, 64.17, 62.62, 65.94 | **64.14** |
| 256 | 63.23, 65.20, 63.36, 61.97, 62.13 | 63.18 |

The order reverses. 64 now leads on the mean and wins 4 of 5 rounds. The
within-value spread (~5%) is larger than the difference between the values
(~1.5%), so the two are indistinguishable at 4 producers as well as at 2.

**The earlier +2.9% was a sampling artifact of three rounds.** It is recorded
here because the reverse mistake — shipping a parameter change on a three-round
result that a five-round result overturns — is the one this project has already
made once, with the availability-array padding.

## Decision

Keep `CLAIM_BACKOFF_MAX = 64`. The sweep produces no code change, and that is a
useful result rather than a wasted one:

- The inherited crossbeam value sits on the plateau, not beside it.
- The plateau spans 16–256, so the value is insensitive to a factor of 16 either
  way. It does not need per-workload tuning.
- Both ends of the curve are genuinely worse, so the parameter is not inert — it
  is simply already right.

## Not swept

- **Growth factor.** Fixed at 2×. A 4× ramp reaches the ceiling sooner and was
  not tried.
- **Starting value.** Fixed at 1.
- **Reset policy.** The counter resets per `try_send`. A counter persisting
  across calls on the same `Sender` was not tried, and would behave differently
  for a producer in a tight send loop.
- **cap4096_p2.** The sweep used the two cap1024 cells. The third probe
  configuration was not swept, though the gate run in `2026-08-11-cas-backoff.md`
  covers it at the shipped value.
- **Producer counts above the core count.** Still unmeasured, as noted in the
  backoff result document.
