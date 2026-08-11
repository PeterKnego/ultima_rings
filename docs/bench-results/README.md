# Reading the benchmark results in this directory

Every file here records a measurement taken on one 4-core Linux VM. Two
properties of that box, both discovered the hard way, determine how much any
number in this directory can carry.

## 1. There is a resolution floor of roughly 10%

**Differences below about 10% between two builds cannot be resolved here, no
matter how many rounds are run.**

This was found by a control that behaved impossibly. While gating the pre-park
spin (`2026-08-11-backoff-isolation.md` and the `feat/park-prespin` branch), the
`busyspin_block` cell moved **+10.4% with clean separation across three
interleaved rounds** — on a code path where the change provably never executes.
The pre-park spin lives inside the `WaitStrategy::Park` arm of
`Receiver::recv`; a `BusySpin` channel takes the `BusySpin` arm and never
reaches it.

Consistent across interleaved rounds means it was not box drift. Adding code to
`recv()` changed inlining or code layout, and that alone was worth ~10% on an
untouched path.

**More rounds do not help.** Code layout is fixed per build, so repeating the
same two binaries re-measures the same two layouts. Extra rounds reduce
measurement noise; they do nothing about layout bias. Removing it properly needs
several builds per variant with deliberately perturbed layout — which nothing
here does.

### What that implies for the results already recorded

| Result | Effect | Standing |
|---|---|---|
| CAS backoff (`2026-08-11-cas-backoff.md`) | +108% to +143% | Far above the floor. Unaffected. |
| Sharded MPSC (`2026-08-07-sharded-mpsc.md`) | 4.51x | Far above the floor. Unaffected. |
| Colocated slot (`2026-08-09-colocated-slot.md`) | +12% to +15% | Near the floor. Three separate configurations moved together, which layout bias explains less readily than a single cell would — but it is closer to the floor than its write-up assumed. |
| Padding, rejected (`2026-08-09-mpsc-perf-v2.md`) | +3.5%, then −0.1% | **Entirely inside the floor.** The rejection still stands, because the conclusion drawn was "no reliable effect" — which is what an unresolvable difference looks like. |
| Backoff ceiling sweep (`2026-08-11-backoff-tuning.md`) | 1% to 3% between candidates | Inside the floor. Again the conclusion was "indistinguishable", which the floor supports. |
| Pre-park spin (`feat/park-prespin`, unmerged) | +16% claimed on one cell | Rejected. Driven by a single outlier, and its control failed. |

The pattern is reassuring rather than alarming: every conclusion drawn below the
floor was a **negative** one. Nothing was adopted on evidence the box cannot
produce. But any *future* claim of a sub-10% improvement needs a method this
directory does not yet have.

## 2. Three rounds is a screen, not a decision

Twice in one session a three-round result reversed under five rounds:

| Question | 3 rounds | 5 rounds |
|---|---|---|
| Backoff ceiling 64 against 256 at 4 producers | 256 by +2.9% | 64 by +1.5% |
| Backoff ceiling 16 against 64 at 64 producers | 16 by +6.7% | 64 by +1.3% |

Both were 3–7% effects, and both had a plausible mechanism ready to explain
them, which is exactly what made them convincing. Require either separation well
clear of the floor above, or a five-round confirmation.

## Practices that these findings produced

- **Interleave by round**, never all runs of one variant then the other. Box
  conditions drift over the tens of minutes a comparison takes, and a block
  design lets that drift look like an effect.
- **Judge box quietness with `vmstat`**, not load average. Load average has read
  above 2.0 on this machine while `vmstat` showed 90% idle.
- **Build to completion before measuring.** Never let a build overlap a run.
- **Include a control cell** that the change cannot affect. That is what caught
  the layout bias, and no other check in this directory would have.
- **Gate across the axis the change acts on.** The CAS backoff's gate covered
  three configurations that varied capacity and producer count while holding the
  wait strategy fixed. It was thorough on the wrong axis and missed a 24%
  regression in `Park` mode (`2026-08-11-bakeoff-v3.md`).
- **Watch for byte-identical repeat numbers.** They mean a filter matched
  nothing and criterion re-reported a stale `estimates.json`, usually after a
  `git stash` took the bench code along with the source.

## Cross-session comparison

Absolute figures are not comparable between sessions. On 2026-08-09 the box ran
about 20% slower than on 2026-08-06 on *unchanged* code — `src/spsc.rs` and the
third-party `rtrb` fell together — and recovered by 2026-08-11
(`2026-08-11-bakeoff-v3.md`). Compare ratios measured within one session.
