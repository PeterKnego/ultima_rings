# The pre-park spin passes: +65% on `park_block`, 20 of 20 pairs

**Date:** 2026-08-13
**Hardware:** the 4-vCPU VM (2 physical cores + SMT) — chosen deliberately, see
below
**Change:** `src/mpsc.rs`, 39 lines. The `Park` consumer spins `PARK_SPINS` (64)
times checking `slot_published` before committing to a park. Sits entirely
*before* the Dekker sequence.

## Result

Ratio is prespin/base computed per (alignment, round) pair, then averaged over
20 pairs — 5 function alignments x 4 rounds.

| cell | base | prespin | ratio | 95% CI | pairs won |
|---|---:|---:|---:|---|---:|
| `backoff_isolation/park_block` | 7.61 | 12.49 | **1.650x** | [1.523, 1.776] | **20/20** |
| `backoff_isolation/busyspin_block` | 36.56 | 40.81 | 1.161x | [1.018, 1.304] | 16/20 |
| `backoff_isolation/park_poll` | 10.58 | 10.70 | 1.022x | [0.943, 1.101] | 11/20 |
| `backoff_isolation/busyspin_poll` | 45.39 | 45.20 | 1.014x | [0.930, 1.098] | 10/20 |
| **`spsc/busy_spin_pipelined`** (control) | 308.83 | 305.76 | 0.990x | [0.973, 1.008] | 10/20 |

**The control is flat and its interval contains 1.0**, so the run is valid.
`src/spsc.rs` is untouched by the patch, which makes this a true control — unlike
`busyspin_block`, which calls `Receiver::recv`, the function being modified.

The two cells that never enter the `Park` arm of `recv` (`park_poll`,
`busyspin_poll`, both `try_*` paths) show no effect, which is what the change
predicts.

## Why this gate succeeded where the first one failed

The first attempt (`2026-08-12-layout-sensitivity.md`) reported prespin runs of
**10.31, 10.92, 16.52** against a baseline of 10.00–12.26 and concluded the
change "does not reliably improve `park_block`".

That conclusion was wrong, and the reason is instructive: **the baseline came
from a different run.** It was taken from the layout sweep, on separate builds at
a separate time, and `park_block`'s absolute value drifts hard between sessions —
it measured 11.02 there and 7.61 here on identical source, a 45% gap. Comparing
one session's A against another session's B cannot resolve anything, whatever the
within-session budget is.

Two changes fixed it:

**Pairing.** Both arms are measured adjacently at the same alignment in the same
round, so drift and layout both cancel inside the ratio rather than being
averaged over. Layout in particular drops out entirely because both arms see the
same five alignments.

**Sample size.** 20 pairs instead of 3 unpaired runs, which cuts the standard
error by roughly √20.

### The 16.52 outlier was probably the only valid run

Applying the measured 1.65x to that session's baseline mean of 11.02 predicts
about 18; the observed outlier was 16.52. The other two gate values, 10.31 and
10.92, both sit inside the *base* range for that session.

The most economical explanation is that two of the three original gate runs
measured the base binary — the same stale-artifact failure mode that session
documented elsewhere, where a `git stash` left criterion re-reporting an
unchanged `estimates.json`. That is a reconstruction from the arithmetic, not
something established; the raw artifacts are gone.

## Why the VM and not the 16-core rig

`park_block`'s minimum detectable effect is ~11% on this VM, 25% on the rig at 16
cores, and 53% at `smt2x2` (`2026-08-12-resolution-budgets-rig.md`). `Park` is
syscall- and scheduler-bound, and the large shared-tenancy instance is much
noisier for it. Running this gate on the bigger machine would have been the
obvious move and the wrong one.

## What it resolves

`2026-08-11-backoff-isolation.md` found the claim-CAS backoff worth +108% to
+143% under `BusySpin` and **−24%** in the both-sides-blocking corner, and
proposed exactly this fix: a short spin before the first park, since `Park` parked
on the *first* empty observation while `Backoff`'s ladder spends 10 spins and 20
yields first.

+65% on `park_block` more than recovers that 24%. The mechanism proposed there —
that the backoff spaces publishes, so an immediately-parking consumer sees empty
more often and pays a park/unpark pair each time — is now supported by the fix
working, though the park/unpark pairs themselves are still not counted.

## The `busyspin_block` side effect

+16.1%, CI [1.018, 1.304], 16 of 20 pairs. This cell calls `recv` but takes the
`BusySpin` arm, so nothing about its behaviour changed — the patch adds 39 lines
to a branch it never executes.

This is a codegen effect on `recv` itself, and it reproduces: the same cell moved
+10.4% under the original gate, which is where the "a dead branch is not a dead
function" correction in `docs/bench-results/README.md` came from. The interval's
lower bound is close to 1.0, so treat the magnitude as soft. It is a bonus rather
than a reason to merge.

## Verification

| check | result |
|---|---|
| loom, 5 models | pass, including `loom_park_no_lost_wakeup` |
| miri, default features | clean |
| miri, `--all-features` | clean |
| `cargo test --all-features` | 51 passed, 0 failed |
| clippy, all targets | clean |

The spin sits entirely before `prepare_park`, so the
`prepare_park` → `SeqCst` fence → re-check → `park`/cancel ordering
(`docs/design.md` §3) is unchanged. It only delays entry into the protocol and
cannot introduce a lost wakeup — which is what `loom_park_no_lost_wakeup`
independently confirms.

## Limits

- **One machine, one session.** The ratio is robust *within* that session at 20
  pairs; absolute values are not comparable to any other document here.
- **`PARK_SPINS = 64` is untuned.** It was picked because 64 spin hints cost
  ~1.7 µs against a ~10 µs park/unpark pair. No sweep was run, so a better value
  probably exists.
- **Only 2 producers, cap 1024.** The producer ladder was not re-run with this
  change.
- **`Backoff` and `BackoffYield` have no cell in `backoff_isolation`**, so the
  change is unmeasured against them. `Backoff` parks and already spins first, so
  it should be unaffected.
- **Not measured on the rig**, deliberately. The claim is about this machine's
  `Park` path; how it behaves at 16 cores is unknown, and the rig cannot resolve
  it anyway.
