# CAS backoff: the dominant MPSC cost, found

**Date:** 2026-08-11

**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap. The box was at 88% idle with
0 runnable tasks.

**Change:** commit `70193fd`. It adds an exponential `spin_loop` backoff between
failed `compare_exchange_weak` attempts on the claim cursor.

The backoff grows 1, 2, 4 up to 64. It resets on each call to `try_send`. The
change moves no memory order. It adds no atomic. It changes no semantics.

All throughput figures below are in millions of elements each second (Melem).

## Result

`mpsc_layout_probe`, in A-B-A blocks. Six runs with the backoff against three
runs without it.

| Cell | baseline (mean) | backoff (mean) | delta | spread between runs |
|---|---:|---:|---:|---:|
| cap1024_p2 | 31.41 | **76.40** | **+143%** | 4.4% |
| cap4096_p2 | 36.41 | **75.88** | **+108%** | 4.6% |
| cap1024_p4 | 26.38 | **62.58** | **+137%** | 9.6% |

No cell shows an overlap between the two distributions. The highest run in the
baseline block is 37.42. The lowest run with the backoff is 59.62. Each delta is
more than 10 times the spread of its own cell.

The gate for three configurations passes by a margin that no earlier change on
this crate came near. A second harness confirms the result. The bake-off cell
gave a spread of 0.5% across three runs.

| | Melem | compared to crossbeam |
|---|---:|---:|
| **`ultima_rings::mpsc`** | **76.41** | **1.26x** |
| crossbeam-channel | 60.76 | 1.00x |

**The change reverses the MPSC gap. It does not only close it.** The ratio moved
from 0.55x to 1.26x.

## This answers the question of the last three rounds

`docs/design.md` §7 and §8 named two candidate costs. Both were measured. Neither
accounts for the gap.

| Lever | Result |
|---|---|
| Shift in place of divide (§7) | inside the noise |
| Padded availability array (§8) | +3.5% at one configuration, then −0.1% at another. Reverted |
| Colocated slot | +12 to 15%. Kept |
| **CAS backoff** | **+108 to 143%** |

Both null results are now clear. The removal of a hardware division did nothing.

The `avail` pad also did nothing. It gave one entry to each cache line. Neither
lever was the bottleneck.

The real bottleneck was the contended cache line of the claim cursor. Each
producer struck that line as fast as the core permits.

The gain of 12 to 15% from the colocated slot is a real second-order effect. It
is small beside this one.

`docs/bench-results/2026-08-09-mpsc-hotpath-analysis.md` measured a CAS failure
rate of 22 to 42% at 2 to 4 producers. That analysis could not separate the retry
cost from the cache traffic that the retries cause. Both come from the same
concurrency.

This result separates them. All other conditions stay the same. More space
between the retries recovers 2.1x to 2.4x.

## How close this came to no test at all

The lever first appeared as "crossbeam backs off and we do not". The disruptor
survey then found that the maintained Rust LMAX port does not back off either.
The record downgraded the lever to "two of three say no, so it is a coin flip".

That method was wrong, and not only the conclusion. A count of the
implementations that make a choice is not evidence about the cost of a workload.
The survey was useful to find the candidate. It was useless to judge it. Three
lines of code and one A-B-A run settled what the vote could not.

## Scope and limits

- **The uncontended path does not change.** With a single producer the claim CAS
  never fails, so the backoff never runs. `src/spsc.rs` does not change.

- **The ceiling is 64 `spin_loop` hints.** This matches the spin ceiling of
  `crossbeam-utils`. The counter resets on each `try_send` call.

- **I did not tune the values.** The 1 to 64 exponential growth comes from
  crossbeam. I ran no sweep. A better ceiling or growth factor is possible.

- **I measured 2 and 4 producers on 4 cores.** For a producer count above the
  core count, I have no measurement.

- **`sharded` does not change.** It held 306 to 308 Melem across these runs. It
  has no shared claim to contend on, which is the purpose of that design.

## Verification

51 tests pass. The 5 loom models pass. Miri reports 50 passed, 1 ignored, and 0
undefined behaviour. Clippy with `--all-features --all-targets -D warnings` is
clean. `cargo fmt --check` is clean.

I edited no test and no loom model. The change affects only the time between
retries. It leaves every memory-order edge that the models cover untouched.
