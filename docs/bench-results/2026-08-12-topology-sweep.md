# The 4-core box is 2 cores: re-running the wait-strategy results on 16

**Date:** 2026-08-12
**Host:** AWS c7i.8xlarge — Intel Xeon Platinum 8488C, 32 vCPU = **16 physical
cores**, 1 socket, 1 NUMA node, THP off, Ubuntu, rustc 1.97.1
**Rig:** `bench-infra/` (adapted from `ultima_db/bench-infra`)
**Method:** one host, one binary built once, pinned with `taskset` at six
topology points. CPU model, kernel, memory and code layout are therefore
identical across points and the only variable is how many CPUs the workload can
see. `URINGS_CORES` scales the thread counts so "2x oversubscribed" means the
same thing at every point.

## Why

Every result in this directory before today was measured on a VM reporting 4
CPUs. That VM has **2 physical cores** with SMT — CPUs 0/1 are siblings, 2/3 are
siblings. So statements of the form "3 threads on 4 cores, so not
oversubscribed" described three runnable threads on two real cores.

The topology points, with `smt2x2` reproducing that VM's exact shape on
different silicon:

| point | CPUs | physical cores |
|---|---:|---:|
| `phys2` | 2 | 2 |
| `smt2x2` | 4 | 2 |
| `phys4` | 4 | 4 |
| `phys8` | 8 | 8 |
| `phys16` | 16 | 16 |

---

## 1. Idle CPU is topology-invariant — confirmed

Consumer CPU as a fraction of one core, one element per 200 µs:

| strategy | phys2 | smt2x2 | phys4 | phys8 | phys16 |
|---|---:|---:|---:|---:|---:|
| `BusySpin` | 100.0% | 100.0% | 100.0% | 100.0% | 100.0% |
| `BackoffYield` | 100.0% | 100.0% | 100.0% | 100.0% | 100.0% |
| `Backoff` | 5.2% | 5.2% | 5.2% | 5.2% | 5.2% |
| `Park` | 3.2% | 3.1% | 3.2% | 3.2% | 3.2% |

Identical to three significant figures across an 8x range of core counts. The
"busy-spinning burns 99.9% of a core" and "`BackoffYield` saves nothing" findings
carry over untouched.

Two figures shift with the machine rather than the topology: `Backoff` measured
10.2% on the original VM against 5.2% here, and `Park` 1.8% against 3.2%. Both
are ladder-timing artifacts of a different CPU, so quote them as "roughly a tenth
of a core" and "a few percent", not to one decimal.

## 2. The threshold is schedulable CPUs, not physical cores

This is the correction. Compare 3 threads (2 producers + 1 consumer) on 2 CPUs
against the same 3 threads on 4 CPUs that are only 2 physical cores:

| | CPUs | cores | `BusySpin` | `BackoffYield` |
|---|---:|---:|---:|---:|
| `phys2` @ 0.5x | 2 | 2 | **7.70** | 32.95 |
| `smt2x2` @ 0.5x | 4 | 2 | **32.38** | 31.80 |

Same physical core count, 4.2x difference in `BusySpin`. Adding hyperthreads —
not cores — restores it completely.

So the collapse is not about parallel execution capacity. It is about whether
every runnable thread has *somewhere to be scheduled*. With 3 threads and 2 CPUs
one thread is always off-CPU, and if a spinner holds a CPU the peer it is waiting
on cannot run. An SMT sibling is a poor execution resource but a perfectly good
*runqueue slot*, which is all this mechanism needs.

**`available_parallelism()` was the right unit after all**, for the wrong reason.
The earlier documents were correct to count 4, and wrong to call those 4 "cores".

## 3. `BackoffYield` beats `BusySpin` at every topology — but the size was a small-machine number

Ratio of `BackoffYield` to `BusySpin`, by oversubscription:

| topology | 0.5x | 1x | 2x | 8x |
|---|---:|---:|---:|---:|
| `phys2` | 4.28x | 5.35x | 7.05x | **12.34x** |
| `smt2x2` | 0.98x | 1.22x | 1.26x | 10.20x |
| `phys4` | 1.02x | 1.43x | 3.39x | 4.61x |
| `phys8` | 0.93x | 1.41x | 2.35x | 2.36x |
| `phys16` | 0.96x | 1.27x | 2.50x | **1.23x** |

Three things follow.

**The published result replicates.** `2026-08-12-cpu-cost-and-heap-payload.md`
reported 7.37x at 8x oversubscription on the 4-CPU VM. `smt2x2` — that VM's
topology on entirely different silicon — gives 10.20x. Same regime, same
direction, same order of magnitude. The mechanism claim was right.

**It does not generalize to large machines.** The same ratio is 1.23x at 16
cores. One wasted core out of two is half the machine; out of sixteen it is 6%.
Anywhere the crate's own README quotes an oversubscription figure it must name
the core count, because 12.34x and 1.23x are the same effect measured either side
of an 8x topology change.

**Below saturation the strategies are indistinguishable everywhere.** The 0.5x
column is 0.93x–1.02x at every point with enough CPUs, which matches the earlier
finding and now has four more machines behind it.

## 4. `BusySpin`'s damage to neighbours shrinks as the machine grows

External throughput retained, with one CPU-bound thread per core outside the
channel:

| strategy | phys2 | smt2x2 | phys4 | phys8 | phys16 |
|---|---:|---:|---:|---:|---:|
| `BackoffYield` | 98% | 98% | 96% | 99% | 100% |
| `Backoff` | 96% | 99% | 97% | 98% | 100% |
| `Park` | 70% | 75% | 84% | 87% | 95% |
| `BusySpin` | **50%** | **75%** | 77% | 88% | 94% |

The original VM measured `BusySpin` at 77%. `smt2x2` gives 75% and `phys4` gives
77% — the published number reproduces exactly, and it is a 4-CPU number.

On two real cores a spinning consumer takes **half** the machine's useful work
from the rest of the process. On sixteen it takes 6%. Both are the same
arithmetic: a spinner occupies one CPU, and one CPU is a larger share of a
smaller machine. `BackoffYield` and `Backoff` stay between 96% and 100%
throughout and are the only strategies that are cheap neighbours at every size.

## 5. Under external load, `Park` wins decisively — and the original box understated it

Channel throughput with the machine already busy (Melem/s):

| strategy | phys2 | smt2x2 | phys4 | phys8 | phys16 |
|---|---:|---:|---:|---:|---:|
| **`Park`** | **10.72** | **4.07** | **11.95** | **14.63** | **10.42** |
| `Backoff` | 1.09 | 0.78 | 1.47 | 0.75 | 1.19 |
| `BackoffYield` | 0.59 | 1.17 | 1.31 | 0.78 | 0.57 |
| `BusySpin` | 0.27 | 3.05 | 2.40 | 0.60 | 0.73 |

`Park` leads at all five points, by **5.0x** at `phys4`, **24x** at `phys8` and
**14x** at `phys16`. The earlier document reported 2.5x on the 4-CPU VM and
hedged it as "stability rather than a win" because one sample overlapped. That
caution was warranted for that data and the effect is much larger than it could
see: `smt2x2` reproduces the weak version (1.33x) and every point with real cores
shows 5x or more.

`Park` also keeps 70–95% of external throughput, comparable to `BusySpin` or
better. So when the machine is busy with unrelated work, `Park` is both the
faster channel and the kinder neighbour.

**This inverts the crate's headline.** `Park` is 6x slower than `BusySpin` on an
idle box and up to 24x faster on a busy one. Nothing in `docs/design.md` says so,
because until now every measurement was taken on an idle box.

The mechanism is the mirror of finding 2: yielding pays when the thread you yield
to is the one you are waiting on, and external threads are strangers who will
never publish. Parking leaves the runqueue entirely and the futex wake schedules
the consumer precisely when work exists, rather than competing for slices it
cannot use.

## What has to change

- **`src/wait.rs`** must state the core count beside every oversubscription
  figure, and must not present `Park` as simply the slow option.
- **`docs/design.md`** has no statement about behaviour under external load,
  which is where `Park` is 5–24x ahead.
- **Nothing measured is retracted.** Every published figure reproduces at the
  topology it was taken on. What was wrong was calling 4 CPUs "4 cores" and
  generalizing 2-core magnitudes.

## Limits

- **One sweep, no repeats across points.** Each point is `cpu_cost.rs`'s own
  median of 3, but the sweep itself ran once. The `phys16` 8x cell (1.23x, out of
  line with 2.50x at 2x) is the one most likely to move on a repeat.
- **One socket, one NUMA node.** Cross-socket behaviour is untested and is where
  a claim cursor would be expected to behave worst.
- **SMT points are limited to `smt2x2` and `smt8x2`.** The interaction of SMT
  with oversubscription at large core counts is not mapped.
- **No cpufreq control.** EC2 exposes no governor; turbo is on. Deliberate — see
  `bench-infra/README.md`.
- **Absolute values do not transfer** to or from the original VM. Only the
  matched-topology rows (`smt2x2`) are meant to be compared with the older
  documents, and then as ratios.
