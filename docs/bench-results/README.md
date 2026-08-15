# Reading the benchmark results in this directory

Most files here record measurements taken on one small Linux VM that reports 4
CPUs and has **2 physical cores** with SMT. Files dated 2026-08-12 and later may
instead come from the 16-core rig in `bench-infra/`; each states its host. Four
things, all discovered the hard way, determine how much any number in this
directory can carry.

**Thread placement is a first-order variable and this harness did not control it
until 2026-08-15.** Moving two threads from one physical core to two costs 1.41x
(rtrb), 1.88x (this crate) and 5.53x (crossbeam) — and it *inverts* the ordering:
this crate leads rtrb 1.16x when threads share a core and trails 0.87x when they
do not (`2026-08-15-thread-placement.md`). These benchmarks are handoff-bound, so
extra cores add no parallelism and only lengthen the path a cache line travels.

Consequences: **competitor ratios are the least trustworthy numbers here**, every
figure recorded before 2026-08-15 has placement uncontrolled inside it, and a
ratio needs a placement as well as a machine and a core count. Comparisons of
this crate against *itself* are much safer, because an A/B holds placement
roughly constant between the arms.

**Quote no ratio without both a machine and a core count.** The MPSC lead over
crossbeam measures 1.88x on the 4-vCPU VM, a tie at the same topology on a Xeon,
and 1.25x at 16 cores (`2026-08-15-bakeoff-rig.md`). Parity with rtrb, reported
across five sessions, turned out to be a property of the VM — on the Xeon this
crate leads rtrb 1.67-1.80x at both topologies.

**Before quoting any wait-strategy number, check the core count it was taken on**
(`2026-08-12-topology-sweep.md`). `BackoffYield`'s lead over `BusySpin` under
oversubscription is 12.3x on 2 cores and 1.2x on 16 — the same effect, measured
either side of an 8x topology change. And before believing any difference,
check it against the per-cell budget below, which also depends on the machine.

## 1. Each cell has its own resolution budget, and it depends on the machine

Measured directly rather than inferred, by building the same source at five
function alignments so every between-build difference is layout by construction.
**Minimum detectable effect**, per cell, per machine:

| cell | 4-vCPU VM | rig, 16 cores | rig, `smt2x2` |
|---|---:|---:|---:|
| `busyspin_poll` | ~6% | **3%** | **2%** |
| `busyspin_block` | ~6% | **3%** | **3%** |
| `bakeoff_mpsc/ultima` | — | **2%** | **3%** |
| `spsc` | ~9% | 17% | 37% |
| `park_block` | ~11% | 25% | 53% |
| `park_poll` | ~6% | 62% | 94% |
| `crossbeam` | — | 22% | 40% |
| `flume` / `kanal` | — | 41–43% | 13–19% |

Sources: `2026-08-12-layout-sensitivity.md` (VM),
`2026-08-12-resolution-budgets-rig.md` (rig).

**The two machines are good at opposite things.** Spin-path cells are 2–3x
tighter on the rig; `Park` cells are 6–15x worse there. So the machine is part
of the experiment design: run spin-path A/Bs on the rig, and anything depending
on `park_block` or `park_poll` on the small VM. Bigger is not better.

**Competitor cells cannot support two-decimal ratios.** crossbeam sits at 22–40%
MDE, so any bake-off ratio quoted against it inherits that. Directions are
trustworthy; trends across topologies mostly are not.

**On the rig, layout is not separable from run-to-run noise** for essentially any
cell — the two statistics come out the same size, which is what a null layout
effect looks like at this sample size. That does not make layout irrelevant; it
means the MDE column is the only thing to plan against.

## 2. Wall-clock throughput hides what the spinning costs

A spinning ring converts idle cores into throughput, and elements-per-second does
not show the cores it spent. On this 4-core box that flatters every `BusySpin`
cell against every parking competitor. `examples/cpu_cost.rs` measures the other
half via `/proc/thread-self/schedstat`, and the two halves point opposite ways:

| | saturated (cpu ns/elem) | idle, 1 elem per 200 µs (% of a core) |
|---|---:|---:|
| `BusySpin` | 32.4 — cheapest in the roster | 99.9% |
| `Park` | 221.1 — most expensive | 1.8% |

Under saturation nothing ever parks, so parking machinery is all cost; when idle,
it is 57x cheaper per element. Neither number alone is the answer, and a
throughput table showing only the first is not wrong so much as half-reported.

**The thread-to-core ratio is a third axis, and it reorders the strategies.**
Every criterion cell in this repo runs at most three threads on four cores. Past
that ratio `BusySpin` collapses — 69.11 Melem/s at 2 producers, 4.84 at 32 —
while `BackoffYield` holds 35.65 and costs less CPU per element doing it. A
strategy comparison drawn at or below core count does not transfer above it,
which is how this directory briefly concluded that `BackoffYield` had no
purpose.

**Core count changes the magnitude of every strategy result, sometimes by 10x.**
A comparison taken on 2 cores is a statement about 2 cores. The sweep in
`2026-08-12-topology-sweep.md` is the reference; the rig that produced it is
`bench-infra/`, which pins one binary on one host with `taskset` so only the
visible CPU count varies.

**And *what* the extra threads are doing reorders them again.** Oversubscribing
with the channel's own producers favours yielding; oversubscribing with threads
that never touch the channel favours parking, because yielding to a stranger
surrenders a slice and gets nothing back. `Park` goes from worst in every idle
table to best and most stable under external load.

**A wait strategy has a cost that lands outside the benchmark.** Measured as the
external threads' throughput relative to running alone, `BusySpin` keeps 50% on
2 cores, 77% on 4 and 94% on 16, while `Backoff` and `BackoffYield` stay between
96% and 100% at every size. Nothing in a throughput table can see the rest of
the process losing half its work.

## 3. Three rounds is a screen, not a decision

Twice in one session a three-round result reversed under five rounds:

| Question | 3 rounds | 5 rounds |
|---|---|---|
| Backoff ceiling 64 against 256 at 4 producers | 256 by +2.9% | 64 by +1.5% |
| Backoff ceiling 16 against 64 at 64 producers | 16 by +6.7% | 64 by +1.3% |

Both were 3–7% effects — at or below the ~6% budget for the cells involved —
and both had a plausible mechanism ready to explain them, which is exactly what
made them convincing. Require either separation well clear of the cell's budget
above, or a five-round confirmation.

## Practices that these findings produced

- **Pair the arms, do not just interleave them.** Measure A and B adjacently at
  the same alignment in the same round and compare per-pair ratios. Drift and
  layout then cancel inside the ratio instead of being averaged over, and both
  arms see the same five layouts. This is what turned the pre-park spin from
  "inconclusive" into "+65%, 20 of 20 pairs"
  (`2026-08-13-park-prespin-gate.md`).
- **Never compare against a baseline from another run.** `park_block` measured
  11.02 in one session and 7.61 in another on identical source — a 45% gap, four
  times its own within-session budget. A cross-run A/B cannot resolve anything.
- **Judge box quietness with `vmstat`**, not load average. Load average has read
  above 2.0 on this machine while `vmstat` showed 90% idle.
- **Build to completion before measuring.** Never let a build overlap a run.
- **Include a control cell** that calls no function the change touches. A cell
  whose branch is merely not taken is not a control — see §1. Note the limit:
  such a control rules out *behavioural* coupling, not *layout* coupling, because
  both modules compile into one binary and changing one shifts where the other
  lands (`2026-08-14-backoff-cells.md` §4). A control immune to layout would have
  to live in a separate binary.
- **Prefer several cells that should agree over one that should not move.**
  `backoff_isolation`'s three self-waking `*_poll` cells are identical code paths
  and sit within 1.6% of each other; if they ever diverge the harness is broken.
  That catches more than a single control does.
- **Gate across the axis the change acts on.** The CAS backoff's gate covered
  three configurations that varied capacity and producer count while holding the
  wait strategy fixed. It was thorough on the wrong axis and missed a 24%
  regression in `Park` mode (`2026-08-11-bakeoff-v3.md`).
- **Report CPU alongside wall time** whenever a spinning config is compared to a
  parking one. `examples/cpu_cost.rs` does this; the criterion groups do not.
  A throughput-only gate let the pre-park spin ship at a constant that raised
  `Park`'s idle CPU 35% for no throughput gain
  (`2026-08-14-park-spins-sweep.md`). Tuning a spin count against throughput
  alone will always pick too large a number, because the cost lands somewhere
  the benchmark is not looking.
- **Vary the payload, not just the crate.** Switching `u64` to a 64-byte
  `String` moved competitors by between 1.6x and 15.8x and reversed one ranking
  outright. A roster measured at one payload is a roster measured at one point.
- **Replicate on a second machine before generalizing.** The `String` cell
  looked like a clean design finding on one box and failed to reproduce at
  matched topology on another. `bench-infra/` exists for this.
- **`taskset` does not pin the allocator.** glibc sizes its arena pool from the
  whole machine, so a core-count sweep does not control allocation-bound cells.
- **Watch for byte-identical repeat numbers.** They mean a filter matched
  nothing and criterion re-reported a stale `estimates.json`, usually after a
  `git stash` took the bench code along with the source.

## Roster gaps: surveyed crates that are not in the bake-off

The bake-off measures crossbeam-channel, flume, kanal, rtrb (SPSC only),
`disruptor` and `thingbuf`.

**`thingbuf` — measured 2026-08-12** (`2026-08-12-thingbuf.md`), closing what was
the most conspicuous gap in the roster: the design document rejects Vyukov packed
stamps partly on thingbuf's bug history (§9) while never measuring the crate. It
sits at 0.26x crossbeam by value and 0.32x by reference, and its blocking path
is 1.68x this crate's `Park`.

**A `String` cell was added the same day** (`bakeoff_mpsc_string`) and it is the
least trustworthy cell in this directory. thingbuf's reference API measures
3.199x crossbeam on the 4-CPU VM and 0.531x at the same topology on an Intel
host — 6x apart, opposite sides of the baseline
(`2026-08-12-topology-sweep.md`). Some of that is glibc arena count, which
`taskset` does not constrain; most of it is unexplained. **Quote no direction
from this cell.**

By contrast the `u64` and `Park` cells replicate well across both machines —
`ultima` leads crossbeam by 1.08x–1.61x and `ultima_park` sits at 0.25x–0.29x
everywhere.

`disruptor` still has no `String` cell. It shares thingbuf's in-place model, so
it is the remaining crate measured only where its design cannot show.

**`heapless::spsc::Queue` — not queued.** A second no-alloc SPSC comparator
beside rtrb, and cited in §10 for the division-regression class (issue #650) that
motivated this crate's `& mask` indexing. Const-generic capacity makes the
usage model differ, though its `split()` erases `N` into a `ViewStorage`, which
brings it closer to this crate's shape than it first appears.

**`std::sync::mpsc` — not queued.** Unsurveyed because since Rust 1.67 it is
crossbeam-derived, so a row would largely duplicate the crossbeam one. Still
arguably worth adding, since it is the baseline most readers start from and
showing "std is crossbeam here" empirically answers the first question they ask.

**Not candidates.** `tokio::sync::mpsc` is async and linked-block rather than a
ring. `concurrent-queue` (bounded MPMC, used by async-channel and smol) is
genuinely relevant but unsurveyed, and this project's order is survey first,
benchmark second — the `disruptor` round showed why.

Competitor gaps are usually large (this crate against flume is 10x, against
crossbeam 1.3x), so most roster comparisons clear the budgets in §1 easily. The
budget matters when a competitor lands within a few percent — report that as a
tie rather than a ranking.

## Two machines, and which to use for what

| question | machine | why |
|---|---|---|
| `Park` anything | the 4-vCPU VM | `park_block` MDE ~11% there, 25-53% on the rig |
| spin-path A/Bs | the rig | 2-3% MDE against the VM's ~6% |
| any competitor ratio | **both**, and pin | they disagree at matched topology, and placement alone spans the whole disagreement |

`bench-infra/` provisions the rig; `make sweep` and `make layout` are the entry
points. Always `make destroy`.

## Report ties as ties

Where two cells' ranges overlap, say "tie" rather than ranking them. Given
crossbeam's 22–40% budget (§1), that rule applies more often than the tables
suggest — v4 turned two of v3's stated wins into ties, including this crate
against rtrb on SPSC, which four sessions had been reporting as a small win or
a small loss when it was neither.

## Cross-session comparison

Absolute figures are not comparable between sessions, and the gap can be much
larger than it first appeared: on 2026-08-14 the box ran roughly **2.4x** slower
than on 2026-08-12 on unchanged competitor code (crossbeam MPSC 58.64 → 24.73).
Earlier, on 2026-08-09 the box ran about 20% slower than on 2026-08-06 on
*unchanged* code — `src/spsc.rs` and the
third-party `rtrb` fell together — and recovered by 2026-08-11
(`2026-08-11-bakeoff-v3.md`). Compare ratios measured within one session.
