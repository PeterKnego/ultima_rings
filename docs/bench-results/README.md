# Reading the benchmark results in this directory

Every file here records a measurement taken on one 4-core Linux VM. Three
things, all discovered the hard way, determine how much any number in this
directory can carry.

## 1. Each cell has its own resolution budget, measured

**Layout is worth about 5%, and per-cell intrinsic noise ranges from 1% to 9%.**
Both were measured directly rather than inferred: the same source built at five
different function alignments, where every difference between builds is layout by
construction (`2026-08-12-layout-sensitivity.md`).

| Cell | layout spread | intrinsic noise | minimum detectable effect |
|---|---:|---:|---:|
| `busyspin_poll` | 5.0% | 1.1% | ~6% |
| `busyspin_block` | 4.6% | 1.5% | ~6% |
| `park_poll` | 4.4% | 1.6% | ~6% |
| `spsc` | 9.3% | 7.5% | ~9% |
| `park_block` | 10.8% | 9.1% | ~11% |

Two things follow. Layout is a real effect roughly three times measurement noise,
so a difference near 5% between two builds may be nothing but code placement. And
`park_block` and `spsc` are not layout-sensitive so much as simply noisy — their
variance shows up without any rebuild, so building differently will not fix it.

To resolve an effect near a cell's budget, build each variant at several
alignments and pool the results; see the recipe in
`2026-08-12-layout-sensitivity.md`.

### An earlier version of this section was wrong twice

It claimed a "~10% floor" on the strength of a control cell that moved on a path
the change never executed. Both halves were mistaken.

The cell was never a control. `busyspin_block` calls `recv()`, and the change
under test added roughly twenty lines **inside** `recv()`, so `recv()` was a
different function in the two builds. The reasoning confused a dead *branch* with
an unexecuted *function*. The four corners split exactly along that line — the
two that avoid `recv()` moved +0.4% and −0.6%, the two that call it moved +10.4%
and +16.0%.

And the floor itself was too pessimistic by half: measured, layout is ~5% for
well-behaved cells, not 10%.

**A true control must exercise no function the change touches** — an SPSC cell,
for instance, since `src/spsc.rs` is untouched by MPSC work. Not merely a cell
whose branch is not taken.

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

- **Interleave by round**, never all runs of one variant then the other. Box
  conditions drift over the tens of minutes a comparison takes, and a block
  design lets that drift look like an effect.
- **Judge box quietness with `vmstat`**, not load average. Load average has read
  above 2.0 on this machine while `vmstat` showed 90% idle.
- **Build to completion before measuring.** Never let a build overlap a run.
- **Include a control cell** that calls no function the change touches. A cell
  whose branch is merely not taken is not a control — see §1.
- **Gate across the axis the change acts on.** The CAS backoff's gate covered
  three configurations that varied capacity and producer count while holding the
  wait strategy fixed. It was thorough on the wrong axis and missed a 24%
  regression in `Park` mode (`2026-08-11-bakeoff-v3.md`).
- **Report CPU alongside wall time** whenever a spinning config is compared to a
  parking one. `examples/cpu_cost.rs` does this; the criterion groups do not.
- **Vary the payload, not just the crate.** Switching `u64` to a 64-byte
  `String` moved competitors by between 1.6x and 15.8x and reversed one ranking
  outright. A roster measured at one payload is a roster measured at one point.
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

**A `String` cell was added the same day** (`bakeoff_mpsc_string`,
`2026-08-12-cpu-cost-and-heap-payload.md`) and it reversed that result: on a
heap-owning payload thingbuf's reference API is 1.82x this crate. Quote the two
together or neither.

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

## Cross-session comparison

Absolute figures are not comparable between sessions. On 2026-08-09 the box ran
about 20% slower than on 2026-08-06 on *unchanged* code — `src/spsc.rs` and the
third-party `rtrb` fell together — and recovered by 2026-08-11
(`2026-08-11-bakeoff-v3.md`). Compare ratios measured within one session.
