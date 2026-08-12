# Reading the benchmark results in this directory

Every file here records a measurement taken on one 4-core Linux VM. Two
properties of that box, both discovered the hard way, determine how much any
number in this directory can carry.

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

## 2. Three rounds is a screen, not a decision

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
- **Watch for byte-identical repeat numbers.** They mean a filter matched
  nothing and criterion re-reported a stale `estimates.json`, usually after a
  `git stash` took the bench code along with the source.

## Roster gaps: surveyed crates that are not in the bake-off

The bake-off currently measures crossbeam-channel, flume, kanal, rtrb (SPSC
only) and `disruptor`. Two crates were surveyed in depth and never measured.

**`thingbuf` — queued and now unblocked.** The resolution work above is done, so
the budget a new competitor would be measured against is known. Its survey
(`docs/superpowers/research/2026-08-06-thingbuf-survey.md`) calls it "the closest
prior art": a fixed-capacity `MaybeUninit`-slot ring with an MPSC channel layer,
loom-tested, the same shape as this crate. It is also load-bearing in the design
document — §9 rejects Vyukov packed stamps citing thingbuf's issues #98 and
#100, and §10's pitfall checklist draws on it as well. Rejecting a design partly
on a crate's bug history while never measuring that crate is the most
conspicuous gap in the roster.

When it is added, note that its natural API is `push_ref`/`pop_ref` returning a
`Ref<T>` for in-place slot reuse — the same ownership model as `disruptor`, where
the queue never moves a `T`. That does strictly less work per element than a
move-in `send(v)`, so it needs the same caveat the `disruptor` cells carry, and
both its by-value and by-reference APIs should be measured separately if both
exist.

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
