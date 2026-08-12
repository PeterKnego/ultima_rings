# Reading the benchmark results in this directory

Every file here records a measurement taken on one 4-core Linux VM. Two
properties of that box, both discovered the hard way, determine how much any
number in this directory can carry.

## 1. There is a resolution floor of roughly 10%

> **Correction, 2026-08-12.** The evidence originally given for this section was
> misdiagnosed. The claim that a *control* cell moved is wrong: that cell was
> never a control. What the section says about the floor may still hold, but it
> rested on a bad inference and is being re-derived by direct measurement. See
> "What the original evidence actually showed" below.

**Differences below about 10% between two builds cannot be resolved here, no
matter how many rounds are run.**

### What the original evidence actually showed

While gating the pre-park spin (`2026-08-11-backoff-isolation.md` and the
`feat/park-prespin` branch), the `busyspin_block` cell moved **+10.4% with clean
separation across three interleaved rounds**. This was written up as a control
failure, on the grounds that the pre-park spin sits inside the
`WaitStrategy::Park` arm of `Receiver::recv` and a `BusySpin` channel takes the
`BusySpin` arm instead.

That reasoning confused a dead *branch* with an unexecuted *function*.
`busyspin_block` calls `recv()`. The pre-park spin added roughly twenty lines
**inside** `recv()`, so `recv()` is a different function in the two builds — a
different size, different inlining of `try_recv`, a different branch layout.
`busyspin_block` was never a control.

The four corners split exactly along that line:

| corner | calls `recv()`? | delta |
|---|---|---:|
| `busyspin_poll` | no | +0.4% |
| `park_poll` | no | −0.6% |
| `busyspin_block` | **yes** | +10.4% |
| `park_block` | **yes** | +16.0% |

Both cells that avoid `recv()` show nothing. Both that call it moved up. That is
consistent with a genuine codegen effect on `recv()`, and it is **not** evidence
of random build-to-build layout bias.

A true control must exercise no function the change touches — an SPSC cell, for
instance, since `src/spsc.rs` is untouched by any MPSC work.

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

## Roster gaps: surveyed crates that are not in the bake-off

The bake-off currently measures crossbeam-channel, flume, kanal, rtrb (SPSC
only) and `disruptor`. Two crates were surveyed in depth and never measured.

**`thingbuf` — queued, blocked on the resolution-floor work above.** Its survey
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

Nothing new should join the roster until the resolution floor is understood. A
competitor measured at unknown resolution adds numbers, not knowledge.

## Cross-session comparison

Absolute figures are not comparable between sessions. On 2026-08-09 the box ran
about 20% slower than on 2026-08-06 on *unchanged* code — `src/spsc.rs` and the
third-party `rtrb` fell together — and recovered by 2026-08-11
(`2026-08-11-bakeoff-v3.md`). Compare ratios measured within one session.
