# MPSC colocated slot — round number moves into the slot

**Date:** 2026-08-09
**Status:** Approved
**Supersedes as top MPSC lever:** the batched claim, which remains untried and
unspecified.

## Purpose

Cut the MPSC data path from two ping-ponging cache lines per element to one, by
storing each slot's availability round *inside* the slot rather than in a
parallel array.

This is the lever identified by
`docs/bench-results/2026-08-09-mpsc-hotpath-analysis.md`, after both levers
design.md §7 and §8 named were tried and produced nothing:

- Removing the availability-round division changed throughput by less than the
  benchmark's run-to-run spread.
- Padding `avail` to one entry per cache line gained +3.5% in one cell, +2.0%
  for the same shape in another harness, and **−0.1% at cap 4096**. Reverted.

Reading crossbeam-channel 0.5.16 showed a structural difference the crate's own
docs never named: crossbeam's `Slot<T> { stamp, msg }` colocates the readiness
stamp with the message, so `write()` and `read()` each touch one contiguous
struct (`array.rs:214-231, 305-320`). `ultima_rings` keeps `buf` and `avail` as
separate allocations, so a publish writes two lines and a consume reads two.

Per element, crossbeam ping-pongs **one** cache line between producer and
consumer; `ultima_rings` ping-pongs **two**, against a measured ~2.4x throughput
gap. The magnitudes are consistent. This also explains why padding failed:
padding attacks false sharing *within* `avail` by spreading entries apart, while
leaving the per-element line count at two — it pushed in the wrong direction.

**Epistemic status:** the cache-line accounting is an argument from reading both
implementations, not a hardware measurement. `perf` is unavailable on this
machine (not installed, `perf_event_paranoid = 4`, sudo requires an interactive
password). This spec therefore treats the hypothesis as unproven and gates the
change on measurement, not on the argument.

## Why §9's rejection does not apply

`docs/design.md` §9 considers and rejects "Vyukov per-slot stamps / packed state
words". That rejection stands, and this spec does not contradict it — because it
objects to something this design does not do.

§9's objection is to folding readiness into a single per-slot atomic **"that
also encodes the claim/generation"**. That coupling is what forces thingbuf's
`push_ref` to detect and skip a slot a lingering reader still holds, and it is
implicated in thingbuf's open issues #98 and #100.

This design keeps the `claim` cursor and the round number as two independent
observables, exactly as §9 requires. It changes only *where the round is
stored*. The claim protocol, the round semantics, the publication edge, and the
contiguous-prefix drain are all unchanged. Nothing here couples readiness to a
writer's CAS target, so no skip-logic is introduced and the single-consumer
property §9 relies on is untouched.

An earlier note in `2026-08-09-mpsc-hotpath-analysis.md` said the soundness
objection "may not transfer". That understates it: the objection does not apply.

## The change

`src/mpsc.rs` only. `src/spsc.rs` is untouched — it has no availability array,
publishing through `head`/`tail` indices instead.

```rust
/// One ring slot: the availability round and its payload, deliberately in one
/// struct so a publish or a consume touches ONE cache line rather than two.
///
/// `repr(C)` with `round` first is load-bearing, not decoration. It guarantees
/// the round sits at offset 0, so for a large `T` whose value spans several
/// lines the round still shares a line with the *start* of the value — which is
/// what the consumer reads first. Reordering these fields, or adding
/// `align(64)`, silently discards the only reason this type exists.
#[repr(C)]
struct Slot<T> {
    /// Published round (`seq >> shift`); -1 = never published.
    round: AtomicI64,
    value: UnsafeCell<MaybeUninit<T>>,
}
```

`Shared<T>` loses `buf` and `avail`, gaining `slots: Box<[Slot<T>]>`.

Seven mechanical sites:

| Site | Before | After |
|---|---|---|
| `Shared<T>` fields | `buf`, `avail` | `slots` |
| `channel()` | builds two boxed slices | builds one |
| `Shared::drop` | `avail[slot].load` then `buf[slot]` drop | `slots[slot].round.load` then `.value` drop |
| `try_send` publish | write `buf[i]`, store `avail[i]` | write `slots[i].value`, store `slots[i].round` |
| `slot_published` | `avail[i].load` | `slots[i].round.load` |
| `try_recv` value read | `buf[head & mask]` read | `slots[head & mask].value` read |
| `drain` hot loop | `avail[slot].load`, `buf[slot]` read | `slots[slot].round.load`, `.value` read |

(An earlier revision of this table said six and omitted `try_recv`'s value read.
`try_recv` gets the round via `slot_published` but reads the payload directly,
so it is a distinct site.)

**No `align(64)`.** Slots stay packed. Aligning each slot to its own line is the
padding experiment that already failed, and it would defeat the point.

## What does not change

Stated explicitly because it is the argument for the verification bar below:

- **Orderings.** Publish is still slot-write then `Release` store of the round;
  consume is still `Acquire` load of the round then slot read. Same edges, same
  orderings, same count of atomic operations.
- **The claim protocol.** Bounded-CAS on `claim`, unchanged.
- **Round semantics.** `seq >> shift`, `-1` sentinel, unchanged.
- **The API.** No public signature changes.
- **`unsafe` operations.** The same `MaybeUninit::write` / `assume_init_read` /
  `assume_init_drop` calls, each still scoped to a single `with`/`with_mut`
  closure adjacent to its Release/Acquire edge — just reached through
  `slots[i].value` instead of `buf[i]`.

## Consequences to state up front

**Packing density changes.** For `T = u64`, `Slot<T>` is 16 bytes — 4 slots per
64-byte line, where today `buf` holds 8 payloads and `avail` 8 rounds per line.
Per element the design goes from two lines to one, but adjacent slots share a
line more densely, so producers writing neighbouring sequences contend on one
line where they previously contended on two. Whether the trade is favourable is
exactly what the gate measures.

**Large `T` dilutes the benefit.** Beyond roughly 56 bytes the value spans
multiple lines and the round shares only the first. Never worse than the current
two-array layout, but the gain shrinks toward zero.

**Zero-sized `T` is unaffected.** `Slot<()>` is just the round.

## Verification

The existing suite must pass with **zero changes to any test or loom model**:

- 51 tests, both feature configurations
- 5 loom models (`RUSTFLAGS="--cfg loom" cargo test --test loom --release`)
- miri, both feature configurations
- `cargo clippy --all-features --all-targets -- -D warnings`, `cargo fmt --check`,
  `cargo doc` with no warnings

That an unchanged model suite passes is the substantive evidence, not a
formality. The loom models exercise the ordering edges this change relocates but
does not alter; if moving the round broke a Release/Acquire pairing, the
existing models would catch it. Writing new models would drive the same edges
under new names.

`survives_many_ring_wraps` (added with the shift, `src/mpsc.rs`) is the most
directly relevant existing test: a wrong round computation or a mis-wired slot
is invisible until the ring wraps and slot indices repeat.

## The gate

Measured with `mpsc_layout_probe` across all three configurations, on a box
verified quiet by `vmstat` (85%+ idle, 0 runnable — not by load average, which
lags badly here), built to completion before measuring, and run interleaved
A-B-A-B against the current layout rather than all-of-one-then-all-of-the-other.

**Keep only if it improves at all three of `cap1024_p2`, `cap4096_p2` and
`cap1024_p4`, by more than each cell's run-to-run spread.**

The all-three requirement is the direct lesson of the padding round, which
passed a single-cell gate at +3.5% with p < 0.01 and then measured −0.1% at cap
4096. A layout change that helps at one capacity and not another has not been
shown to be an improvement.

**If it fails, revert and record it.** That outcome would be genuinely
informative: with the division gone, false sharing addressed and rejected, and
colocation rejected too, the MPSC gap would have survived every layout and
arithmetic hypothesis — pointing at the claim protocol itself, and making the
batched claim the remaining candidate.

Results to `docs/bench-results/<date>-colocated-slot.md`, with design.md §8 and
§9 updated either way: §8 gains the outcome, §9 gains a note distinguishing this
layout change from the Vyukov protocol it already rejects.

## Out of scope

The batched claim; any API or semantics change; `src/spsc.rs`; `src/sharded.rs`;
`align(64)` on slots (already measured and rejected as padding); async; and
adding loom models or layout-assertion tests (the existing suite is the agreed
bar).

## Known stale documentation to fix alongside

`docs/design.md` §10's pitfall-checklist row on heapless's division-regression
class still reads "MPSC's availability-round computation (`seq / cap`) does
execute as a runtime division on the hot publish/consume path (a known v1 cost,
noted in §7); the v2 optimization is precomputed shifts." The shift shipped in
`170318d`; §7 was updated and §10 was missed. Correct it in this round.
