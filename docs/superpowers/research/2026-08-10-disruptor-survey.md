# Survey: `disruptor` 4.4.0 (nicholassm) — the batched claim in practice

**Date:** 2026-08-10
**Crate:** `disruptor` 4.4.0, the maintained Rust port of the LMAX Disruptor
**Why surveyed:** the batched claim is the last untried lever for `src/mpsc.rs`
(`docs/bench-results/2026-08-09-mpsc-hotpath-analysis.md`). This crate ships it,
recommends it, and is the closest structural relative of this one. It is the only
implementation that can answer whether the batched claim's costs are solved,
lived with, or avoided by design.

Read: `producer/multi.rs` (450 lines, the whole multi-producer claim and
availability protocol), `ringbuffer.rs`, `wait_strategies.rs`.

## The headline: publication is a bitmap, not a per-slot word

```rust
/// AtomicU64s each track availability of 64 slots.
/// Each bit in the AtomicU64 encodes whether the slot was published in an even or odd round.
available: Box<[AtomicU64]>,
```

One **bit** per slot, 64 slots per word, the bit encoding round parity (even/odd)
rather than a round number. At cap 1024 the entire availability structure is
**128 bytes**; this crate's is 8 KiB as a separate array, or one `AtomicI64` per
slot colocated after `cf74e97`.

- **Publish one:** `availability.fetch_xor(1 << bit, Release)` — flip the bit.
- **Publish a range** (`publish_range`, multi.rs:303-330): accumulate a
  `flip_mask` across the batch and commit **one `fetch_xor` per 64-slot word**.
  A 64-item batch costs **one** atomic RMW.
- **Consume** (`get_after`, multi.rs:334-366): one Acquire load of a word, then
  walk bits in-register. Checking 64 slots' availability is one load.

This is the piece that makes batching pay, and it is a bigger idea than the
batched claim itself. Batching only the *claim* still costs N publication stores;
batching claim **and** publication over a bitmap collapses both to O(1) per 64.

**The trade, stated fairly.** Their availability word is shared by 64 slots and
written with a contended read-modify-write; ours is per-slot and written with a
plain `Release` store to a line the producer already owns (post-colocation, the
same line as the payload). For *single* publication ours is very likely cheaper —
a `store` beats a `fetch_xor`, and an uncontended line beats a shared one. For
*batched* publication theirs is dramatically cheaper. The two designs are
optimised for different call shapes, and the bitmap is not a free upgrade.

## Answers to the three objections raised against the batched claim

### 1. Head-of-line blocking — real, unsolved, acknowledged

`get_after` walks forward from the consumer's position and returns at the first
bit whose parity does not match (multi.rs:343-345). A producer that has claimed a
batch but not yet published it stops the consumer dead, and everything other
producers published behind it is unreachable until that producer finishes. The
source comments acknowledge the coupling directly: producers "can never overtake
each other", all bounded by the slowest consumer.

So the concern was correct, and a mature implementation simply lives with it.
That is a defensible choice for the Disruptor's target — a pinned, spinning,
throughput-first pipeline — and a worse one for a design whose stated target is a
latency-critical path.

### 2. Panic mid-batch — permanent consumer stall, no guard

`apply_updates` (multi.rs:210-225) calls the user's closure and *then*
`publish_range`. A panic inside the closure means the claimed sequences are never
published, `get_after` stops at that hole forever, and the consumer never
advances again. There is no `catch_unwind`, no `Drop`-based publish, no
skip-marker.

The crate is consistent about this posture: `MultiProducer::clone` calls
`process::abort()` when the handle counter could overflow (multi.rs:83-85). It
trades unwinding-safety ceremony for latency deliberately.

**This is the finding that matters most for us**, because `ultima_rings` cannot
copy it. `docs/design.md` §10 pre-empts exactly this class in two rows — rtrb's
publish-before-drop bug, and `drain`'s `PublishGuard` being panic-audited rather
than assumed sound. Shipping a batch API whose failure mode is "consumer stalls
forever" would contradict the crate's own stated pitfall checklist.

### 3. API shape — in-place mutation, not move-in

```rust
fn try_batch_publish<'a, F>(&'a mut self, n: usize, update: F)
    where F: FnOnce(MutBatchIter<'a, E>)
```

The caller receives an iterator of `&mut E` and fills slots in place. This is not
incidental — the whole crate is built that way:

```rust
slots: Box<[UnsafeCell<E>]>,          // always initialized
RingBuffer::new(size, event_factory)  // every slot constructed up front
```

No `MaybeUninit`, no move-in, no drop-drain. Slots are constructed once by a
factory and reused forever; a producer mutates the existing `E`.

That ownership model is *why* they can survive a panic mid-batch without
unsoundness: an unpublished slot still holds a valid, if stale, `E`. In
`ultima_rings`, a claimed-but-unwritten slot holds uninitialised memory, so the
same panic is not merely a liveness bug — there is no safe recovery that
publishes it, and no safe way to skip it without encoding skip state into the
availability value.

## Consequence for this crate

**The batched claim is not independently adoptable.** It comes as a bundle:

| Piece | Needed for | Cost to us |
|---|---|---|
| Batched claim CAS | fewer claim CASes | acceptable alone |
| Bitmap availability | making batched *publication* cheap | reverses the colocation we just measured at +12–15% |
| In-place `&mut E` slots | panic-safety under batching | an API and ownership rewrite; loses move-in `send(v)` |

Taking the claim alone leaves N publication stores and buys much less. Taking the
bitmap reverses a change measured to help. Taking neither the bitmap nor the
in-place model leaves a batch API with a permanent-stall failure mode that §10
forbids.

The honest conclusion is that batching is a **different channel design**, not an
optimisation of this one — the same shape of finding as the sharded round, where
the answer was a separate type rather than a change to `mpsc`.

## Two smaller findings

**No CAS backoff — and this cuts against my own earlier recommendation.** The
retry path (multi.rs:181-184) is `Err(new_current) => { current = new_current;
n_next = current + n; }` — immediate retry, no `spin_loop`, no exponential
backoff. Identical in shape to ours. So of three implementations examined,
crossbeam-channel backs off and `disruptor` does not.

That downgrades "add exponential backoff to the CAS retry" from *clearly correct*
to *cheap to test, genuinely uncertain*. It remains worth measuring — it is ~3
lines and gated by an existing three-configuration harness — but the earlier
framing ("crossbeam does it and we don't") overstated the consensus. Two of three
do not.

**Wait strategies are a real trait, and the taxonomy is finer than ours.**

```rust
pub trait WaitStrategy: Copy + Send { fn wait_for(&self, sequence: Sequence); }
```

Genuinely pluggable, unlike this crate's closed enum (see README's
"selectable, not pluggable"). Their variants: `BusySpin` (does *nothing* — a true
empty spin), `BusySpinWithSpinLoopHint` (a `hint::spin_loop()`), and `Sleep`.

Worth noting they separate "spin with no hint" from "spin with `PAUSE`" as
distinct published strategies, where `ultima_rings::WaitStrategy::BusySpin`
conflates them by always emitting the hint. There is no parking strategy at all —
the Disruptor assumes pinned, dedicated threads.

## Postscript: it was then measured, and it is slower than what we ship

After this survey, `disruptor` was added to the bake-off
(`bakeoff_disruptor_mpsc`, results in `docs/bench-results/2026-08-09-bakeoff-v2.md`).
Median of three runs, 2 producers, cap 1024, `u64`:

| | Melem/s | vs. this crate's `mpsc` |
|---|---:|---:|
| `ultima_rings::mpsc` | 33.20 | 1.00× |
| `disruptor` (batched consume) | 27.18 | **0.82×** |
| `disruptor` (`take(1)`) | 1.33 | 0.04× (see below) |

**The batched design measures slower than the design we already have** — and it
does so while doing *less* work per element, since its in-place slots mean no
move, no `MaybeUninit`, and no drop bookkeeping.

The "don't adopt" conclusion above was reached from an argument. It now has a
number, and the number is the more decisive of the two. Note the caveat: batched
*publication* was not measured, because the harness's producers hold one item at
a time; this is disruptor with batched consumption and single publication.

The `take(1)` figure also confirmed the bitmap's cost profile concretely.
`EventPoller::take` runs the full availability walk before applying its limit
(`event_poller.rs:253-259`), so single-item consumption is O(backlog) per event
and O(backlog²) to drain. The general shape — **bitmap: batch O(n/64),
single O(n); per-slot round: single O(1), batch O(n)** — is a reason to keep the
per-slot round that stands independent of any throughput number, because this
crate's hot path consumes single items.

## What to take, what to leave

- **Leave** the batched claim as an optimisation of `src/mpsc.rs`. It is only
  coherent alongside the bitmap and the in-place ownership model, and the panic
  behaviour it ships with is one this crate's §10 explicitly pre-empts.
- **Record** the bitmap availability idea in design.md §9. It is a genuine
  alternative this crate never considered, it is strictly more compact, and its
  merits are call-shape dependent rather than absolute.
- **Downgrade** the CAS-backoff recommendation to an untested hypothesis and
  measure it, rather than asserting crossbeam's approach is the consensus.
- **Note** the `BusySpin` vs `BusySpinWithSpinLoopHint` distinction as a possible
  future refinement; not currently measurable as a gap.
