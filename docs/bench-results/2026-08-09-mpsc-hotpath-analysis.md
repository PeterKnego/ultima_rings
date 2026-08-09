# MPSC hot path: what the instrumentation shows, and what crossbeam does differently

**Date:** 2026-08-09
**Motivation:** v2 tried both levers design.md §7 and §8 name for the MPSC gap.
Removing the division changed nothing; padding `avail` changed nothing that
survived a second configuration. §8 now records the dominant cost as
unidentified. This is an attempt to identify it.

**Method note:** `perf` is unavailable on this machine (not installed,
`perf_event_paranoid = 4`, sudo requires an interactive password), so there are
no hardware counters here. What follows is (1) instrumented counts from the real
hot path and (2) a structural reading of crossbeam-channel 0.5.16's source. The
cache-line accounting in part 2 is an **argument, not a measurement** — it is
labelled as such throughout.

## Part 1: instrumented counts

Thread-local counters in `try_send`, read once per producer thread at exit.

An earlier attempt used shared `AtomicU64` counters and had to be discarded: it
dropped throughput from ~30 to 15–21 Melem/s, meaning the instrumentation was
itself contended and roughly doubled the cost of the path being measured, while
inflating the very retries it was counting. Thread-locals removed the effect
(31–70 Melem/s, comparable to uninstrumented).

`cap 1024`, `BusySpin`, `BATCH` 100k:

| producers | Melem/s | CAS attempts/elem | retries/elem | retry rate |
|---:|---:|---:|---:|---:|
| 2 | 70.6 | 1.000 | 0.000 | 0.0% |
| 2 | 42.2 | 1.000 | 0.000 | 0.0% |
| 2 | 34.8 | 1.288 | 0.288 | 22.4% |
| 2 | 31.0 | 1.716 | 0.716 | 41.7% |
| 4 | 46.2 | 1.005 | 0.005 | 0.5% |
| 4 | 26.8 | 1.646 | 0.646 | 39.2% |
| 4 | 17.4 | 1.318 | 0.318 | 24.1% |
| 4 | 15.6 | 1.697 | 0.697 | 41.1% |

**CAS retries are real.** Under genuine concurrency 22–42% of claim CAS attempts
fail, so a send costs 1.2–1.7 attempts rather than 1.0. §7's claim that the
bounded-CAS claim carries a retry cost is confirmed as real — it is not zero.

**But this does not isolate it, and the correlation is a trap.** Zero-retry runs
are the fast ones, which looks like proof that retries are the cost. It is not:
zero retries means the two producers were not actually executing concurrently
(one descheduled while the other ran). Under that condition CAS contention *and*
cross-core cache traffic both vanish together. The instrumentation cannot
separate them, because both are caused by the same thing.

`head` re-reads are usually ~0 but occasionally 1–2 per element; those runs are
the slowest (15.6, 5.7 Melem/s). That is backpressure — the consumer not keeping
up — and a separate effect from either candidate.

## Part 2: what crossbeam does differently

The decisive difference is one the crate's own docs never name.

**crossbeam colocates the readiness stamp with the message.**

```rust
struct Slot<T> {
    stamp: AtomicUsize,
    msg: UnsafeCell<MaybeUninit<T>>,
}
```

`write()` does `slot.msg.write(msg)` then `slot.stamp.store(Release)`; `read()`
does `slot.msg.read()` then `slot.stamp.store(Release)`
(`crossbeam-channel-0.5.16/src/flavors/array.rs:214-231, 305-320`). Both touch
one contiguous struct.

**We keep the round number in a separate array.** `buf: Box<[UnsafeCell<MaybeUninit<T>>]>`
and `avail: Box<[AtomicI64]>` are two allocations. A publish writes `buf[i]` and
then `avail[i]`; a consume reads `avail[i]` and then `buf[i]`.

### Cache-line accounting per element (argument, not measured)

For a handoff of one `u64` between producer and consumer:

| | lines that ping-pong producer↔consumer |
|---|---|
| crossbeam | **1** — `msg` and `stamp` are adjacent in one `Slot` |
| ultima_rings | **2** — `buf[i]` on one line, `avail[i]` on another |

Both designs additionally CAS a producer-side index on its own padded line
(`tail` / `claim`). On the consumer index we are *cheaper*: single-consumer lets
us `store` `head` where crossbeam, being MPMC, must CAS it.

So on the data path we appear to pay roughly **twice the coherence traffic per
element**, against a measured throughput gap of ~2.4× (30 vs 71 Melem/s). The
magnitudes are consistent. That is suggestive, not conclusive — no cache-miss
counters were available to confirm it.

### Why this fits both v2 null results

This hypothesis explains the two things v2 could not:

- **Removing the division did nothing** because the path is bound by cross-core
  transfer, not ALU work. A `div` is 20–40 cycles; a cache line arriving from
  another core is far more, and it dominates.
- **Padding `avail` did nothing** — and this is the interesting part. Padding
  attacks false sharing *within* `avail` by spreading entries onto separate
  lines. If the real problem is that `avail` is a separate line **at all**, then
  padding pushes in the wrong direction: it makes the array larger and less
  cache-resident without reducing the number of distinct lines a single element
  touches, which stays at two. Measured: +2.0% at cap 1024, −0.1% at cap 4096.

### One difference that is probably *not* a cost

crossbeam encodes disconnection as a mark bit inside the `tail` value it already
loads, so it gets liveness for free. We do a separate `rx_dropped.load(Acquire)`
on every `try_send`. That looks like an extra shared read per element, but the
line holding `rx_dropped`/`senders`/`strategy` is written only on handle drop —
read-mostly lines stay valid in every core's cache, so the load should be an L1
hit. Noted for completeness; not believed to matter, and not measured.

## What this suggests as the next lever

**Colocating the round number with the payload** — a per-slot `{ round, value }`
rather than two parallel arrays — is the change this analysis points at, and it
is a bigger lever than either v2 tried.

design.md §9 already describes this shape as "Vyukov per-slot stamps" and rejects
it, citing thingbuf's open issues #98 and #100. That rejection deserves
re-examination rather than being treated as settled: both cited issues concern an
MPMC reader *lingering* on a slot (thingbuf's `HAS_READER` bit) and the skip
logic that coupling forces. `ultima_rings` is single-consumer, so no reader can
linger in that sense — §9 itself says this is precisely why the design "never
needs that skip-logic at all". The soundness objection may not transfer, while
the performance argument does.

This is a real change to the `unsafe` core with a full loom/miri re-verification
cost, so it wants its own spec rather than being bolted onto this analysis. It
should be weighed against the batched claim, which remains the other untried
lever.

## Status of the claims in this document

- **Measured:** CAS retry rates (22–42% under concurrency), the observer effect
  from shared-atomic instrumentation, and the v2 null results referenced above.
- **Read from source:** crossbeam's `Slot` layout and its write/read paths.
- **Argued, not measured:** the cache-line accounting and its link to the
  throughput gap. Confirming it needs hardware counters this machine cannot
  provide.
