# ultima_rings — sharded MPSC prototype (N×SPSC + round-robin consumer)

**Date:** 2026-08-07
**Status:** Approved
**Supersedes in priority:** `2026-08-07-mpsc-perf-v2-design.md` (approved, deliberately
deferred — see "Relationship to the v2 spec")

## Purpose

Measure whether a **sharded** MPSC — one SPSC ring per producer, consumer
round-robins — closes the v1 bake-off gap, where `mpsc` `BusySpin` reached
29.9 Melem/s against crossbeam-channel's 71.0 (~0.42×,
`docs/bench-results/2026-08-06-bakeoff.md`).

The current `src/mpsc.rs` pays two costs on every element: a bounded-CAS claim
retry loop (design.md §7) and false sharing across the packed availability
array (design.md §8). Sharding removes **both at once** — each shard has a
single writer, so a producer's hot path becomes the SPSC path already measured
at 620 Melem/s: one Release store, no CAS, no retry, no contended line. It also
removes a head-of-line stall the shared-claim design has, where a producer
preempted between its claim and its `avail` store blocks delivery of every
already-published item behind it (`src/mpsc.rs:293`).

This spec builds the **cheapest artifact that produces a trustworthy number**,
not a shippable channel. The output is a measurement and a go/no-go decision.

## What sharding costs (why this is a prototype, not a replacement)

Three properties `src/mpsc.rs` guarantees that a sharded design cannot:

1. **Global FIFO.** The CAS on `claim` (`src/mpsc.rs:125`) linearizes all
   producers into one sequence, and `drain` consumes the contiguous prefix, so
   delivery order *is* claim order across all producers. Sharded gives
   per-producer FIFO only; cross-producer order is an artifact of scan
   position. crossbeam-channel, flume, and kanal all provide the global order.
2. **Global bound.** `channel(1024)` today means 1024 items total, any
   distribution, and `Full` means actually full. Sharded makes backpressure a
   producer-local property.
3. **Cheap emptiness.** One Acquire load answers "is anything there" today.
   Sharded requires a full `n_shards` scan to conclude `Empty` or
   `Disconnected`.

These are contract changes, not implementation details. If the prototype wins,
the production form is a **separate type** with these trade-offs documented —
never a silent replacement for `mpsc::channel`.

## Scope decisions (settled during brainstorming)

| Decision | Choice | Why |
|---|---|---|
| Producer set | **Fixed count up front**, no `Sender: Clone` | The dynamic shard registry (concurrent shard list, mid-flight discovery, drain-then-reap on sender drop) is the expensive part and is not needed to answer the throughput question. The bake-off harness spawns exactly 2 producers up front (`benches/throughput.rs:254`). |
| Depth | **`BusySpin` only**; `try_send`/`try_recv` | Lands exactly the cell the gap lives in. Park mode needs a new N-way Dekker protocol (consumer rescans every shard after the `SeqCst` fence), which is real new concurrency and is not worth building before the number justifies it. |
| Capacity | **Total cap, split across shards** | `channel(2, 1024)` → 2×512 = 1024 total, matching `crossbeam_channel::bounded(1024)` exactly. Per-shard cap would compare 2048 slots against 1024 and quietly credit the buffer for what should be the design's win. |
| Scan policy | **Sticky cursor with a 32-item visit budget** | Amortizes the shard switch over a run of items so the hot path stays on one ring's cache lines, while the budget caps how long one producer starves the others. |
| Shard construction | **Compose existing `spsc::channel`** | `src/spsc.rs` is already loom-modeled and miri-audited and is the fastest thing in the bake-off. Composition means the prototype adds **zero `unsafe`**, so any number it produces is attributable to sharding rather than to a freshly written ring. |
| `drain` | **Omitted** | The MPSC bake-off drives the consumer with single-item `try_recv`; `drain` is not on the path to the number. |
| Visibility | **Feature-gated `experimental-sharded`, default off** | A half-verified module must never look like a shipped one. |

## Architecture

```
sharded::channel::<T>(n_shards, total_cap, strategy)
    -> (Vec<Sender<T>>, Receiver<T>)

  per_shard = total_cap / n_shards
  builds n_shards independent spsc::channel(per_shard, strategy)

  Sender<T>   { inner: spsc::Sender<T> }         // one per producer thread, no Clone
  Receiver<T> { shards: Vec<spsc::Receiver<T>>,  // consumer-private
                cursor: usize,                   // current shard
                budget: usize }                  // items taken from cursor's shard
```

**File:** `src/sharded.rs`, ~150 lines, no `unsafe`, no atomics of its own.
Registered in `src/lib.rs` as `#[cfg(feature = "experimental-sharded")] pub mod sharded;`.

### Capacity constraint

`per_shard = total_cap / n_shards` must be a positive power of two, enforced by
the existing `assert_cap`. Since `total_cap` is itself a power of two, this
means `n_shards` ∈ {1, 2, 4, 8, …}. `channel` panics with a message naming both
values when the split is invalid. Cursor advance is a compare-and-reset
(`if cursor + 1 == n { 0 } else { cursor + 1 }`), never a modulo — no division
enters the hot path.

## Semantics

### `Sender::try_send(v) -> Result<(), TrySendError<T>>`

Delegates to `spsc::Sender::try_send` unchanged. This is the entire point: the
producer hot path is one Release store with no CAS, no retry loop, and no
per-item awareness that other producers exist.

`Full(v)` now means **this producer's shard** is full — with `channel(2, 1024)`
a producer stalls at 512 outstanding items even while the other shard sits
empty. `Disconnected(v)` when the receiver is gone.

### `Receiver::try_recv() -> Result<T, TryRecvError>`

Scans at most `n_shards` shards starting at `cursor`:

- item from `shards[cursor]` → `budget += 1`, return `Ok(v)`
- `Empty`, or `budget == VISIT_BUDGET` (32) → reset budget, advance cursor,
  continue scanning
- `Disconnected` → increment a per-scan counter, advance cursor, continue
- full scan yielding no item → `Disconnected` if every shard reported
  `Disconnected`, otherwise `Empty`

Aggregating disconnects by counting per scan is sound because
`spsc::Receiver::try_recv` returns `Disconnected` only once its shard is both
sender-dropped and drained (`src/spsc.rs:170-180`), and that state is stable
once reached — no dead-shard bookkeeping is needed at these shard counts.

### Disconnect, both directions

Falls out of composition. Dropping a `Sender` drops its inner `spsc::Sender`,
flagging that shard. Dropping the `Receiver` drops every inner
`spsc::Receiver`, so each producer's next `try_send` returns `Disconnected`.

### Ordering contract

**Per-producer FIFO only.** No cross-producer ordering guarantee. Stated
prominently in the module docs, since this is the property `src/mpsc.rs`
provides and this type does not.

## Verification

- **Unit tests** (`src/sharded.rs`, `#[cfg(all(test, not(loom)))]`): capacity
  assertion panics for an invalid split; per-shard `Full` reported at
  `total_cap / n_shards`, not `total_cap`; deterministic sticky-cursor order
  for a hand-seeded two-shard case; partial disconnect yields `Empty` while
  total disconnect yields `Disconnected`; ZST payload round-trips.
- **Stress test** (`tests/sharded_stress.rs`): 2 and 4 producers × 100k items.
  Each producer sends values tagged with its shard index; the consumer asserts
  **per-tag monotonicity** and a total count. This is the direct test of the
  weakened ordering contract — it asserts exactly what the type promises and
  nothing stronger.
- **Drop-drain test**: a `Drop`-counting payload confirms that dropping a
  partially consumed sharded channel drops every unconsumed item exactly once,
  verifying composition neither leaks nor double-drops.
- **miri** over the full suite.
- **No loom models, for a structural reason rather than as a shortcut:** the
  sharded layer declares no atomics and contains no `unsafe`. Every ordering
  edge belongs to `spsc.rs`, which `tests/loom.rs` already models. Adding
  models here would re-model the SPSC core under a new name.

## Measurement

New criterion group `bakeoff_sharded_mpsc` in `benches/throughput.rs`, gated on
`experimental-sharded`: 2 producers, `total_cap` 1024 → 2×512, `BusySpin`, the
same barrier-released harness and `BATCH` as the existing MPSC cells.

Compared against `mpsc/busy_spin_2_producers` and `bakeoff_mpsc/crossbeam`,
both **re-measured in the same run** rather than quoted from the v1 file.

**Run conditions.** The box is 4 cores with 15 GiB RAM, no swap, and `/tmp` is
a tmpfs whose contents are charged against RAM. Build to completion first, then
measure — never overlapping, matching the v1 file's stated conditions. The
harness runs 2 producers plus 1 consumer all spinning, so the box is saturated
by design; sharding adds no threads, so the comparison shape is unchanged from
v1.

Results recorded to `docs/bench-results/2026-08-07-sharded-mpsc.md` with the v1
numbers alongside for the delta.

## The gate

- **≥ 142 Melem/s** (2× crossbeam's 71.0, the bar the project already set
  itself): decisive. Design the dynamic shard registry and the production type.
- **> 71.0 Melem/s**: sharding wins. Worth designing the registry, with the
  contract costs weighed explicitly against the gain.
- **≤ 71.0 Melem/s**: a real finding, not a failure. With zero CAS contention
  and zero availability-array false sharing, a loss means the gap is not where
  §7 and §8 assume it is. The v2 shift/padding levers return to the table, and
  design.md §7/§8 need correcting to say the dominant cost is unidentified.

Whichever branch lands gets recorded. A reference implementation does not hide
a lost benchmark.

## Relationship to the v2 spec

`2026-08-07-mpsc-perf-v2-design.md` (shift-not-divide, padded availability
array, Park bench) remains approved and unimplemented. It is deferred, not
cancelled: its levers are cheap and semantics-preserving, and they stay the
correct next move if this prototype loses. Both specs target the same gap, so
running v2 first would only change the baseline this prototype is measured
against.

## Out of scope

Dynamic producers / `Sender: Clone` and the shard registry; `Park` and
`Backoff` wait strategies; blocking `send`/`recv`; `drain`; loom models; the
single-allocation shard array (approach B); any change to `src/mpsc.rs` or
`src/spsc.rs`; uc2 integration.

**Documentation follow-ups are deliberately excluded from this build.** The
design.md §9 "Alternatives considered" entry for sharded SPSC — a gap this
brainstorming surfaced, since §9 covers Vyukov stamps, kanal's stack transfer,
and flume's lock but not the most obvious alternative to a shared-claim MPSC —
and any §7/§8 correction the gate's third branch would require are follow-up
work informed by the measurement, not prerequisites for it. Writing them before
the number exists would mean guessing at the conclusion.
