# What the spinning costs, and what happens when the payload owns memory

**Date:** 2026-08-12
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; `vmstat` 84–92% idle
**Method:** two additions, `examples/cpu_cost.rs` (median of 3) and the
`bakeoff_mpsc_string` group (three interleaved rounds, median with range).

Two gaps closed at once, and each one moves a headline number.

Until now every cell in `benches/throughput.rs` reported elements per second of
**wall** time, carrying a **`u64`**. Both choices flatter this crate. Wall-clock
throughput hides the cores a spinner spends to get it, and a `u64` payload denies
`thingbuf` and `disruptor` the one thing they are built to do.

---

# Part 1: CPU cost

Each thread reads its own `/proc/thread-self/schedstat` before and after its
work. Field 0 is nanoseconds on CPU, so summed deltas give total CPU time at
nanosecond resolution with no dependency.

**The accounting was validated before use:** two threads spinning for 1.0 s wall
registered 0.977 s and 0.975 s of CPU, and a thread sleeping 1.0 s registered
19 µs. So the counter distinguishes on-CPU from blocked, which is the entire
premise.

## Saturated (2 producers, cap 1024, 1M elements per iteration)

| Config | Melem/s | cores | cpu ns/elem |
|---|---:|---:|---:|
| ultima `BusySpin` (poll) | 72.91 | 2.36 | **32.4** |
| crossbeam (poll) | 55.20 | 2.66 | 48.1 |
| crossbeam (block) | 51.66 | 2.65 | 51.2 |
| thingbuf (block) | 21.29 | 2.81 | 132.2 |
| thingbuf (poll) | 15.02 | 2.78 | 185.1 |
| ultima `Park` (block) | 12.29 | 2.72 | **221.1** |

**Every config burns 2.4–2.8 cores, including the blocking ones.** Under
saturation the ring is never empty long enough for anyone to reach a park, so
every "blocking" path here is really spinning. This table therefore measures
each parking mechanism's *cost* with none of its *benefit* — which is precisely
why Part 2 of the example exists, and the same structural blind spot that made
the original one-way paced bench measure nothing.

Read that way, the honest reading is `cpu ns/elem`:

- This crate's `BusySpin` is the cheapest in the roster at 32.4 ns/elem, **1.48x**
  cheaper than crossbeam's polling path. The 1.24x wall-clock lead is not bought
  by spending more CPU — it is a genuine efficiency lead.
- `Park` is the most expensive at 221.1 ns/elem, **6.8x** its own `BusySpin`,
  while delivering 5.9x less throughput. Its per-publish `SeqCst` fence and
  consumer wake are pure overhead when the consumer never actually sleeps.

That second point sharpens `2026-08-11-backoff-isolation.md`. That document
proposed park/unpark churn as the leading hypothesis for `Park`'s weakness but
noted "this mechanism is not itself measured." It still is not counted directly,
but 6.8x the CPU per element for 1/5.9th the throughput is the CPU signature such
churn would leave.

## Paced — one element every 200 µs, consumer CPU only

This is where parking either pays for itself or does not.

| Config | cores | % of a core | cpu ns/elem |
|---|---:|---:|---:|
| ultima `BackoffYield` | 1.000 | **100.0%** | 258,754 |
| ultima `BusySpin` | 0.999 | **99.9%** | 263,530 |
| ultima `Backoff` | 0.102 | 10.2% | 26,065 |
| crossbeam (block) | 0.044 | 4.4% | 11,351 |
| ultima `Park` | 0.018 | **1.8%** | 4,604 |
| thingbuf (block) | 0.018 | 1.8% | 4,551 |

**Busy-spinning burns 99.9% of a core.** That was asserted in `docs/design.md`
and in this session's conversation; it is now a measurement. To deliver 5,000
elements per second — one every 200 µs — a `BusySpin` consumer holds a core
saturated the entire time, at 263 µs of CPU per element against `Park`'s 4.6 µs.
**A 57x difference in CPU cost per element**, in the opposite direction from the
saturated table.

Three further findings:

**`BackoffYield` burns 100.0% of a core — it saves nothing.** `src/wait.rs`
already warned that "this does not reduce CPU use on an idle machine", and that
warning is now exactly right to a tenth of a percent. With nothing else runnable,
`yield_now` returns immediately, so the yield ladder is a busy-wait with a
syscall in it. It buys politeness under contention, not idle CPU. This is worth
stating loudly because the name suggests otherwise, and because the strategy was
added earlier in this session on a latency argument alone.

**`Backoff` is 10.2% of a core, not zero.** `src/wait.rs` called both it and
`Park` "no idle CPU". That was right about `Park` to within 1.8% and overstated
`Backoff` by a tenth of a core. Corrected in the source docs, with the measured
table.

**crossbeam's blocking `recv` sits at 4.4%, 2.4x `Park`'s 1.8%.** It spins before
parking, so it pays some idle CPU to shorten its wake. That is a deliberate
position on the same trade curve, not a defect.

### What this does to the throughput tables

Nothing in `docs/bench-results/` is retracted, but every wall-clock comparison
between a spinning and a parking config now carries a second number. The four
`backoff_isolation` corners in particular compare `BusySpin` against `Park` on
throughput alone, and on this evidence they were comparing configurations that
differ by 57x in idle CPU cost.

---

# Part 2: a payload that owns memory

`bakeoff_mpsc_string` is the same harness with a 64-byte `String`. Producers
build a message per element, which is what a logging or serialization pipeline
does. Move-based crates pay one allocation and one free per element; slot-owning
crates pay neither after warm-up. That difference is the architectural claim
under test, not an artifact of the harness.

It is also the first cell in the file to exercise this crate's drop bookkeeping.
A `u64` has no destructor, so every number recorded before today was measured on
a path where `Slot::drop` does nothing.

## Result (2 producers, cap 1024, 200k Strings)

| Competitor | Melem/s | range | spread | vs. crossbeam |
|---|---:|---|---:|---:|
| **`thingbuf` (`try_send_ref`)** | **11.87** | 10.68–12.03 | 12.6% | **3.20x** |
| ultima_rings `mpsc` | 6.52 | 6.31–6.70 | 6.2% | 1.76x |
| crossbeam-channel | 3.71 | 3.71–3.83 | 3.3% | 1.00x |
| `thingbuf` (`try_send`) | 3.59 | 3.57–3.69 | 3.2% | 0.97x |

## This crate loses, by 1.82x

On the payload the closest prior art is designed for, **thingbuf's reference API
is 1.82x this crate** — 11.87 against 6.52, with no overlap in range. Yesterday's
headline, that we are 4.85x thingbuf, was true only of the payload that denies
thingbuf its whole mechanism.

Both statements are correct and neither is complete on its own. The pair is the
finding:

| | `u64` | `String` |
|---|---:|---:|
| ultima vs thingbuf (ref) | **3.89x ahead** | **1.82x behind** |

## Payload sensitivity is the mechanism

The same crates, the same harness, switching only the payload:

| Competitor | `u64` | `String` | slowdown |
|---|---:|---:|---:|
| crossbeam | 58.64 | 3.71 | 15.8x |
| ultima_rings | 72.67 | 6.52 | 11.1x |
| thingbuf (value) | 14.99 | 3.59 | 4.2x |
| **thingbuf (ref)** | 18.66 | **11.87** | **1.6x** |

Every move-based design loses an order of magnitude to the allocator. The
slot-owning design loses 1.6x. That is not a small edge in a benchmark — it is
the design's entire thesis, and it holds.

## thingbuf's by-value API discards everything the crate is for

3.31x slower than its own reference API on `String`, against 1.25x on `u64`.

The gap grows with the payload exactly as the design predicts. `try_send(v)`
moves a value into the slot and drops the previous occupant, so the pooled
allocation is freed and the next send reallocates — documented upstream, and
here measured. At 0.97x crossbeam, thingbuf's by-value path is *slower than a
plain channel*: the caller pays for the recycling machinery and then defeats it.

A caller reaching for `try_send` because it looks like every other channel API
gets the worst cell in this table.

## What this crate could take from it

Nothing here suggests copying `Ref<T>` wholesale — the survey
(`2026-08-06-thingbuf-survey.md`) already catalogued its costs: every operation
exists twice, and mixing the two APIs silently drops the pooling. But 1.82x on
heap-owning payloads is a large enough gap that "we do not do that" should be a
recorded decision with a reason, not an omission. Filed as a design question
rather than a change.

---

## Limits

- **One payload size.** 64 bytes, one `String` shape. The slowdowns above will
  move with allocation size, and a payload small enough for a small-string
  optimization (which `String` does not have) or large enough to dominate the
  handoff would both behave differently.
- **glibc malloc, 3 threads.** Allocator contention is part of what the move-based
  cells measure. A different allocator would move those rows and not the
  `thingbuf_ref` one.
- **`disruptor` has no `String` cell yet.** It shares thingbuf's in-place model
  and would be expected to show the same low payload sensitivity. Untested.
- **The paced section uses one producer.** Idle CPU for a *blocked producer*
  (ring full) is unmeasured; only the consumer side is.
- **Thread-spawn cost** is inside the measured wall time in `cpu_cost.rs`, at
  roughly 1% of the total. It is not subtracted.
- **Saturated `cores` never reaches 3.0** (2.4–2.8) because producers finish
  before the consumer drains, and the tail is single-threaded.
