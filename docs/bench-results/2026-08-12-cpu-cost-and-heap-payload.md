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

**`BackoffYield` burns 100.0% of a core.** `src/wait.rs` already warned that
"this does not reduce CPU use on an idle machine", and that warning is right to a
tenth of a percent. With nothing else runnable, `yield_now` returns immediately,
so the yield ladder is a busy-wait with a syscall in it.

Do not read that as "the strategy has no purpose" — this table cannot see its
purpose. See Part 2, which measures the regime where it exists.

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

# Part 2: oversubscription, and what `BackoffYield` is actually for

> **Re-measured across 2–16 physical cores the same day**
> (`2026-08-12-topology-sweep.md`). Everything below reproduces at the topology
> it was taken on, and the host it was taken on is **2 physical cores with SMT**,
> not 4 cores. Two corrections follow from that: the collapse threshold is
> schedulable CPUs rather than cores, and the 7.37x below is 1.23x on a 16-core
> machine. The direction holds everywhere; the magnitude does not travel.

Part 1 runs at most three threads on four cores, in both its sections. That is
the one regime in which `BackoffYield` is definitionally inert: `yield_now`
returns immediately unless another thread is runnable, so with nothing else
runnable it is `BusySpin` with a syscall in the loop.

Reading only the paced table, the strategy looks pointless — 100.0% of a core
against `BusySpin`'s 99.9%, at 26x coarser wake granularity, dominated on both
axes. **That conclusion is an artifact of the measurement, and it is wrong.**

Blocking `send`/`recv`, 4 cores, 200k elements, median of 3 runs:

| strategy | p2 | p8 (2x over) | p32 (8x over) |
|---|---:|---:|---:|
| `BusySpin` | **69.11** | 35.67 | 4.84 |
| `BackoffYield` | 71.45 | **62.77** | 35.65 |
| `Backoff` | 58.72 | 61.17 | **36.64** |
| `Park` | 10.92 | 11.13 | 11.76 |

Melem/s, medians. And the CPU cost per element over the same sweep:

| strategy | p2 | p8 | p32 |
|---|---:|---:|---:|
| `BusySpin` | 28.3 | 48.9 | 712.5 |
| `BackoffYield` | 28.2 | 29.2 | 35.5 |
| `Backoff` | 25.3 | 34.6 | 39.3 |
| `Park` | 229.4 | 275.4 | 237.5 |

## Findings

**`BackoffYield` is 1.76x `BusySpin` at p8 and 7.37x at p32**, separated in both
cases — no overlap across three runs. It is simultaneously *cheaper*: 29.2
against 48.9 CPU ns per element at p8, and 35.5 against 712.5 at p32. Faster and
cheaper at once, which is not a trade at all once threads outnumber cores.

The mechanism is the one `src/wait.rs` claimed: a spinner burns the core that
the thread it is waiting on needs to make the progress it is waiting for.
Yielding hands that core over. It requires threads to outnumber cores, which is
exactly why Part 1 could not see it.

**Below saturation there is no difference.** 71.45 against 69.11 at p2 — a 3%
gap against `BusySpin`'s own 27.7% run-to-run spread. An earlier single run
showed 1.35x here and did not reproduce; two further runs put it inside the
noise.

**`BusySpin` does not merely degrade under oversubscription, it becomes
unpredictable.** Its p32 samples were 4.71, 4.84 and 19.93 Melem/s — a 4.2x
range, against `BackoffYield`'s 33.78–43.17. A strategy whose throughput varies
4x run to run is hard to build a latency budget on.

**`Park` is flat and slowest: 10.92, 11.13, 11.76 across a 16x change in
producer count.** It is the only strategy indifferent to the thread-to-core
ratio, because a parked thread is not competing for a core at all. Always last,
never collapses.

**`BackoffYield` and `Backoff` are a wash on throughput** at every producer
count measured. So the case for `BackoffYield` over `Backoff` rests entirely on
wake granularity — ~0.7 µs against a ~64 µs OS-timer floor — paid for with a
held core while idle (100.0% against 10.2%). That is a real niche and a narrow
one, and it is now stated in terms of measured numbers rather than intent.

## Correction

An earlier reading of Part 1 in this session concluded that `BackoffYield` had
no advantage and was dominated by `BusySpin`. That was drawn from the paced
table alone, which measures the single regime where the strategy cannot work.
The advantage is real and up to 7.37x.

## Oversubscribing with threads outside the channel gives a different answer

> **Understated, not wrong.** On real cores `Park`'s lead here is 5.0x to 24x
> rather than the 2.5x measured below, and it holds at every topology tested
> (`2026-08-12-topology-sweep.md`). The hedge below — "stability rather than a
> win" — was right for this data and too cautious for the effect.

The sweep above oversubscribes with the channel's own producers. That is the
easy case to construct and the rarer one to meet. The ordinary case is a channel
inside a process that is already busy, so: channel topology fixed at 2 producers
+ 1 consumer, plus **4 CPU-bound threads that never touch the channel**. Seven
threads on four cores. Six samples.

The new column is what nothing here had measured — what the wait strategy costs
*everyone else*. `ext kept` is the external threads' throughput as a fraction of
those same threads running alone.

| strategy | Melem/s | range | ext kept | range | cpu ns/elem |
|---|---:|---|---:|---|---:|
| `Park` | **4.55** | 3.91–5.24 | 86% | 84–93 | 104.5 |
| `BackoffYield` | 2.79 | 1.38–8.09 | 96% | 72–104 | 21.4 |
| `BusySpin` | 1.79 | 1.33–14.02 | **77%** | 75–83 | 508.9 |
| `Backoff` | 1.73 | 1.06–3.03 | **98%** | 95–103 | 22.7 |

### `BusySpin` costs the rest of the process 23% of its throughput

77% kept, range 75–83%, separated from `Backoff`'s 95–103% across all six
samples. This is the cost that every table before this one was blind to: a
spinning consumer does not merely fail to help, it takes roughly a quarter of
the machine's useful work away from the code around it. `Backoff` costs 2%,
`Park` 14%.

`Park`'s 14% is worth noting as the price of its wake protocol — the per-publish
fence and futex traffic is real CPU, and it comes out of the neighbours.

### The ranking inverts against the producer-oversubscription sweep

| | oversubscribed by producers (p32) | oversubscribed by strangers |
|---|---|---|
| best throughput | `BackoffYield` / `Backoff` | `Park` |
| worst throughput | `BusySpin` (7.4x behind) | `BusySpin` / `Backoff` |

**The two kinds of oversubscription are not interchangeable, and the mechanism
is why.** Yielding pays when the thread you would yield to is the one you are
waiting on — you hand over the core, it publishes, you get woken by the work you
wanted. Yielding to a stranger returns nothing: you surrender your slice and the
recipient will never publish anything. `Park` wins here precisely because it
leaves the runqueue altogether and is woken when there is genuinely work, rather
than competing for slices it cannot use.

That also explains why `BusySpin`, `BackoffYield` and `Backoff` land within noise
of each other on channel throughput in this table while separating cleanly in the
previous one.

### `Park` is the stable choice under external load

Its range is 3.91–5.24, a 1.34x spread, against `BusySpin`'s 1.33–14.02 at 10.5x.
Excluding a single sample in which the box was evidently disturbed — `BusySpin`
reported 14.02 Melem/s there, 4x anything else it produced, and `BackoffYield`
simultaneously dropped to its 72% outlier — `Park` separates from `BusySpin`
outright. With that sample included it does not, so the throughput claim is
stated as stability rather than as a win.

### Everything gets much slower

The channel runs at 1.7–4.6 Melem/s here against roughly 70 Melem/s at p2 on an
idle box — a **15–40x** collapse from adding four unrelated CPU-bound threads to
a four-core machine. Any latency or throughput budget derived from the idle
tables in this directory does not survive contact with a busy process.

---

# Part 3: a payload that owns memory

> **This section's central conclusion does not replicate on another machine.**
> `2026-08-12-topology-sweep.md` re-ran the bake-off at `smt2x2` — 4 CPUs on 2
> physical cores, this VM's exact shape, on an Intel host — and got **0.531x**
> where the measurement below gives **3.199x**. Same topology, 6x apart,
> opposite sides of crossbeam.
>
> Part of the gap is that `taskset` does not constrain glibc's malloc arena
> pool, which is sized from the whole machine and flatters the move-based
> crates; a probe puts that at roughly 17% of the difference. The rest is
> unexplained.
>
> **Quote nothing directional from this section.** The numbers are what this VM
> produced; the comparison does not survive a change of machine, and an earlier
> revision of this note blamed core count, which was also wrong.

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

## This crate loses, by 1.82x — on this machine

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
slot-owning design loses 1.6x.

**On this VM.** The ordering is the other way round on an Intel host at the same
topology, so this paragraph describes one machine and not a design property. See
the replication section of `2026-08-12-topology-sweep.md`.

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

**Much less than it looked.** The 1.82x gap that motivated this section is a
one-machine result: at matched topology on an Intel host this crate is 1.9x
ahead instead. Task #28 (record a design decision on `Ref<T>`-style access) is
answered with "measured, does not replicate" rather than with a redesign.

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
- **The oversubscription sweep has high variance**, particularly `BusySpin` at
  p32 (4.71–19.93) and `Backoff` at p2 (33.13–72.68). The separations quoted
  hold across three runs, but the point estimates should not be quoted to two
  significant figures.
- **Oversubscription is simulated with producers only.** Competing threads that
  are not part of the channel — the more common real case — are untested.
- **Thread-spawn cost** is inside the measured wall time in `cpu_cost.rs`, at
  roughly 1% of the total. It is not subtracted.
- **Saturated `cores` never reaches 3.0** (2.4–2.8) because producers finish
  before the consumer drains, and the tail is single-threaded.
