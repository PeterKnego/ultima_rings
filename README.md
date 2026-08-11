# ultima_rings

Bounded, lock-free, generic-`T` SPSC and MPSC ring channels for Rust, with selectable wait
strategies (`BusySpin` / `BackoffYield` / `Backoff` / `Park`) and a
`std::sync::mpsc`-shaped API. The
strategy is a closed enum picked once per channel at construction — there is no trait to
implement, so it is selectable rather than pluggable in the LMAX Disruptor sense. The
algorithms are ports of `hi-perf-cmp`'s `thread-handoff-{ring,mpsc_ring}` benchmark
cells — SPSC's cache-padded head/tail publish protocol and MPSC's LMAX-style
availability-round publication — generified from their original `u64`-only bench form and
hardened into a standalone crate: generic payloads, blocking and non-blocking APIs,
`std::sync::mpsc`-shaped close/disconnect semantics, and a loom + miri verified concurrency
core. See [`docs/design.md`](docs/design.md) for the full memory-ordering argument behind
every atomic in the crate.

## API

```rust
use ultima_rings::{WaitStrategy, mpsc};

let (tx, mut rx) = mpsc::channel::<Event>(1024, WaitStrategy::Park);
let tx2 = tx.clone();
// producers:            consumers:
tx.send(event)?;         while let Ok(ev) = rx.recv() { handle(ev); }
```

`spsc::channel::<T>(cap, strategy)` has the identical shape (`mpsc::Sender<T>: Clone` for
multiple producers; `spsc::Sender<T>` is not). `cap` must be a positive power of two.
Non-blocking `try_send`/`try_recv` and a batch `drain(max, f)` are also available on both —
see the crate's rustdoc for the full surface.

## Wait strategies

Chosen per channel at construction, applying to both blocked directions (consumer-on-empty,
producer-on-full):

| Strategy | Behavior | Use when |
|---|---|---|
| `BusySpin` | `spin_loop()` until progress (~27 ns granularity); one core pinned per blocked side | latency matters at any CPU cost |
| `BackoffYield` | 10 spins, then `yield_now()` indefinitely (~0.7 µs granularity); never parks, self-waking | you want near-`BusySpin` latency but must not starve other runnable threads |
| `Backoff` | Aeron-style idle ladder — 10 spins → 20 yields → timed park doubling 64 µs → 1 ms, self-waking | a balanced default: low latency while active, low CPU while idle |
| `Park` | Fully blocking park/wake via the notify layer; ~10 µs median wake latency | idle CPU efficiency matters more than the last few microseconds of latency |

`BackoffYield` still consumes a core when the machine is otherwise idle — `yield_now()`
returns immediately with nothing else runnable. It buys prompt preemption under
contention, not idle CPU; reach for `Backoff` or `Park` if CPU is the concern. The
`Backoff` park floor is 64 µs because `thread::park_timeout` cannot deliver sub-floor
sleeps: a 1 µs request measured ~60 µs on a 4-core Linux VM, so finer rungs would be
fiction (see `src/wait.rs`'s `PARK_MIN`).

## Measured numbers

Two distinct sources, kept separate deliberately — they measure different things on
different hardware and are not directly comparable to each other.

### AWS bench-cell provenance (`hi-perf-cmp` run `20260806T053918Z`, `c6id.2xlarge`)

The algorithms `ultima_rings` ports were benchmarked cross-host-adjacent on real AWS
hardware before extraction:

- **SPSC** (`ring`, Rust): 387 M ops/s pipelined throughput; one-way handoff p50 ≈ 200 ns
  (p99 327 ns, mean 216 ns).
- **MPSC** (`mpsc_ring`, Rust, 2 producers): 9.4 M ops/s throughput; one-way handoff p50
  277 ns (p99 389 ns, mean 289 ns).

**Provenance caveat:** these are the original *bench-cell* numbers — `u64` payload,
`%`-indexed, `fetch_add`-claimed MPSC — not a re-run of `ultima_rings`' own generified,
bounded-CAS/mask-indexed cores. `docs/design.md` §7 argues the publish/consume wire protocol
is unchanged between the bench cell and this crate, so these numbers are directionally
representative of what this crate's cores can do, but they are not this crate's own
certified AWS measurement — that re-run has not happened yet.

### This crate's own bake-off (criterion, single 4-core dev box)

`cargo bench` (see `benches/throughput.rs`, full results in [`docs/bench-results/2026-08-09-bakeoff-v2.md`](docs/bench-results/2026-08-09-bakeoff-v2.md))
measures `ultima_rings` throughput head-to-head against `crossbeam-channel`, `flume`,
`kanal`, and (SPSC-only) `rtrb`. Median of three runs in one session, range alongside:

| Group | Competitor | Melem/s (median) | range | vs. crossbeam |
|---|---|---:|---|---:|
| SPSC | rtrb | 561.6 | 513.1 – 575.8 | 15.19× |
| SPSC | **ultima_rings `BusySpin` (pipelined)** | **480.7** | 466.1 – 522.8 | **13.00×** |
| SPSC | crossbeam-channel | 37.0 | 34.3 – 37.4 | 1.00× |
| SPSC | flume | 10.4 | 8.9 – 10.7 | 0.28× |
| SPSC | kanal | 4.9 | 4.8 – 10.9 | 0.13× ⚠ |
| MPSC (2 producers) | **ultima_rings `sharded`** (experimental) | **317.4** | 309.4 – 329.8 | **5.28×** |
| MPSC (2 producers) | crossbeam-channel | 60.1 | 59.4 – 60.6 | 1.00× |
| MPSC (2 producers) | **ultima_rings `BusySpin`** ‡ | **76.4** | 76.3 – 76.7 | **1.26×** |
| MPSC (2 producers) | flume | 7.8 | 7.5 – 7.8 | 0.13× |
| MPSC (2 producers) | kanal | 6.0 | 5.4 – 9.1 | 0.10× ⚠ |
| MPSC (2 producers) | `disruptor` (batched consume) † | 27.2 | 25.6 – 27.7 | 0.45× |
| MPSC (2 producers) | `disruptor` (`take(1)`) † | 1.3 | 1.3 – 1.4 | 0.02× ⚠ |
| MPSC blocking | crossbeam-channel blocking | 42.7 | 41.3 – 43.7 | 1.00× |
| MPSC blocking | **ultima_rings `Park`** | **14.6** | 13.4 – 14.7 | **0.34×** |

⚠ kanal's spread is 129% (SPSC) and 68% (MPSC) across three runs, where every other cell is
under 20%. Its median is shown for completeness but is not a meaningful point estimate.

† [`disruptor`](https://crates.io/crates/disruptor) is the maintained Rust port of the LMAX
Disruptor, and the only competitor here built on the same lineage as `src/mpsc.rs` (claim
cursor + per-slot availability publication) rather than being a channel. Measured in a later
session whose comparators reproduced within ~5% of this table. Its batched-consume figure
(27.2) is **below this crate's own MPSC** (35.1) — notable because its in-place slots mean it
moves no values and does no drop bookkeeping, so it is doing less work per element and still
finishing behind. Its `take(1)` figure is *not* a like-for-like single-item comparison:
`EventPoller::take` runs a full availability walk before applying its limit, making
single-item consumption O(backlog) per event. Batched *publication* was not measured. See
[`docs/superpowers/research/2026-08-10-disruptor-survey.md`](docs/superpowers/research/2026-08-10-disruptor-survey.md).

**Compare ratios, not absolute figures across sessions.** This box measured ~20% slower than
it did on 2026-08-06 on *unchanged* code: `src/spsc.rs` was untouched between the two runs
and fell from 620.1 to 466–523, while `rtrb` — a third-party crate — fell in lockstep from
626.5 to 513–576. Deltas computed against the older
[`2026-08-06-bakeoff.md`](docs/bench-results/2026-08-06-bakeoff.md) table are therefore
meaningless; the ratios above are same-session and are the comparable quantity.

SPSC leads `crossbeam-channel` by **13.0×**. It does *not* reach parity with `rtrb`: rtrb led
in all three runs (0.91×, 0.91×, 0.86×), where the v1 measurement had recorded 0.99×. Since
`src/spsc.rs` is unchanged between the two, this is not a regression in this crate — either
the earlier pairing was favourable noise or the two crates respond differently to whatever
changed about the box, and that has not been diagnosed. Note also that this crate's `drain`
uses batched consumption while competitors single-pop, so the comparison is not
like-for-like on API shape.

‡ **MPSC now leads `crossbeam-channel` at 1.26× its throughput.** Earlier revisions of this
section recorded it trailing at 0.58×, and treated the reason as undiagnosed. It is
diagnosed: the claim CAS fails 22–42% of the time under 2–4 producers, and retrying it
immediately re-attacks the contended cursor line as fast as the core allows. An exponential
`spin_loop` backoff between failed attempts is worth **+108% to +143%** across three
configurations (`docs/bench-results/2026-08-11-cas-backoff.md`). This row is measured in a
later session than the rest of the table; crossbeam was re-measured alongside it at 60.76,
within 1% of its row above, so the two are comparable.

**What the gap was not:** earlier revisions said crossbeam wins via a `fetch_add` claim.
That is wrong — `crossbeam-channel`'s array flavor contains no `fetch_add` at all, claiming
with `compare_exchange_weak` on `tail` (and on `head` for its consumer, since it is MPMC).
Both crates do one CAS per element on a contended cursor, and this crate's is the *weaker*
ordering of the two (`Relaxed/Relaxed` against crossbeam's `SeqCst/Relaxed`). The difference
was never the claim instruction — it was how hard each crate retries after a failed one.

The `fetch_add` contrast that §7 does draw is with the `hi-perf-cmp` bench cell this crate
was ported from, which claims with an unconditional `fetch_add(1)`. Against *that* design
the bounded claim buys real properties (`try_send` can report `Full` without claiming a
slot, and a blocked producer holds no unpublished hole for the consumer to reason about),
at the cost of a CAS-retry loop.

A batched claim (reserving a contiguous run of sequences per CAS) was the long-standing
candidate for closing the gap. It is no longer: `disruptor` — the maintained Rust LMAX port,
which ships exactly that design plus a bitmap availability structure — measures **27.2
Melem/s against this crate's 33.2** in the table above, while doing *less* work per element
(its pre-constructed in-place slots move no values and need no drop bookkeeping). The
batched design measured slower than the one already shipped, so it is recorded as tried and
rejected rather than pending. A reference implementation does not hide a lost benchmark, and
does not keep advertising a fix it has since measured and dropped.

## Verification

- **loom** models every Acquire/Release edge in both rings and the park/wake protocol
  exhaustively under bounded thread interleavings: SPSC publish/consume with wraparound,
  MPSC two-producer claim/publish/drain with wraparound, the park/wake lost-wakeup
  protocol, close-vs-park races, and the producer waiter-list vs. receiver-drop race — 5
  models, all passing. This is what catches the Acquire/Release bugs x86's strong memory
  model would otherwise hide.
- **miri** runs the full test suite (51 tests with all features) over every `MaybeUninit`
  write/read/drop call site in the crate — zero undefined-behavior findings.
- **Stress tests** (`tests/*_stress.rs`) exercise no-loss/no-duplication delivery under
  sustained multi-producer contention and generic-`T` drop-accounting (every value dropped
  exactly once — no leak, no double-drop), on top of a dedicated close-semantics suite
  ported from `crossbeam-channel`'s own test corpus.
- The crate's committed verification bar additionally requires a weak-memory **ARM64** CI
  lane (`ubuntu-24.04-arm`) alongside x86, specifically because loom's exhaustive model and
  miri's UB detector are strongest combined with real weak-memory-model hardware, not x86
  alone.

See `docs/design.md` for the full argument behind every ordering choice, the alternatives
considered against six ecosystem crates (`crossbeam-channel`, `flume`, `kanal`, `rtrb`,
`thingbuf`, `heapless`), and a soundness pitfall checklist mapping each crate's own
historical bugs to the `ultima_rings` mitigation.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).
