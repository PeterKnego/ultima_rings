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

## When to reach for this crate

A bounded ring channel is the right tool when three properties matter at once:

- **Fixed capacity, pre-allocated.** No allocation on the hot path; a slow consumer
  produces backpressure (`Full`) rather than unbounded memory growth.
- **No lock, even uncontended.** A handoff is a slot write plus a couple of atomic
  operations — the like-for-like SPSC bake-off below measures 334 Melem/s against
  `crossbeam-channel`'s 8.1 in the same session on the same box, a 41.4× ratio on a day
  crossbeam's cell read unusually low. Across four bake-offs on that box the
  same-session ratio has ranged 13.0×–41.4×, with crossbeam's cell owning the spread —
  so the claim this README stands behind is *at least 13×, on that box* (conditions in
  *Measured numbers* below).
- **A topology you can commit to at construction.** Exactly one consumer, and either one
  producer (SPSC — no CAS at all) or N producers (MPSC — one claim CAS on the producer
  side, retried only under contention; the consumer never CASes). Deleting the general
  case is where the speed comes from.

That combination is the standard seam in pipeline-stage architectures — trading systems,
audio, logging, thread-per-core services — wherever one thread's output is another's
input and the handoff is on the latency budget. `ultima_rings` was built for exactly
such a spot: a latency-critical state-machine-replication hot path (see
[`docs/design.md`](docs/design.md) §9), where even an uncontended mutex is too much and
the tail latency of a contended one is disqualifying. If you instead need multiple
receivers, an unbounded queue, or arbitrary capacities, reach for `crossbeam-channel`
or `flume` — the generality you'd be paying for here is generality this crate
deliberately does not have.

## Documentation

Full documentation lives under [`docs/`](docs/README.md), organized by need:

- [Your first pipeline](docs/tutorials/your-first-pipeline.md) — a ten-minute
  hands-on lesson building a two-thread pipeline.
- [How-to guides](docs/how-to/README.md) — choosing a topology and strategy,
  backpressure, clean shutdown, batching, and thread placement.
- [Reference](docs/reference/README.md) — channel guarantees, the
  wait-strategy table, and error/disconnect semantics; per-item API docs via
  `cargo doc --open`.
- [Explanation](docs/explanation/README.md) — the
  [design document](docs/design.md) and how to read the benchmark numbers.

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

| Strategy | Behavior |
|---|---|
| `BusySpin` | `spin_loop()` until progress; one core pinned per blocked side |
| `BackoffYield` | spins, then `yield_now()` indefinitely; never parks, self-waking |
| `Backoff` | Aeron-style idle ladder — spins → yields → timed park doubling 64 µs → 1 ms, self-waking |
| `Park` | fully blocking park/wake via the notify layer |

Measured idle-CPU and wake-latency figures for all four are in the
[wait-strategy reference](docs/reference/wait-strategies.md); for picking one
to match a latency/CPU budget, see
[How to choose a topology and wait strategy](docs/how-to/choose-a-topology-and-wait-strategy.md).

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

### This crate's own bake-off (criterion, two machines)

`cargo bench` (see `benches/throughput.rs`) measures `ultima_rings` head-to-head against
`crossbeam-channel`, `flume`, `kanal`, `thingbuf`, `disruptor` †, and (SPSC-only) `rtrb`.
It has run on two machines — the dev box (4 vCPUs on 2 physical cores; every session
before 2026-08-14 believed it was 4 real cores) and a 16-core Xeon rig — plus a
placement-pinned cell on the dev box. The one-sentence summary of that program:
**a competitor ratio means nothing without a machine, a core count, and a thread
placement attached** — moving two threads from SMT siblings to separate physical cores
changes crossbeam's throughput 5.53× with no code change at all
([`docs/bench-results/2026-08-15-thread-placement.md`](docs/bench-results/2026-08-15-thread-placement.md)).

The most recent full bake-off on the dev box
([`docs/bench-results/2026-08-14-bakeoff-v4.md`](docs/bench-results/2026-08-14-bakeoff-v4.md))
is the first with a like-for-like SPSC cell — single-item `try_recv`, matching the
competitors' single-pop APIs, where the 480–620 Melem/s figures of earlier sessions
came from a batched-`drain` cell. (v4 measured `drain` 10% *slower* than `try_recv` on
a saturated pipeline, so the old asymmetry had, if anything, understated this crate.)
Median of five rounds, range alongside:

| Group | Competitor | Melem/s (median) | range | vs. crossbeam |
|---|---|---:|---|---:|
| SPSC | **ultima_rings `BusySpin`** | **334.1** | 327.5 – 342.3 | **41.4×** |
| SPSC | rtrb | 325.4 | 316.0 – 334.2 | 40.3× |
| SPSC | ultima_rings (`drain`) | 301.3 | 296.9 – 312.0 | 37.3× |
| SPSC | crossbeam-channel | 8.07 | 7.31 – 8.87 | 1.00× |
| SPSC | flume | 5.45 | 5.05 – 9.75 | 0.68× ⚠ |
| SPSC | kanal | 1.02 | 0.96 – 1.14 | 0.13× |
| MPSC (2 producers) | **ultima_rings `sharded`** (experimental) | **78.4** | 74.7 – 80.5 | 3.17× |
| MPSC (2 producers) | **ultima_rings `BusySpin`** | **46.5** | 41.7 – 53.0 | **1.88×** |
| MPSC (2 producers) | crossbeam-channel | 24.7 | 22.3 – 30.1 | 1.00× |
| MPSC (2 producers) | flume | 7.34 | 7.22 – 7.87 | 0.30× |
| MPSC (2 producers) | `disruptor` (batched consume) † | 6.54 | 6.49 – 6.95 | 0.26× |
| MPSC (2 producers) | `thingbuf` (ref) | 3.00 | 2.89 – 3.13 | 0.12× |
| MPSC (2 producers) | `thingbuf` (value) | 2.96 | 2.85 – 3.01 | 0.12× |
| MPSC (2 producers) | kanal | 1.61 | 1.29 – 1.87 | 0.07× ⚠ |
| MPSC (2 producers) | `disruptor` (`take(1)`) † | 1.10 | 1.06 – 1.12 | 0.04× |
| MPSC `String` (2 producers) | **ultima_rings** | **2.45** | 2.27 – 2.63 | 1.35× |
| MPSC `String` (2 producers) | `thingbuf` (ref) | 2.30 | 2.26 – 2.34 | 1.26× |
| MPSC `String` (2 producers) | crossbeam-channel | 1.82 | 1.74 – 1.91 | 1.00× |
| MPSC `String` (2 producers) | `thingbuf` (value) | 1.71 | 1.67 – 1.75 | 0.94× |
| MPSC blocking | crossbeam-channel blocking | 27.8 | 22.5 – 28.8 | 1.00× |
| MPSC blocking | **ultima_rings `Park`** | **11.4** | 11.0 – 12.6 | **0.41×** |
| MPSC blocking | `thingbuf` blocking | 3.29 | 3.21 – 3.43 | 0.12× |

⚠ flume's SPSC spread is 93.1% and kanal's MPSC spread 45.3% — listed for completeness,
not point estimates. **Absolutes here do not compare across sessions**: this box ran
~2.4× slower that day than three days earlier on unchanged code, so ratios are the only
comparable quantity — and the rig run shows even ratios move with the machine. The same
groups at two pinned topologies on the 16-core Xeon
([`docs/bench-results/2026-08-15-bakeoff-rig.md`](docs/bench-results/2026-08-15-bakeoff-rig.md)):

| ratio | dev box (v4) | rig, 4 CPUs on 2 cores | rig, 16 cores |
|---|---:|---:|---:|
| SPSC ultima / rtrb | 1.03× *(tie)* | **1.67×** | **1.80×** |
| MPSC ultima / crossbeam | **1.88×** | 0.95× *(tie)* | **1.25×** |
| String ultima / crossbeam | 1.35× | 1.39× *(tie)* | 1.00× *(tie)* |
| String ultima / thingbuf (ref) | 1.07× *(tie)* | **2.05×** | **2.35×** |
| Park ultima / crossbeam blocking | 0.41× | 0.18× ± | 0.24× ± |
| sharded / mpsc | 1.68× | **5.71×** | **6.20×** |

± `crossbeam_blocking` carries 31.7% and 62.2% spread at these two points — the rig
cannot resolve this comparison, and the dev box's 0.41× stands as the measurement of
record.

What the program supports, each claim with its conditions:

- **SPSC vs. `crossbeam-channel`: at least 13× on the dev box.** Same-session ratios
  read 15.5×, 13.0×, 17.4×, and 41.4× across four bake-offs, and crossbeam's cell owns
  that spread (8.1–39.9 Melem/s across sessions; 118% spread on the rig at 16 cores,
  where it supports no ratio at all).
- **SPSC vs. `rtrb`: decided by machine and placement, not by the two ring buffers.**
  Four dev-box sessions say parity (0.86–1.03×); the Xeon says this crate leads
  1.67–1.80×; and the pinned cell shows that on one box, ultima leads 1.16× with the
  two threads on SMT siblings and rtrb leads 1.15× with them on separate physical
  cores. Both are true, and neither is a fact about the two ring buffers alone.
- **MPSC vs. `crossbeam-channel`: ahead everywhere measured since the CAS backoff
  landed, except one tie** — roughly 1.9× on the dev box, a 0.95× tie at the rig's
  packed topology, 1.25× at 16 cores. The direction survives; the magnitude does not
  travel.
- **`Park` trails crossbeam's blocking path at 0.41×.** A caller who wants a blocking
  API and throughput should pick `Backoff` instead: its blocking cell reaches 38.4
  Melem/s against `Park`'s 12.1
  ([`docs/bench-results/2026-08-14-backoff-cells.md`](docs/bench-results/2026-08-14-backoff-cells.md)).
- **The `String`-payload cells against `thingbuf`'s reference API support no
  direction.** Three configurations produced three answers — 1.82× behind, tied, and
  2.05–2.35× ahead — so this README quotes none.
- **The sharded prototype scales** — 1.68× the production `mpsc` on the dev box,
  5.71–6.20× on the Xeon, where the shared claim cursor it deletes is the bottleneck.
  It is feature-gated, takes every wait strategy but `Park`, and gives up global FIFO
  and a global capacity bound; a direction, not a shipping path.

Two findings behind the MPSC row are kept here because earlier revisions of this README
claimed otherwise:

**The MPSC lead is a retry-policy result, not a claim-shape result.** This crate once
trailed crossbeam at 0.58×, and the reason was treated as undiagnosed. It is diagnosed:
the claim CAS fails 22–42% of the time under 2–4 producers, and retrying it immediately
re-attacks the contended cursor line as fast as the core allows. An exponential
`spin_loop` backoff between failed attempts is worth **+108% to +143%** across three
configurations ([`docs/bench-results/2026-08-11-cas-backoff.md`](docs/bench-results/2026-08-11-cas-backoff.md)).
The gap was never crossbeam claiming via `fetch_add` — its array flavor contains no
`fetch_add` at all, claiming with `compare_exchange_weak` exactly as this crate does,
and with the *stronger* ordering of the two (`SeqCst/Relaxed` against this crate's
`Relaxed/Relaxed`). The `fetch_add` contrast that `docs/design.md` §7 does draw is with
the `hi-perf-cmp` bench cell this crate was ported from, which claims with an
unconditional `fetch_add(1)`. Against *that* design the bounded claim buys real
properties (`try_send` can report `Full` without claiming a slot, and a blocked producer
holds no unpublished hole for the consumer to reason about), at the cost of the
CAS-retry loop.

**A batched claim is measured and rejected, not pending.** Reserving a contiguous run of
sequences per CAS was the long-standing candidate for closing the gap. It is no longer:
`disruptor` — which ships exactly that design plus a bitmap availability structure —
measures **6.54 Melem/s against this crate's 46.5** in the table above, while doing
*less* work per element (its pre-constructed in-place slots move no values and need no
drop bookkeeping). The batched design measured slower than the one already shipped, so
it is recorded as tried and rejected rather than pending. A reference implementation
does not hide a lost benchmark, and does not keep advertising a fix it has since
measured and dropped.

† [`disruptor`](https://crates.io/crates/disruptor) is the maintained Rust port of the
LMAX Disruptor, and the only competitor here built on the same lineage as `src/mpsc.rs`
(claim cursor + per-slot availability publication) rather than being a channel. Its
`take(1)` figure is *not* a like-for-like single-item comparison: `EventPoller::take`
runs a full availability walk before applying its limit, making single-item consumption
O(backlog) per event. Batched *publication* was not measured. See
[`docs/superpowers/research/2026-08-10-disruptor-survey.md`](docs/superpowers/research/2026-08-10-disruptor-survey.md).

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
