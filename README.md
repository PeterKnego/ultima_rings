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
| `Park` | Fully blocking park/wake via the notify layer; ~1–5 µs wake latency | idle CPU efficiency matters more than the last few microseconds of latency |

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

`cargo bench` (see `benches/throughput.rs`, full results in [`docs/bench-results/2026-08-06-bakeoff.md`](docs/bench-results/2026-08-06-bakeoff.md))
measures `ultima_rings` `BusySpin` throughput head-to-head against `crossbeam-channel`,
`flume`, `kanal`, and (SPSC-only) `rtrb`:

| Group | Competitor | Melem/s (mid) |
|---|---|---:|
| SPSC | **ultima_rings `BusySpin` (pipelined)** | **620.1** |
| SPSC | rtrb | 626.5 |
| SPSC | crossbeam-channel | 39.9 |
| SPSC | kanal | 20.7 |
| SPSC | flume | 10.9 |
| MPSC (2 producers) | crossbeam-channel | 71.0 |
| MPSC (2 producers) | **ultima_rings `BusySpin`** | **29.9** |
| MPSC (2 producers) | flume | 7.7 |
| MPSC (2 producers) | kanal | 5.3 |

SPSC leads `crossbeam-channel` by **~15.5×** and lands at parity with `rtrb` (also a
minimal-overhead, wait-strategy-free lock-free SPSC ring); note that this crate's `drain`
uses batched consumption while competitors single-pop, yet the single-pop `rtrb` result
(626 Melem/s) is only 1% higher, so batching is not the headline's source. **MPSC currently
trails `crossbeam-channel`, at ~0.42× its throughput** — under this 2-producer/4-core
contention shape, crossbeam's `fetch_add` claim with a colocated per-slot stamp+payload
beats this crate's bounded-CAS claim over a separate availability array. This is an
accepted, documented trade for v1, not an oversight: the bounded claim buys correctness
properties `fetch_add` structurally cannot (`try_send` can report `Full` without claiming
a slot, and a blocked producer holds no unpublished hole for the consumer to reason about —
see `docs/design.md` §7) at the cost of a CAS-retry loop plus the availability array's
per-slot false sharing (`docs/design.md` §8); a batched claim (reserving a contiguous run
of sequences per CAS instead of one at a time) is the identified v2 lever to close this
gap. A reference implementation does not hide a lost benchmark.

## Verification

- **loom** models every Acquire/Release edge in both rings and the park/wake protocol
  exhaustively under bounded thread interleavings: SPSC publish/consume with wraparound,
  MPSC two-producer claim/publish/drain with wraparound, the park/wake lost-wakeup
  protocol, close-vs-park races, and the producer waiter-list vs. receiver-drop race — 5
  models, all passing. This is what catches the Acquire/Release bugs x86's strong memory
  model would otherwise hide.
- **miri** runs the full test suite (32 tests) over every `MaybeUninit`
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
