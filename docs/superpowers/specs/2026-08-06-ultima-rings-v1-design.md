# ultima_rings v1 — Design

**Date:** 2026-08-06
**Status:** Approved

## Purpose

A standalone, production-grade Rust crate of bounded lock-free SPSC and MPSC
rings — extracted from the proven `hi-perf-cmp` thread-handoff work and hardened
into a reference implementation for real projects. Rust-first, anchored on a
concrete consumer: `ultima_cluster`'s `uc2_net` routes `NetEvent`/`CtrlMsg`/
`HandshakeDatagram` over `std::sync::mpsc::sync_channel`, the slowest channel in
the hi-perf-cmp grid (~0.4–2 M ops/s vs ~10–390 M ops/s for the rings, one-way
handoff ~200–400 ns). The API is designed so that swap is mechanical — but the
swap itself is a separate, later project (see Out of scope).

Provenance: the SPSC and MPSC (LMAX-style fetch-add claim + per-slot
availability rounds) algorithms are ports of `hi-perf-cmp`'s
`thread-handoff-{ring,mpsc_ring}` cells — stress-tested (no-loss/no-dup under
contention, Go twin race-detector-clean), review-verified memory-ordering story,
and AWS-benchmarked (run `20260806T053918Z`).

**Identity decisions (settled):** Rust-first (Go/Java ports are possible later
phases, not v1); new sibling repo `~/ultima/ultima_rings`, own git history,
consumable as a git/path dependency like `ultima_db`; crate and repo named
`ultima_rings`; Apache-2.0 (family license); prepared for crates.io but not
published in v1.

## v1 API surface

std-shaped names so the `sync_channel` replacement is mechanical:

```rust
let (tx, rx) = ultima_rings::mpsc::channel::<T>(cap, WaitStrategy::Park);
// spsc::channel::<T>(cap, strategy) identical shape; mpsc::Sender<T>: Clone

tx.try_send(v) -> Result<(), TrySendError<T>>   // Full(v) | Disconnected(v)
tx.send(v)     -> Result<(), SendError<T>>      // blocks per strategy; Disconnected(v)
rx.try_recv()  -> Result<T, TryRecvError>       // Empty | Disconnected
rx.recv()      -> Result<T, RecvError>          // blocks per strategy
rx.drain(max: usize, f: impl FnMut(T)) -> usize // batch consume
```

- `cap` must be a positive power of two (checked at construction; panic with a
  clear message, mirroring the bench cells).
- Bounds: `T: Send` only. Handles are `Send`, not `Sync`; single-consumer and
  (SPSC) single-producer ownership is enforced by `!Sync` + `&mut self`/move
  semantics as in the bench cells.
- Disconnect semantics mirror std: all senders dropped → `recv`/`try_recv`
  drain remaining items, then report `Disconnected`; receiver dropped → sends
  fail returning the value. Published messages are never lost by a disconnect.

**Wait strategies** (per-channel, at construction, applying to both blocked
directions — consumer-on-empty and producer-on-full):

- `BusySpin` — `spin_loop()` until progress; lowest latency, one core per
  blocked side. The bench-cell behavior.
- `Backoff` — the Aeron idle ladder from the `backoff` cell: 10 spins → 20
  yields → timed park doubling 1 µs → 1 ms. Self-waking (timed parks need no
  notification), so zero fast-path cost on the other side.
- `Park` — fully blocking via the notify layer; ~1–5 µs wake latency, zero
  idle CPU. The `sync_channel` replacement mode.

## Architecture: layered core + notify (Approach A, settled)

The lock-free cores stay exactly the proven algorithms, generified; all
parking logic lives in a separate notify layer. Rejected alternatives:
futex-integrated waiting (platform-specific, entangles waiting with the core's
ordering story, hard to loom) and a Disruptor-style sequencer abstraction
(YAGNI for single-consumer v1).

### Layout

```
ultima_rings/
├── Cargo.toml              # edition 2024, zero runtime deps; dev-deps: loom, criterion
├── src/
│   ├── lib.rs              # crate docs + re-exports
│   ├── spsc.rs             # generic SPSC core (cache-padded head/tail, cached
│   │                       #   opposite indices; port of thread-handoff/ring)
│   ├── mpsc.rs             # generic MPSC core (fetch-add claim cursor +
│   │                       #   per-slot availability rounds; port of mpsc_ring)
│   ├── notify.rs           # parker/eventcount layer (the only new concurrency logic)
│   ├── wait.rs             # WaitStrategy and the Backoff ladder
│   └── atomic.rs           # facade over std vs loom atomics (cfg(loom))
├── tests/                  # stress, drop-accounting, close-semantics
├── benches/                # criterion micro-benches (regression guard)
├── docs/design.md          # memory-ordering invariants + arguments (the
│                           #   "reference" documentation, written with the code)
└── .github/workflows/ci.yml
```

### Generic storage (the new unsafe surface)

Slots become `UnsafeCell<MaybeUninit<T>>`. The publish edges are unchanged from
the proven cores — SPSC: slot `ptr::write` → tail store Release; MPSC: slot
`ptr::write` → `avail[slot]` store Release (round number `seq / cap`, `-1`
sentinel) — consumed by `ptr::read` after the corresponding Acquire load.
Memory orderings are carried over: head loads Acquire / stores Release;
consumer advances the shared head once per drain.

**Amendment (2026-08-06, planning):** the MPSC claim is a **bounded CAS**, not
the bench cell's `fetch_add`: a producer claims `seq` only after proving
`seq − head < cap` (CAS from an observed claim value). Head is monotonic, so a
successful claim's slot is always already consumed — the in-publish
backpressure spin disappears, `try_send` can report `Full` without consuming a
sequence (impossible with `fetch_add`), and a parked sender never holds an
unfilled slot (no publication holes from blocked senders). The
availability-publication orderings above are unchanged. Slot indexing uses
`& (cap−1)` rather than the bench cells' `%` (cap is a checked power of two).

Ring `Drop` drains and drops the initialized-but-unconsumed range (SPSC:
`head..tail`; MPSC: the contiguous published prefix — by drop time no live
producers exist, so no unpublished claimed slot can be pending). Zero per-op
allocation; one shared allocation per channel.

### Notify layer

**Consumer side (single consumer — single-slot parker).** Blocking `recv` in
`Park` mode: spin briefly → store `consumer_parked = true` (Release) →
**re-check the ring** → if still empty, `thread::park()`; else clear the flag
and consume. Producer after publishing: load `consumer_parked` (Acquire); if
set, clear and `unpark`. Store-then-recheck vs publish-then-check closes the
lost-wakeup race (eventcount protocol); loom verifies it exhaustively.
Fast-path cost with no parker: one load per publish on a quiescent line.
Spurious `unpark`s are harmless (park may return spuriously by contract; the
recv loop re-checks).

**Producer side.** SPSC's single producer: the same single-slot parker with
roles reversed. MPSC's N producers blocked on full: a cold-path
`Mutex<Vec<Thread>>` waiter list guarded by a `producers_waiting` flag the
consumer checks (Acquire) after advancing head. The mutex is acceptable by
construction — it only runs after the ring has been full long enough to park,
which is already the slow path — and it keeps the lock-free core untouched.
Documented as a deliberate trade.

**Close.** Sender count in `AtomicUsize` (MPSC `Clone`/`Drop`); a
`disconnected` flag set by `Receiver::Drop`. Every disconnect transition wakes
all parked threads (parker + waiter list) so nothing sleeps through a close.

## Verification bar (settled: loom + miri + ARM CI)

- **loom** (through `atomic.rs`): models for (1) SPSC publish/consume, (2) MPSC
  two-producer claim/publish/drain with wrap, (3) park/wake lost-wakeup
  protocol, (4) close-vs-park races. Small caps and counts; all interleavings
  and orderings. This is what catches Acquire/Release bugs x86 hides.
- **miri**: unit + miniaturized stress tests targeting the
  `MaybeUninit`/`ptr::write`/`ptr::read` surface and the `Drop` drain.
- **Stress tests** (ported from the bench cells and extended): no-loss/no-dup
  under contention (4 producers × 30 k × 5 reps, cap 256), generic-`T`
  drop-accounting (every value dropped exactly once — no leak, no
  double-drop), close-under-load in both directions, power-of-two rejection.
- **CI lanes (all required):** x86 test+clippy+fmt; **ARM**
  (`ubuntu-24.04-arm`) full tests — the weak-memory hardware check; miri job;
  loom job.
- **Benches:** criterion micro-benches (uncontended throughput; paced one-way
  handoff) as a regression guard against the AWS-measured numbers. The
  hi-perf-cmp grid remains the cross-language rig; these benches only guard
  this crate.

## Documentation

`docs/design.md` states each ring's invariants and the argument for every
memory ordering (why claim is Relaxed, what each Release/Acquire pair
publishes, the availability-round wrap argument, the eventcount protocol).
README carries the API tour and the real (AWS) numbers with their provenance.
Rustdoc on every public item. This documentation is half of what "reference
implementation" means here.

## Conventions

Pinned stable toolchain (`rust-toolchain.toml`), edition 2024, rustfmt/clippy
clean, Apache-2.0 (family license), author Peter Knego. Repo layout and commit
style follow the ultima siblings.

## Out of scope (v1)

- MPMC / multi-consumer, dependency graphs.
- Producer batch-claim API (`batch_publish`) — v2 candidate; the criterion
  study shows that's where Disruptor-level throughput lives.
- futex-integrated waiting; async/`Future` APIs.
- Go/Java ports (possible later phase; the polyglot conformance suite idea is
  noted but deferred).
- `ultima_cluster` integration — separate project, only after the pending
  `uc2_net` fixes branch lands.
- crates.io publication (metadata prepared; publishing is a later decision).
