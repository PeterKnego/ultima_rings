# flume survey — informing `ultima_rings`

**Date:** 2026-08-06
**Method:** `git clone --depth 1 https://github.com/zesterer/flume` into scratchpad, read
source (`src/lib.rs`, `src/signal.rs`, `src/async.rs`, `src/select.rs`) and the test suite
(`tests/*.rs`, 11k lines) directly. Cloned at whatever commit `HEAD` was on 2026-08-06;
`Cargo.toml` reports version `0.12.0` (2025-12-08).

## 1. "No unsafe, still fast" architecture

Flume's core (`src/lib.rs`) is a single global lock (`ChanLock<Chan<T>>`) guarding a
`VecDeque<T>` queue plus two `VecDeque` of parked-thread "hooks" (one for blocked senders,
one for blocked receivers). Every `send`/`recv`/`try_send`/`try_recv`/`len`/`is_empty`/
`disconnect_all` takes that one lock (`wait_lock`, `src/lib.rs:492-724`). There is no
lock-free fast path at all — MPMC-safety and the "no unsafe" claim both come from the same
lock. This is architecturally the opposite of `ultima_rings`' lock-free ring: flume trades a
single mutex/spinlock for zero `unsafe`, and pays for it with one global serialization point
per op, on both bounded and unbounded channels alike.

Two things soften the cost:

- **Lock choice is pluggable and defaults to the OS primitive, not a spinlock.** `ChanLock<T>`
  is `std::sync::Mutex<T>` by default; a `spin` Cargo feature swaps in `spin::Mutex` instead
  (`src/lib.rs:266-438`). The CHANGELOG (`0.10.10`, 2022-01-11) records the switch: *"Switched
  to scheduler-driven locking by default, with a `spin` feature to reenable the old
  behaviour"* — flume's own maintainer found the OS mutex faster/fairer than a naive spinlock
  under contention on typical hardware, and made spinning opt-in rather than default.
- **The lock only ever guards a short critical section** (a `VecDeque` push/pop and a hook
  fire), never the actual wait. Waiting is layered *outside* the lock via `thread::park`/
  `unpark` (`SyncSignal` in `src/signal.rs:17-45`): a blocked `send`/`recv` registers a "hook"
  (an `Arc<Mutex<Option<T>>>` + a `Signal`) while holding the lock, drops the lock, then parks.
  The waking side takes the lock just long enough to pop a hook and call `.fire()` (which
  itself just calls `unpark()`), so contention on the lock is proportional to op rate, not to
  how long either side sleeps. There genuinely is no busy-spin-for-the-message anywhere in the
  sync path — `wait_lock`'s spin-then-yield-then-sleep escalation (`src/lib.rs:401-424`) is
  *only* for acquiring the queue lock itself, a sub-microsecond critical section, not for
  waiting on a message.

**What the safety costs actually are, in flume's own numbers:** the repo ships no raw
benchmark data or a text table — `README.md` points to a PNG (`misc/benchmarks.png`,
un-renderable from this survey) generated from the **crossbeam-channel** benchmark suite, with
the caveat "don't take it from here that Flume is quick" pointing at flume's own criterion
suite (`benches/basic.rs`) instead. That suite (`create`/`oneshot`/`inout`/`hydra`/`kitsune`/
`robin_u`/`robin_b`/`mpsc_bounded*`, each run against flume, `crossbeam-channel`, and
`std::sync::mpsc`) exists but produces no numbers checked into the repo — README claims are
qualitative only ("Always faster than `std::sync::mpsc` and sometimes `crossbeam-channel`").
Structurally, though, the *shape* of the cost is clear from the code, independent of exact
numbers: every op pays a full mutex acquire/release (uncontended OS mutex: tens of ns; under
contention, OS-scheduler-dependent and can be µs) plus a `VecDeque` push/pop, versus
`ultima_rings`' lock-free path (`fetch_add`/CAS + a `ptr::write`/Release store, no syscall on
the fast path, no blocking of the *other* side ever). crossbeam-channel, flume's closest
comparator, spends real engineering (its own hand-rolled lock-free array/list/zero
implementations, `crossbeam-channel`'s `flavors/` module) specifically to beat a
mutex-protected `VecDeque` — the fact that a mature, widely-used channel crate still finds it
worthwhile to hand-roll lock-free variants is itself evidence that "safe lock+park" leaves
throughput on the table under contention, which is exactly the gap `ultima_rings` is written
to close for the SMR hot path.

**What this says about when unsafe lock-free is warranted:** flume is optimizing for a
different point in the design space than `ultima_rings` — MPMC generality, `Clone`-able
`Sender`/`Receiver` on both ends, "drop-in `std::sync::mpsc` replacement," casual-maintenance
simplicity ("Few dependencies... fast to compile"). Its own docs concede the safe/lock-based
design is a deliberate trade, not a free lunch: a single global lock is *fine* when (a) the
critical section is short and uncontended most of the time, (b) you need N:M flexibility that
a lock-free ring doesn't cleanly give you (flume supports MPMC with multiple receivers
competing for messages — `ultima_rings` deliberately scopes out multi-consumer), and (c) you
are not on a latency-critical, single-producer/single-consumer or fixed-shape hot path where
every wasted acquire matters. `ultima_rings`' SMR use case is precisely the case where none of
those hold: known fixed producer/consumer shape (SPSC/MPSC only), sub-microsecond target
latency (bench-cell provenance: ~200-400 ns one-way handoff), and a codebase already
comfortable auditing `unsafe` under loom/miri. Recommendation: don't chase "no unsafe" as a
goal in itself for `ultima_rings` — flume proves that goal is achievable and still reasonably
fast for a general MPMC channel, but its own architecture shows the price is a mutex on every
op, which is exactly what the ring's `UnsafeCell`/`MaybeUninit` design in the v1 spec is meant
to avoid.

## 2. Close / disconnect semantics

Mechanism (`src/lib.rs:471-724, 843-1075`):

- `Shared` holds `disconnected: AtomicBool` plus `sender_count`/`receiver_count: AtomicUsize`.
- `Sender::drop` decrements `sender_count`; if it hits 0, calls `disconnect_all()`
  (`src/lib.rs:860-867`). `Receiver::drop` does the same on `receiver_count`
  (`src/lib.rs:1067-1075`). Symmetric on both sides — either side reaching zero closes the
  channel for *both* directions.
- `disconnect_all()` sets the flag, then walks *every* parked hook (both the sending-side and
  the waiting-receivers `VecDeque`s) and fires its signal (`src/lib.rs:678-691`) — guaranteeing
  nothing sleeps through a close. This is the same guarantee `ultima_rings`' spec already
  commits to ("every disconnect transition wakes all parked threads").
- Order matters and is explicit in every wait loop: disconnect is checked *before* the message
  each time (`// Check disconnect *before* msg` appears four times in `src/lib.rs:342-398`) —
  so a message sent right before the last sender drops is never lost even if disconnect fires
  concurrently. `ultima_rings`'s "published messages are never lost by a disconnect" invariant
  matches this.
- `WeakSender` (`src/lib.rs:877-921`): a `Weak<Shared<T>>` that does not keep the channel open;
  `upgrade()` does a `fetch_update` CAS loop that refuses to resurrect a sender count already
  at zero (so upgrade can't race a disconnect into re-opening a closed channel). Tests
  `weak_close`/`weak_upgrade` (`tests/basic.rs:437-456`) are a clean 8-line spec for this. Not
  in the current `ultima_rings` v1 API surface (which only has `Sender`, `Clone`, `Drop`) —
  worth a one-paragraph "considered, deferred" note if a future consumer needs a
  non-channel-holding sender handle (e.g. a metrics/debug hook that shouldn't keep the ring
  alive).
- Error taxonomy is richer than `std::sync::mpsc`, and `ultima_rings`' v1 spec already mirrors
  the shape: `SendError`, `TrySendError::{Full,Disconnected}`, `SendTimeoutError::{Timeout,
  Disconnected}`, `RecvError::Disconnected`, `TryRecvError::{Empty,Disconnected}`,
  `RecvTimeoutError::{Timeout,Disconnected}` — all with `Debug`/`Display`/`std::error::Error`
  and `into_inner()` to reclaim the unsent value. `ultima_rings`' v1 surface currently only
  specifies `try_send`/`send`/`try_recv`/`recv` (no timeout variants) — see recommendation #2.
- **Richer-than-std features worth considering:**
  - `recv_deadline`/`send_deadline` (absolute `Instant`) alongside `recv_timeout`/
    `send_timeout` (relative `Duration`) — the deadline variant avoids re-computing "now +
    dur" on each retry loop iteration, which matters when a caller is deadline-driven (e.g. an
    SMR replication round with a shared round deadline across several channel ops).
  - `try_iter` (non-blocking iterator, stops at `Empty` *or* `Disconnected`) vs `iter`
    (blocking, stops only at `Disconnected`) vs **`drain`** — `drain()` atomically snapshots
    and empties the current queue contents into a fixed-size `ExactSizeIterator`
    (`src/lib.rs:991-1000, 1139-1161`) without attempting to pull any more values, which is a
    distinct contract from `try_iter` (which keeps polling until empty). `ultima_rings`' v1
    spec already has `drain(max, f)` — flume's version has no `max` (drains everything, single
    lock acquisition) but the same "batch consume without re-blocking" intent. Confirm whether
    `ultima_rings`' capped `drain` should also expose an uncapped/`usize::MAX` convenience, and
    make sure a `Drain`-style iterator (or the `f: FnMut` callback chosen) makes the "already
    removed the recv commits, doesn't put them back on early drop" guarantee explicit in
    `docs/design.md` — flume states it in the doc comment on `Drain` and the `try_iter`/`drain`
    distinction is exactly the kind of subtlety worth a rustdoc note.
  - `sender_count()`/`receiver_count()` and `same_channel()` (`Arc::ptr_eq` on the shared
    state) — cheap introspection with an explicit caveat that `receiver_count()` is racy
    ("makes no guarantees that a subsequent send will succeed"). Low-cost additions worth
    having in `ultima_rings` for diagnostics/tests even if not part of the hot path.

## 3. Async bridge shape

`src/async.rs` is a separate, feature-gated module (`#[cfg(feature = "async")]`) layered
entirely on top of the same `Shared<T>`/`Chan<T>` core used by the sync API — it adds **no**
new fields to `Shared`, only a new `Signal` impl:

- `AsyncSignal { waker: Spinlock<Waker>, woken: AtomicBool, stream: bool }`
  (`src/async.rs:19-48`) implements the same `Signal` trait as `SyncSignal`
  (`src/signal.rs:7-15`). `fire()` sets `woken` and calls `waker.wake_by_ref()` instead of
  `Thread::unpark()`. This is the entire adaptation surface — the blocking-vs-async decision
  is *which `Signal` impl the hook is parameterized over*, chosen at the call site
  (`send`/`send_async`, `recv`/`recv_async`), not a different code path through the queue.
- `Shared::send`/`Shared::recv` (`src/lib.rs:492-633`) are already generic over `S: Signal` and
  take `make_signal`/`do_block` closures — the sync callers (`send_sync`/`recv_sync`,
  `src/lib.rs:559-674`) and the async callers (`SendFut::poll`/`RecvFut::poll_inner`,
  `src/async.rs:207-253, 410-469`) both call into the *same* generic `send`/`recv` method, just
  instantiating `S = SyncSignal` vs `S = AsyncSignal` and blocking (`park`) vs returning
  `Poll::Pending` in `do_block`. This is the key structural fact: **the queue/lock core knows
  nothing about sync or async** — it only knows how to fire an abstract `Signal`.
  `RecvFut`/`SendFut` additionally implement `Future`, `FusedFuture`, and (for receive) a
  `Stream` (`RecvStream`, `src/async.rs:524-592`) and (for send) a `Sink` (`SendSink`,
  `src/async.rs:262-331`), all thin wrappers reusing the same poll logic.
- The `Signal::fire()` return value doing double duty as "is this a stream-style receiver that
  doesn't necessarily consume on wake" (`src/signal.rs:8-11`, exercised in `Shared::send`'s
  retry loop at `src/lib.rs:516-522`) is a wrinkle specific to flume's stream semantics
  (a woken `Stream::poll_next` might get cancelled/dropped before pulling the message, so the
  sender must be prepared to hand the message to the *next* waiter) — not something
  `ultima_rings` needs to replicate, since v1 has no stream/select layer, but worth knowing the
  reason it exists if a future async layer adds anything stream-like.
- Waker races are handled explicitly and are exactly the class of bug loom is good at catching:
  `update_waker` (`src/async.rs:53-66`) re-clones the waker only if it "won't wake" the current
  one (avoids needless clones on repeated polls of the same task) and, if the hook was *already
  fired* between the last poll and this one, immediately re-wakes the *new* waker rather than
  trusting the old one — "Avoid the edge case where the waker was woken just before the wakers
  were swapped." `RecvFut::poll_inner` (`src/async.rs:410-446`) re-checks disconnect status
  *after* re-registering the waker, with a comment calling out the exact race: "the channel
  might have gotten shut down before we had a chance to push our hook."

**What this says a sync-first crate should leave open for a later async layer** (directly
relevant since `uc2` may add async later, per the v1 spec's explicit non-goal): flume's shape
implies three preconditions, all satisfiable by `ultima_rings`' current design:

1. The wake mechanism must already be an abstraction, not baked to `thread::park`/`unpark`
   directly in the core. `ultima_rings`' v1 spec's `notify.rs` layer (a parker/eventcount layer
   separate from the ring core) is structurally the right cut — flume's `Signal` trait is the
   same idea in a more general (any-waiter-type) form. If/when async is added, the addition
   should be a new notify backend (waker-based) alongside the existing park-based one, not a
   rewrite of `spsc.rs`/`mpsc.rs`.
2. The core op (claim/publish/consume) must be separable from "what happens while blocked" —
   flume's `send`/`recv` taking `make_signal`/`do_block` closures is exactly this separation.
   `ultima_rings`' spec already separates "layered core + notify" for the same reason; the
   notify layer's public shape (what a blocked producer/consumer registers, and how it's woken)
   is the piece to keep future-proof, e.g. by not hard-committing the parker to storing
   `std::thread::Thread` if an enum/trait-object (or even just a generic small vtable) over
   {thread, waker} costs little now.
3. Disconnect-vs-wait races (message arrives / channel closes concurrently with
   registering-then-parking) need the same "register, then re-check" protocol regardless of
   whether the waiter is a thread or a task — flume's park path and future path both
   re-implement this check independently (`src/lib.rs:342-398` sync, `src/async.rs:410-446`
   async) rather than sharing one guarantee. `ultima_rings`' single eventcount protocol
   (store-parked-flag → recheck → park) as specified is the more disciplined version of this;
   if async is added later, the same recheck-after-register discipline should hold for a waker
   registration, and loom coverage of the protocol (already planned) should be written
   generically enough to add a "waker" waiter variant without a new model.

None of this requires adding async surface now — the useful takeaway is purely architectural:
keep the notify layer's waiter-registration API narrow and generic (a trait or a small closed
enum, not `std::thread::Thread` sprinkled through `spsc.rs`/`mpsc.rs`), so a waker-based
backend is a new notify impl, not a ring-core change.

## 4. Test-suite corner cases worth porting

Flume's test suite (11,014 lines across 18 files) is largely **ported from other channel
implementations' own suites**, credited in file headers:

- `tests/mpsc.rs` (2095 lines): *"Tests copied from `std::sync::mpsc`... modified to work with
  `crossbeam-channel`"* (and then flume) — the deepest-pedigree suite here (std → crossbeam →
  flume).
- `tests/golang.rs` (1445 lines): *"Tests copied from Go and manually rewritten in Rust"* —
  entirely commented out in this checkout (every line prefixed `//`), so currently dormant in
  flume itself, but the intent (port Go's `chan` test corpus) is a reusable idea.
- `tests/array.rs`, `tests/list.rs`, `tests/zero.rs`, `tests/ready.rs`, `tests/select.rs`,
  `tests/tick.rs` mirror crossbeam-channel's own internal test file names/structure
  (bounded/unbounded/rendezvous-flavor split) — i.e. flume re-ran crossbeam's test suite
  against its own implementation as a conformance check.

Concrete cases worth porting into `ultima_rings`' close-semantics and stress test plan
(file:line references are to this survey's clone):

- **`disconnect_wakes_sender` / `disconnect_wakes_receiver`** (`tests/array.rs:317-347`,
  mirrored in `tests/zero.rs:222-254`): one thread blocks in `send`/`recv` on a full/empty
  bounded channel, a second thread sleeps then drops the other endpoint, and the test asserts
  the blocked call returns (not just eventually via a timeout — the assertion is *inside* the
  spawned closure, so the `scope(...).unwrap()` join itself is the liveness check). This is
  precisely `ultima_rings`' "every disconnect transition wakes all parked threads" guarantee,
  and is a cheap, high-value stress test to have for both SPSC and MPSC, in both directions
  (consumer-parked-on-empty and producer-parked-on-full).
- **`send_after_disconnect` / `recv_after_disconnect`** (`tests/array.rs:222-253`): send several
  values, drop the *other* endpoint, then assert (a) further sends return
  `Disconnected` immediately for `send`/`try_send`/`send_timeout` alike (all three error
  variants exercised in one test), and (b) a receiver can still drain the values that were
  already published before consuming the `Disconnected` error on the final call. (b) is the
  "published messages are never lost by a disconnect" invariant made into an executable test —
  `ultima_rings` should have the direct equivalent (already implied by the spec's prose, worth
  making literal).
- **`drops`** (`tests/array.rs:485-...`, `tests/zero.rs:390-...`): a `DropCounter` type with a
  static `AtomicUsize` counter, sent/received a random number of times across concurrent
  threads (`rand`-seeded, 100 runs), then asserting the drop count equals exactly the number of
  values that were sent-but-never-received once both endpoints are finally dropped. This is
  exactly the "generic-`T` drop-accounting (every value dropped exactly once — no leak, no
  double-drop)" item already in `ultima_rings`' verification bar — flume's version is a good
  concrete template (randomized step count catches off-by-one drain boundaries that a fixed
  count might miss).
- **`weak_close` / `weak_upgrade`** (`tests/basic.rs:437-456`): only relevant if/when a
  `WeakSender`-equivalent is added (see §2) — noted for completeness, not an immediate port
  target.
- **`rendezvous`** (`tests/basic.rs:228-254`): for a zero-capacity (rendezvous) channel, asserts
  a blocked `send` only completes *after* a sleeping receiver actually calls `recv` — verifies
  the handshake actually blocks (timed via `Instant`, asserting `duration_since > 100ms`) rather
  than silently degrading to a buffered send. `ultima_rings`' v1 doesn't scope a rendezvous
  (cap 0) mode explicitly (`cap` must be "a positive power of two" per the spec, so 0 is
  excluded) — no action needed unless zero-capacity is added later, but worth knowing flume
  treats it as a first-class case with its own tests.
- **`len`** (`tests/array.rs:257-306`): concurrent producer/consumer threads each assert
  `s.len()`/`r.len() <= CAP` after every single op, alongside a rigid pre/post-loop exact
  count — a good template for a `ultima_rings` invariant test that the ring's observable length
  never exceeds `cap` even mid-flight under concurrent SPSC traffic.

## 5. Top 5 recommendations for `ultima_rings`

1. **Do not adopt flume's global-lock architecture as a fallback or hybrid mode** — its own
   CHANGELOG (`0.10.10`) shows even flume's maintainer moved *away* from spinning toward the OS
   mutex as the default for the lock it must take, and the entire design is optimized for MPMC
   generality/compile-time simplicity, not the fixed-shape, latency-critical SPSC/MPSC hot path
   `ultima_rings` targets (`src/lib.rs:492-633`, `Cargo.toml` feature list) — keep the
   `UnsafeCell`/`MaybeUninit` lock-free core as specified, verified by loom/miri rather than
   sidestepped by "just take a lock."
2. **Add `recv_deadline`/`send_deadline` (absolute `Instant`) alongside timeout variants** to
   the v1 API surface — flume exposes both relative (`Duration`) and absolute (`Instant`)
   forms (`src/lib.rs:756-784, 952-972`) precisely because a deadline-driven caller (an SMR
   round with a shared round deadline) shouldn't have to recompute "now + remaining" per retry;
   `ultima_rings`' current spec has neither, only unconditional blocking `send`/`recv`.
3. **Keep the notify layer's waiter-registration surface generic (trait or closed enum), not
   hard-typed to `std::thread::Thread`**, mirroring flume's `Signal` trait
   (`src/signal.rs:7-15`) that lets the identical `Shared::send`/`recv` core serve both
   `SyncSignal` (park/unpark) and `AsyncSignal` (waker) callers (`src/lib.rs:492-633` vs
   `src/async.rs:207-253, 410-469`) — this is what makes a possible future async layer for
   `uc2` an additive notify backend instead of a rewrite of `spsc.rs`/`mpsc.rs`.
4. **Write the disconnect-vs-park race test explicitly, both directions, as a named stress
   test** — port flume's `disconnect_wakes_sender`/`disconnect_wakes_receiver`
   (`tests/array.rs:317-347`) pattern (assertion *inside* the blocked thread, joined via scope)
   for both SPSC and MPSC; it is a stronger, more direct check of "every disconnect transition
   wakes all parked threads" than a timeout-based test would be.
5. **Add a randomized drop-accounting test with a `DropCounter` and a random step count**,
   following flume's `drops` test (`tests/array.rs:485ff`, `tests/zero.rs:390ff`) — the
   randomized send/receive/leftover split across repeated runs is a better catcher of
   off-by-one drain-boundary bugs in the `Drop` path (the spec's "drains and drops the
   initialized-but-unconsumed range") than a single fixed-count run.
