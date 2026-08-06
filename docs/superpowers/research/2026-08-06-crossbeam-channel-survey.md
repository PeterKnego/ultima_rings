# crossbeam-channel survey — informing `ultima_rings`

Source: `github.com/crossbeam-rs/crossbeam`, `crossbeam-channel` subcrate, shallow-cloned at
survey time (HEAD of default branch, 2026-08-06). Files read in full: `crossbeam-channel/src/flavors/array.rs`
(the bounded ring flavor — Vyukov's bounded MPMC queue), `crossbeam-channel/src/waker.rs`
(`Waker`/`SyncWaker`), `crossbeam-channel/src/context.rs` (the actual park/unpark protocol),
`crossbeam-utils/src/backoff.rs` (`Backoff`), plus the test suites in `crossbeam-channel/tests/`
(`array.rs`, `zero.rs`, `list.rs`, `mpsc.rs`, `select_macro.rs`, `ready.rs`, `select.rs`).

## 1. Array flavor: slot/stamp design vs an LMAX availability-array design

`array.rs` implements Dmitry Vyukov's bounded MPMC queue. Layout:

- `head: CachePadded<AtomicUsize>`, `tail: CachePadded<AtomicUsize>` — each packs `{lap, mark, index}`
  into one word. `index` is the slot position; `lap` disambiguates "this pass around the ring" from
  the previous one; the top `mark_bit` of `tail` doubles as the **disconnect flag** (`tail.fetch_or(mark_bit)`
  in `disconnect_senders`/`disconnect_receivers` — no separate atomic needed for close state).
- Each `Slot<T>` carries its own `stamp: AtomicUsize` alongside the payload (`UnsafeCell<MaybeUninit<T>>`).
  The stamp is initialized to the slot's own index (lap 0) and thereafter cycles through expected values as
  the ring wraps.
- **Producer** (`start_send`): CAS `tail` from `tail` to `tail+1` (or wrap to next lap) *only if* the slot's
  current stamp equals the reserved `tail` value — i.e., the slot is proven empty for this lap before the
  claim succeeds. After the CAS wins, it writes the payload, then `slot.stamp.store(tail+1, Release)` —
  that store is the publish/ready signal.
- **Consumer** (`start_recv`): CAS `head` from `head` to `head+1` only if `slot.stamp == head+1` (i.e. the
  producer already published this slot for this lap). After winning, it reads the payload then stores the
  *next* expected stamp (`head + one_lap`) to hand the slot back to producers.

This differs from an LMAX Disruptor-style design mainly in **where the "is this slot ready" signal lives**.
LMAX separates the monotonic claim cursor (single field, or CAS'd for multi-producer) from a distinct
`availableBuffer[]` sized to the ring, because Disruptor consumers must batch-scan up to the *highest
contiguously published* sequence and support multiple independent consumers reading the same ring via a
shared cursor/sequence-barrier. Crossbeam has neither requirement (one channel = one logical stream, no
multicast to independent consumer groups), so it folds "claim ticket" and "ready flag" into the *same*
per-slot atomic (`stamp`), colocating readiness with the payload in one cache line and avoiding a second
array entirely. The cost is the lap-encoding arithmetic (`one_lap`, `mark_bit` derived from
`cap.next_power_of_two()+1` in `with_capacity`) to disambiguate a stale previous-lap stamp from a
not-yet-written one, instead of LMAX's simpler `sequence >> log2(bufferSize)` comparison against a plain
monotonic sequence. Net: crossbeam trades a bit of arithmetic complexity for one fewer array and better
locality; LMAX trades a second array for supporting multi-consumer batch/broadcast reads that
`ultima_rings` (single consumer) will never need either.

**Relevant to `ultima_rings`:** because the design is generically MPMC, *both* `head` and `tail` go through
a CAS-retry loop with `Backoff` even though in `ultima_rings` the consumer is guaranteed single — the
head side never needs a CAS at all (a single consumer can plain-load/store its own cursor with
Release/Acquire, no compare-exchange, no `backoff.spin()` contention loop on that side). This is a real,
free simplification versus copying crossbeam's algorithm verbatim: keep Vyukov's per-slot stamp on the
producer (tail) side for MPSC safety, but drop the head-side CAS.

## 2. `SyncWaker`/`Waker` park/wake machinery vs a Dekker flag+fence+recheck protocol

Crossbeam's actual synchronization variable is **not** a raw flag — it's `Context::inner.select: AtomicUsize`
(`Selected::Waiting/Operation/Aborted/Disconnected`), CAS'd from `Waiting` by whoever wins the race
(`Context::try_select`, `context.rs:98`). `thread::park()`/`park_timeout()` (`context.rs:157,166`) are just
the OS-level sleep primitive layered on top; the token semantics of `std::thread::park` (unpark-before-park
still returns immediately) protect against losing the *unpark call*, but the actual "did the condition
become true" race is closed by the CAS state machine, not by park's token alone.

The registration protocol (`array.rs` `send`/`recv`, `context.rs`):
1. `senders.register(oper, cx)` / `receivers.register(oper, cx)` — takes `SyncWaker`'s internal
   `Mutex<Waker>` and pushes an `Entry{oper, cx}` onto a `Vec` (`waker.rs:58-64`).
2. **Recheck after registering**: `if !self.is_full() || self.is_disconnected() { cx.try_select(Aborted) }`
   (`array.rs:373`/`433`) — this is the Dekker-style "recheck the condition after announcing intent" step.
   If the condition already flipped between the fast-path failure and registration, this self-aborts and
   `wait_until` returns without ever parking.
3. `cx.wait_until(deadline)` loops: check `select` state (non-`Waiting` → return), else `park`/`park_timeout`.
4. On the writer side, `write`/`read` call `self.receivers.notify()` / `self.senders.notify()`
   (`array.rs:233,324`) — under the *same* `SyncWaker` mutex, `notify()` drains `observers`, CAS's each
   entry's `select` away from `Waiting`, then calls `cx.unpark()`.

The mutex is the mechanism that makes step 2/3 race-free: register() and notify() serialize on it, so if
notify()'s snapshot (taken under lock) misses an entry, that entry's register() call hadn't returned yet at
that point — meaning the payload write (which happens-before notify, since notify is called *after* the
Release store of the slot stamp) is already visible, so that thread's own post-register recheck (step 2)
is guaranteed to observe it and self-abort. `start_send`/`start_recv` also insert an explicit
`atomic::fence(Ordering::SeqCst)` (`array.rs:200,283`) between reading the "my side's" cursor and reading
the "other side's" cursor when deciding full/empty — this is the classic Dekker fence that prevents both
head and tail loads from being reordered such that each side observes a stale view of the other and both
conclude nothing changed.

**How this compares to a Dekker flag+SeqCst-fence+`park` protocol** (what `ultima_rings` plans): crossbeam's
version is functionally the same three-step shape (announce → SeqCst-fenced recheck → park) but generalized
to support `select!` over arbitrarily many channels/threads, which is why it needs a `Mutex<Vec<Entry>>`
per channel side instead of a single flag. For `ultima_rings`'s single-consumer case, the receiver's "I'm
parked" state can be a single `AtomicBool`/`AtomicU8` + one `Thread` handle (no list, no mutex) — exactly
what a plain Dekker flag gives you, and crossbeam's design is strictly more machinery than `ultima_rings`
needs on the *consumer* side. The one thing the simpler protocol should not drop: **the explicit SeqCst
fence between "store my waiting flag" and "reload the ring's occupancy to recheck,"** matching
`array.rs:200`/`283`. Without that fence, the classic lost-wakeup bug is store-flag/load-condition and
condition-store/load-flag getting reordered independently on each side so *both* parties conclude "nothing
to do."

**What our simpler protocol needs that crossbeam's list-based one gets for free:** on the **producer**
(MPSC-full) side, crossbeam can have arbitrarily many parked senders because `Waker` is a `Vec<Entry>` and
`notify()`/`disconnect()` iterate/wake all of them. A single `AtomicBool` flag can't distinguish "0 vs N
parked producers" or wake a specific one; `ultima_rings` will need either (a) a wake-all broadcast to all
parked producers on every dequeue (accept the thundering herd — it only fires under backpressure, which is
already the slow path), or (b) a small mutex/spinlock-guarded intrusive list of parked producer `Thread`
handles mirroring `Waker`'s `Vec<Entry>` but without `select!`'s generality. Recommend (a) for simplicity
given `ultima_rings`'s narrower scope.

## 3. Close/disconnect corner cases covered by their tests

Worth porting into `ultima_rings`'s own test suite (names below are the crossbeam originals, mostly in
`crossbeam-channel/tests/array.rs` unless noted):

- **`send_after_disconnect`** (`array.rs:222`) — sender keeps working (`send`/`try_send`/`send_timeout`) up
  until the receiver drops; every send call afterward returns the disconnected-error variant with the
  value handed back (not silently dropped) — this is the "send-after-rx-drop returns the value" case the
  task called out.
- **`recv_after_disconnect`** (`array.rs:240`) — sender sends 3 items then drops; receiver must still drain
  all 3 buffered items via `recv()` before finally getting the disconnected error. This is
  "recv-drains-before-disconnect."
- **`disconnect_wakes_sender`** / **`disconnect_wakes_receiver`** (`array.rs:316,333`, duplicated for the
  zero-capacity flavor in `zero.rs:220,236` and through `select!` in `select_macro.rs:1523,1541`) — a
  thread parked on `send()`/`recv()` must be woken (not left hanging) the instant the other side is dropped,
  even though no data crossed. This is "drop wakes all [parked] waiters" — crossbeam's `Waker::disconnect`
  (`waker.rs:155`) iterates every registered entry so it generalizes to N waiters even though these specific
  tests only exercise one; `ultima_rings` should add a multi-producer variant (spawn several parked senders
  on a full channel, drop the receiver, assert all wake) since that's the scenario its own wake-all design
  will hit.
- **`drop_unreceived`** (`array.rs:717`) — an `Rc`/refcounted message sent but never received must be
  dropped *immediately* when the last receiver is destroyed, verified via `Weak::upgrade().is_none()` — not
  deferred to when the sender itself later drops. Backing mechanism is `discard_all_messages`
  (`array.rs:536`), called from `disconnect_receivers` under the safety contract that it only runs once, on
  the last receiver, after all other receiver destructions have been observed with acquire-or-stronger
  ordering (see the `# Safety` doc comment on `disconnect_receivers`, `array.rs:507`).
- **`drops`** (`array.rs:485`, `zero.rs:388`, `list.rs:392`) — randomized fuzz test: send/recv a random
  prefix concurrently, then send an "additional" random tail synchronously, then drop both ends; asserts
  total drop count equals total messages sent exactly once each — catches double-drop/leak bugs in the
  wrap-around and disconnect paths together, which per-scenario unit tests tend to miss.
- **`panic_on_drop`** (`array.rs:660`) — if a message's `Drop` impl panics while the channel itself is being
  torn down (`discard_all_messages`), the *remaining* undropped messages are intentionally leaked rather
  than double-dropped or aborting — an explicit, tested policy decision worth deciding on purpose rather
  than by accident.
- **`oneshot_single_thread_try_send_closed`** / **`try_recv_closed`** / **`try_recv_closed_with_data`**
  (`mpsc.rs:458,472` and duplicated `1249,1269,1276`) — `try_send`/`try_recv` on an already-closed channel
  must report closed, and `try_recv_closed_with_data` specifically checks the drain-before-closed ordering
  still holds for the non-blocking path, not just blocking `recv`.
- **`drop_full`** / **`drop_full_shared`** (`mpsc.rs:214,220`) — dropping a channel that still holds
  buffered, unreceived messages (including when there are multiple live sender handles / clones) must not
  leak or double-free those messages.
- Zero-capacity (`zero.rs`) mirrors nearly the entire `array.rs` suite for the rendezvous (cap-0) channel —
  **`ultima_rings` explicitly excludes zero-capacity**, so this whole file's scenarios (rendezvous hand-off
  semantics, `recv_in_send`-style same-thread deadlock traps) can be skipped by design; worth a one-line
  note in `ultima_rings` docs that zero-capacity is out of scope precisely because it needs this distinct
  rendezvous flavor (`crossbeam-channel/src/flavors/zero.rs`) rather than the array/ring one.

## 4. Spinning before parking

`Backoff` (`crossbeam-utils/src/backoff.rs`): `SPIN_LIMIT = 6`, `YIELD_LIMIT = 10`.
- `spin()` (used inside the lock-free retry loops in `start_send`/`start_recv` themselves, e.g. on CAS
  failure or while waiting for a stamp to flip): busy-spins `1 << min(step, 6)` iterations of
  `core::hint::spin_loop()` (PAUSE/YIELD instruction), i.e. 1, 2, 4, ... up to 64 pause-instructions per
  call, capping growth at step 6.
- `snooze()` (used in `send`/`recv`'s outer retry loop before deciding whether to park, `array.rs:347-359`
  and `403-419`): same doubling pause-loop for `step <= 6`, then switches to `std::thread::yield_now()` for
  `7 <= step <= 10`.
- `is_completed()` returns true once `step > 10` — i.e. **after roughly 6 pure-spin steps (≤64 pauses each)
  followed by ~4 `yield_now()` steps**, `send`/`recv` give up on `Backoff` and fall through to registering +
  parking (`array.rs:354-359`, `414-419`). So the spin-before-park budget is small and fixed
  (11 escalating steps total), not time-based — it's "give the other side ~11 short bursts of a chance to
  finish its in-flight CAS/store" rather than a real busy-wait-for-latency strategy; the actual
  wait-for-availability sleep is `thread::park`/`park_timeout`, not an extended spin.

## 5. Top 5 recommendations for `ultima_rings`

1. Keep Vyukov's per-slot `stamp` (claim-ticket + ready-flag folded into one atomic) for the MPSC producer
   side, but drop the CAS-retry loop on the consumer/head side entirely — `ultima_rings` guarantees a single
   consumer, so `head` only ever needs a plain Acquire-load/Release-store, unlike crossbeam's generic MPMC
   `start_recv` (`crossbeam-channel/src/flavors/array.rs:238-308`) which CAS's `head` because it must also
   support multiple consumers.
2. Reuse the "disconnect bit packed into the tail word" trick (`tail.fetch_or(mark_bit)`,
   `array.rs:492-522`) instead of a separate disconnected `AtomicBool`, so close state is checked for free
   on every existing tail load.
3. Between storing the "I'm parking" flag and rechecking ring occupancy (and symmetrically, after writing a
   slot and before deciding whether to notify), insert an explicit `atomic::fence(Ordering::SeqCst)` exactly
   as `array.rs:200` and `array.rs:283` do — this is the piece a naive Acquire/Release-only flag+recheck
   protocol most often gets wrong and silently loses wakeups under reordering.
4. Port `send_after_disconnect`, `recv_after_disconnect`, `disconnect_wakes_sender`/`_receiver` (extended to
   multiple parked producers), `drop_unreceived`, and the `drops` fuzz test
   (`crossbeam-channel/tests/array.rs:222,240,316,333,485,717`) verbatim into `ultima_rings`'s test suite —
   these are exactly the corner cases a from-scratch ring channel gets wrong first.
5. Do not copy `SyncWaker`'s `Mutex<Vec<Entry>>` (`crossbeam-channel/src/waker.rs:32-38,182-188`) — it exists
   to support `select!` over arbitrary channel sets and per-thread targeted wakeup, which `ultima_rings`
   doesn't need; a single `AtomicBool` + `Thread` handle suffices for the lone consumer, and a wake-all
   broadcast (accepting the thundering herd, since it only fires on backpressure) suffices for parked MPSC
   producers.
