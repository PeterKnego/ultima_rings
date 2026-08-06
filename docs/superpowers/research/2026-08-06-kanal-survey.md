# kanal survey — informing ultima_rings design

Source: `github.com/fereidani/kanal`, cloned shallow at HEAD (2026-08-06) into scratch;
main files read: `src/pointer.rs`, `src/signal.rs`, `src/mutex.rs`, `src/internal.rs`,
`src/lib.rs`, `tests/loom.rs`, `README.md`. Issue history pulled via `gh api
repos/fereidani/kanal/issues?state=all`.

## 1. Where the speed comes from

kanal is a bounded/unbounded MPMC channel (not SPSC) built around one central
`Mutex`-protected `ChannelInternal<T>` holding a `VecDeque<T>` queue plus a
`VecDeque<DynamicSignal<T>>` wait-list, split into `send`/`recv` halves. There is no
lock-free ring buffer at all — the reported speed comes from three orthogonal tricks,
not from lock-freedom:

1. **Direct sender→receiver transfer via `KanalPtr` (`src/pointer.rs`).** On the
   rendezvous/starved path, instead of writing into the shared queue, a signal
   (`SyncSignal<T>`/`AsyncSignal<T>`, `src/signal.rs`) carries a `KanalPtr<T>` —
   `UnsafeCell<MaybeUninit<*mut T>>` — that points into the **counterpart's own stack
   frame**. If `size_of::<T>() <= size_of::<*mut T>()`, the value is serialized
   directly into the pointer's bit pattern (no indirection, no load); otherwise the
   writer does `ptr::copy_nonoverlapping` straight into the receiver's stack slot. This
   is the "Golang-style" trick the README calls out, and it removes both a heap
   allocation and an extra pointer chase for the zero/low-capacity-channel case.
2. **A hand-rolled test-and-test-and-set spinlock (`src/mutex.rs` `RawMutexLock`)**
   tuned for the channel's short, predictable critical sections, plugged into
   `lock_api::Mutex` — not `std::sync::Mutex` (a `std-mutex` feature flag exists as a
   fallback but is slower by their own claim). This bounds the "internal mutex" cost
   the task description flags, and batches the wait-list draining (`take_recvs`,
   `take_sends`, `drain_into_blocking`) so a single lock acquisition can service
   several waiters at once.
3. **A tiered wait strategy (`SyncSignal::wait`/`blocking_wait` in `src/signal.rs`):**
   spin on a relaxed load for ~25 µs / 256 os-yields, then CAS into a "starvation" state
   and `park()`. This is essentially the Aeron-style ladder this project already uses
   elsewhere (`thread-handoff`'s `backoff` experiment) — evidence the same idea
   generalizes to MPMC channels, not just SPSC ping-pong.

Net effect: kanal's edge is algorithmic (fewer allocations/loads on the hot path,
cheap lock) rather than "lock-free vs locked" — worth internalizing since
`ultima_rings`'s design brief (lock-free SPSC/MPSC) is explicitly choosing not to
follow kanal's central-mutex approach; the loop/park ladder is the transferable part.

## 2. Loom/miri compatibility of each trick

- **Tiered spin→park wait strategy:** fully compatible and already loom-modeled in
  kanal (`tests/loom.rs` has a `#[cfg(loom)]` variant of `wait`/`blocking_wait` that
  skips the timed spin phase — "loom does not model time... Models must not rely on
  the timeout-based APIs elapsing"). Directly reusable guidance for `ultima_rings`'s
  Park wait strategy: gate the loom build behind a `#[cfg(loom)]` swap of the whole
  primitives module (kanal does this in `src/primitives.rs`), not conditional
  branches inside the algorithm.
- **The tuned spinlock (`RawMutexLock`):** compatible — it's loom-tested standalone
  (`src/mutex.rs` `loom_raw_mutex_provides_mutual_exclusion`,
  `loom_raw_mutex_try_lock`) with a plain `AtomicBool` CAS, no exotic tricks. Good
  reference for how thin a loom-verifiable mutex can be.
- **`KanalPtr` cross-stack raw-pointer transfer: inherently the riskiest piece and NOT
  something a loom/miri-clean reference design should copy as-is.** It requires the
  writer to prove, across an opaque `dynamic_ptr()`/thread-unpark hop, that the
  pointed-to stack frame is still alive and not concurrently read/written from two
  places — exactly the invariant that produced most of kanal's historical soundness
  bugs (below). It also depends on `size_of::<T>()` branching to decide
  serialize-in-pointer vs write-through-pointer, which interacts badly with generic
  `MaybeUninit<T>` layouts and needed a `repr(C)` fix once already (issue #37/#49) for
  an unrelated but adjacent transmute. `ultima_rings`'s MaybeUninit-slot ring design
  is the *safer* structural alternative to this: the slot lives in the *shared* ring
  buffer (owned by the `Arc`/allocation, not a stack frame), so its lifetime is tied
  to the channel's own lifetime instead of to a specific parked thread's stack —
  eliminates the whole "does the peer's frame still exist when I write into it" class
  of bug that issue #3 and #17/#19 below are about. Recommend not attempting a
  stack-write optimization at all, or if attempted later, gating it as a distinct,
  separately loom+miri-verified module.
- **Tagged-pointer dispatch (`tag_pointer`/`untag_pointer` in `src/signal.rs`,
  stealing the LSB of a `*const AsyncSignal<T>` to distinguish sync vs async
  signals):** compatible with miri (`ptr::map_addr`, no int-to-ptr casts through
  `as usize`), but adds a whole discriminated-union-over-raw-pointer surface only
  needed because kanal unifies sync+async signals in one channel. `ultima_rings`
  doesn't need this if it keeps wait-strategy selection as a compile-time/const
  generic parameter rather than a runtime dynamic dispatch over signal kinds.

## 3. Concrete soundness issues raised against kanal (pitfall classes for ultima_rings)

All found via `gh api repos/fereidani/kanal/issues?state=all` (68 issues, all listed
below closed except feature requests #39/#43/#48/#65/#66/#68). Grouped into pitfall
classes to test against:

1. **Stack-pointer-outlives-scope UB** — [#3 "unsafe use of pointer to object on
   stack"](https://github.com/fereidani/kanal/issues/3) (closed 2022-10-16): a pointer
   taken from a stack local, `forget`-ten, then later dereferenced — the compiler is
   free to reuse/corrupt that stack slot in the meantime. Fixed alongside
   [#4](https://github.com/fereidani/kanal/issues/4) ("Fix some sync sender/receiver
   undefined behavior") by changing the signal's pointer field to avoid two live
   mutable references and by cloning the `Thread` handle before unparking (unparking
   can let the owning thread run and destroy the `Thread` object mid-call — a
   use-after-free window). **Pitfall class: never hold a raw pointer into a peer's
   stack/local across a park/unpark or wake boundary without an ownership handoff
   that the type system enforces.**
2. **Reproducible double-free / invalid-free from the pointer protocol** — [#17
   "pointer bugs"](https://github.com/fereidani/kanal/issues/17) and [#19 "pointer
   bugs 2"](https://github.com/fereidani/kanal/issues/19) (both closed 2022-11-11/12,
   with external repro repo `beckend/kanal-bug`): `free(): invalid pointer` /
   `double free detected in tcache`, one triggered specifically by
   `sender.clone_sync()`. **Pitfall class: any clone/split operation on a
   sender/receiver handle must be audited for double-free — verify via miri, not just
   inspection, since these escaped code review.**
3. **Aliasing/retag violations caught only by miri** — [#2 "Undefined behaviour inside
   library"](https://github.com/fereidani/kanal/issues/2) (closed 2022-10-22): miri's
   stacked-borrows retag check failed on `Signal::Sync(sig) => (**sig).recv()` —
   `SharedReadWrite` retag from a tag no longer in the borrow stack. [#28 "Miri error
   when forgetting Box<T>"](https://github.com/fereidani/kanal/issues/28) (closed
   2023-01-13): `mem::forget`-ing a successfully-moved `Box<T>` was flagged as both a
   dangling-box construction and a cross-thread data race on the same allocation.
   **Pitfall class: `mem::forget`/`ManuallyDrop` "I moved it, don't drop it" patterns
   are exactly where miri catches what human review misses — run miri on every
   send/receive/cancel path, not just the happy path.**
4. **`&mut self`/`&self` aliasing races on the async signal** — [#14 "Data race with
   waker access"](https://github.com/fereidani/kanal/issues/14) and [#16 "Access to
   `AsyncSignal` is not sound"](https://github.com/fereidani/kanal/issues/16) (both
   closed 2022-11-01/11): `AsyncSignal::poll` took `&mut self` while `send` could run
   concurrently through `&self` on the same object — theoretically UB even where the
   compiler doesn't currently exploit it, and the reporter explicitly rejected
   "just wrap it in `UnsafeCell`" as a non-fix since `UnsafeCell` only licenses
   *shared* `&self` interior mutability, not a genuine `&mut`/`&` alias. Related:
   [#15 "Lost wake-ups due to race"](https://github.com/fereidani/kanal/issues/15) —
   non-UB but functionally broken interleaving between register-waker and
   check-state. **Pitfall class: never let a "parked/suspended" object be reachable
   by both an exclusive (`&mut`) and shared (`&`) API at once; model this exact
   interleaving in loom (kanal's current `tests/loom.rs` has a dedicated
   `async_recv_future_drop_races_sender` model for the closely related
   drop-races-delivery case).**
5. **Unsound `transmute`/layout assumptions** — [#36 "Unsound implementation of
   `as_sync`"](https://github.com/fereidani/kanal/issues/36) (closed 2023-10-28,
   reported by a static-analysis research group, SunLab GMU) and its fix [#37/#49
   "make struct repr(C)"](https://github.com/fereidani/kanal/issues/49): transmuting
   `AsyncSender<T>` to `Sender<T>` relied on identical field layout between two
   structs without `repr(C)`, which Rust's default layout algorithm does not
   guarantee even for structurally-identical structs. **Pitfall class: any
   `transmute`/pointer-cast between two distinct generic structs needs `repr(C)` (or
   better, avoid the transmute — use an enum or a shared inner type) — flag this
   explicitly in review since it's an easy miss that got past one library author and
   several early reviewers.**
6. **Send/Sync bound correctness** — [#33 "Incorrect Send and Sync
   bounds"](https://github.com/fereidani/kanal/issues/33) (closed 2023-05-21):
   sender/receiver types were unconditionally `Send + Sync` regardless of whether `T`
   itself was `Send`. [#45 "OneshotSender is not Sync"](https://github.com/fereidani/kanal/issues/45)
   is the opposite-direction bug (too-restrictive bound blocking a legitimate use
   case). **Pitfall class: derive/assert `Send`/`Sync` bounds explicitly and test
   both directions — a `T: !Send` channel exposing `Send` is unsound; an
   over-restrictive bound just annoys users, but both indicate the bound was
   hand-written rather than derived from the actual unsafe-access pattern.**
7. **Data races surfaced only under miri, in the oneshot fast path** — [#35 "Oneshot:
   Data race detected"](https://github.com/fereidani/kanal/issues/35) (closed
   2025-03-19, reported "continuing from #34" via `cargo +nightly miri test`) and its
   real-world manifestation [#47 "Oneshot: UB triggered when using
   Axum"](https://github.com/fereidani/kanal/issues/47, closed 2024-07-21) — a future
   cancellation (HTTP client disconnect → Axum drops the future) raced the sender's
   delivery and panicked; this exact scenario is now the loom model
   `async_recv_future_drop_races_sender` in kanal's current test suite, added as a
   direct consequence. **Pitfall class: cancellation of a pending
   receive/send (future drop, thread never parking) is a first-class race to model —
   not just the "happy path" send/recv interleavings.**
8. **A 2026-vintage regression demonstrating this is an ongoing risk class, not just
   early-days growing pains** — [#63 "ReceiveStream with buffer_unordered causes
   memory corruption and double free"](https://github.com/fereidani/kanal/issues/63)
   (closed 2026-07-19, reproduced on kanal 0.1.1 and 0.2.0-beta1): `AsyncReceiver`'s
   `Stream` adapter combined with `futures::StreamExt::buffer_unordered` corrupted
   owned payloads and double-freed; did not reproduce with plain `recv().await`.
   **Pitfall class: combinator/adapter surface (stream/iterator wrappers over the
   core channel) is a distinct attack surface from the core send/recv path and needs
   its own soundness tests — don't assume "core is proven sound" implies "every
   convenience wrapper over it is."**

Taken together, roughly 15 of kanal's ~50 closed non-PR issues are soundness/UB
reports spanning 2022 through mid-2026 — this is not a "fixed in v0.1, done" story,
it's a recurring cost of the design's raw-pointer/cross-context transfer trick.
`ultima_rings`'s loom+miri verification bar should be read as directly answering this
history, and the MaybeUninit-slot-owned-by-the-ring (rather than
pointer-into-a-stack-frame) design sidesteps pitfall classes #1, #2, and (mostly) #4.

## 4. API decisions worth noting

- **`close()` as an explicit, idempotent broadcast operation, not drop-only**
  (`src/lib.rs:241`, `pub fn close(&self) -> Result<(), CloseError>`, plus
  `is_closed()` at `:265`). Any live `Sender`/`Receiver`/`AsyncSender`/`AsyncReceiver`
  handle can call `close()` and it propagates to *all* other handles sharing the
  channel (README: "close channels using the `Close` function, enabling you to
  broadcast a close signal from any channel instance"). This is strictly more
  expressive than "channel closes when last sender/receiver drops" (std mpsc, most
  Rust channels): it supports a supervisor/watchdog forcing shutdown without holding
  or dropping every handle. Worth considering for `ultima_rings` if SMR shutdown
  paths need "abort this ring from any participant," but it adds an extra state
  (`TERMINATED`, distinct from `UNLOCKED`) to every signal and every wait/park loop
  that must be exercised in loom models (kanal does: `send_races_receiver_drop`,
  `recv_races_sender_drop`, `async_send_races_receiver_drop` in `tests/loom.rs`).
- **One channel, both sync and async APIs, convertible in either direction**
  (`as_sync()`/`as_async()`/`to_sync()`/`to_async()`, exercised in kanal's loom
  suite as `sync_send_async_recv` / `async_send_sync_recv`). This is the source of
  the tagged-pointer dispatch in `src/signal.rs` (section 2) and of two soundness
  issues (#36 unsound `as_sync` transmute, #16 `AsyncSignal` aliasing) — i.e. the
  cross-mode conversion is exactly where kanal's soundness bugs concentrate.
  `ultima_rings`'s brief doesn't mention needing sync/async unification; if it stays
  sync-only (matching Park/BusySpin/Backoff wait strategies, no `Waker`), the whole
  class of bugs in section 3 items #3, #4, #5, #7 (all async-signal or
  sync↔async-conversion specific) is structurally avoided rather than merely
  mitigated — worth stating as an explicit non-goal in `ultima_rings`'s design doc if
  that's the intent.
- **MPMC by default, not SPSC** — kanal is symmetric MPMC (`Sender`/`Receiver` both
  cloneable, arbitrary fan-in/fan-out), which is why it needs a mutex-guarded
  wait-list at all; a true SPSC ring (no contended wait-list, single producer index /
  single consumer index) is a fundamentally simpler and more loom-tractable target
  than what kanal solves. This reinforces that `ultima_rings` should not borrow
  kanal's `ChannelInternal`/wait-list architecture wholesale — it's solving a harder
  problem (arbitrary MPMC fairness/batching) that the SPSC/MPSC-with-lock-free-ring
  brief doesn't require.

## 5. Top 5 recommendations for ultima_rings

1. **Own every slot in the shared ring allocation (`MaybeUninit<T>` array behind the
   `Arc`), never in a participant's stack frame** — this is the direct fix for
   kanal's costliest and most recurring bug class (issues #2, #3, #4, #17, #19: stack
   pointer lifetime/UAF/double-free), and it's already `ultima_rings`'s stated design,
   so treat it as validated by kanal's failure history rather than merely a stylistic
   choice.
2. **Model every parked-waiter/cancel-races-delivery interleaving in loom before
   trusting it**, specifically: sender-parks-then-receiver-drops,
   receiver-parks-then-sender-drops, and (if any async-style cancellation exists)
   future-drop-races-in-flight-delivery — kanal's `tests/loom.rs` names these exactly
   (`send_races_receiver_drop`, `recv_races_sender_drop`,
   `async_recv_future_drop_races_sender`) and issue #47/#35 show the cost of skipping
   this class.
3. **Run miri on `forget`/`ManuallyDrop`/raw-pointer-write paths specifically, not
   just as a blanket CI gate** — issues #2 and #28 were both miri-only catches
   (stacked-borrows retag violation, dangling-box-on-forget) that ordinary review and
   even the existing test suite missed; treat "does the happy-path test suite pass"
   as insufficient evidence of soundness for any unsafe transfer code.
4. **Keep the Park wait strategy's spin→backoff→park ladder loom-portable by gating
   real time behind `#[cfg(loom)]`**, following kanal's `SyncSignal::wait`/
   `blocking_wait` pattern exactly (spin-phase skipped, timeout ignored under loom,
   comment explicitly warning "Models must not rely on the timeout-based APIs
   elapsing") — this is a directly reusable pattern for `ultima_rings`'s own
   BusySpin/Backoff/Park strategies given this project already has a matching
   Aeron-style ladder in `thread-handoff`'s `backoff` experiment.
5. **If any `transmute`/pointer-cast ever appears between two generic wrapper structs
   (e.g. a `Sender<T>`/blocking-view vs non-blocking-view split), require `repr(C)`
   and a codified layout-equality check (or avoid the transmute entirely via an inner
   shared type)** — this was a real unsoundness (#36) caught only by an external
   static-analysis research group two years after the code shipped, i.e. exactly the
   kind of API-surface bug that a "reference implementation, transparent by design"
   goal should preempt via explicit design rather than rely on external audits to
   find.
