# Survey: `thingbuf` (hawkw/thingbuf) — informing `ultima_rings`

**Date:** 2026-08-06
**Scope:** read-only survey of `github.com/hawkw/thingbuf`, cloned shallow to a scratch dir (not
under version control here). Source references below are paths relative to the thingbuf repo root
at the cloned commit (default branch tip, depth-1 clone).

`ultima_rings` is a new bounded lock-free SPSC/MPSC ring crate: generic `T` over `MaybeUninit`
slots, MPSC via bounded-CAS claim + LMAX-style per-slot availability rounds, single consumer, wait
strategies, loom-verified. thingbuf is the closest prior art: a fixed-capacity `MaybeUninit`-slot
ring (`ThingBuf`/`Core`) wrapped by an MPSC channel layer (`mpsc::channel`) with async and blocking
wait flavors, extensively loom-tested, with a multi-year issue history that catalogs real pitfalls
in this design space.

---

## 1. Slot-recycling design (`push_ref`/`pop_ref` → `Ref<T>`)

### Mechanism

thingbuf's core insight: instead of moving a `T` into/out of the queue by value, `push_ref`/`pop_ref`
return a `Ref<'slot, T>` — exclusive, `DerefMut`-only, RAII-guarded access to a slot's `MaybeUninit<T>`
that is *already initialized* (after the first lap) with the previous occupant's value. The caller
mutates in place; drop of the `Ref` publishes the slot (`src/lib.rs` `Ref::drop`, `Slot::drop`
equivalent — see `src/thingbuf.rs:565-580`).

Concretely, in `Core::push_ref` (`src/thingbuf.rs:256-287`):
- On generation 0 (slot never used), the recycler's `new_element()` is called to initialize it.
- On generation > 0, `recycle.recycle(ptr.assume_init_mut())` is called on the *existing* value
  in the slot — never dropped, never reallocated.

The `Recycle<T>` trait (`src/recycling.rs:20-37`) has two methods: `new_element()` (cold path, first
touch) and `recycle(&mut T)` (hot path, steady state). Two built-in policies:
- `DefaultRecycle` — `T: Default + Clone`, recycles via `clone_from(&T::default())`. Only "buys"
  allocation reuse for types whose `Clone::clone_from` is overridden to retain capacity (stdlib
  `Vec`/`String`/`VecDeque`/`BinaryHeap` are; a naive user `#[derive(Clone)]` struct usually is
  *not*, silently defeating the whole point — a documented gotcha, see `src/recycling.rs:46-58`).
- `WithCapacity` — explicit `min`/`max` capacity bounds, `clear()` + `shrink_to(max)` on recycle.
  Guarantees capacity reuse for stdlib collection types (impls for `Vec`, `String`, `VecDeque`,
  `BinaryHeap`, `HashMap`, `HashSet` in `src/recycling.rs:388-489`), gated so unbounded growth of one
  outlier message can't permanently bloat every pooled slot.

`pop`/`push` (by-value convenience methods) still exist and internally call `push_ref`/`pop_ref` +
`core::mem::replace` (`recycling::take`, `src/recycling.rs:196-201`) — i.e. by-value use is just
sugar over the ref API and gets none of the reuse benefit; mixing `push` with `pop_ref` (or vice
versa) silently drops pooled allocations (documented at `src/thingbuf.rs:405-409`).

### What it buys

For message types that own a heap buffer (log lines / `String`, serialized frames / `Vec<u8>`,
`HashMap`-based batches), steady-state throughput does zero allocator calls after warm-up: capacity
converges to the high-water mark and stays there. This is the crate's whole reason for existing (see
its worked `String`-log-line example, `src/thingbuf.rs:54-178`) — pure by-value MPMC queues
(crossbeam, flume) can't do this because they don't own the "cold" slot storage independently of the
"hot" in-flight value.

### What it costs

- **API surface duplication.** Every operation exists twice: `push`/`push_ref`, `pop`/`pop_ref`,
  `send`/`send_ref`, `recv`/`recv_ref`, each with async/blocking/static variants — `Ref` wrapper
  types multiply through `SendRefInner`/`RecvRefInner`/per-flavor newtype macros
  (`src/mpsc.rs:575-637`). A generic `T` consumer has to *choose* the ref path and consistently avoid
  the value path, an invariant the type system doesn't enforce (issue #67, "confusing documentation
  around single/multi consumer"; closed but the ref-vs-value duplication itself is inherent).
- **Recycle trait is a second thing to get right per-`T`.** `DefaultRecycle`'s "guarantee" is a lie
  for any `T` that doesn't override `clone_from` — this is a foot-gun that requires reading the docs
  closely (or profiling an unexpected allocation) to discover.
- **Holding a `Ref` stalls the ring.** Because the slot is claimed-but-not-yet-published, a `Ref`
  held across an `await` or a long compute stalls *all* other producers/consumers behind that slot
  once the ring wraps — this is exactly issue #88 (see §4) and is a direct, load-bearing consequence
  of returning a live reference instead of a value.
- **Load-bearing struct-field drop order.** `SendRefInner`/`RecvRefInner` require the `Ref` field to
  drop *before* the notify-guard field, so the "release slot" happens-before "wake the other side" —
  currently enforced only by *field declaration order* + a code comment (`src/mpsc.rs:268-306`), not
  by the type system. A refactor that reorders fields silently reintroduces a race, only caught (if at
  all) by loom.

### v2 `send_ref`-style API sketch for `ultima_rings` (NetEvent-reuse use case)

For a `NetEvent { conn_id: u64, kind: EventKind, buf: Vec<u8> }`-shaped message reused across a hot
network loop, borrow the ref-return shape but tighten two things thingbuf leaves loose:

```rust
// Claim a slot; slot already holds the *previous* occupant's NetEvent (after warm-up).
// SlotGuard<'ring, T> is #[must_use], !Send across await boundaries by default (see below).
pub struct SlotGuard<'ring, T> {
    slot: &'ring Slot<T>,
    // ring/session/round bookkeeping needed to publish on drop
}

impl<T> ultima_rings::Producer<T> {
    /// Non-blocking claim. Err(Full) if no slot is available *right now* —
    /// never allocates, never recycles a value the caller doesn't end up publishing.
    pub fn try_claim(&self) -> Result<SlotGuard<'_, T>, Full>;
}

impl<'ring, T> SlotGuard<'ring, T> {
    /// Mutate in place. For NetEvent: reuse `buf`'s capacity via `buf.clear(); buf.extend(...)`
    /// rather than reassigning a new Vec.
    pub fn get_mut(&mut self) -> &mut T;
    /// Explicit publish — returns () and consumes self; equivalent to `Drop` but makes the
    /// happens-before point visible at the call site instead of implicit-on-scope-exit.
    pub fn publish(self);
    /// Explicit abandon: returns the slot to the *unclaimed* state without publishing (answers
    /// thingbuf issue #70, "cancelling an existing SendRef" — thingbuf has no way to do this;
    /// dropping a SendRef always publishes).
    pub fn cancel(self);
}
```

Differences from thingbuf, each motivated by an issue below:
1. **Recycling is not a trait callback the ring invokes for you — it's just "the buffer already has
   the old bytes; caller decides."** This avoids the `DefaultRecycle`/`clone_from` foot-gun entirely:
   for `Vec<u8>`, `get_mut().buf.clear()` unconditionally reuses capacity, no trait indirection, no
   silent fallback to reallocation.
2. **`cancel()` as a first-class op**, not just `drop`, directly answering thingbuf #70 (open,
   unresolved 3+ years) — useful when a NetEvent claim turns out to be a no-op (e.g. a duplicate ACK)
   and the producer wants to release the slot without publishing garbage or eating a recycle cost.
3. **`SlotGuard` should not implement `Send` and should be documented as "do not hold across an
   await/blocking call"** to make thingbuf issue #88's footgun (holding a ref stalls the ring)
   *harder* to hit — a compile-time nudge, not a soft warning in prose. If an async variant is needed
   later, make the async-hold case an explicit, separately-typed API so the sync hot path can't
   accidentally regress.
4. Keep `publish`/`cancel` as *explicit* consuming methods in addition to `Drop` (which defaults to
   publish) — this makes the happens-before point self-documenting at call sites and gives a place to
   put a debug-assert against holding a guard too long, rather than relying on a struct-field-order
   comment as thingbuf does.

---

## 2. MPSC claim/publication scheme: thingbuf's per-slot state field vs. an availability-round array

### thingbuf's scheme

thingbuf's `Core` (`src/thingbuf.rs:97-119`) is a **single MPMC ring** (Vyukov 1024cores bounded
MPMC queue) that the `mpsc` module *restricts* to single-consumer only by API convention (`Receiver`
is not `Clone`) — the underlying `pop_ref` CAS loop is itself safe for concurrent consumers; thingbuf
just never exposes that.

Each `Slot<T>` carries one `AtomicUsize` "state" word, **not an availability round/generation array
separate from the slot**. The state field packs:
- the low bits: the slot's index/generation stamp (`idx | gen`, compared against `tail`/`head + 1` to
  decide writability/readability — `src/thingbuf.rs:112-119`)
- the top bit (`HAS_READER`, MSB): whether a reader currently holds a `Ref` into this slot.

`head`/`tail` are separate `CachePadded<AtomicUsize>` counters. `push_ref`: CAS `tail` forward, then
if you won the CAS, either the slot's `HAS_READER` bit is clear (you own it, write in place) or it's
set (a reader is still draining that slot from a previous lap — you must *skip* it: advance the state
to the next generation and retry, `src/thingbuf.rs:238-255`). `pop_ref` does the mirror: CAS `head`
forward when `state == head + 1`; if the state is stuck at `== head` (writer claimed the tail slot but
hasn't published it yet) with no closed/full condition, `pop_ref` backs off with an exponential
spin/yield `Backoff` (`src/util.rs`) and eventually returns `Empty` rather than block
(`src/thingbuf.rs:406-413`) — so the **wait strategy is layered on top of the lock-free core**, not
part of it: `mpsc::poll_recv_ref` (`src/mpsc.rs:404-451`) only starts really parking/registering a
waker after the non-blocking `try_recv_ref` gives up.

Full/empty detection uses a "fake RMW" (`fetch_or(0, SeqCst)`) instead of a plain load, purely to get
loom to model the intended memory ordering correctly — flagged in the source as something the author
is "deeply uncomfortable" with (`src/thingbuf.rs:299-311`, `389-396`) but necessary because loom
apparently reorders an explicit SeqCst-fence-then-load differently than an equivalent RMW. Worth
knowing before assuming loom output validates a fence-based design as written.

### Differences vs. an LMAX-style availability-round array

An availability-round design (Disruptor-style) typically keeps a **separate `availableBuffer[]`**
array (one int per slot, storing which "round"/lap last published that slot) decoupled from the
claim counter, so a consumer scans forward checking `available[idx] == expectedRound` without needing
a CAS on read at all (single consumer, so head advances via plain load/store, no CAS on the consume
side). thingbuf's design differs in two structural ways:

1. **thingbuf CASes on *both* sides** (`head.compare_exchange_weak` in `pop_ref`,
   `tail.compare_exchange_weak` in `push_ref`) because its `Core` is written to support MPMC, not
   just MPSC. A true single-consumer ring can drop the head-side CAS to a load-check-store (or even
   just a load, since only the single consumer ever advances head), which is strictly cheaper — this
   is exactly the LMAX advantage and the reason ultima_rings' "single consumer" constraint should be
   exploited at the type level (no `Clone` on the consumer handle, and *no CAS in the consume path at
   all*), not just conventionally as thingbuf does.
2. **thingbuf folds "availability" into the same word as the claim generation**, using the MSB as a
   side-channel for "a reader is still here, skip me." An availability-round array separates these
   concerns: the claim counter (tail) is independent of the per-slot published-round marker, so a slow
   consumer holding a stale reference doesn't have to be reasoned about via an extra bit smuggled into
   the writer's CAS target — it's just "the array says round N isn't published yet." This is simpler
   to loom-model (per-slot state has fewer legal transitions) and simpler to reason about correctness
   for, at the cost of a second cache line touch per slot (claim counter is padded/separate from the
   availability array) — a real trade thingbuf's single-word-per-slot design avoids.

### Correctness trade-offs

- thingbuf's "skip a slot still held by a reader" path (`push_ref`'s `Ok(_) if
  check_has_reader(raw_state)` branch, `src/thingbuf.rs:238-255`) is the crate's most subtle piece of
  logic — it's directly covered by loom tests (`mpsc_test_skip_slot`, see §3) and directly implicated
  in the open bug in issue #98 (self-requeue invariant violation, §4) and issue #100 (hang/crash on
  close-while-reader-active, §4). An availability-round design that keeps the read-in-progress
  concern *out* of the writer's CAS target removes an entire class of these interactions — the writer
  never needs to "skip" a slot for reader-liveness reasons, because in a genuinely single-consumer
  design there's no other consumer to race the writer around a not-yet-fully-drained slot; the writer
  just needs the availability array to say "not yet republished."
- thingbuf's MPMC-capable `Core` underneath an MPSC-only API is arguably doing more work than
  necessary for the target use case, and that extra generality (support for concurrent readers
  contending on `head`) is precisely where the two open, unresolved correctness issues live.

---

## 3. loom usage — practices for our loom lane

### Structure

- `src/loom.rs` — thin abstraction: under `#[cfg(all(test, loom))]`, re-exports `loom::{cell, future,
  hint, sync, thread}` and a real `loom::model::Builder`; under normal builds, hand-rolled zero-cost
  shims (`core::cell::UnsafeCell` wrapper, `core::sync::atomic`, no-op `traceln`) so **the exact same
  algorithm source compiles against both loom's shadow primitives and real `core`/`std` primitives** —
  no `#[cfg]` forking of the algorithm itself, only of which atomic/cell types back it
  (`src/loom.rs:1-322`). This is the single biggest practice worth copying: write the lock-free code
  once against `crate::loom::{atomic, cell}`, not against `std::sync::atomic` directly.
- `run_builder`/`model` (`src/loom.rs:34-141`) wraps `loom::model::Builder::check` with a per-iteration
  trace buffer captured via a `tracing` subscriber into a thread-local `String`, flushed to stderr
  *only on panic* (via a custom panic hook installed once with `std::sync::Once`). This means loom's
  extremely verbose iteration/thread traces don't spam passing runs, but a failure gets full
  `test_println!`/`test_dbg!` context for exactly the failing interleaving without re-running.
- `test_dbg!`/`test_println!`/`assert_dbg!`/`assert_eq_dbg!` macros (`src/macros.rs`, referenced
  throughout `src/thingbuf.rs`) are the crate's own `dbg!`/`println!`/`assert!` that route through
  `crate::loom::traceln` in loom builds and are no-ops (or real macros) otherwise — cheap in
  production, informative under loom.
- Loom tests live colocated with the code under `mod tests { ... }` gated `#[cfg(all(loom, test))]`
  (e.g. `src/thingbuf/tests.rs`, `src/mpsc/tests/mpsc_async.rs`, `src/mpsc/tests/mpsc_blocking.rs`) —
  not in a separate top-level `tests/` dir (that dir is reserved for non-loom integration tests).

### Model-size discipline (the main tractability lesson)

- **Capacities and thread/message counts are kept tiny by construction** — ring capacities of 1–4,
  message counts of 2–9 per producer, 2–4 producer threads (`src/thingbuf/tests.rs` uses capacity 3
  and 6 threads-of-3-messages; `mpsc_async.rs`'s `spsc_send_recv_in_order_wrap` uses `N_SENDS/2 = 1`
  capacity specifically to force wraparound with minimal state space). This is the standard loom
  discipline: state-space size is roughly exponential in (thread count × operations per thread), so
  every test picks the *smallest* capacity/iteration count that still forces the interleaving under
  test (e.g. wraparound needs capacity < message count; the "skip slot" test needs capacity exactly 2
  in a 3-slot channel purely to force the skip, `src/mpsc/tests/mpsc_async.rs:80-142` with its comment
  walking through the exact intended interleaving by hand).
- **`#[ignore]` for combinatorially-huge but valuable models**, run manually/rarely: `linearizable`
  in `src/thingbuf/tests.rs:92-125` ("this takes about a million years to run"), `rx_close_unconsumed_mpsc`
  in `src/mpsc/tests/mpsc_async.rs:213-250` ("takes over an hour to run"). These stay in the tree as
  documentation of an intended property even though CI never runs them — worth doing the same for any
  ultima_rings property too expensive for routine loom, rather than deleting the test.
  - Their existence as `#[ignore]`d tests, not deleted, means a developer chasing a hard bug has a
    ready-made stress test to re-enable locally.
- **CI splits loom models into two tiers by cost, not by module**, via a `--cfg ci_skip_slow_models`
  compile flag applied to a `#[cfg_attr(ci_skip_slow_models, ignore)]` attribute on the individually
  expensive tests (`.github/workflows/ci.yml:163-224`; six named slow models:
  `mpsc_send_recv_wrap`, `mpsc_try_send_recv`, `mpsc_try_recv_ref`, `mpsc_test_skip_slot`,
  `mpsc_async::rx_close_unconsumed`, `mpsc_blocking::rx_close_unconsumed`). The slow ones each get
  their **own CI job / matrix leg**, run with `LOOM_MAX_PREEMPTIONS=1`; everything else runs grouped
  by module scope (`mpsc_blocking`, `mpsc_async`, `thingbuf`, `util`) with `LOOM_MAX_PREEMPTIONS=2`.
  A trailing dummy `all_models` job with `needs: [slow_models, models]` gives one required check for
  branch protection despite the fan-out (`.github/workflows/ci.yml:226-234`).
  - `LOOM_MAX_PREEMPTIONS` is the other lever besides state size: capping preemptions bounds loom's
    search (this is literally called out in a comment as matching what Tokio's CI does, "good enough",
    `.github/workflows/ci.yml:189-192`) — a pragmatic acknowledgment that exhaustive preemption search
    doesn't scale and a bounded search still catches real bugs.
- **`[profile.loom]` in `Cargo.toml`** (`inherits = "test"`, `lto = true`, `opt-level = 3`) — loom
  itself is what explodes runtime, so the *model code* is compiled optimized to keep the constant
  factor per-permutation as low as possible; this is a cheap, easy win worth copying verbatim.
- **A `Track<T>` allocation-leak sentinel** (`src/loom.rs:143-200`, wrapping `loom::alloc::Track`)
  is used as the element type in loom tests instead of a plain `String`/`usize`, so any leaked or
  double-freed slot is caught by loom's leak checker even in tests that aren't specifically about
  memory safety.
- A **"fake RMW instead of fence+load" workaround** for loom's memory-model differences from real
  hardware (`src/thingbuf.rs:299-311`) is worth flagging as a real gotcha for our loom lane: loom does
  not always model an explicit `atomic::fence(SeqCst)` + relaxed load the same way it models an
  equivalent RMW, per the author's own Godbolt-verified comment. If ultima_rings' round-based design
  wants to use fences (common in availability-array designs to avoid a dummy RMW), budget time to
  verify loom actually explores the interleavings intended, not just what compiles.

---

## 4. Notable issues — pitfall classes for slot-based MPSC

| # | Title | State | Pitfall class |
|---|-------|-------|----------------|
| [#98](https://github.com/hawkw/thingbuf/issues/98) | "ThingBuf breaks a basic invariant under self-requeue test" | **Open** (filed 2025-07-07, no maintainer response as of this survey) | Two threads racing `pop()` then immediately `push()`-ing the *same* value back into a `StaticThingBuf<u8,4>`, 1M iterations each — panics (an already-occupied slot gets overwritten) under real `std::thread`, but *not* under a mutex-based or `ring-channel`-based reimplementation of the same test. Reporter explicitly ruled out a test-harness bug by cross-checking against two other implementations. This is the strongest signal in the tracker that the lock-free claim/publish protocol has a genuine, currently-unfixed correctness bug reachable by an unremarkable "pop-then-repush" workload — exactly the shape of load an SMR-replay ring will produce. **Lesson: a self-requeue / pop-then-immediate-repush loom test is cheap to write and should be in ultima_rings' loom suite from day one**, since this is precisely the kind of interleaving that's easy to omit from a hand-picked test list. |
| [#100](https://github.com/hawkw/thingbuf/issues/100) | "blocking `SendRef`/`RecvRef` causes hang or crash when closing channel while a borrowed slot is active" | **Open** | Closing the channel (dropping the `Receiver`) while a `RecvRef` is still held live corrupts the wait-queue during shutdown — reproduced with a hang (`send_ref_timeout` spins forever) and a hard allocator crash (`tcache_thread_shutdown(): unaligned tcache chunk detected`) under ASAN, with a full backtrace through `WaitQueue::notify → notify_slow → List::dequeue` triggered from the `NotifyTx` drop guard inside `RecvRefInner`'s drop. **Lesson: "close while a guard/ref is outstanding" is an under-tested state transition** — the interaction between (a) an in-flight slot guard's `Drop`, (b) the wait-queue notify path, and (c) channel-close teardown is exactly the kind of three-way interaction that needs its own dedicated loom model (spawn a holder, close from another thread, drop the guard last) — this looks like a case that thingbuf's own loom suite did not cover, since it shipped and was field-discovered in production, not caught pre-merge. |
| [#88](https://github.com/hawkw/thingbuf/issues/88) | "Behavior of `SendRef` can surprise the user" | Closed / accepted-as-working-as-intended | Holding a `SendRef` (not yet dropped/published) blocks the receiver from ever advancing past that slot, even though *other*, later-sent messages are sitting ready behind it in the ring — because thingbuf's ring is strictly FIFO-ordered at the ring level, a stalled producer stalls every consumer regardless of how much other data is ready. A maintainer-adjacent commenter states this is intentional ("reserving my spot in the queue") but agrees it needs clearer docs. **Lesson: this is not really a "bug," it is the direct, unavoidable cost of the ref-return API design (§1) — ultima_rings should decide explicitly whether to accept this trade-off (simple FIFO, guard-holds-are-load-bearing) or design around it (e.g. a bounded max-hold-time, or documented as a hard API misuse to avoid), and should document the behavior up front rather than let users discover it via a multi-day debugging session as happened here.** |
| [#83](https://github.com/hawkw/thingbuf/issues/83) | "hanging up for parallel `try_send_ref` and `send`/`send_ref` from sync thread and async task" | Closed (root cause: user's `poll_recv_ref` interaction under concurrent sync+async senders got stuck in the `push_ref` CAS retry loop indefinitely — reporter's own log traces show two threads livelocked exchanging `tail` CAS failures) | Mixing wait styles (a `std::thread` polling `try_send_ref` in a tight loop against `tokio` tasks doing `send(..).await`) is not obviously guaranteed to make forward progress — a CAS-retry loop with no upper bound can, in principle, livelock two contending writers that keep invalidating each other's CAS. **Lesson: document (and loom-test) that ultima_rings' bounded-CAS claim path has a progress bound under contention, or explicitly bound the backoff/retry so pure-spin livelock is structurally impossible** (thingbuf's `Backoff` (`src/util.rs`) is exponential spin→yield but has no *hard* cap forcing eventual queuing/parking, which is presumably why this class of report recurs). |
| [#14](https://github.com/hawkw/thingbuf/issues/14) | "wait queue is quite bad" | Closed (fixed by intrusive-list rewrite in PR #16) | Original wait queue was a spinlock-guarded `Vec`; under many concurrent waiting senders this produced severe tail latency (reallocation under the lock) and memory that never shrank back down after drain — evidenced with a violin plot in the issue. Maintainer's own comment weighs three designs (Vyukov intrusive MPSC list — rejected because futures need to self-cancel/remove from the middle of the list; non-intrusive Vyukov list — allocates per-waiter; Tokio's intrusive doubly-linked list — no allocation, needs a lock) and picked the Tokio-style intrusive list. **Lesson for ultima_rings' wait-strategy design: a naive `Vec`/`Mutex`-backed waiter registry is a known, previously-hit tail-latency trap under contention; if a wait strategy needs cancellable waiters (async), budget for an intrusive-list design (or explicitly punt async support) rather than reaching for a growable collection first.** |

Other issues worth a skim but not detailed here: #62 ("mpsc: add owned refs" — a long-requested,
still-open ask for a `Ref` that doesn't borrow the channel, relevant if ultima_rings' `SlotGuard` ever
needs an owned variant for cross-thread handoff); #70 ("cancelling an existing SendRef", open,
motivates the `cancel()` method proposed in §1); #58/#54 (`no_std`/macOS compile breakage — a
reminder that portability regressions are easy to introduce in a crate this low-level and worth CI
coverage across target triples from the start, not bolted on later).

---

## 5. Top 5 recommendations for `ultima_rings`

1. **Exploit the single-consumer constraint at the type level, not just by convention** — thingbuf's
   `Core` is a full MPMC ring (CAS on both `head` and `tail`) that the `mpsc` wrapper merely declines
   to expose concurrent-consumer access to; a real single-consumer design should have *no CAS on the
   consume side at all* (plain load/store, since only one thread ever advances `head`), which is
   cheaper and — per issue #98/#100 — removes an entire class of reader-liveness interactions from the
   writer's claim path (`src/thingbuf.rs:238-255`, `src/thingbuf.rs:363-431`).
2. **Separate the availability marker from the claim counter (LMAX-style array), rather than packing
   a "has-reader" bit into the same word as the generation stamp** — this is exactly the design
   ultima_rings is already targeting; thingbuf's single-word-per-slot state (`src/thingbuf.rs:110-119`)
   is the more compact alternative but is implicated in both open correctness bugs (#98, #100), so
   treat the availability-array approach as the safer default, not merely a performance choice.
3. **Write loom tests up front for the three interleavings thingbuf got wrong or never covered**: (a)
   pop-then-immediate-repush of the same value under contention (#98), (b) close-the-channel while a
   slot guard is still held live, racing the guard's drop against teardown (#100), and (c) two
   concurrent claimants livelocked in a bounded-CAS retry loop with no forward-progress bound (#83) —
   these are cheap, small-state-space models (capacity 2–4, 2 threads) precisely because they don't
   need to be bigger to reproduce the bug class.
4. **Adopt thingbuf's loom harness architecture wholesale**: a `loom`-vs-`core/std` shim module behind
   a single set of `crate::loom::{atomic, cell, thread, ...}` imports so the algorithm is written once
   (`src/loom.rs`); `test_dbg!`/`test_println!` macros that compile to nothing outside loom; a
   panic-hook-flushed per-iteration trace buffer; a `Track<T>` leak sentinel as the loom test element
   type; small capacities/iteration counts by construction; `#[ignore]`d (not deleted) tests for
   combinatorially expensive properties; and a CI split into a small named-model "slow" matrix
   (`LOOM_MAX_PREEMPTIONS=1`) plus a scope-grouped "fast" tier (`LOOM_MAX_PREEMPTIONS=2`) — this exact
   two-tier split is what keeps thingbuf's loom suite runnable in CI at all
   (`.github/workflows/ci.yml:163-234`).
5. **Make the recycle/reuse contract explicit and type-driven instead of a `Default+Clone` trait with
   a silent-failure fallback** — thingbuf's `DefaultRecycle` only actually reuses allocations for types
   whose `clone_from` happens to be capacity-preserving (true for stdlib collections, false for an
   arbitrary `#[derive(Clone)]` struct), which is a foot-gun users discover via profiling, not via a
   compile error (`src/recycling.rs:39-58`). For the NetEvent-reuse use case, prefer an explicit
   `get_mut()`-and-mutate-in-place API (no implicit recycle callback) over reintroducing a `Recycle`
   trait, and give `SlotGuard` an explicit `cancel()` in addition to publish-on-drop, closing the gap
   left open by thingbuf issue #70 for three-plus years.

---

## Files read (thingbuf repo, cloned to scratch dir)

- `src/lib.rs`, `src/thingbuf.rs` (`Core`, `Ref`, `Slot`, push/pop CAS loops, unit tests)
- `src/thingbuf/tests.rs` (loom tests: `push_many_mpsc`, `spsc`, `linearizable`)
- `src/recycling.rs` (`Recycle`, `DefaultRecycle`, `WithCapacity`)
- `src/mpsc.rs` (`ChannelCore`, `SendRefInner`/`RecvRefInner`, notify-on-drop, `poll_recv_ref`)
- `src/mpsc/tests/mpsc_async.rs` (loom tests: skip-slot, wrap, close-drains-queue, etc.)
- `src/loom.rs` (loom/core shim, trace buffer, `Track<T>`)
- `src/util.rs` (`Backoff`, `CachePadded`)
- `src/wait.rs` (`WaitCell` vs `WaitQueue` design rationale)
- `Cargo.toml` (`[profile.loom]`)
- `.github/workflows/ci.yml` (loom CI matrix, `ci_skip_slow_models`, `LOOM_MAX_PREEMPTIONS`)
- GitHub issues #98, #100, #88, #83, #82, #70, #62, #14 (via `gh issue view`)
