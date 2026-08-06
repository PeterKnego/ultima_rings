# ultima_rings — Design

This document is the reference companion to the code: for every atomic operation in
`src/spsc.rs` and `src/mpsc.rs` it states the invariant that operation upholds, the
ordering it needs, and why a weaker ordering would not suffice. It also records the
alternatives considered against the ecosystem survey and the soundness pitfalls other
lock-free channel crates hit, mapped to the mitigation `ultima_rings` actually has. Where
this document and the code disagree, the code is authoritative — but as of this writing
every claim below has been checked against `src/spsc.rs`, `src/mpsc.rs`, `src/notify.rs`,
`src/wait.rs`, and `src/atomic.rs` on `feat/v1`.

## 1. Rings and invariants

### SPSC

The SPSC ring (`src/spsc.rs`) is two monotonic `usize` counters, `tail` (total items
pushed, owned by the producer) and `head` (total items popped, owned by the consumer),
each pinned to its own 64-byte cache line (`CachePadded`) so producer-local and
consumer-local traffic never bounce the same line. The ring's logical occupancy is
`tail - head`, and the core invariant the whole design hangs off is:

```
0 <= tail - head <= cap        (always, for every observer)
```

`tail` and `head` only ever increase (`self.tail += 1`, `self.head += 1` in program
order on their respective owning threads — no wraparound, no decrement), so `tail - head`
is well-defined `usize` arithmetic for the lifetime of the channel: the physical slot for
sequence `n` is `n & mask` where `mask = cap - 1`, and because `cap` is a checked power of
two, two sequences map to the same slot iff they differ by a multiple of `cap`.

The producer maintains the invariant by refusing to advance `tail` past `head + cap`: a
push first checks `tail - cached_head == cap` (a full ring), and only if the cache is
stale does it re-load the real `head`; if the ring is still full after that reload,
`try_send` returns `Full` without touching the slot or advancing `tail`. The consumer's
half is symmetric: a pop only proceeds when `head != cached_tail` (backed by a fresh
`Acquire` load of `tail` if the cache is stale).

The second invariant is the one that makes the ring memory-safe without per-slot
bookkeeping: **every sequence's slot is written exactly once before its `tail` Release-
publish, and read exactly once after the matching `Acquire`-observe of that publish.**
Concretely: `try_send` writes the slot (`(*p).write(v)`), *then* increments and stores
`tail` with `Release` — so no other thread can observe the write before it is complete.
`try_recv`/`drain` load `tail` with `Acquire` (or rely on a previously Acquire-loaded
cached value) before reading the slot with `assume_init_read`, so the read can never race
the write. Because indices are monotonic and slots are only reused after `head` has
proven the previous occupant of that index was already consumed (the full check above),
a slot is never written twice without an intervening read, and never read twice without
an intervening write — the standard single-writer/single-reader-per-index discipline that
makes `MaybeUninit<T>` safe here without a separate "is this slot initialized" flag: the
index pair `(head, tail)` *is* that flag.

### MPSC

The MPSC ring (`src/mpsc.rs`) generalizes the producer side to N threads and replaces
`tail` with two things: a **claim cursor** (`claim: AtomicUsize`, "next sequence to
claim") and a **per-slot availability array** (`avail: Box<[AtomicI64]>`, one round
number per slot, `-1` meaning "never published"). The consumer side (`head`) is unchanged
from SPSC — there is still exactly one consumer, so `head` needs no CAS, only a plain
load/store.

The load-bearing invariant is the **bounded-CAS claim**: a producer's
`compare_exchange_weak(seq, seq + 1, ...)` on `claim` is only attempted after the same
loop iteration has proven `seq - head < cap` (comparing against a possibly-stale cached
head, refreshed with an `Acquire` load of the real `head` whenever the cheap check fails).
Because `head` only increases, and because the check is against a value that is always
`<=` the true `head` at the moment of the CAS (a stale cache only under-counts progress,
never over-counts it), **every CAS that succeeds does so with `seq - head < cap` true at
that instant, for the true `head`.** This is materially different from the classic
LMAX/Vyukov `fetch_add` claim, which increments unconditionally and defers the
full-ring wait to *after* the claim — see §7.

That bound buys the safety argument for free: `seq - head < cap` means `head > seq - cap`,
i.e. the consumer has already advanced past sequence `seq - cap` — the previous occupant
of the *same physical slot* (`(seq - cap) & mask == seq & mask` since `cap` is a power of
two and subtracting a multiple of `cap` doesn't change the remainder). So **the slot's
previous occupant is provably already consumed before the CAS that lets a new producer
write into it succeeds** — the same "index pair proves the slot is safe to touch" argument
as SPSC, just carried by `claim`/`head` instead of `tail`/`head`.

The last piece is the wrap/ABA argument for the availability array. If `avail[slot]` were
a plain boolean "ready" flag, a slow consumer could confuse round `r`'s publish with round
`r - 1`'s stale flag after a full wrap. `ultima_rings` avoids this by storing the **round
number itself** (`seq / cap`) rather than a boolean: the consumer's readiness check is not
"is this slot marked ready" but "does `avail[slot]` equal *exactly* the round I am
expecting" (`avail[seq & mask] == seq / cap`, in `Receiver::slot_published`). Rounds
strictly increase per slot (each successive occupant of a given slot has a round exactly
one greater than the last, since occupants of the same slot are `cap` sequences apart, and
`(seq + cap) / cap == seq / cap + 1`), so a stale round-`r - 1` value in `avail[slot]` can
never satisfy a round-`r` equality check — the consumer never mistakes a `r-1` leftover for
a `r`-round publish. This is the standard Disruptor-style defense against the ABA problem
that a single reused boolean would have.

## 2. Ordering table

| Atomic | Op (side) | Ordering | Pairs with | Publishes / consumes |
|---|---|---|---|---|
| spsc `tail` | store (producer, `try_send`) | Release | `tail` load Acquire (consumer) | The slot write just performed |
| spsc `tail` | load (consumer, `try_recv`/`drain`/Park recheck) | Acquire | `tail` store Release (producer) | — |
| spsc `head` | store (consumer, `try_recv`/`drain`) | Release | `head` load Acquire (producer) | The slot as reusable (freed for a future write) |
| spsc `head` | load (producer, `try_send`/Park recheck) | Acquire | `head` store Release (consumer) | — |
| spsc `disconnected` | store (`Sender`/`Receiver` `Drop`) | Release | `disconnected` load Acquire | Every write made before the drop (paired with the SeqCst fence, see §5) |
| spsc `disconnected` | load (`try_send`/`try_recv`/Park recheck) | Acquire | `disconnected` store Release | — |
| mpsc `claim` | CAS (producer, `try_send`) | Relaxed / Relaxed | (nothing — see below) | Uniqueness of the claimed sequence only |
| mpsc `claim` | load (producer, retry loop / Park recheck) | Relaxed | — | — |
| mpsc `avail[slot]` | store (producer, `try_send`) | Release | `avail[slot]` load Acquire (consumer) | The slot write just performed |
| mpsc `avail[slot]` | load (consumer, `slot_published`/`drain`/`Shared::drop`) | Acquire | `avail[slot]` store Release (producer) | — |
| mpsc `head` | store (consumer, `try_recv`/`drain`) | Release | `head` load Acquire (producer) | The slot as reusable |
| mpsc `head` | load (producer, `try_send`/Park recheck) | Acquire | `head` store Release (consumer) | — |
| mpsc `senders` | fetch_add (`Sender::clone`) | Relaxed | (part of the AcqRel RMW chain below) | — |
| mpsc `senders` | fetch_sub (`Sender::drop`) | AcqRel | itself (the RMW chain) and `senders` load Acquire | Every write this producer made, transitively, once the count reaches zero |
| mpsc `senders` | load (`try_recv`/`recv`, `== 0` check) | Acquire | the last `fetch_sub` that reached zero | — |
| mpsc `rx_dropped` | store (`Receiver::drop`) | Release | `rx_dropped` load Acquire | Every write made before the drop (paired with the SeqCst fence, see §5) |
| mpsc `rx_dropped` | load (`try_send`/Park recheck) | Acquire | `rx_dropped` store Release | — |

**Why the claim CAS is Relaxed.** `claim`'s only job is to hand out disjoint sequence
numbers to competing producers — it is a ticket dispenser, not a data channel. The
CAS's own total modification order on `claim` already guarantees two producers never
win the same sequence (that's what compare-exchange *is*, regardless of ordering); no
producer needs to observe *any other memory* as a side effect of winning the CAS, because
the thing that actually publishes the slot's payload to the consumer is the `avail[slot]`
Release store, done separately, after the winning producer has finished writing the slot.
Ordering "rides entirely on `avail`," as the module doc puts it: the consumer never reads
`claim` at all, so there is no cross-thread edge for the CAS's ordering to carry.

**Why `head` loads on the producer side and `tail`/`avail` loads on the consumer side are
Acquire, not Relaxed.** These are the one genuine cross-thread edges in each direction:
the producer's `head` load must see the consumer's prior `head` Release-store (so the
full-check reflects real progress, not a stale view that could let a claim/write race an
unconsumed slot); the consumer's `tail`/`avail` load must see the producer's prior
Release-store (so `assume_init_read` never races the `write` that produced the value).

**Why `senders`'s `fetch_sub` is `AcqRel` on every decrement, not just the last one.**
The textbook `Arc`-drop optimization uses `Release` for every decrement and pays the
`Acquire` cost only on the thread that observes the count reach zero (via a separate
fence). `ultima_rings` instead makes every `fetch_sub` `AcqRel`, unconditionally. Since
`fetch_sub` is a read-modify-write on the same atomic, and every decrement (and the
`Relaxed` `fetch_add` in `Clone`) shares one total modification order, each `AcqRel`
decrement's *acquire* half synchronizes-with whatever wrote the value it read — which is
either an earlier `AcqRel` decrement's *release* half, or the initial `senders = 1` store.
The result is a transitive happens-before chain through every sender's own drop, so the
receiver's plain `Acquire` load observing the count reach `0` happens-after not just the
*last* dropping sender's own final `avail` publish, but (transitively, through the chain)
every earlier sender's final publish too. It is more conservative than the classic
optimization (every decrement pays the acquire cost, not just the last), but it is paid
on the cold `Drop` path, once per producer's lifetime — not the hot send path — so the
simplicity is free.

**Why every `disconnected`/`rx_dropped` transition is Release and paired with a SeqCst
fence** rather than just relying on Release/Acquire: see §5 and §3 — the flag alone would
be enough to prevent races *on the flag's own data*, but the wake protocol additionally
needs the flag-check and the parked-waiter-registration-check to never both miss each
other, which is what the fence buys, not the Release/Acquire pair by itself.

## 3. The Dekker wake protocol

`Park` mode is the only strategy that requires cross-thread notification (`BusySpin` and
`Backoff` are self-waking — see §8). The protocol that makes `Park` mode lose no wakeups is
a textbook Dekker's-algorithm shape, implemented once in `src/notify.rs` and used
identically by both rings' both directions.

**The two racing sequences.** The waiter (a thread about to block because the ring looked
empty/full):

1. `prepare_park()` — stores its "I intend to park" flag (`parked = true`, `Relaxed`) and
   registers its `Thread` handle.
2. `fence(SeqCst)`.
3. **Re-check the ring's real state** (an `Acquire` load of the condition atomic —
   `tail`/`head`/`avail`/`disconnected`/`senders`, depending on caller).
4. If the re-check now shows progress, `cancel()` (withdraw the registration) and retry the
   op instead of parking. Otherwise, call `park()`.

The waker (a thread that just published data, freed a slot, or is tearing the channel
down):

1. Publish the change (the slot write + the `Release` store of `tail`/`head`/`avail`, or
   the `Release` store of `disconnected`/`rx_dropped`).
2. `fence(SeqCst)`.
3. **Check the "is anyone parked" flag** (a `Relaxed` load inside `Parker::wake`/
   `WaiterList::wake_all`) and unpark if so.

**Why this can't lose a wakeup.** `SeqCst` fences participate in a single global total
order shared by *all* `SeqCst` operations in the program. Consider the waiter's fence
(step 2 above) and the waker's fence (its own step 2): one of them is first in that total
order.

- If the **waker's fence is first**: by the time the waiter's fence executes (later in the
  total order), the waker's Release-publish (step 1, which precedes its fence in program
  order, and `SeqCst` fences are also `Release`+`Acquire` barriers themselves) is
  guaranteed visible to the waiter's subsequent re-check (step 3) — the waiter's re-check
  sees the new data and never parks.
- If the **waiter's fence is first**: by the time the waker's fence executes, the waiter's
  flag-store (step 1, program-order-before its fence) is guaranteed visible to the waker's
  subsequent flag-check (step 3) — the waker sees the waiter and wakes it.

Either way, at least one side observes the other's write — the classic Dekker argument.
Without the fences, a plain `Release`/`Acquire` pair on *each individual atomic*
(flag-store paired with flag-load, data-store paired with data-load) does not prevent both
sides' independent reorderings from each concluding "nothing changed": the waiter's
flag-store could be reordered after its re-check from the CPU's perspective absent a
stronger barrier tying the two operations together, and symmetrically for the waker. The
`SeqCst` fence is what forecloses that reordering on *both* sides simultaneously via one
shared total order, not the individual Release/Acquire edges.

**Why `std::thread::park`'s token absorbs the unpark-before-park race.** Steps 3–4 above
still leave a narrow window: the waiter's re-check (step 3) could observe "still
empty/full" and decide to call `park()`, but the waker's `unpark()` (triggered by its own
step 3, racing concurrently) could fire *before* the waiter's `park()` call actually
executes. If `thread::park()` were a bare "sleep the OS thread" primitive, this would be a
lost wakeup. It is not: `std::thread::park`'s contract keeps a per-thread token, and
`unpark()` sets that token whether or not the target is currently inside `park()` — a
subsequent `park()` call consumes an already-set token and returns immediately rather than
blocking. So the narrow post-recheck, pre-park race is *not* a correctness gap: whichever
order the unpark and the park land in, the thread never blocks past the point its
condition became true. This is exactly why the custom `Parker` in `src/notify.rs` layers
its own flag/registration protocol *on top of* `std::thread::park`/`unpark` rather than
reinventing a wait/notify primitive from scratch: it gets this absorption for free.

**Why the fence is only paid in `Park` mode.** On every ordinary `try_send`/`try_recv`/
`drain` publish, the fenced wake sequence (`crate::atomic::fence(Ordering::SeqCst)` +
`wake`/`wake_all`) is guarded by `if sh.strategy == WaitStrategy::Park`, and the parking
loop itself (with its own fence) is only reached from the `WaitStrategy::Park` arm of
`send`/`recv`'s strategy match — so `BusySpin` and `Backoff` never register a waiter and
never call `wake`/`wake_all` on the hot path, and their fast path is exactly the lock-free
core's own Release/Acquire edges, nothing more. (The one exception is `Sender`/`Receiver`
`Drop`, §5: the disconnect fence+wake there runs unconditionally regardless of strategy,
since a `WaiterList`/`Parker` with nothing registered is a cheap no-op wake and the close
path is cold regardless.) This is the "Park-mode's one `SeqCst` fence per operation on
each side" cost quantified in §8.

## 4. Waiter list (MPSC producers)

SPSC's Park mode needs only a single-waiter `Parker` on each side (one producer, one
consumer). MPSC's producer side can have *N* threads blocked on a full ring at once, so it
needs a list, not a single flag+`Thread` slot: `WaiterList` (`src/notify.rs`) is a
`Mutex<Vec<Thread>>` guarded by a `waiting: AtomicBool` fast-path flag, with `prepare_wait`
pushing the current thread and `wake_all` draining the whole list and unparking every
entry.

**Cold-path-by-construction.** `WaiterList::prepare_wait`/`park` are only ever reached from
`Sender::send`'s `WaitStrategy::Park` arm, itself only reached after `try_send` has already
returned `Full` — i.e. only once the ring has been observed full. A mutex here is
acceptable specifically *because* the code path that takes it is already the slow path
(backpressure), not the hot send loop; the lock-free claim/publish core (`try_send`'s
happy path) never touches `WaiterList` at all. This mirrors the trade the crossbeam-channel
survey makes for its own `SyncWaker` — see §9.

**Wake-all with per-waiter re-check.** `Receiver::wake_producers` calls `wake_all()`
unconditionally on every successful `try_recv`/`drain` (when in `Park` mode) — it does not
try to wake only "the one producer that now has room." Every woken producer thread resumes
its own `send` loop and re-attempts `try_send`; if the space was already claimed by another
producer that woke first, this producer's `try_send` simply returns `Full` again and it
re-parks. This "thundering herd, but each thread re-validates its own precondition before
acting" pattern is correct by construction: no woken thread ever assumes it personally is
entitled to the freed slot, so no coordination beyond the CAS itself is needed between
competing wakers.

**Spurious unparks are harmless.** Two sources of spuriousness exist and both are
explicitly accounted for. First, `std::thread::park`'s own contract permits spurious
wakeups (a `park()` call may return without a matching `unpark()`); every parking loop
in this crate re-checks its condition immediately after `park()` returns, in a `loop`, so
a spurious return just costs one extra iteration. Second, and specific to `WaiterList`:
a producer that registers (`prepare_wait`), then re-checks and finds space *without*
calling `park()` (the `send` loop's "space appeared or disconnected: skip the park" branch
in `mpsc.rs`) does **not** remove itself from the waiter list — it leaves a `Thread` handle
registered that a later `wake_all()` will still unpark. That unpark is not a bug: the
target thread is not parked at that moment, so `unpark()` merely sets its token for next
time (see §3's token argument), and the *next* time that thread calls `park()` (for an
unrelated future backpressure event) it will return immediately and simply re-check its
condition again — one extra harmless spin, never a correctness issue.

## 5. Disconnect

Both rings guarantee that a disconnect can never lose data that was already published, and
never let a parked thread sleep through a close. The argument has two independent
directions per ring.

**SPSC, sender-drops-first direction.** `Sender::drop` does exactly two things in program
order: `disconnected.store(true, Release)`, then `fence(SeqCst)` + wake both parkers. Any
`try_send` call the sender made before dropping already completed its own `tail`
`Release`-store in program order *before* `drop` runs (drop only runs after the `Sender`
value itself is no longer in use). Because the `disconnected` store is `Release`, a
consumer that later observes it with an `Acquire` load (in `try_recv`) has a
happens-before edge back to that store — and, transitively through program order on the
sender's thread, back to every `tail` store the sender made before dropping. `try_recv`'s
disconnect branch takes advantage of this explicitly: after seeing `disconnected == true`,
it takes **one more `Acquire` load of `tail`** before concluding `Disconnected` — that
final load is guaranteed (by the happens-before edge just described) to see the sender's
very last published `tail` value, so a message published in the same instant the sender
drops is never mistaken for "nothing left."

**SPSC, receiver-drops-first direction.** Symmetric: `Receiver::drop` stores
`disconnected` the same way, and a `try_send` after that observes it via its own `Acquire`
load and returns `Disconnected(v)`, handing the value back rather than dropping it
silently — no message is ever swallowed by a racing disconnect on the send side, because
`try_send` only writes the slot *after* confirming the receiver is still live.

**MPSC, all-senders-drop direction.** Same shape, over the `senders` counter instead of a
boolean: the transitive `AcqRel` chain described in §2 guarantees that the receiver's
`Acquire` load observing `senders == 0` happens-after every sender's own final `avail`
publish. `try_recv`'s disconnect branch mirrors SPSC's: after seeing `senders == 0`, it
re-checks `slot_published(self.head)` once more before concluding `Disconnected`, so a
message published concurrently with the last sender's drop is still drained, not lost.

**MPSC, receiver-drops direction.** `Receiver::drop` stores `rx_dropped` (`Release`) and
unconditionally fences + `wake_all`s the producer waiter list (not gated on `Park` mode
being the strategy — the fence/wake calls are cheap and correct to make unconditionally;
a `WaiterList` with no registered waiters is a no-op wake). Every `try_send` checks
`rx_dropped` (`Acquire`) *before* attempting a claim, so a send racing a receiver drop
either completes (if it observed `rx_dropped == false`, meaning its check happened-before
the drop in the "hasn't happened yet" sense — the receiver was still conceptually live
enough for the message to matter) or is rejected up front, never claiming a slot that will
never be drained.

**Why a parked thread can never sleep through a close.** Both `Sender::drop` and
`Receiver::drop`, on both rings, unconditionally run the same `store(Release)` +
`fence(SeqCst)` + wake sequence that §3 already proved cannot lose a wakeup against a
concurrently-parking thread — disconnect is, from the notify layer's point of view, just
another kind of "publish" that the Dekker protocol covers. There is no separate,
weaker-guaranteed close path; it is the exact same fenced publish-and-wake shape used for
ordinary data publication.

## 6. Drop-drain

When both handles of a channel are gone, `Shared::drop` (on each ring) walks the
initialized-but-never-consumed range and drops each value in place, so generic `T` never
leaks and is never double-dropped.

**SPSC** is simple: at the point `Shared::drop` runs, no other thread can be observing the
ring (both handles are gone, so `&mut self` is exclusive), so no ordering-sensitive race is
possible on the `head`/`tail` reads there — the code still loads both with `Acquire`
(matching every other read site in the crate, for uniformity rather than necessity), and
gives the exact range `head..tail` of slots that were written (by `try_send`) but never
read (by `try_recv`/`drain`): every index in that range is guaranteed initialized because
`tail` only advances *after* the write, and guaranteed not-yet-read because `head` only
advances *after* the read.

**MPSC** is the same idea over the contiguous published prefix rather than a plain range:
starting from `head`, `Shared::drop` walks forward while `avail[slot] == seq / cap` holds,
dropping each such slot, and **stops at the first sequence where that equality fails** —
that failure is precisely how a claimed-but-never-published slot (a "hole") is detected and
safely handled. In the current code every `try_send` call that wins its CAS goes on to
write and publish unconditionally (there is no early-return between claiming and
publishing), so no hole can actually occur on the paths that exist today — but the design
does not *rely* on that being true to stay sound. The `avail[slot] == seq/cap` check is the
single source of truth for "is this sequence actually present," checked independently at
every read site (`try_recv`, `drain`, and this cleanup walk); a hole, if one ever existed
(say, from a future amendment where a claim could be abandoned), would simply make the
prefix-scan stop one sequence early, leaving that one slot's uninitialized memory
untouched — never read, never dropped, never double-freed. The claim cursor and the
availability array are deliberately two separate observables specifically so that "claimed"
and "published" are independently checkable, and every consumer of the ring (the live
`Receiver` and the terminal `Shared::drop` walk alike) only ever trusts the latter.

**The `PublishGuard` RAII (both rings' `drain`).** `drain(max, f)` calls `f` once per
consumed item, and `f` is caller-supplied — it can panic. If it does, the function must
still leave the ring's shared `head` consistent with exactly how many items were actually
moved out of the slots (each `assume_init_read` already happened before `f` runs), or a
panic mid-drain would either re-expose consumed slots to the next drain/recv (a double
read of already-moved-out `MaybeUninit` storage) or, worse, leave `Shared::drop`'s cleanup
walk believing those slots are still live and dropping already-moved-out values a second
time. Both `spsc::Receiver::drain` and `mpsc::Receiver::drain` guard against this
identically: a local `PublishGuard` struct borrows the consumer's private cursor and the
shared `head` atomic, and its `Drop` impl — which **runs on both the normal return path
and on an unwind from a panicking `f`** — stores the current (possibly partially-advanced)
cursor into the shared `head` with `Release`, but only if it moved at all (`*self.head !=
self.start`, avoiding a needless store on a no-op drain). This makes the shared `head`
always reflect exactly the count of items actually taken out of the ring, regardless of
whether the loop finished, broke early because the ring ran dry, or unwound because `f`
panicked — so `Shared::drop` never re-drops a slot `drain` already moved out of (the
crate's explicit "leak-not-double-drop" policy on the panic path, stated in the comment
directly above each `PublishGuard::drop` impl). Both `spsc.rs` and `mpsc.rs` implement
this guard with the identical shape (private cursor reference + shared atomic reference +
start snapshot); it is the single mechanism that makes `drain`'s panic-safety story
consistent between the two rings.

## 7. Deviations from the bench cells

`ultima_rings`' cores are ports of `hi-perf-cmp`'s `thread-handoff-{ring,mpsc_ring}` Rust
cells, not verbatim copies. Two deliberate deviations exist, both scoped to the MPSC
producer side plus one crate-wide indexing change:

**Bounded-CAS claim instead of `fetch_add`.** The bench cell's MPSC claim is an
unconditional `fetch_add(1)` on the claim cursor: every producer gets a sequence number
immediately, with no relationship to whether the ring currently has room for it, and
backpressure is handled *after* the claim, by spinning until the slot the producer already
owns becomes free. `ultima_rings` instead makes the claim itself conditional (§1): a CAS
only succeeds when the target slot is provably free. This buys three things the `fetch_add`
version cannot offer: (1) `try_send` can report `Full` **without having claimed anything** —
impossible with `fetch_add`, where every call claims a sequence unconditionally and the
"full" check happens on the *already-claimed* slot; (2) a producer that decides to block
(`send`'s `Park`/`Backoff`/`BusySpin` paths) holds **no claimed-but-unwritten sequence**
while blocked, so a parked or slow producer can never leave an unpublished hole in the
middle of the consumer's contiguous-prefix scan; (3) as a consequence of (2), the
consumer's `drain`/`try_recv` never has to reason about "a slot that's claimed but not
written yet, is that a hole I should stop at or wait past" — the claim/head bound already
guarantees every claimed-and-in-progress slot is one the consumer hasn't reached yet. The
cost is a CAS-retry loop on the claim path instead of a single `fetch_add`, which is part
of why the MPSC bake-off number in the README trails `crossbeam-channel`'s classic
`fetch_add`-based design under heavy contention — an accepted v1 trade, not an oversight
(see the README's numbers section and §8).

**`& mask` instead of `%`.** Both rings index the physical buffer with `seq & (cap - 1)`
rather than `seq % cap`, matching the bench cells' own indexing convention. Because `cap`
is checked to be a power of two at construction (`assert_cap`), the two are numerically
identical, but the mask form is guaranteed to compile to a single `AND` instruction
regardless of whether the optimizer can see `cap` as compile-time-constant, whereas `%`
can lower to a division instruction the moment the divisor's constant-ness is hidden from
the compiler (e.g. behind a generic function boundary or a type-erased slice) — exactly
the regression the `heapless` survey documents happening in that crate's own history
(issue #650/#652: erasing a const-generic capacity silently reintroduced
`__aeabi_uidivmod` calls in the hot path). `ultima_rings` never had a const-generic
capacity to begin with (`cap` is a runtime `usize` from day one), so starting from a mask
sidesteps that regression class structurally rather than needing heapless's later fix.

**What stayed byte-equivalent.** The actual publish/consume edges — SPSC's `tail`
Release/`head` Acquire pair, and MPSC's `avail[slot] = seq / cap` Release/Acquire
round-encoding — are unchanged from the bench cells. Only the *claim* mechanism (MPSC) and
the *indexing arithmetic* (both rings) changed; what "published" means to a consumer, and
the wire-level protocol between producer and consumer, is identical to what the AWS numbers
in the README measured.

## 8. Costs

**Park-mode's one `SeqCst` fence per operation, on each side.** As established in §3, every
publish that could plausibly need to wake a parked peer pays exactly one `fence(SeqCst)` in
`Park` mode — the producer after every successful `try_send`, the consumer after every
successful `try_recv`/non-empty `drain`, and (in the reverse direction) each side's own
parking attempt pays one more before its re-check. `BusySpin` and `Backoff` never take this
path at all: their fast path is the bare lock-free core, nothing more, so the fence is a
cost `Park` mode alone opts into in exchange for zero idle CPU.

**`Backoff`'s zero cross-side cost.** The `Backoff` strategy's idle ladder
(`src/wait.rs`'s `Idle`: 10 spins → 20 yields → timed parks doubling 1 µs → 1 ms) is
entirely self-contained on the blocked side — `Idle::idle()` never touches the peer's
state and never calls `wake`/`wake_all`. The *other* side (the one making progress) pays
**nothing extra** when the channel is configured for `Backoff`: no fence, no flag check, no
conditional branch beyond the ordinary `strategy == WaitStrategy::Park` guards that are
false for `Backoff`. This is the direct payoff of choosing a self-waking ladder (timed
parks that give up and re-check on their own) over a notification-based design for this
strategy: the entire cost of "did my peer make progress" is paid by the blocked thread
polling on a timer, never by the productive thread being asked to additionally notify
anyone.

**The false-sharing reality of the interleaved availability array.** `avail` is a flat
`Box<[AtomicI64]>`, 8 bytes per slot, laid out contiguously with **no per-slot padding** —
unlike `claim`/`head` (both individually `CachePadded` to their own 64-byte line), eight
consecutive slots of `avail` share one cache line. Under sustained multi-producer
contention, producers claiming adjacent sequences (the common case, since the claim cursor
hands out consecutive integers) publish to `avail` entries that live on the same cache
line, so their Release-stores contend for that line the same way false-sharing always
does — this is a real, structural cost of keeping the availability array compact rather
than padding every slot to its own line (which would cost 64 bytes per ring slot instead
of 8, an 8× memory blow-up for a structure whose entire value proposition is being a
small, cache-resident array). `ultima_rings` does not pad it, and does not claim to: this
layout is carried over unchanged from the `hi-perf-cmp` bench cell the AWS numbers in the
README measure (9.4 M ops/s, p50 277 ns, 2-producer MPSC on `c6id.2xlarge`) — so whatever
cache-line contention this causes is already priced into that measured number, not an
unmeasured risk the port introduced. It is also a plausible partial contributor (alongside
the CAS-retry cost from §7) to the MPSC bake-off result documented in the README (§ below):
`ultima_rings`' bounded-CAS MPSC currently trails `crossbeam-channel`'s `fetch_add`+colocated-
stamp design under the bake-off's specific 2-producer/4-core contention shape, though this
document does not claim to have isolated false sharing as the dominant cause versus the CAS
retry cost — both are real, neither has been measured in isolation. Padding the
availability array (or interleaving it with the buffer, LMAX-cacheline-padding-style) is
the concrete, identified v2 lever if this cost needs to be paid down; a batched claim (see
README) is the other.

## 9. Alternatives considered

**Vyukov per-slot stamps / packed state words** (crossbeam-channel's array flavor;
thingbuf's `Core`). Both fold the "is this slot ready" signal into a single per-slot atomic
that also encodes the claim/generation, rather than keeping a separate claim cursor and
availability array. This is more compact (one atomic touched per slot instead of two
logically-separate structures) and gives crossbeam a clean MPMC design, but it couples
readiness to the writer's own CAS target: thingbuf's `push_ref` has to detect and skip a
slot a lingering reader still holds (its `HAS_READER` bit), and that exact coupling is
implicated in two open, unresolved correctness issues in thingbuf's own tracker (#98,
self-requeue invariant violation under a plain pop-then-repush workload; #100, hang/crash
closing the channel while a slot guard is live) — see the thingbuf survey. `ultima_rings`
keeps the claim cursor and the availability array as two independent observables
specifically so that a genuinely single-consumer design (no reader ever "lingers" on a
slot the way an MPMC consumer might) never needs that skip-logic at all, at the cost of one
extra array and the false-sharing profile discussed in §8 rather than a single per-slot
word.

**kanal's direct cross-stack transfer.** kanal's fast path (for small/rendezvous payloads)
writes the value directly into a `KanalPtr` pointing into the *receiving thread's own stack
frame*, avoiding the shared ring buffer entirely for that case. This is measurably fast,
but it is structurally incompatible with a loom/miri-verifiable "every slot is owned by the
shared allocation, never by a specific thread's stack" design: the pointer's validity
depends on the target thread's stack frame still being alive across a park/unpark hop, an
invariant that produced the majority of kanal's own historical soundness bugs (issues #3,
#4, #17, #19 — see §10). `ultima_rings`' `MaybeUninit`-slot-owned-by-the-`Arc` design is the
direct structural alternative: a slot's lifetime is tied to the channel's own lifetime, not
to any one thread's stack, which is exactly what makes the loom models in `tests/loom.rs`
tractable to write and trust.

**flume's lock-based core.** flume's entire channel (bounded and unbounded alike) is a
`VecDeque` behind one `Mutex`, with waiting layered outside the lock via `thread::park`.
This buys flume "no `unsafe`" and genuine MPMC generality (multiple receivers competing for
messages, which `ultima_rings` deliberately does not support) at the cost of a full
mutex acquire/release on every single op, contended or not — a cost the flume survey
itself frames as the reason mature channel crates like crossbeam still bother hand-rolling
lock-free designs at all. `ultima_rings`' target is the opposite end of that trade: a fixed,
known producer/consumer shape (SPSC/MPSC only, chosen by the caller ahead of time) on a
latency-critical SMR hot path, where a lock's uncontended-but-nonzero cost and its
contended-and-unbounded tail latency are both unacceptable — the lock-free core, verified
under loom/miri instead of "proven safe by construction," is the correct trade for this
crate's stated use case even though it is not the correct trade for every channel.

## 10. Soundness pitfall checklist

Each row is a documented failure class from another lock-free/channel crate's own issue
tracker, and the concrete `ultima_rings` property (design choice, test, or verification
lane) that covers it.

| Pitfall class (source) | What it looks like | `ultima_rings` mitigation |
|---|---|---|
| Stack-pointer escape (kanal #3, #4) | A pointer into a thread's stack frame outlives that frame, or a `Thread` handle is used after the owning thread could have exited | Every slot lives in the shared `Arc<Shared<T>>` allocation, never on a participant's stack (§9); `Parker`'s `Thread` handle is taken via `thread::current()` and stored behind a `Mutex`, consumed exactly once by `wake`/`wake_all`, never dereferenced after the owning thread could plausibly have exited its `park()` call |
| Clone/split double-free (kanal #17, #19) | A sender/receiver clone or split operation frees or aliases shared state incorrectly | `Sender::clone` (MPSC) is `Arc::clone` plus a `Relaxed` `fetch_add` on `senders` — no unsafe code in the clone path at all; there is no split operation (`channel()` returns owned handles exactly once, never re-splittable), which per the heapless survey's own recommendation (§4 there) is the structural fix for that entire bug class rather than a patched-after-the-fact one |
| `forget`/`ManuallyDrop` vs. real drop (kanal #2, #28) | Code that "moved" a value without dropping the old location, or forgot a value it should have dropped, gets caught only by miri | The crate uses neither `mem::forget` nor `ManuallyDrop` anywhere; the only unsafe moves are `MaybeUninit::write`/`assume_init_read`/`assume_init_drop`, each scoped to a single `with`/`with_mut` closure immediately adjacent to its Release/Acquire edge — audited by the miri lane (Task 8: 32/32 tests, 0 UB, covering every such call site) |
| Aliasing on parked/suspended objects (kanal #14, #16) | A "parked" object is reachable through both an exclusive and a shared API simultaneously | `Parker`'s `Thread` slot and `WaiterList`'s waiter vector are both behind a `std::sync::Mutex`, never exposed as a raw shared reference; the crate has no async/await surface (§11), so there is no waker-vs-poller aliasing surface to begin with in v1 |
| Non-`repr(C)` transmute between generic structs (kanal #36/#49) | Transmuting between two structurally-similar-but-not-guaranteed-identical generic types | The crate contains no `transmute` anywhere (confirmed by search over `src/`); `Sender`/`Receiver` are never cast between representations, only constructed directly |
| Hand-written `Send`/`Sync` bounds (kanal #33, #45) | A manual `unsafe impl Send/Sync` that is too permissive (unsound) or too restrictive | `Shared<T>`'s `unsafe impl<T: Send> Send/Sync` is the only hand-written bound in the crate, gated on `T: Send` and justified in an adjacent `SAFETY` comment; `Sender<T>`/`Receiver<T>` themselves derive their auto-trait status from their fields (no manual impl), and are in practice `Sync` whenever `T: Send` — exclusivity of the single producer (SPSC) / single consumer (both rings) is enforced by the API surface (no `Clone` on `Receiver`, every mutating method taking `&mut self`), not by a `!Sync` bound; a stale `SAFETY` comment in `src/spsc.rs`/`src/mpsc.rs` describing this as "`!Sync` handles" is a known, previously-flagged documentation inaccuracy (tracked, not yet corrected in-source) — the actual mechanism is no-`Clone` + `&mut self`, stated correctly here |
| Cancel-races-delivery (kanal #15, #35, #47) | A pending receive/send is cancelled (future drop, thread giving up) concurrently with the peer completing delivery | Every blocking `send`/`recv` re-validates its condition immediately before parking (the Dekker re-check, §3) and after every `park()` return (the retry loop), so a "cancel" (a producer bailing out of its wait because space appeared, or a disconnect arriving) can never race a delivery into a lost message — the loom models `loom_full_parked_sender_vs_recv_and_rx_drop` and `loom_close_wakes_parked_consumer` (`tests/loom.rs`, Task 7) exhaustively check exactly this class of interleaving for the cases this crate has (no async cancellation exists in v1, so kanal's future-drop-specific variant does not apply) |
| Combinator/adapter surface as a distinct attack surface (kanal #63) | A convenience wrapper (stream/iterator adapter) over an already-sound core reintroduces a bug the core doesn't have | `drain`'s `PublishGuard` (§6) is exactly this class of concern pre-empted: a batch/adapter-shaped API (`drain(max, f)`, the closest thing this crate has to a combinator) is independently panic-safety-audited, not merely assumed sound because the single-item `try_recv` path is |
| rtrb's publish-before-drop rule (issue #185, open upstream) | A chunk/batch commit drops consumed slots *before* advancing the published index, so a panicking `Drop` mid-batch leaves the index stale and the same slots get dropped again on the next read | `PublishGuard` (§6) advances the shared `head` to reflect exactly what was consumed **via its own `Drop` impl, which runs on the unwind path too** — the index update and the "how much was actually taken" bookkeeping can never desynchronize, whether `f` panics or not; this was designed in from `drain`'s introduction (Task 2/4), specifically to avoid reproducing rtrb's still-open bug |
| `Arc::strong_count` trap (rtrb issue #114) | Relying on another type's undocumented/incidental synchronization behavior (here, `Arc::strong_count`'s ordering) for a correctness invariant, which broke when that behavior changed upstream | `ultima_rings` never inspects `Arc::strong_count`; disconnect/liveness tracking is entirely the crate's own explicit atomics (`disconnected`, `rx_dropped`, `senders`) with orderings documented in §2 and argued in §5 — no dependency on any other type's incidental guarantees |
| heapless's division-regression class (issue #650) | Type-erasing a capacity that was compile-time-constant silently reintroduces a runtime division in the hot index-update path | Both rings index with `seq & (cap - 1)`, never `%` (§7) — `AND` cannot lower to a division regardless of whether the optimizer can see `cap`'s constant-ness, so this class of regression cannot occur here structurally, not just by current-code inspection |

## 11. Future layering note

`ultima_rings` v1 is sync-only: the notify layer (`src/notify.rs`) speaks in
`std::thread::Thread` handles, not `std::task::Waker`s, and there is no `Future`/`async`
surface anywhere in the crate. This is a deliberate, explicit non-goal for v1, not an
oversight — but the layering is already shaped to make async support additive rather than
a rewrite, should a future version want it. The lock-free cores (`spsc.rs`/`mpsc.rs`) never
call `thread::park`/`unpark` directly; every blocking wait goes through `notify.rs`'s
`Parker`/`WaiterList` abstraction, which is the *only* place that would need a second
implementation. flume's own architecture (surveyed in `docs/superpowers/research/`) is the
existence proof for this shape: its `Signal` trait lets the identical `send`/`recv` core
serve both a `SyncSignal` (park/unpark) and an `AsyncSignal` (store a `Waker`, call
`wake_by_ref`) by parameterizing only the "what happens while blocked" step — the queue
core itself knows nothing about sync vs. async. An async backend for `ultima_rings` would
follow the same shape: a new `notify` implementation that stores a `Waker` instead of a
`Thread` and wakes it instead of unparking, registered the same way (announce intent →
`SeqCst` fence → re-check → suspend) as the existing `Parker`/`WaiterList` protocol,
without touching `spsc.rs`'s or `mpsc.rs`'s claim/publish/consume logic at all. This is
explicitly out of scope for v1.
