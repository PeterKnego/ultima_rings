# ultima_rings — Design

This document is the reference companion to the code. For each atomic operation
in `src/spsc.rs` and `src/mpsc.rs`, it states three things:

- The invariant that the operation upholds.
- The memory ordering that the operation needs.
- The reason that a weaker ordering is not enough.

It also records the alternatives that the project weighed against the ecosystem
survey. And it maps the soundness pitfalls that other lock-free channel crates
hit to the mitigation that `ultima_rings` has. If this document and the code
disagree, the code is authoritative. But we checked every claim in this document against
`src/spsc.rs`, `src/mpsc.rs`, `src/notify.rs`, `src/wait.rs`, and
`src/atomic.rs` on `feat/v1`.

## 1. Rings and invariants

### SPSC

The SPSC ring (`src/spsc.rs`) is two monotonic `usize` counters. `tail` counts
every pushed item, and the producer owns it. `head` counts every popped item,
and the consumer owns it. `CachePadded` pins each counter to its own 64-byte
cache line. So producer-local traffic and consumer-local traffic never bounce
the same line.

The logical occupancy of the ring is `tail - head`. The whole design depends on
one core invariant:

```
0 <= tail - head <= cap        (always, for every observer)
```

`tail` and `head` only increase. Each owner runs `self.tail += 1` or
`self.head += 1` in program order on its own thread. There is no wraparound and
no decrement. So `tail - head` stays valid `usize` arithmetic for the lifetime
of the channel.

The physical slot for sequence `n` is `n & mask`, with `mask = cap - 1`. The
constructor makes sure that `cap` is a power of two. If two sequences differ by
a multiple of `cap`, they map to the same slot. If they do not, they map to
different slots.

#### How each side keeps the invariant

The producer keeps the invariant because it never advances `tail` past
`head + cap`. A push first checks `tail - cached_head == cap`, the full
condition. Only a stale cache makes the producer reload the real `head`. If the
ring is still full after that reload, `try_send` returns `Full`. It then
touches no slot and does not advance `tail`.

The consumer's half is symmetric. A pop proceeds only while
`head != cached_tail` holds. If the cache is stale, a fresh `Acquire` load of
`tail` backs that check.

#### The write-once, read-once invariant

The second invariant makes the ring memory-safe without per-slot state. The
producer writes the slot for each sequence exactly once, before the `Release`
publish of `tail`. The consumer reads the slot exactly once, after an `Acquire`
observation of that publish.

Concretely: `try_send` writes the slot, `(*p).write(v)`. Then it increments
`tail` and stores it with `Release`. So no other thread can observe the write
before the write is complete.

`try_recv` and `drain` load `tail` with `Acquire` before they read the slot
with `assume_init_read`. A cached `tail` value from an earlier `Acquire` load
also works. So the read can never race the write.

That full check lets the ring reuse a slot only after `head` proves one fact:
the consumer already consumed the previous occupant of that index. So the
producer never writes the same slot twice in a row. Between the two writes,
there is always a read. And the consumer never reads the same slot twice in a
row. Between the two reads, there is always a write.

This is the standard discipline of one writer and one reader for each index. It
makes `MaybeUninit<T>` safe here, without a separate 'is this slot initialized'
flag. The index pair `(head, tail)` is that flag.

### MPSC

The MPSC ring (`src/mpsc.rs`) generalizes the producer side to N threads. It
replaces `tail` with two things. The first is a claim cursor,
`claim: AtomicUsize`, the next sequence to claim. The second is a per-slot
round number that lives beside its payload: `slots: Box<[Slot<T>]>`, where each
`Slot<T>` holds `round: AtomicI64` beside its `value`, and `-1` means 'never
published'.

The consumer side, `head`, is identical to SPSC. There is still exactly one
consumer, so `head` needs no CAS, only a plain load and store.

#### The bounded-CAS claim

The invariant that carries the design is the bounded-CAS claim. A producer
attempts `compare_exchange_weak(seq, seq + 1, ...)` on `claim` only after the
same loop iteration proved `seq - head < cap`. The check compares against a
possibly stale cached head. Whenever the cheap check fails, the producer
refreshes the cache with an `Acquire` load of the real `head`.

`head` only increases. And a stale cache only under-counts the consumer's
progress, it never over-counts it. So at the moment of the CAS, the compared
value is always `<=` the true `head`. Therefore every CAS that succeeds does so
with `seq - head < cap` true at that instant, for the true `head`.

This differs materially from an unconditional claim. An unconditional claim
increments the cursor first and defers the wait for a full ring until after
the claim. That is the shape of the `hi-perf-cmp` bench cell that this ring came
from, `fetch_add(1)`, see §7.

#### Bounded against unconditional, not CAS against `fetch_add`

Note the distinction: bounded against unconditional, not CAS against
`fetch_add`. It is also narrower than an earlier revision of this document
claimed. Actual LMAX and Vyukov implementations claim conditionally with a CAS,
not with `fetch_add`. The maintained Rust LMAX port uses
`compare_exchange_weak` on its cursor
(`docs/superpowers/research/2026-08-10-disruptor-survey.md`). The array flavor
of `crossbeam-channel` does the same on `tail`.

Those designs condition on something different, a per-slot stamp rather than a
`head` bound. But they are not unconditional.

#### The safety argument

That bound buys the safety argument for free. `seq - head < cap` means
`head > seq - cap`. So the consumer already advanced past sequence
`seq - cap`. That sequence is the previous occupant of the same physical slot:
`(seq - cap) & mask == seq & mask`, because `cap` is a power of two, and
subtraction of a multiple of `cap` does not change the remainder.

So the CAS that lets a new producer write into a slot succeeds only after the
consumer provably consumed the previous occupant of that slot. This is the same
'the index pair proves that the slot is safe to touch' argument as SPSC. Here
`claim` and `head` carry it, instead of `tail` and `head`.

#### The wrap and ABA argument

The last piece is the wrap argument for the round number in each slot. Suppose
`slots[i].round` were a plain boolean 'ready' flag. After a full wrap, a slow
consumer could then confuse the publish of round `r` with the stale flag of
round `r - 1`. `ultima_rings` avoids this: it stores the round number itself,
`seq >> shift`, with `shift = log2(cap)` cached at construction (see §7), not a
boolean.

The readiness check of the consumer is not 'does this slot carry a ready mark'.
It is 'does `slots[seq & mask].round` equal exactly the round that I expect':
`slots[seq & mask].round == seq >> shift`, in `Receiver::slot_published`.

Rounds strictly increase for each slot. Occupants of the same slot are `cap`
sequences apart, and `(seq + cap) >> shift == (seq >> shift) + 1`. So each
successive occupant of a slot has a round exactly one greater than the last. A
stale round-`r - 1` value in `slots[i].round` can therefore never satisfy a
round-`r` equality check. The consumer never mistakes an `r - 1` leftover for a
round-`r` publish. This is the standard Disruptor-style defense against the ABA
problem that a single reused boolean would have.

## 2. Orderings

Below is the complete list of atomic operations in both rings. Each entry
names the operation and its side, the ordering, the operation that it pairs
with, and what it publishes.

- SPSC `tail`, store by the producer in `try_send`: `Release`. It pairs with
  the consumer's `Acquire` load of `tail`. It publishes the slot write that
  came before it.

- SPSC `tail`, load by the consumer in `try_recv`, `drain`, and the Park
  recheck: `Acquire`. It pairs with the producer's `Release` store of `tail`.

- SPSC `head`, store by the consumer in `try_recv` and `drain`: `Release`. It
  pairs with the producer's `Acquire` load of `head`. It publishes the slot as
  reusable, free for a future write.

- SPSC `head`, load by the producer in `try_send` and the Park recheck:
  `Acquire`. It pairs with the consumer's `Release` store of `head`.

- SPSC `disconnected`, store in `Sender::drop` and `Receiver::drop`:
  `Release`. It pairs with each `Acquire` load of `disconnected`. Together
  with the `SeqCst` fence, it publishes every write from before the drop
  (see §5).

- SPSC `disconnected`, load in `try_send`, `try_recv`, and the Park recheck:
  `Acquire`. It pairs with the `Release` store of `disconnected`.

- MPSC `claim`, CAS by a producer in `try_send`: `Relaxed` on success and on
  failure. It pairs with nothing (see below). It publishes only the uniqueness
  of the claimed sequence.

- MPSC `claim`, load by a producer in the retry loop and the Park recheck:
  `Relaxed`.

- MPSC `slots[i].round`, store by the producer in `try_send`: `Release`. It
  pairs with the consumer's `Acquire` load of the round. It publishes the slot
  write that came before it.

- MPSC `slots[i].round`, load by the consumer in `slot_published`, `drain`,
  and `Shared::drop`: `Acquire`. It pairs with the producer's `Release` store
  of the round.

- MPSC `head`, store by the consumer in `try_recv` and `drain`: `Release`. It
  pairs with the producer's `Acquire` load of `head`. It publishes the slot as
  reusable.

- MPSC `head`, load by the producer in `try_send` and the Park recheck:
  `Acquire`. It pairs with the consumer's `Release` store of `head`.

- MPSC `senders`, `fetch_add` in `Sender::clone`: `Relaxed`. It is part of the
  `AcqRel` RMW chain below.

- MPSC `senders`, `fetch_sub` in `Sender::drop`: `AcqRel`. It pairs with
  itself (the RMW chain) and with the `Acquire` load of `senders`. Once the
  count reaches zero, it publishes every write that this producer made,
  transitively.

- MPSC `senders`, load in `try_recv` and `recv` for the `== 0` check:
  `Acquire`. It pairs with the last `fetch_sub`, the one that reached zero.

- MPSC `rx_dropped`, store in `Receiver::drop`: `Release`. It pairs with each
  `Acquire` load of `rx_dropped`. Together with the `SeqCst` fence, it
  publishes every write from before the drop (see §5).

- MPSC `rx_dropped`, load in `try_send` and the Park recheck: `Acquire`. It
  pairs with the `Release` store of `rx_dropped`.

#### Why the claim CAS is `Relaxed`

`claim` has only one job: it gives each producer that competes a disjoint
sequence number. It is a ticket dispenser, not a data channel. The total modification
order of `claim` itself already guarantees that two producers can never win
the same sequence. That is what compare-exchange means, at every ordering.

No producer needs to observe anything else in memory as a side effect of a CAS
win.
The `Release` store of `slots[i].round` is what publishes the payload of the
slot to the consumer. That store happens separately, after the winner finishes
the slot write. The ordering rides entirely on that round store, not on the
claim CAS. The consumer never reads `claim` at all, so there is no cross-thread
edge for the ordering of the CAS to carry.

#### Why the `head`, `tail`, and round loads are `Acquire`, not `Relaxed`

These loads are the one genuine cross-thread edge in each direction. The
producer's `head` load must see the consumer's earlier `Release` store of
`head`. Then the full check reflects real progress, not a stale view that
could let a claim or write race an unconsumed slot. The consumer's load of
`tail` or `slots[i].round` must see the producer's earlier `Release` store.
Then `assume_init_read` never races the `write` that produced the value.

#### Why `fetch_sub` on `senders` is `AcqRel` on every decrement

The textbook `Arc`-drop optimization uses `Release` for every decrement. Only
the thread that observes the count at zero pays the `Acquire` cost, through a
separate fence. `ultima_rings` instead makes every `fetch_sub` `AcqRel`,
unconditionally.

`fetch_sub` is a read-modify-write on one atomic. Every decrement, and the
`Relaxed` `fetch_add` in `Clone`, shares one total modification order. So the
`Acquire` half of each `AcqRel` decrement synchronizes-with whatever wrote the
value that it read. That writer is either the `Release` half of an earlier
`AcqRel` decrement, or the initial `senders = 1` store.

The result is a transitive happens-before chain through the drop of every
sender. So the receiver's plain `Acquire` load that observes the count reach
`0` happens-after the final round publish of the last sender. Through the
chain, it also happens-after the final publish of every earlier sender.

This is more conservative than the classic optimization: every decrement pays
the `Acquire` cost, not only the last. But the cost lands on the cold `Drop`
path, once in each producer's lifetime, not on the hot send path. So the
simplicity is free.

#### Why the disconnect flags also get a `SeqCst` fence

Every `disconnected` and `rx_dropped` transition is a `Release` store plus a
`SeqCst` fence, not a `Release` store alone. The flag alone would prevent
races on the flag's own data. But the wake protocol needs more: the flag check
and the parked-waiter registration check must not both miss each other. The
fence buys that, not the `Release` and `Acquire` pair by itself. See §3 and
§5.

## 3. The Dekker wake protocol

`Park` mode is the only strategy that needs cross-thread wakes. `BusySpin` and
`Backoff` wake themselves, see §8. The protocol that lets `Park` mode lose no
wakeups is a textbook Dekker shape. `src/notify.rs` implements it once, and
both rings use it identically in both directions.

#### The two sequences that race

The waiter is a thread about to block, because the ring looked empty or full:

1. `prepare_park()` stores the 'I intend to park' flag, `parked = true`, with
   `Relaxed`, and registers the `Thread` handle.

2. `fence(SeqCst)`.

3. Re-check the real state of the ring: an `Acquire` load of the condition
   atomic. The condition atomic is `tail`, `head`, `slots[i].round`,
   `disconnected`, or `senders`, whichever the caller waits on.

4. If the re-check now shows progress, `cancel()` withdraws the registration,
   and the operation retries instead of a park. Otherwise the thread calls
   `park()`.

The waker is a thread that just published data, freed a slot, or started the
channel teardown:

1. Publish the change: the slot write plus the `Release` store of `tail`,
   `head`, or `slots[i].round`, or the `Release` store of `disconnected` or
   `rx_dropped`.

2. `fence(SeqCst)`.

3. Check the 'is anyone parked' flag. If a waiter shows, unpark it.
   `Parker::wake` does a `Relaxed` load. `WaiterList::wake_all` does a
   `Relaxed` swap, an RMW whose read carries the same guarantee.

#### Why no wakeup gets lost

`SeqCst` fences take part in a single global total order, shared by all
`SeqCst` operations in the program. Consider the waiter's fence (its step 2)
and the waker's fence (its own step 2). One of them is first in that total
order.

- If the waker's fence is first: the waiter's fence comes later in the total
  order. The waker's `Release` publish (step 1) precedes its fence in program
  order, and a `SeqCst` fence is also a `Release` plus `Acquire` barrier
  itself. So the waiter's re-check (step 3) observes the new data, and the
  waiter never parks.

- If the waiter's fence is first: the waker's fence comes later. The waiter's
  flag store (step 1) precedes its fence in program order. So the waker's flag
  check (step 3) observes the flag, and the waker wakes the waiter.

In each case, at least one of the two sides observes the other side's write. This is the
classic Dekker argument.

Without the fences, plain `Release` and `Acquire` pairs on each atomic do not
prevent a double miss. The pairs only cover the flag store with the flag load,
and the data store with the data load. The CPU can reorder the waiter's flag
store after its re-check, because no barrier ties those two operations
together. The same reorder is possible on the waker's side. Then each side
concludes that nothing changed. The `SeqCst` fence forecloses that reorder on
both sides at the same time, through the one shared total order.

#### Why the park token absorbs the unpark-before-park race

Steps 3 and 4 above leave a narrow window. The waiter's re-check (step 3) can
observe 'still empty' or 'still full' and decide to call `park()`. The waker's
`unpark()`, from its own step 3, can fire before the waiter's `park()` call
runs. If `thread::park()` were a bare 'sleep the OS thread' primitive, this
window would lose the wakeup.

It is not. The contract of `std::thread::park` keeps a per-thread token.
`unpark()` sets that token whether or not the target is currently inside
`park()`. A later `park()` call consumes a set token and returns immediately.
It does not block.

So the narrow race after the re-check and before the park is not a correctness
gap. In each order of the unpark and the park, the thread can never block past the
point where its condition became true. This is exactly why the custom `Parker`
in `src/notify.rs` layers its own flag and registration protocol on top of
`std::thread::park` and `unpark`. It does not reinvent a wait and wake
primitive from scratch. It gets this absorption for free.

#### Why only Park mode pays the fence

On every ordinary publish in `try_send`, `try_recv`, and `drain`, a guard
wraps the fenced wake sequence. The sequence is
`crate::atomic::fence(Ordering::SeqCst)` plus `wake` or `wake_all`. The guard
is `if sh.strategy == WaitStrategy::Park`. The park loop, with its own fence,
is only reachable from the `WaitStrategy::Park` arm of the strategy match in
`send` and `recv`.

So `BusySpin` and `Backoff` never register a waiter and never call `wake` or
`wake_all` on the hot path. Their fast path is exactly the `Release` and
`Acquire` edges of the lock-free core, nothing more.

There is one exception: `Sender::drop` and `Receiver::drop`, see §5. The
disconnect fence and wake there run unconditionally, in every strategy. A
`WaiterList` or `Parker` with no registered waiter is a cheap no-op wake, and
the close path is cold in every case. §8 quantifies this as the Park-mode cost
of one `SeqCst` fence for each operation, on each side.

## 4. Waiter list (MPSC producers)

SPSC Park mode needs only a single-waiter `Parker` on each side, because there
is one producer and one consumer. The MPSC producer side can have N threads
blocked on a full ring at the same time. So it needs a list, not one flag with
one `Thread` slot. `WaiterList` (`src/notify.rs`) is a `Mutex<Vec<Thread>>`
behind a `waiting: AtomicBool` fast-path flag. `prepare_wait` pushes the
current thread onto the list. `wake_all` drains the whole list and unparks
every entry.

#### Cold path by construction

Only the `WaitStrategy::Park` arm of `Sender::send` reaches
`WaiterList::prepare_wait` and `park`. That arm runs only after `try_send`
returned `Full`, that is, only after the ring was observably full. A mutex is
acceptable here specifically because this path is already the slow path,
backpressure. The lock-free claim and publish core, the happy path of
`try_send`, never touches `WaiterList` at all. This mirrors the trade that the
`crossbeam-channel` survey makes for its own `SyncWaker`, see §9.

#### Wake all, and each waiter re-checks

In Park mode, `Receiver::wake_producers` calls `wake_all()` unconditionally on
every successful `try_recv` and `drain`. It does not try to wake only the one
producer that now has room. Each producer thread that wakes resumes its own `send` loop and retries
`try_send`. If another producer was faster and claimed the space, this
producer's `try_send` returns `Full` again, and the producer parks again.

The pattern is: wake every thread, and each thread re-validates its own
precondition before it acts. It is correct by construction. No woken thread
assumes that it personally owns the freed slot. So the threads need no
coordination beyond the CAS itself.

#### Spurious unparks are harmless

Two sources of spurious wakeups exist, and the design accounts for both.
First, the contract of `std::thread::park` itself lets a `park()` call return
without a matched `unpark()`. Every park loop in this crate re-checks its
condition immediately after `park()` returns, in a loop. So a spurious return
costs only one extra iteration.

The second source is specific to `WaiterList`. A producer can register with
`prepare_wait`, then re-check, find space, and skip the `park()` call. That is
the 'space appeared or disconnected: skip the park' branch of the `send` loop
in `mpsc.rs`. That producer does not remove itself from the waiter list. It
leaves a `Thread` handle that a later `wake_all()` will still unpark.

That unpark is not a bug. The target thread is awake at that moment, so
`unpark()` only sets its token for the next time, see the token argument in
§3. The next `park()` call from that thread, for an unrelated future
backpressure event, returns immediately. The thread then re-checks its
condition again. The total cost is one extra harmless spin, never a
correctness fault.

## 5. Disconnect

Both rings give two guarantees. A disconnect can never lose data that a peer
already published. And a parked thread can never sleep through a close. The
argument runs in two independent directions for each ring.

#### SPSC: the sender drops first

`Sender::drop` does exactly two things, in program order. It stores
`disconnected = true` with `Release`. Then it runs `fence(SeqCst)` and wakes
both parkers. Every `try_send` call from before the drop already completed its
own `Release` store of `tail`, in program order. The reason: drop runs only
after the `Sender` value itself is out of use.

The `disconnected` store is a `Release` store. So a consumer that later
observes it with an `Acquire` load, in `try_recv`, has a happens-before edge
back to that store. Through program order on the sender's thread, the edge
extends back to every `tail` store from before the drop.

The disconnect branch of `try_recv` uses this edge explicitly. After it sees
`disconnected == true`, it takes one more `Acquire` load of `tail` before it
concludes `Disconnected`. That happens-before edge makes sure that this final load sees the sender's
last published `tail` value. So the consumer
never mistakes a message from the instant of the drop for 'nothing left'.

#### SPSC: the receiver drops first

This direction is symmetric. `Receiver::drop` stores `disconnected` in the
same way. A later `try_send` observes the flag through its own `Acquire` load
and returns `Disconnected(v)`. It hands the value back to the caller, it does
not drop the value silently. No message ever disappears into a disconnect race
on the send side. The reason: `try_send` writes the slot only after it makes
sure that the receiver is still live.

#### MPSC: all senders drop

The shape is the same, over the `senders` counter instead of a boolean. The
transitive `AcqRel` chain from §2 gives the guarantee: the receiver's
`Acquire` load that observes `senders == 0` happens-after the final round
publish of every sender. The disconnect branch of `try_recv` mirrors the SPSC
branch. After it sees `senders == 0`, it re-checks `slot_published(self.head)`
once more before it concludes `Disconnected`. So the ring still drains a
message that a sender published concurrently with the last drop. The ring does
not lose it.

#### MPSC: the receiver drops

`Receiver::drop` stores `rx_dropped` with `Release`. Then it fences and calls
`wake_all` on the producer waiter list, unconditionally. No Park-mode gate
guards these calls: they are cheap and correct in every strategy, and a
`WaiterList` with no registered waiters is a no-op wake.

Every `try_send` checks `rx_dropped` with `Acquire` before it attempts a
claim. So a send that races the drop of the receiver has one of two outcomes.
Either it completes: it observed `rx_dropped == false`, so its check
happened-before the drop. In that sense the receiver was still live enough for
the message to matter. Or the check rejects the send up front. In no case does the send
claim a slot that no one will ever drain.

#### Why a parked thread can never sleep through a close

Both `Sender::drop` and `Receiver::drop`, on both rings, unconditionally run
the same sequence: `store(Release)`, then `fence(SeqCst)`, then wake. §3
already proved that this sequence cannot lose a wakeup against a thread that
parks concurrently. To the `notify` layer, a disconnect is only one more kind of publish, and the
Dekker protocol covers it. There is no
separate close path with a weaker guarantee. The close path is the exact same
fenced publish-and-wake shape as ordinary data publication.

## 6. Drop-drain

After both handles of a channel drop, `Shared::drop` on each ring walks every
slot that still holds an unconsumed value, and drops each value in place. So a
generic `T` never leaks and never drops twice.

#### SPSC cleanup

The SPSC case is simple. At the time `Shared::drop` runs, no other thread can
observe the ring: both handles no longer exist, so `&mut self` is exclusive.
So no ordering-sensitive race on the `head` and `tail` reads there is
possible. Still, the code loads both with `Acquire`. That matches all the
other read sites in the crate, for uniformity, not from necessity.

The exact range `head..tail` covers the slots that `try_send` wrote and that
`try_recv` and `drain` never read. Every index in that range holds an
initialized value, because `tail` advances only after the write. And every
index in that range is still unread, because `head` advances only after the
read.

#### MPSC cleanup

The MPSC case applies the same idea to the contiguous published prefix instead
of a plain range. From `head`, `Shared::drop` walks forward while
`slots[slot].round == seq >> shift` holds, and drops each such slot. It stops
at the first sequence where that equality fails. That failure is precisely how
the walk detects a hole, a slot that a producer claimed but never published,
and handles it safely.

In the current code, every `try_send` call that wins its CAS then writes and
publishes unconditionally. There is no early return between the claim and the
publish. So no hole can occur on the paths that exist today. But the design
does not depend on that fact for soundness.

The check `slots[slot].round == seq >> shift` is the single source of truth
for 'is this sequence actually present'. Each read site checks it
independently: `try_recv`, `drain`, and this cleanup walk. Suppose a hole
existed one day, for example through a future amendment that let a producer
abandon a claim. The prefix scan would then simply stop one sequence early.
The scan would leave the uninitialized memory of that one slot untouched:
never read, never dropped, never freed twice.

The claim cursor and each slot's round number are deliberately two separate
observables. So 'claimed' and 'published' stay independently checkable. Every
consumer of the ring, the live `Receiver` and the terminal `Shared::drop` walk
alike, only ever trusts the round.

#### The `PublishGuard` RAII in the `drain` of both rings

`drain(max, f)` calls `f` once for each consumed item. The caller supplies
`f`, so `f` can panic. Each `assume_init_read` already happened before `f`
runs. So after a panic, the shared `head` must still match the exact count of
items that left the slots.

Otherwise a panic mid-drain has one of two bad outcomes. The next `drain` or
`recv` could read an already-moved-out slot a second time. Or, worse, the
cleanup walk of `Shared::drop` could believe that those slots are still live,
and drop already-moved-out values a second time.

Both `spsc::Receiver::drain` and `mpsc::Receiver::drain` guard against this
identically. A local `PublishGuard` struct borrows the consumer's private
cursor and the shared `head` atomic. Its `Drop` impl runs on the normal return
path and also on an unwind from a panic in `f`. The impl stores the current
cursor, possibly only partially advanced, into the shared `head` with
`Release`. If the cursor did not move at all, `*self.head == self.start`, it
skips the store, so a no-op drain costs nothing.

So the shared `head` always reflects exactly the count of items that actually
left the ring. That holds whether the loop finished, broke early on a dry
ring, or unwound from a panic in `f`. So `Shared::drop` never re-drops a slot
that `drain` already emptied. This is the crate's explicit 'leak, do not
double-drop' policy on the panic path. The comment directly above each
`PublishGuard::drop` impl states that policy.

Both `spsc.rs` and `mpsc.rs` build this guard with the identical shape: a
private cursor reference, a shared atomic reference, and a start snapshot. It
is the single mechanism that keeps the panic-safety story of `drain`
consistent between the two rings.

One Park-mode note remains. If `f` panics, the wake step for parked producers
does not run. The `head` store from the guard still broadcasts over the `Release` and
`Acquire` edge. A parked producer then wakes on the next
normal operation, or on receiver drop. v2 could move the wake into the guard,
to wake earlier.

## 7. Deviations from the bench cells

The cores of `ultima_rings` are ports of the `thread-handoff-ring` and
`thread-handoff-mpsc_ring` Rust cells of `hi-perf-cmp`, not verbatim copies.
Two deliberate deviations exist: a different claim on the MPSC producer side,
and one crate-wide change to the index arithmetic.

#### Bounded-CAS claim instead of `fetch_add`

The claim in the bench cell is an unconditional `fetch_add(1)` on the claim
cursor. Every producer gets a sequence number immediately, with no relation to
the current room in the ring. Backpressure comes after the claim: the producer
spins until the slot that it already owns becomes free.

`ultima_rings` instead makes the claim itself conditional, see §1. A CAS
succeeds only after the ring proves that the target slot is free. This buys
three things that the `fetch_add` version cannot offer.

1. `try_send` can report `Full` without a claim. With `fetch_add` that is
   impossible: every call claims a sequence unconditionally, and the full
   check happens on the already-claimed slot.

2. A producer that decides to block, on the `Park`, `Backoff`, or `BusySpin`
   path of `send`, holds no claimed-but-unwritten sequence. So a parked or
   slow producer can never leave an unpublished hole in the middle of the
   consumer's contiguous-prefix scan.

3. As a consequence of point 2, the consumer's `drain` and `try_recv` never
   reason about a claimed-but-unwritten slot. The question 'is that a hole to
   stop at, or to wait past' never comes up. The claim-to-head bound already
   guarantees that every claimed and in-progress slot lies beyond the
   consumer's position.

The cost is a CAS retry loop on the claim path, instead of a single
`fetch_add`. That was an accepted v1 trade against the bench cell, not an
oversight.

#### Not the cause of the bake-off gap

An earlier revision of this section asserted that this CAS cost explained the
MPSC bake-off gap against `crossbeam-channel`. It does not. The array flavor
of `crossbeam-channel` has no `fetch_add`: it claims with
`compare_exchange_weak` on `tail`. So it pays a CAS for each element too, and
with a stronger success ordering than this crate's, `SeqCst` against
`Relaxed`. The difference 'CAS against `fetch_add`' does not exist between
these two crates, so it cannot cause the gap. See the numbers section of the
README and §8. The honest position at the time of that paragraph: the dominant
cost was still unidentified.

A later round found the cost. It is the CAS retry loop. The cost is the rate
of the retries, not the retry itself.

The claim CAS fails 22 to 42% of the time with 2 to 4 producers. An immediate
retry strikes the contended `claim` line again, as fast as the core can.

An exponential backoff with `spin_loop` spaces the retries farther apart. It
grows 1, 2, 4 up to 64. It resets on each `try_send` call. It is worth +108%
to +143% across all three `mpsc_layout_probe` configurations. It moves the
head-to-head result from 0.55x to 1.26x against `crossbeam-channel`. See
`docs/bench-results/2026-08-11-cas-backoff.md`.

§7 was correct to name the CAS retry loop as a cost. One assumption was wrong:
that only fewer claims can pay that cost down.

#### `& mask` instead of `%` for the slot index

Both rings compute the physical slot with `seq & (cap - 1)`, not with
`seq % cap`. That matches the index convention of the bench cells.
`assert_cap` makes sure at construction that `cap` is a power of two, so the
two forms give the same numbers. But the mask form compiles to a single `AND`
instruction, never to a division. That holds whether or not the optimizer sees
`cap` as a compile-time constant.

The `heapless` survey recommends the same structurally sound approach. Its
issues #650 and #652 show the risk: erased const-generic capacity can
reintroduce `__aeabi_uidivmod` calls into the hot path. The slot index path is
therefore division-free by construction.

#### The availability-round division: resolved in v2, with no measurable change

The per-slot round number of MPSC, beside its payload in `Slot<T>` (§1),
detects wrap-around without an ABA problem. The code computes it on the
publish path, every `try_send`, and on the consume path, every `try_recv` and
`drain`. Through v1 the computation was `seq / cap`. `cap` is a runtime field,
so the compiler could not strength-reduce it: it ran as a hardware division on
the producer and on the consumer. v2 replaced it with `seq >> shift`, where
`shift = log2(cap)` is a value cached at construction. `assert_cap` guarantees
the power of two, and a `debug_assert` pins the equivalence.

The change stays in, because the removal of a division from two hot paths at
zero semantic risk is correct in itself. But it produced no measurable
throughput change. The result sat inside the ~2.9% run-to-run spread of the
benchmark cell (`docs/bench-results/2026-08-09-mpsc-perf-v2.md`). A `div`
costs roughly 20 to 40 cycles against about 1 for a shift, once for each item,
and it vanished into the noise. That is evidence that cross-core
traffic, not ALU work, dominates the MPSC hot path. And it is evidence against more micro-optimization of the arithmetic as a way
to move this design.

#### What stayed byte-equivalent

The actual publish and consume edges match the bench cells exactly. Those
edges are the SPSC pair of `tail` `Release` and `head` `Acquire`, and the MPSC
round encoding, `slots[i].round = seq >> shift`, with its `Release` and
`Acquire` pair. Only two things changed: the claim mechanism on MPSC, and the
index arithmetic on both rings. What 'published' means to a consumer is
identical to the bench cells. The wire-level protocol between producer and
consumer is identical to what the AWS numbers in the README measured.

## 8. Costs

#### Park mode: one `SeqCst` fence for each operation, on each side

§3 established the shape. In Park mode, every publish that could need to wake
a parked peer pays exactly one `fence(SeqCst)`. The producer pays it after
every successful `try_send`. The consumer pays it after every successful
`try_recv` and after every non-empty `drain`. In the other direction, the park attempt on each side pays one fence of its
own, before the re-check.

`BusySpin` and `Backoff` never take this path at all. Their fast path is the
bare lock-free core, nothing more. The fence is a cost that Park mode alone
opts into, in exchange for zero idle CPU.

#### `Backoff`: zero cross-side cost

The idle ladder of the `Backoff` strategy lives in `Idle`, in `src/wait.rs`:
10 spins, then 20 yields, then timed parks that double from 64 µs to 1 ms.
The ladder is fully self-contained on the blocked side. `Idle::idle()` never
touches the peer's state and never calls `wake` or `wake_all`.

The other side, the one that makes progress, pays nothing extra under
`Backoff`. No fence, no flag check, and no conditional branch beyond the
ordinary `strategy == WaitStrategy::Park` guards, which are false for
`Backoff`. The same holds for `BackoffYield`. It shares the `Idle` type and
simply never climbs past its yield rung.

This is the direct payoff of a ladder that wakes itself, against a design that
leans on cross-thread wakes. The timed parks give up and re-check on their
own.
The blocked thread pays the entire cost of 'did my peer make progress',
through polls on a timer. The productive thread pays nothing, and no one ever
asks it to wake a peer.

#### The false sharing that motivated colocation: the v1 layout

Through v1, the availability round lived in a separate flat array,
`avail: Box<[AtomicI64]>`, 8 bytes for each slot, contiguous, with no padding
between slots. `claim` and `head` are each `CachePadded` to their own 64-byte
line, but `avail` was not: eight consecutive `avail` slots shared one cache
line. And every publish also touched the physically separate cache line of the
payload buffer itself.

Under sustained multi-producer contention, producers claim adjacent sequences,
the common case, because the claim cursor gives the producers consecutive
integers. Their publishes then land on `avail` entries within one line. So
their `Release` stores contended for that line, the way false sharing always
contends. That traffic came on top of the separate line that each payload
write touched.

That two-array layout came over unchanged from the `hi-perf-cmp` bench cell
that the AWS numbers in the README measure: 9.4 M `ops/s`, p50 277 ns,
2-producer MPSC on `c6id.2xlarge`. So the cache-line traffic from that layout, whatever its size, was already
part of that measured number. The port introduced no unmeasured risk there.

The layout was also a plausible partial contributor, beside the CAS-retry cost
of §7, to the MPSC bake-off result in the README. The MPSC of `ultima_rings`
trails the CAS-claim, colocated-stamp design of `crossbeam-channel` under the
specific 2-producer, 4-core contention shape of the bake-off. This section
does not claim to isolate either cost as dominant over the other. Both are
real. And the bake-off numbers predate the colocation change below.

#### v2 measured padding of the two-array layout, and rejected it

Padded `avail`, one 64-byte line for each entry, was the first concrete lever
tried against the cost above. v2 implemented and measured it
(`docs/bench-results/2026-08-09-mpsc-perf-v2.md`). The result did not survive
contact with a second configuration.

The numbers: +3.5% at cap 1024 with 2 producers, in a single cell. The same
shape in a different harness gave +2.0%. And cap 4096 gave −0.1%, which is
nothing at all.

The cause is visible in the trade itself. Padding buys freedom from false
sharing at the price of cache residency, and the two scale in opposite
directions. Unpadded, `avail` at cap 1024 was 8 KiB and fit a typical 32 to 48
KiB L1d. Padded, it was 64 KiB and did not fit.

At cap 4096, padded, it was
256 KiB, and the residency cost canceled the benefit from false sharing
exactly. The memory price was `cap × 64 B`: an 8× blow-up on the array and
about 4.5× on the whole channel. So v2 reverted the padding.

#### Colocation measured, and kept

Colocation was the second lever tried against the same cost, and the first to
clear its gate. Instead of a padded two-array layout, v2 merged the round and
its payload into one array, `slots: Box<[Slot<T>]>`. So a publish or a consume
touches a single cache line, not two, see §1 and §2.

This attacks the same cost by a different route than padding. Padding spreads
the entries of one array apart. Colocation instead halves the count of cache
lines that a publish touches. Adjacent producers still share a line, four
`Slot<u64>` values to a line against eight `avail` entries before. So
colocation reduces the line traffic of each publish, but does not remove false
sharing between neighbor sequences. Unlike padding, it adds no memory: no
padded rounds, and no residency trade that scales against capacity. It removes
the cache-line traffic of a second array from the hot path entirely.

The measurement ran in interleaved A-B-A blocks, against all three
`mpsc_layout_probe` configurations, six colocated runs against three baseline
runs for each cell (`docs/bench-results/2026-08-09-colocated-slot.md`). The
results: `cap1024_p2` +15.45%, `cap4096_p2` +14.59%, `cap1024_p4` +11.93%.
Every cell cleared its own run-to-run spread, by 1.3× to 4.5×. And no single
baseline run overlapped any colocated run in any cell.

Unlike padding, the improvement held at every capacity and at every producer
count tested. The residency trade that killed padding at cap 4096 does not
apply here. Colocation reduces total cache-line traffic. It does not
redistribute the footprint of one array.

This is the first of the three MPSC layout and arithmetic hypotheses tried on
this path to clear an all-cells gate. The three: the division removal of §7,
the padding above, and colocation. The division removal produced no measurable
change. Padding cleared only a single-cell gate, then failed at a second
configuration.

Colocation alone does not resolve the MPSC bake-off gap against
`crossbeam-channel`. The CAS-retry claim cost of §7 stays untouched, and the
bake-off numbers above predate this change. But it is real, reproducible
evidence for one point. The cache-line traffic of each publish was a
measurable part of the MPSC hot-path cost. Arithmetic and array padding were
not.

#### The batched claim: tried by proxy, and it lost

This document long recorded the batched claim as the one lever still untried.
A proxy then tried it, and it lost. `disruptor` 4.4 ships that design: batched
claim, bitmap availability, in-place slots. It measures 27.2 `Melem/s` against
33.2 for this crate, on the same box and the same workload, while it does less
work for each item. See the addendum of
`docs/bench-results/2026-08-09-bakeoff-v2.md` and the survey in
`docs/superpowers/research/2026-08-10-disruptor-survey.md`. The record is:
tried and rejected, not open. For callers who can give up global FIFO,
`src/sharded.rs` (§9) stays the answer. And the claim-side cost stays
unexplained, not merely unaddressed.

## 9. Alternatives weighed

#### Sharded SPSC: one private ring for each producer

This is the most structurally obvious alternative to a shared-claim MPSC. It
is also the only entry in this section with a real build and real
measurements, not only an argument. It lives behind the `experimental-sharded`
feature in `src/sharded.rs`, with results in
`docs/bench-results/2026-08-07-sharded-mpsc.md`. Instead of many producers
that contend for one ring, each producer owns a private `src/spsc.rs` ring,
and the single consumer sweeps the rings with a sticky cursor.

This deletes, in one move, both costs that §7 and §8 attribute the MPSC gap
to. Those costs are the bounded-CAS retry loop, and the cross-producer
cache-line traffic of the round. A single-writer ring needs neither a claim nor a shared round at
all, not even the colocated round that §8 measures a gain from.

It also removes a head-of-line stall that the shared-claim design has. A
producer preempted between its claim and its round store blocks the delivery
of every already-published item behind it, because `drain` must stop at the hole
(`src/mpsc.rs:330`).

The measurement, on a 4-core Linux VM at equal total buffered capacity: 321.5
`Melem/s`, against 29.3 for this crate's own `mpsc` (about 11×) and 71.25 for
`crossbeam-channel` (4.51×).

Even so, `mpsc` stays the crate's default MPSC. The three things that sharding
gives up are exactly what users usually expect from a channel.

- Global FIFO. The CAS on `claim` linearizes all producers into one sequence,
  and `drain` consumes the contiguous prefix. So delivery order is claim order
  across producers. Sharding offers per-producer FIFO only. Cross-producer
  order becomes an artifact of scan position. `crossbeam-channel`, `flume`,
  and `kanal` all provide the global order.

- A global bound. `channel(1024)` means 1024 items in total, and `Full` means
  genuinely full. Sharding makes backpressure a producer-local property. One
  producer can block at `total / n` while the other shards sit empty.

- Cheap emptiness. For `mpsc`, one `Acquire` load answers 'is anything there'.
  Sharding needs a full scan of `n_shards` rings before it can conclude
  `Empty` or `Disconnected`.

The design also carries a precondition, not merely a limitation. Its speed
comes from one writer for each ring. That holds because `sharded::Sender` is
not `Clone`, and construction fixes the shard set. Support for dynamic
producers would need a shard for each clone, with the registry, lifecycle, and
reaper work that implies. Or it would forfeit the result.

No one plans that work: the intended workload fixes its producer set at
startup. So the measured figures describe the type as it exists. They do not
bound a future version.

#### Vyukov per-slot stamps and packed state words

This is the shape of the array flavor of `crossbeam-channel`, and of `Core` in
`thingbuf`. Both fold the 'is this slot ready' signal into a single per-slot
atomic that also encodes the claim or generation. Neither keeps a separate
claim cursor and a separate per-slot round. The fold is more compact, one atomic
touched for each slot instead of two logically separate observables. It also
gives crossbeam a clean MPMC design.

But the fold couples readiness to the writer's own CAS target. `push_ref` in
`thingbuf` must detect and skip a slot that a slow reader still holds, through
its `HAS_READER` bit. That exact coupling appears in two open, unresolved
correctness issues in the tracker of `thingbuf` itself. Issue #98: a
self-requeue invariant violation under a plain pop-then-repush workload. Issue
#100: a hang or crash on channel close while a slot guard is live. See the
`thingbuf` survey.

`ultima_rings` keeps the claim cursor and each slot's round as two independent
observables on purpose. A genuinely single-consumer design has no reader that
stays on a slot the way an MPMC consumer can. So it never needs that skip
logic at all. The price: one extra field for each slot, and the cache-line
traffic of a touch separate from the claim cursor, now measurably reduced, see
§8. The alternative would fold the two into a single per-slot word.

#### Layout against protocol: what this crate couples, and what it does not

The colocation change of §8, the round moved into `Slot<T>` beside the
payload, can look similar to the Vyukov and `thingbuf` design above. Both put
more than a single thing 'in the slot'. But they are different axes.

Colocation of the round with the payload is a layout change. It changes only the
cache line that a byte lives on. It has no effect on what the round means or on who
can write it. The fold of readiness into an atomic that also encodes the
claim, the Vyukov, crossbeam, and `thingbuf` shape, is a protocol change. It
changes the write that makes a slot both claimed and ready in one step. And it is
exactly the coupling behind #98 and #100 in `thingbuf`.

`ultima_rings` colocates on the layout axis: measured, and kept, see §8. It
never adopted, and does not propose, a fold of the claim into the round's
atomic. The claim cursor and each slot's round stay independent: two writes, with two
meanings. Only their storage location moved.

#### kanal's direct cross-stack transfer

The fast path of `kanal`, for small and rendezvous payloads, writes the value
directly into a `KanalPtr`. That pointer points into the stack frame of the
receiver thread itself. For that case it avoids the shared ring buffer entirely. This
is measurably fast.

But it is structurally incompatible with a design that loom and miri can
check. In such a design, every slot belongs to the shared allocation, and
never to a thread's stack. The validity of the pointer depends on one condition: the stack frame of the
target thread must stay alive across a park and unpark hop. That
invariant produced the majority of kanal's own historical soundness bugs,
issues #3, #4, #17, and #19, see §10.

The design of `ultima_rings`, a `MaybeUninit` slot owned by the `Arc`, is the
direct structural alternative. A slot's lifetime follows the channel's own
lifetime, not the stack of any one thread. Exactly that is what makes the loom
models in `tests/loom.rs` tractable to write and to trust.

#### flume's lock-based core

The entire channel of `flume`, bounded and unbounded alike, is a `VecDeque`
behind one `Mutex`, with waits layered outside the lock through
`thread::park`. This buys `flume` two things: no `unsafe`, and genuine MPMC
generality, multiple receivers that compete for messages, which `ultima_rings`
deliberately does not support. The cost is a full lock and unlock of the mutex
on every single operation, contended or not. The `flume` survey itself frames
that cost as the reason that mature crates such as crossbeam still hand-roll
lock-free channel designs at all.

The target of `ultima_rings` is the opposite end of that trade. The caller
picks a fixed, known producer and consumer shape ahead of time, SPSC or MPSC
only. The channel sits on a latency-critical SMR hot path. There, the small, but nonzero, cost of an uncontended lock is unacceptable,
and so is the unbounded tail latency of a contended one. For the stated use case of this
crate, the correct trade is the lock-free core, checked under loom and miri
instead of 'safe by construction'. That trade is not correct for every
channel.

## 10. Soundness pitfall checklist

Each entry below is a documented failure class from the issue tracker of
another lock-free or channel crate. Each entry also names the concrete
property of `ultima_rings` that covers it: a design choice, a test, or a
verification lane.

#### Stack-pointer escape (kanal #3, #4)

The failure: a pointer into a thread's stack frame outlives that frame. Or
code uses a `Thread` handle after the owner thread possibly already exited.

The mitigation: every slot lives in the shared `Arc<Shared<T>>` allocation,
never on a participant's stack, see §9. The `Thread` handle of `Parker` comes
from `thread::current()` and sits behind a `Mutex`. `wake` and `wake_all`
consume it exactly once. No code dereferences it after the point where the
owner thread can plausibly exit its `park()` call.

#### Clone and split double-free (kanal #17, #19)

The failure: a clone or split of a sender or receiver frees shared state
incorrectly, or aliases it incorrectly.

The mitigation: `Sender::clone` on MPSC is `Arc::clone` plus a `Relaxed`
`fetch_add` on `senders`, with no unsafe code in the clone path at all. And no
split operation exists: `channel()` returns owned handles exactly once, never
re-splittable. The `heapless` survey (its §4) recommends exactly that as the
structural fix for this whole bug class, rather than a patch after the fact.

#### `forget` and `ManuallyDrop` against real drop (kanal #2, #28)

The failure: code moves a value without a drop of the old location, or it
forgets a value that needs a drop. Only miri catches this.

The mitigation: the crate does not use `mem::forget` or `ManuallyDrop`
anywhere. The only unsafe moves are `MaybeUninit::write`, `assume_init_read`,
and `assume_init_drop`. Each sits in a single `with` or `with_mut` closure
directly beside its `Release` or `Acquire` edge. The miri lane runs an audit over every such call site: 51 of 51 tests pass
with all features, with 0 UB.

#### Aliases on parked or suspended objects (kanal #14, #16)

The failure: a parked object stays reachable, at the same time, through an
exclusive API and a shared API.

The mitigation: the `Thread` slot of `Parker` and the waiter vector of
`WaiterList` both sit behind a `std::sync::Mutex`, never exposed as a raw
shared reference. And the crate has no async surface, see §11. So no
waker-against-poller alias surface exists in v1 at all.

#### Non-`repr(C)` transmute between generic structs (kanal #36, #49)

The failure: a transmute between two structurally similar generic types,
without a guarantee of identical layout.

The mitigation: the crate contains no `transmute` anywhere, and a search over
`src/` shows it. `Sender` and `Receiver` never cast between representations.
The code only constructs them directly.

#### Hand-written `Send` and `Sync` bounds (kanal #33, #45)

The failure: a manual `unsafe impl Send` or `Sync` that is too permissive, so
unsound, or too restrictive.

The mitigation: the only hand-written bound in the crate is
`unsafe impl<T: Send> Send/Sync` for `Shared<T>`, gated on `T: Send` and
justified in an adjacent `SAFETY` comment. `Sender<T>` and `Receiver<T>`
themselves take their auto-trait status from their fields, with no manual
impl. In practice they are `Sync` for every `T: Send`.

The API surface enforces the exclusivity of the single producer (SPSC) and the
single consumer (both rings): no `Clone` on `Receiver`, and every method that
mutates takes `&mut self`. No `!Sync` bound enforces it. A stale `SAFETY`
comment in `src/spsc.rs` and `src/mpsc.rs` still describes the handles as
`!Sync`. That is a known, previously flagged documentation fault, tracked but
not yet corrected in the source. The correct statement is the one here: the
mechanism is no `Clone` plus `&mut self`.

#### Cancel races delivery (kanal #15, #35, #47)

The failure: a peer completes delivery concurrently with a cancel of an
in-flight receive or send, a future drop or a thread that gives up.

The mitigation: every `send` and `recv` that can block re-validates its
condition directly before it parks, the Dekker re-check of §3. It re-validates
again after every `park()` return, in the retry loop. So a cancel can never race a delivery into a lost message. A cancel here is a
producer that leaves its wait because space appeared, or a disconnect that
arrives.

The loom models `loom_full_parked_sender_vs_recv_and_rx_drop` and
`loom_close_wakes_parked_consumer` in `tests/loom.rs` (Task 7) exhaustively
check exactly this interleaving class, for the cases that this crate has. No
async cancellation exists in v1, so the future-drop variant from kanal does
not apply.

#### Combinator and adapter surface as a distinct attack surface (kanal #63)

The failure: a convenience wrapper, a stream or iterator adapter, over an
already-sound core reintroduces a bug that the core does not have.

The mitigation: the `PublishGuard` of `drain` (§6) pre-empts exactly this
class. `drain(max, f)`, a batch-shaped API, is the closest thing this crate
has to a combinator. It has its own independent panic-safety audit. The audit
does not assume that the soundness of the single-item `try_recv` path carries
over.

#### rtrb's publish-before-drop rule (issue #185, open upstream)

The failure: a chunk or batch commit drops consumed slots before it advances
the published index. A `Drop` that panics mid-batch then leaves the index
stale, and the next read drops the same slots again.

The mitigation: `PublishGuard` (§6) advances the shared `head` to exactly what
left the ring, through its own `Drop` impl, and that impl runs on the unwind
path too. The index update and the count of items actually taken can never
desynchronize, whether `f` panics or not. This came in with the introduction
of `drain` (Tasks 2 and 4), specifically to avoid a reproduction of rtrb's
still-open bug.

#### The `Arc::strong_count` trap (rtrb #114)

The failure: a correctness invariant leans on the undocumented, incidental
synchronization behavior of another type, here the ordering of
`Arc::strong_count`, and breaks after an upstream change.

The mitigation: `ultima_rings` never reads `Arc::strong_count`. Disconnect and
liveness state is entirely the crate's own explicit atomics: `disconnected`,
`rx_dropped`, and `senders`. §2 documents their orderings, and §5 argues them.
Nothing depends on the incidental guarantees of any other type.

#### heapless's division regression class (issue #650)

The failure: type erasure of a capacity that was a compile-time constant
silently reintroduces a runtime division in the hot index-update path.

The mitigation: both rings compute the slot index with `seq & (cap - 1)`,
never with `%`, see §7. The `AND` cannot lower to a division, whether or not
`cap` is a compile-time constant. So the index path is structurally
division-free. The availability-round computation of MPSC is division-free
too: the round is `seq >> shift`, with `shift = log2(cap)` cached at
construction, not `seq / cap`. `assert_cap` guarantees the power of two. That
is the v2 optimization of §7.

## 11. Future layers

`ultima_rings` v1 is sync-only. The `notify` layer, `src/notify.rs`, speaks in
`std::thread::Thread` handles, not in `std::task::Waker` values. The crate
contains no `Future` and no async surface at all. This is a deliberate,
explicit non-goal for v1, not an oversight.

If a future version wants to add async, the layers, as they stand, already
make it additive, not a rewrite. The lock-free cores, `spsc.rs` and `mpsc.rs`, never
call `thread::park` or `unpark` directly. Every wait goes through the `Parker`
and `WaiterList` abstraction of `notify.rs`. That abstraction is the only
place that would need a second implementation.

The architecture of `flume`, surveyed in `docs/superpowers/research/`, is the
existence proof for this shape. Its `Signal` trait lets the identical `send`
and `recv` core serve both a `SyncSignal`, park and unpark, and an
`AsyncSignal`, store a `Waker` and call `wake_by_ref`. Only the step 'what
happens while blocked' is a parameter. The core of the queue knows nothing
about sync against async.

An async backend for `ultima_rings` would follow the same shape. A new
`notify` implementation stores a `Waker` instead of a `Thread`, and wakes it
instead of an unpark. It registers the same way as the current `Parker` and
`WaiterList` protocol: announce intent, then the `SeqCst` fence, then the
re-check, then suspend. It would not touch the claim, publish, and consume
logic of `spsc.rs` or `mpsc.rs` at all. This stays explicitly out of scope for
v1.
