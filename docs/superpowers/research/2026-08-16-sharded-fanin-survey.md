# Survey: prior art for the sharded fan-in shape (N SPSC rings, one consumer)

**Date:** 2026-08-16
**Why surveyed:** `sharded` graduated to a stable flavor in v0.2.0 with every
figure measured against this crate's own `mpsc` and nothing else
([`2026-08-16-sharded-ladder-skew.md`](../../bench-results/2026-08-16-sharded-ladder-skew.md)
runs `sharded_shard_ladder` against `mpsc_producer_ladder`). The seven earlier
surveys all evaluated single-channel competitors, so the crate has never
answered a basic positioning question: **does anyone else ship one-ring-per-
producer with a single sweeping consumer, and if so, how do they route
producers to shards?**

**Method.** crates.io search API, five queries sorted by downloads (`sharded
queue`, `spsc`, `mpsc ring buffer`, `fan-in channel`, `multi producer single
consumer lock-free`), 12 results each; crate metadata and docs.rs for every
plausible hit; source reads of the vendored competitor crates already in
`~/.cargo/registry` (crossbeam-channel 0.5.16, flume 0.11.1, kanal 0.1.1,
thingbuf 0.1.6, rtrb 0.3.4, disruptor 4.4.0); upstream source reads for
`affinitypool` 0.8.0 and JCTools `MpscCompoundQueue`. All network fetches
2026-08-16. Claims below are source-backed unless explicitly marked
**[design-level, unverified]**.

## Verdict

No Rust crate ships this shape as a *channel*. Two implementations ship the
mechanism in other clothes — `affinitypool` as a threadpool's internal
injector set, JCTools as a queue — and both route producers to shards by
**hashing a thread id**, which is the one design decision this crate makes
differently. Binding one producer to one shard permanently (`Sender` is not
`Clone`) is what removes the CAS; hash routing keeps it, because two producers
can hash to the same shard.

The pattern itself is not novel and should not be claimed as such. What
appears unoccupied is the packaging: a bounded, safe Rust channel with a fixed
producer set, per-shard capacity, aggregate disconnect, and a shard-sweeping
`drain`.

## 1. The five benched competitors do not shard at all

`grep -ril shard` over the vendored `src/` of crossbeam-channel 0.5.16,
flume 0.11.1, kanal 0.1.1, thingbuf 0.1.6 and rtrb 0.3.4 returns **zero
files** in each. Their shapes are unchanged from the 2026-08-06 surveys: one
shared structure that every producer contends on (crossbeam, flume, kanal,
thingbuf), or SPSC with no fan-in story at all (rtrb).

rtrb remains the natural build-it-yourself substrate — N rings plus your own
sweep *is* this design — but it hands you no consumer-side sweep, no aggregate
disconnect, and no blocking `recv` that waits across rings.

## 2. `affinitypool` 0.8.0 — the closest thing in Rust, and it is a threadpool

SurrealDB's blocking-job pool (860k downloads, updated 2026-08-11) describes
itself as delivering tasks through "a sharded, lock-free queue. Each producer
thread routes consistently to its own shard." Its `src/queue.rs` (763 lines,
of which the first ~140 are a design comment) confirms the mechanism:

- Up to `MAX_SHARDS` (default **8**, always a power of two, overridable via
  `Builder::shards`) cache-padded `crossbeam_deque::Injector`s.
- Producers pick a shard by **cached hash of the thread id**; the same
  producer lands on the same shard every time.
- Workers steal across shards *and* from each other's local deques; the
  documented fast path is "worker steals a batch from its preferred injector".
- A `SPILL_THRESHOLD` counter fixes the single-producer degenerate case: after
  N consecutive pushes to the same preferred shard, later pushes rotate to
  neighbouring shards so more than one worker has something to steal.
- The idle protocol is `parked` flag + `SeqCst` fences + condvar, with the
  fence-pairing argument written out in the module comment.

Three things separate it from `sharded`: the consumers are plural and steal
(work *distribution*, not fan-in), the injectors are unbounded (so there is no
backpressure — the thing our `cap` exists to provide), and hash routing means
a shard can receive more than one producer, which is why it needs a lock-free
MPMC injector per shard rather than an SPSC ring.

Worth noting as independent corroboration: their design comment says
per-producer shard routing "is a win when multiple producers are active (the
`multi_producer` benches)" — the same direction as our ladder, from an
unrelated codebase.

## 3. `sharded_queue` 2.0.1 — shards to reduce lock contention, not to remove CAS

40k downloads, last updated 2023-08-17. Shard count is chosen by the caller
from `available_parallelism()`. Its own docs state the trades plainly: shards
are spin-lock guarded and resizable, and **FIFO order is not guaranteed** —
the doc walks a resize interleaving where the order breaks. MPMC, unbounded,
two methods (`push_back`, `pop_front_or_spin_wait_item`), no length tracking.

Same word, different design: it shards a *locked* collection to spread lock
contention. This crate shards to make each ring single-writer so there is no
atomic claim at all.

## 4. JCTools `MpscCompoundQueue` (Java) — the closest named prior art

Class comment: *"Use a set number of parallel MPSC queues to diffuse the
contention on tail."* The mechanism, from source:

- **Routing** (`offer`): `int start = (int)(Thread.currentThread().getId() &
  parallelQueuesMask)`, then `queues[start].offer(e)`.
- **Spill** (`slowOffer`): on failure it scans every other queue with
  `failFastOffer`, and only returns `false` when all `queueCount` sub-queues
  refuse. A producer's items can therefore land in any shard.
- **Sweep** (`poll`): resumes at a retained `consumerQueueIndex`, walks up to
  `parallelQueues` shards, returns the first non-empty, and stores the
  position back.
- **Sub-queues** are `MpscArrayQueue` — themselves multi-producer.

Head to head:

| | JCTools `MpscCompoundQueue` | `ultima_rings::sharded` |
|---|---|---|
| Producer → shard | `threadId & mask`, may collide | bound 1:1 at construction, `Sender` not `Clone` |
| Per-shard queue | `MpscArrayQueue` (CAS claim survives) | SPSC ring (no CAS) |
| On a full shard | spills to any other shard | blocks/fails on **this** shard |
| Ordering | no per-producer FIFO once spilled | per-producer FIFO, guaranteed |
| Consumer sweep | sticky index, one item per call | sticky cursor, `VISIT_BUDGET = 32` items per shard before advancing |
| Producer set | dynamic | fixed at construction |

The spill is the interesting divergence. It buys dynamic producers and better
occupancy under skew, and it costs both the per-producer FIFO guarantee and
the single-writer property. Our contract sells exactly those two things, so
the spill is not an option we can adopt without becoming a different channel.

## 5. The same shape at system scale, outside Rust

- **Aeron/agrona** ships both `ManyToOneRingBuffer` and `OneToOneRingBuffer`
  (both present in `agrona/.../concurrent/ringbuffer/`, confirmed 2026-08-16)
  — the same fork in the road as our `mpsc` vs `sharded`.
- **disruptor 4.4.0** (Rust) builds one ring per `build_*` call and its
  multi-producer path is the shared-claim `MultiProducerSequencer` covered in
  [`2026-08-10-disruptor-survey.md`](2026-08-10-disruptor-survey.md). No
  multi-ring gatherer; the Disruptor answer to producer contention is a manual
  "diamond" wiring.
- **Seastar** (shard-per-core with SPSC queues between shard pairs, polled by
  the reactor) and **DPDK** (per-lcore `rte_ring`s polled by one core) are the
  production-scale version of this design. **[design-level, unverified — not
  read this round.]**

## 6. `crossbeam::Select` is the ecosystem's substitute, and a different machine

The usual Rust answer to "one consumer, many channels" is `Select`. Its cost
shape is not comparable to a sweep (`crossbeam-channel-0.5.16/src/select.rs`):
`run_select` calls `utils::shuffle(handles)` on every pass (`select.rs:199`,
again at `:350`), tries each handle in turn, then on failure **registers** on
each handle (`:226-230`) and **unregisters** each registered one on wake
(`:268-269`). So every blocked operation pays an O(n) shuffle, an O(n) try
pass, and O(n) register/unregister — and it parks, whereas our sweep is
self-waking and does no registration at all. Select also leaves each
constituent channel free to be MPSC, meaning the per-channel CAS survives.

## Implications for ultima_rings

1. **Add an external baseline bench cell: `n × rtrb` plus a hand-rolled
   sweep.** It is the honest comparator — the thing a user would build if this
   crate did not exist — and today `sharded` has no external number at all.
   Second cell worth having: `crossbeam::Select` over N bounded channels, to
   put a figure on §6 rather than an argument.
2. **Position `sharded` with its prior art, not as an invention.** A line in
   the README or `fan-in-from-a-fixed-producer-set.md` naming JCTools'
   compound queue and Aeron's one-to-one buffers costs two sentences and makes
   the fixed-producer-set trade legible to anyone who has met either.
3. **The fixed producer set is the differentiator, and it survived contact.**
   Both independent implementations of this shape hash producers to shards and
   both therefore keep a multi-producer queue per shard. Our 1:1 binding is
   the reason the shard can be a plain SPSC ring.
4. **Skew has a known upstream answer we deliberately do not take.**
   `affinitypool` steals across shards and JCTools spills across queues; both
   would break per-producer FIFO here. Our answer stays `VISIT_BUDGET` plus
   the measured skew cells — worth stating explicitly wherever skew is
   documented, since readers coming from either system will look for stealing.

## Sources

- crates.io search + crate API, fetched 2026-08-16:
  https://crates.io/api/v1/crates
- `affinitypool` 0.8.0 — https://github.com/surrealdb/affinitypool,
  `src/queue.rs` (read at `main`, 2026-08-16), README
- `sharded_queue` 2.0.1 —
  https://docs.rs/sharded_queue/latest/sharded_queue/struct.ShardedQueue.html
- JCTools `MpscCompoundQueue` —
  https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/MpscCompoundQueue.java
- agrona ring buffers —
  https://github.com/aeron-io/agrona/tree/master/agrona/src/main/java/org/agrona/concurrent/ringbuffer
- `fibre` 0.6.4 (checked and excluded: specialized `spsc`/`mpsc`/`spmc`/`mpmc`
  channels, but its MPSC is a single lock-free list or array, not sharded) —
  https://github.com/excsn/fibre
- Vendored sources under `~/.cargo/registry/src/index.crates.io-*/`:
  crossbeam-channel-0.5.16, flume-0.11.1, kanal-0.1.1, thingbuf-0.1.6,
  rtrb-0.3.4, disruptor-4.4.0
