# How to choose a topology and wait strategy

Both choices are fixed at construction; changing either later means building
a new channel and re-plumbing its handles. Decide them together.

## Choose the topology

- If exactly one thread produces, use `spsc::channel` — it is the fastest
  path in the crate and pays no CAS at all.
- If several threads produce into one consumer, use `mpsc::channel` and
  clone the `Sender` once per producer.
- If the producer threads are **known up front** and you only need each
  producer's own values in order — worker pool into an aggregator, pinned
  per-core collectors — use `sharded::channel`. It trades three things
  `mpsc` gives you (global FIFO, one global capacity bound, `Sender: Clone`)
  for the removal of all cross-producer contention, and the trade grows with
  producer count: 6× `mpsc` at 2 producers on 16 cores, more past that
  ([`2026-08-16-sharded-ladder-skew.md`](../bench-results/2026-08-16-sharded-ladder-skew.md)).
  Its consumer benefits from `drain` far more than the other flavors'
  ([`2026-08-16-sharded-string-drain.md`](../bench-results/2026-08-16-sharded-string-drain.md)).
- Do not funnel multiple producers through one `spsc::Sender` behind a
  mutex — the lock reintroduces exactly the cost the crate exists to remove.
  If you need more producers later, switch to `mpsc`.
- If you need multiple *consumers*, this crate is the wrong tool; use
  `crossbeam-channel` or `flume`.

## Choose the capacity

- Pick a positive power of two (anything else panics — see
  [capacity rules](../reference/channels.md#capacity-rules)).
- Size for your worst tolerated burst: a full ring is your backpressure
  signal, so `cap` is the amount of producer lead you are willing to buffer
  before producers feel it.

## Choose the wait strategy

Take the cheapest strategy your latency budget tolerates; the measured
figures behind each trade are in the
[wait-strategy reference](../reference/wait-strategies.md).

- If latency matters at any CPU cost and you can dedicate a core per blocked
  side, use `BusySpin`.
- If you want near-`BusySpin` wakes but must not starve other runnable
  threads — more runnable threads than cores, cotenant services — use
  `BackoffYield`. Do not use it to save CPU on an idle machine: it burns a
  full core there, same as `BusySpin`.
- If you want low CPU while idle and can tolerate wakes at the OS-timer
  floor (~60 µs), use `Backoff`. This is the balanced default.
- If idle CPU efficiency matters more than the last microseconds — a mostly
  idle channel, many channels per core — use `Park`, and accept that the
  *productive* side pays for wakes on every operation.
- If you want a blocking API *and* throughput under sustained load, prefer
  `Backoff` over `Park`: `Park` is the crate's weakest blocking path under
  load (measured in
  [`2026-08-14-backoff-cells.md`](../bench-results/2026-08-14-backoff-cells.md)).

If two strategies both seem plausible, benchmark your own workload on your
own machine — the crate's own program of measurements shows these trades
move with hardware and thread placement.
