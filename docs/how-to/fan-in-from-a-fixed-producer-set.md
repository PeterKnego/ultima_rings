# How to fan in from a fixed set of producers

Use `sharded::channel` when the producer threads are known when you build
the channel — a worker pool feeding an aggregator, per-core collectors
feeding one writer — and each producer's own values need to arrive in
order. It gives every producer a private ring, so producers never contend
with each other.

If producers are created and destroyed as work arrives, or a consumer
downstream depends on one global order across producers, use `mpsc`
instead; [choosing a topology](choose-a-topology-and-wait-strategy.md)
covers that decision.

## Build the channel and hand out the senders

Pass the number of producers, the total buffered capacity across all of
them, and a wait strategy. You get one `Sender` per shard:

```rust
use ultima_rings::{TryRecvError, WaitStrategy, sharded};

let (senders, mut rx) = sharded::channel::<Job>(4, 4096, WaitStrategy::Backoff);
```

`total_cap` must divide evenly by the shard count, and the resulting
per-shard capacity must be a power of two — 4096 / 4 = 1024 here. Both
rules panic at construction when broken; the
[capacity rules](../reference/channels.md#capacity-rules) state them.

Move exactly one sender into each producer thread. `sharded::Sender` is not
`Clone`, so the compiler enforces one writer per shard for you:

```rust
for (worker_id, mut tx) in senders.into_iter().enumerate() {
    std::thread::spawn(move || {
        for job in work_for(worker_id) {
            if tx.send(job).is_err() {
                return; // consumer is gone
            }
        }
    });
}
```

If you need a producer count that is not known until runtime, compute it
before the `channel` call — it is fixed from that point on. To add a
producer later you must build a new channel; there is no way to grow the
shard set.

## Size capacity per producer, not per channel

A producer sees `Full` when **its own** shard holds `total_cap / n_shards`
items, even while every other shard sits empty. Size `total_cap` so that
`total_cap / n_shards` absorbs one producer's worst burst — sizing it for
the sum of all bursts over-allocates by roughly the shard count.

Everything else about a full ring works as it does elsewhere: block with
`send`, shed with `try_send`, or retry on your own clock, per
[handling backpressure](handle-backpressure.md).

## Consume with drain

The consumer sweeps the shards. On a saturated pipeline use `drain`, which
takes a run of items per shard and publishes each shard's cursor once per
batch rather than once per item:

```rust
loop {
    if rx.drain(256, |job| handle(job)) == 0 {
        match rx.try_recv() {
            Ok(job) => handle(job),
            Err(TryRecvError::Empty) => std::hint::spin_loop(),
            Err(TryRecvError::Disconnected) => break,
        }
    }
}
```

The `drain`-returns-`0` check is doing real work here: `0` means "empty" and
"closed" alike, so `try_recv` is what distinguishes them. If your consumer
has other duties, keep the `max` argument as your work bound per turn —
[batch-consume with drain](batch-consume-with-drain.md) covers that shape,
including the one-visit-per-shard rule.

If you would rather block than poll, call `rx.recv()`; it sweeps and waits
on the channel's strategy. Note that `sharded::channel` rejects
`WaitStrategy::Park` — pick `Backoff` when you want a blocking consumer
that idles cheaply.

## Shut down

Drop every sender. The consumer keeps delivering buffered values and sees
`Disconnected` only once **every** shard is both dropped and drained, so no
value is lost to a shard whose producer finished early. Joining the
producer threads before the consumer's loop exits is the usual way to make
that ordering obvious. For cancellation from the consumer side and the
other shutdown shapes, see
[shutting down a pipeline](shut-down-a-pipeline.md).

## If one producer runs much hotter than the rest

Skew is tolerable: with one hot producer and fifteen idle shards, the
consumer's wasted sweeps cost 11–14% of throughput
([`2026-08-16-sharded-ladder-skew.md`](../bench-results/2026-08-16-sharded-ladder-skew.md)).

What does bite is the capacity bound. The hot producer stalls at
`total_cap / n_shards` while the idle shards hold their slots in reserve,
and no rebalancing happens. If that stall is unacceptable, either raise
`total_cap` until the hot shard's share covers its burst, or switch to
`mpsc`, where one global bound is shared by all producers.
