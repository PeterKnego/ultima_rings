# How to batch-consume with drain

`drain(max, f)` consumes up to `max` currently-available values in one call,
invoking `f` per value and advancing the shared cursor once at the end.

## Use it to bound work per wakeup

If the consumer interleaves channel work with other duties — a tick loop, an
event loop turn — use `max` as the work bound per turn:

```rust
let consumed = rx.drain(BATCH, |event| handle(event));
```

Anything beyond `BATCH` stays in the ring for the next turn.

## Use it to publish consumption once per batch

If the consumer's per-item work is tiny and the producer is blocked on a
full ring, drain frees `max` slots with a single cursor store instead of one
per item, handing the producer a run of free slots at once.

## Distinguish "empty" from "closed"

`drain` returns `0` both when the ring is empty and when every sender is
gone. If your loop exits on quiescence, pair it with `try_recv`:

```rust
if rx.drain(BATCH, |v| handle(v)) == 0 {
    match rx.try_recv() {
        Err(TryRecvError::Disconnected) => break, // closed and drained
        Err(TryRecvError::Empty) => { /* idle; wait or do other work */ }
        Ok(v) => handle(v),
    }
}
```

## Do not switch to drain for throughput alone

On a saturated pipeline, single-item `try_recv` measured ~10% *faster* than
`drain` (see
[`2026-08-14-bakeoff-v4.md`](../bench-results/2026-08-14-bakeoff-v4.md)).
Reach for `drain` when you need its shape — bounded work per turn, batched
cursor publication — not as a default speedup.
