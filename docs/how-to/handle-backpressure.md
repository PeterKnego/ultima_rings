# How to handle backpressure from a full ring

A bounded ring signals a slow consumer by refusing new values instead of
growing. Decide what a producer should do with that signal before it fires.

## Block until space frees

If the producer can afford to wait, call `send`. It blocks under the
channel's wait strategy until the consumer frees a slot, and returns
`Err(SendError(v))` only if the receiver is gone.

## Shed load

If the producer must not stall — an ingest thread, a logging call site — use
`try_send` and treat `TrySendError::Full(v)` as the shed point. The value
comes back in the error, so you choose its fate:

```rust
match tx.try_send(event) {
    Ok(()) => {}
    Err(TrySendError::Full(event)) => drop_count += 1, // or spill `event` elsewhere
    Err(TrySendError::Disconnected(_)) => return,      // consumer is gone
}
```

Count what you shed. A silent drop counter that nobody reads is the usual
way this policy goes wrong in production.

## Retry on your own schedule

If you want bounded waiting — try for a while, then escalate — loop on
`try_send` with your own clock and backoff. `Full(v)` hands the value back
on every attempt, so nothing is lost between retries.

## Resize the buffer

If bursts routinely fill the ring but the consumer keeps up on average, the
capacity is your lever: build the channel with a larger power-of-two `cap`.
Capacity is fixed at construction — there is no runtime resize.

If none of these fit — the producer can't block, can't shed, and can't
buffer enough — the consumer is genuinely too slow, and no channel policy
fixes that; profile the consumer instead.
