# How to shut down a pipeline cleanly

Shutdown is driven by dropping handles; there is no separate close call.
The full semantics are in the
[disconnect reference](../reference/errors-and-disconnect.md).

## Stop from the producer side (drain-then-stop)

To finish a pipeline without losing in-flight values, drop the senders and
let the consumer run to completion:

1. Drop every `Sender` (for `mpsc`, every clone — the channel stays open
   until the last one goes).
2. Keep consuming. `recv` continues returning buffered values and returns
   `Err(RecvError)` only once the ring is empty; with `try_recv`, treat
   `Disconnected` — not `Empty` — as the exit signal.

A consumer blocked in `recv` under `Park` is woken by the last sender's
drop; you do not need to send a sentinel value to unblock it.

## Stop from the consumer side (cancel)

To cancel a pipeline, drop the `Receiver`. Every producer's next `send` or
`try_send` fails with the value handed back (`SendError(v)` /
`Disconnected(v)`), including producers currently blocked in `send` — they
wake and return. Use the handed-back value if the producer needs to reroute
or persist what didn't make it.

## Abandon everything

If you drop both sides with values still buffered, the ring drops each
unconsumed value exactly once. If those values carry side effects on drop —
file handles, guards — that cleanup runs on whichever thread drops the last
handle.

## Shut down a fan-in

For `mpsc` with many producer threads, the drain-then-stop order is: signal
your producers to finish (your own mechanism), join them so their `Sender`
clones drop, then drain the consumer to `Disconnected`. Joining first
guarantees the consumer's `Disconnected` means "no producer will ever send
again", not "no producer happened to be sending just now".
