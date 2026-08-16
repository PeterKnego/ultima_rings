# How-to guides

Goal-oriented guides for working with the crate. Each one serves a job you
came with, assumes you know Rust and threads, and links to the
[reference](../reference/README.md) for exhaustive facts rather than
repeating them.

Setting up a channel:

- [How to choose a topology and wait strategy](choose-a-topology-and-wait-strategy.md)
  — both are fixed at construction; pick them for your latency and CPU
  budget.

Running under load:

- [How to handle backpressure from a full ring](handle-backpressure.md) —
  block, shed, retry, or resize when the consumer falls behind.
- [How to batch-consume with drain](batch-consume-with-drain.md) — bound
  work per wakeup and publish consumption once per batch.
- [How to place threads so the handoff stays fast](pin-threads-for-placement.md)
  — placement moves handoff throughput more than most code changes.

Winding down:

- [How to shut down a pipeline cleanly](shut-down-a-pipeline.md) —
  drain-then-stop, cancel from the consumer side, and fan-in shutdown
  ordering.
