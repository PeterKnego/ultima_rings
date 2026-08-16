# Changelog

## 0.2.0 — 2026-08-16

### `sharded` is now a first-class channel flavor

`sharded` graduated from a feature-gated prototype to a stable module. The
**`experimental-sharded` feature is gone** — the module is always available.
Callers who enabled the feature should drop it from their `Cargo.toml`; no
code changes are needed.

**Its stable contract is the fixed producer set.** `sharded::Sender` is not
`Clone` and the shard count is fixed at construction. One writer per ring is
the entire source of the design's speed, so dynamic producers are out of
scope by design rather than pending work — code that needs `Sender: Clone`,
one global FIFO order, or a single channel-wide capacity bound wants `mpsc`,
which is unchanged and remains the general-purpose MPSC.

Added in this release:

- `sharded::Sender::send` — blocking send, bounded by **this shard's**
  capacity (`total_cap / n_shards`), not the channel's total.
- `sharded::Receiver::recv` — blocking receive; sweeps the shards and waits
  self-waking between sweeps.
- `sharded::Receiver::drain(max, f)` — batched consume. Visits each shard at
  most once per call and publishes each shard's head once per batch. Worth
  6.10x the per-item path on a saturated pipeline, which *inverts* the spsc
  guidance to avoid `drain` for throughput.
- `WaitStrategy::Backoff` and `WaitStrategy::BackoffYield` are accepted.
  `WaitStrategy::Park` still panics at construction: there is no cross-shard
  parker, and adding one would put a fence plus a wake on every send.

Evidence behind the graduation, all on a 16-core Xeon rig:

- Scaling holds past 2 shards — 8 to 16 shards keep 95–97% of the 2-shard
  figure, while `mpsc` loses ~89% over the same range
  (`docs/bench-results/2026-08-16-sharded-ladder-skew.md`).
- Skew is mild — 15 idle shards with one hot producer cost 11–14%.
- The `String`-payload cell holds direction: fastest MPSC in the group at
  both placements (`docs/bench-results/2026-08-16-sharded-string-drain.md`).
- The composition is loom-modeled (`tests/loom.rs::sharded_composition`), and
  the crossbeam disconnect/drop corner cases are ported to it.

## 0.1.0

Initial release: `spsc` and `mpsc` bounded lock-free rings with four
selectable wait strategies.
