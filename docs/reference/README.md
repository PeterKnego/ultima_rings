# Reference

Dry, factual description of the machinery. The canonical per-item API
reference is the rustdoc — run `cargo doc --open`. The pages here carry
only what no single API item can: cross-cutting tables and semantics.

- [Channel types](channels.md) — the `spsc` / `mpsc` / `sharded` matrix:
  producers, ordering guarantees, capacity rules, handle rules.
- [Wait strategies](wait-strategies.md) — the four variants with measured
  idle-CPU and wake-latency figures, which flavors accept which, ladder
  constants, and provenance.
- [Errors and disconnect semantics](errors-and-disconnect.md) — every error
  type, the disconnect matrix, `sharded`'s per-shard terms, and what happens
  to values left in the ring.

Measured performance lives in two places: the curated tables with their
conditions are in the [README's "Measured numbers"](../../README.md#measured-numbers)
section, and the dated experiment log behind them is
[`docs/bench-results/`](../bench-results/).
