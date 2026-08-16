# Errors and disconnect semantics

All error types live at the crate root (`src/lib.rs`). Per-item rustdoc:
`cargo doc --open`.

## Error types

| Type | Returned by | Meaning | Value handed back |
|---|---|---|---|
| `TrySendError::Full(T)` | `try_send` | ring is full | yes, in the variant |
| `TrySendError::Disconnected(T)` | `try_send` | receiver was dropped | yes, in the variant |
| `SendError(T)` | `send` | receiver was dropped | yes, in field `.0` |
| `TryRecvError::Empty` | `try_recv` | ring is empty; senders still live | — |
| `TryRecvError::Disconnected` | `try_recv` | all senders dropped and the ring is drained | — |
| `RecvError` | `recv` | all senders dropped and the ring is drained | — |

## Disconnect matrix

| Event | `send` / `try_send` | `recv` / `try_recv` |
|---|---|---|
| Receiver dropped | `SendError(v)` / `Disconnected(v)`, including for a producer currently blocked in `send` | — |
| All senders dropped (`mpsc`: the last clone; `sharded`: every shard's sender) | — | remaining buffered values are still delivered; `Disconnected` / `RecvError` only after the ring is drained |

- A thread parked in `send` or `recv` under `WaitStrategy::Park` is woken by
  the counterpart's drop; no parked thread sleeps through a close
  (`docs/design.md` §3, §5).
- `drain(max, f)` returns `0` both when the ring is empty and after
  disconnect; pair it with `try_recv` to distinguish the two.
- `mpsc` disconnect is refcounted: cloning a `Sender` increments the live
  producer count, dropping decrements it, and the disconnected state is
  reached when the count hits zero.

## `sharded`: both error terms are per-shard

`sharded` composes one independent ring per producer, so two of the terms
above bind to a shard rather than to the channel.

| Term | On `spsc` / `mpsc` | On `sharded` |
|---|---|---|
| `TrySendError::Full(v)` | the channel holds `cap` items | **this sender's shard** holds `total_cap / n_shards` items; other shards may be empty |
| `TryRecvError::Disconnected` | all senders dropped and the ring is drained | **every** shard is both sender-dropped and drained |

- A `sharded::Sender` disconnects only its own shard when dropped. There is
  no producer refcount: `sharded::Sender` is not `Clone`, and the shard set
  is fixed at construction.
- While any one shard still has a live sender, `try_recv` returns `Empty`,
  never `Disconnected` — including when every other shard is dropped and
  drained.
- Dropping the `Receiver` disconnects every shard: each sender's next
  `try_send` returns `Disconnected(v)`, and a sender blocked in `send`
  returns `SendError(v)`.
- Both `Empty` and `Disconnected` cost a full `n_shards` scan, against one
  atomic load for `mpsc` (`src/sharded.rs`).
- Values left in the shards when both sides are dropped are dropped exactly
  once, as for the other flavors. Exercised by
  `tests/close_semantics.rs` and `tests/sharded_stress.rs`; the sweep's
  disconnect accounting is loom-modeled in
  `tests/loom.rs::sharded_composition`.

## Values left in the ring

When both sides' handles are dropped, values that were sent but never
consumed are dropped by the ring itself, each exactly once — no leak, no
double-drop. Exercised by the drop-accounting stress tests
(`tests/spsc_stress.rs`, `tests/mpsc_stress.rs`, `tests/sharded_stress.rs`)
and argued in `docs/design.md` §6.
