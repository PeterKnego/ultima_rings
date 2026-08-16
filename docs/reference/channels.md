# Channel types

Cross-cutting facts about the crate's channel flavors. The canonical per-item
API reference is the rustdoc: `cargo doc --open`.

## The three flavors

| | `spsc` | `mpsc` | `sharded` (experimental) |
|---|---|---|---|
| Constructor | `spsc::channel(cap, strategy)` | `mpsc::channel(cap, strategy)` | `sharded::channel(n_shards, total_cap, strategy)` |
| Producers | exactly 1 | N (`Sender: Clone`) | exactly `n_shards`, fixed at construction (`Sender` is not `Clone`) |
| Consumers | exactly 1 | exactly 1 | exactly 1 |
| Delivery order | FIFO | claim order across all producers (global FIFO) | per-producer FIFO only; cross-producer order is scan-position dependent |
| Backpressure bound | `cap` items total | `cap` items total | `total_cap / n_shards` items **per producer** |
| Blocking `send` / `recv` | yes | yes | no — `try_send` / `try_recv` only |
| `drain` | yes | yes | no |
| Wait strategies | all four | all four | `BusySpin` only (any other panics) |
| Availability | always | always | feature `experimental-sharded` |

`sharded` is a prototype: `BusySpin`-only, not a stable API
(see the feature-flag note in `Cargo.toml`).

## Capacity rules

- `cap` must be a positive power of two. Violation panics at construction:
  `"capacity must be a positive power of two"`.
- `sharded`: `n_shards` must be positive, `total_cap` must divide evenly by
  `n_shards`, and the per-shard capacity (`total_cap / n_shards`) must itself
  be a positive power of two.
- The full buffer is allocated at construction. Send and receive paths do not
  allocate.

## Type bounds and handle rules

- The payload type must be `Send`. There is no `'static` requirement beyond
  what `Send` and the handles' own lifetimes impose.
- All handles are `Send`. Send-side and receive-side methods take `&mut self`;
  a handle is used from one thread at a time.
- `mpsc::Sender::clone` registers a new producer; the channel disconnects
  when the last clone drops. `spsc::Sender` and `sharded::Sender` are not
  `Clone`.

## Values left in the ring

Values that are still buffered when both sides' handles have been dropped are
dropped in place by the ring, each exactly once. See
[errors-and-disconnect.md](errors-and-disconnect.md) for the full disconnect
semantics.
