# Wait strategies

`WaitStrategy` is a closed enum of four variants, chosen once per channel at
construction. It applies to both blocked directions: consumer-on-empty and
producer-on-full. There is no trait to implement; callers cannot supply their
own strategy. Per-variant rustdoc: `cargo doc --open`.

`spsc::channel` and `mpsc::channel` accept all four variants.
`sharded::channel` accepts three: `BusySpin`, `Backoff`, and `BackoffYield`.
Passing `Park` to `sharded::channel` panics at construction with a message
naming the three accepted variants (`src/sharded.rs`). See
[channel types](channels.md) for the full per-flavor matrix.

## Behavior table

| Variant | Waiting side does | Idle CPU (one blocked side) | Wake granularity / latency | Cost to the productive side |
|---|---|---|---|---|
| `BusySpin` | `spin_loop()` until progress | 99.9% of a core | one spin iteration | none |
| `BackoffYield` | 10 spins, then `yield_now()` indefinitely; never parks; self-waking | 100.0% of a core | one `yield_now`, ~0.7 µs | none |
| `Backoff` | 10 spins, then 20 yields, then timed parks doubling 64 µs → 1 ms; self-waking | 10.2% of a core | OS-timer floored, ~60 µs | none |
| `Park` | parks until notified | 1.8% of a core | ~10.2 µs p50 publish-to-delivery | `SeqCst` fence plus a wake check on every operation |

Ladder constants (`src/wait.rs`): `SPINS = 10`, `YIELDS = 20`,
`PARK_MIN = 64 µs`, `PARK_MAX = 1 ms`.

## Fixed facts

- `BackoffYield` does not reduce idle CPU: with nothing else runnable,
  `yield_now` returns immediately. Its measured idle burn (100.0%) is not
  lower than `BusySpin`'s (99.9%).
- `thread::park_timeout` does not deliver sub-floor sleeps on the measured
  host: a 1 µs request measured ~60 µs. `PARK_MIN` is 64 µs (`src/wait.rs`).
- `BusySpin`, `BackoffYield`, and `Backoff` are self-waking: the other side
  never needs to notify. Only `Park` requires the productive side to
  participate in wakes.
- The self-waking three are exactly the variants `sharded::channel` accepts.
  A `sharded` consumer waits between whole-shard sweeps rather than on any
  one shard, and no shard notifies it (`src/sharded.rs`).

## Provenance

All figures above were measured on a 4-vCPU (2 physical cores, SMT) Linux
VM. Idle-CPU figures: `examples/cpu_cost.rs`, recorded in
[`docs/bench-results/2026-08-12-cpu-cost-and-heap-payload.md`](../bench-results/2026-08-12-cpu-cost-and-heap-payload.md).
Park wake latency: `examples/wake_latency.rs`, 10.19–10.40 µs p50 across
three runs, recorded in
[`docs/bench-results/2026-08-09-wake-latency.md`](../bench-results/2026-08-09-wake-latency.md).
Yield granularity (~0.7 µs) and the OS-timer floor comparison: recorded in
[`docs/bench-results/2026-08-12-cpu-cost-and-heap-payload.md`](../bench-results/2026-08-12-cpu-cost-and-heap-payload.md).
The ~60 µs `park_timeout` overshoot: measured in
[`docs/bench-results/2026-08-08-wait-strategies.md`](../bench-results/2026-08-08-wait-strategies.md).
