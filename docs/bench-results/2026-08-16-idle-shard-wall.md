# The idle-shard wall: where a dynamic producer set would stop paying

**Date:** 2026-08-16
**Host:** AWS c7i.8xlarge — Xeon Platinum 8488C, 16 physical cores, THP off.
Rig: `bench-infra/`. Raw output in `raw/2026-08-16-idle-shard-wall/`.
**Method:** `sharded_idle_wall`, 3 rounds at `full` (16 physical cores) and
`smt2x2` (4 CPUs on 2 cores). Values are medians of the 3 round-mids.

## The question

`sharded` fixes its producer set at construction, which keeps the consumer's
O(`n_shards`) sweep a designed constant. A dynamic-registration variant —
the most-requested extension, and the one that would also deliver
non-uniform capacity for free — makes that sweep O(peak registered
producers) instead, because a registered producer that is merely *idle*
still costs a probe on every sweep. `mpsc`'s empty check is O(1) and an idle
`mpsc` producer costs the consumer nothing.

So: **at what registered-producer count, and what idle fraction, would
dynamic `sharded` lose to `mpsc`?** That number decides whether the feature
is niche or the crate's future.

The spike answers it without implementing dynamic producers. A channel of
`n_total` shards where only `n_active` senders ever send, with the rest
alive and empty, is exactly the steady state of a dynamic channel holding
`n_total` registrations of which `n_active` are hot. Idle senders are held
in the harness thread, so thread count tracks `n_active`, not `n_total`.

Per-shard capacity is held at 64 while `n_total` grows, rather than holding
a fixed total and letting per-shard capacity collapse to 2 slots at n=512 —
that would confound sweep cost with producer stalls, and
`2026-08-16-sharded-ladder-skew.md` already showed shard depth does not move
throughput while the consumer keeps up. The variable under test is the
sweep, so the sweep is what varies.

## Result: there is no crossover in the measured range

`full` (16 physical cores), Melem/s:

| registered shards | 1 active | 4 active |
|---:|---:|---:|
| 4 | 111.2 | 114.0 |
| 64 | 83.3 | 106.2 |
| 128 | 67.1 | 101.7 |
| 256 | 51.7 | 82.5 |
| 512 | **27.6** | **60.2** |
| `mpsc` baseline | **22.2** | **15.2** |

`smt2x2` (2 physical cores), Melem/s:

| registered shards | 1 active | 4 active |
|---:|---:|---:|
| 4 | 115.4 | 116.7 |
| 64 | 101.3 | 90.4 |
| 128 | 73.9 | 77.1 |
| 256 | 51.3 | 58.8 |
| 512 | **28.8** | **38.2** |
| `mpsc` baseline | **21.8** | **28.8** |

At 512 registered producers with a single active one — 511 idle rings swept
for every 32-item run, the worst case the design can present — `sharded`
still delivers 27.6 Melem/s against `mpsc`'s 22.2. It is **1.24x ahead at
the far end of the sweep**, not behind. Every other cell is further ahead.

This contradicts the prediction that motivated the spike, which held that a
few hundred idle registrations would make dynamic `sharded` "dramatically
slower" than `mpsc`. The direction was right — the sweep does cost, and it
costs steeply past n=128 — but the magnitude was wrong by enough to change
the conclusion.

## The cost model checks out

Sweep cost per item, `full`, 1 active:

| shards | 4 | 64 | 128 | 256 | 512 |
|---|---:|---:|---:|---:|---:|
| ns/item | 8.99 | 12.01 | 14.89 | 19.33 | 36.21 |

That is 0.054 ns per idle shard per item (`full`) and 0.051 (`smt2x2`) —
two machines' placements agreeing closely. The predicted model was
`VISIT_BUDGET`-amortized: the sticky cursor resets its budget whenever a
sweep steps over an empty shard, so one full n-shard scan amortizes across
each 32-item run, giving n/32 probes per item. At 0.054 ns/shard that
implies ~1.7 ns per ring probe — the right order for a partially-cached
Acquire load pair. The mechanism is confirmed, which is what licenses the
extrapolation below.

## Extrapolated crossover

Linear fit of ns/item against shard count, solved against the `mpsc`
baseline. **These points are outside the measured range and are estimates,
not measurements:**

| configuration | `mpsc` cost | crossover (est.) |
|---|---:|---:|
| `full`, 1 active | 45.1 ns/item | **~680 registered** |
| `smt2x2`, 1 active | 45.8 ns/item | **~730 registered** |
| `smt2x2`, 4 active | 34.7 ns/item | ~760 registered |
| `full`, 4 active | 65.8 ns/item | ~3,700 registered |

Two independent topologies put the single-active-producer crossover at
**roughly 700 registered producers**, which is the number the spike was run
to find.

More active producers push the crossover further out, sharply so at 16
cores: `mpsc` gets *worse* with producer count (claim-CAS contention drops
it to 15.2 Melem/s at 4 producers) while `sharded` amortizes its sweep over
more delivered items. The regime that hurts `sharded` is specifically **many
registered, few active** — a server holding many mostly-idle connections,
which is exactly the workload a dynamic API would attract.

## What this does and does not settle

Settled: the steady-state sweep cost of holding many idle registrations. It
is real, it is ~0.05 ns per idle shard per item, and it does not overtake
`mpsc` until several hundred registrations with a single hot producer.

Not settled, and each needs an implementation rather than a simulation:

- **Registration cost** — publishing a new shard to a sweeping consumer.
- **Reaping** — dropped-producer shards must be removed or the sweep grows
  monotonically. This measurement says an unreaped channel tolerates
  hundreds of dead shards before it loses to `mpsc`, so reaping is a
  scaling requirement rather than a correctness emergency.
- **The refcount** — "no more producers will ever register" has no meaning
  in today's contract and would have to be added.
- **Latency**, not throughput. An item landing in a cold shard waits for the
  cursor to reach it; the p99 of that was not measured here.
- **Memory.** 512 shards x 64 slots x 8 B is 262 KB of ring per channel,
  and it scales with `size_of::<T>()`.

## Limits

- The extrapolated crossovers are fits, not data; the measured range stops
  at 512 shards.
- Active shards are the first `n_active` in cursor order. Scattering them
  would change per-item cursor distance but not the full-sweep cost that
  dominates here.
- `sharded_n64_a1` carries a 75.4–102.9 spread at `full` (the widest cell);
  the n>=128 rows, which carry the argument, hold within 6%.
- One machine, two placements, `BusySpin` only, `u64` payload.
