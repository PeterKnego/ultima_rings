# Sharded MPSC: the shard ladder and the skewed-load probe

**Date:** 2026-08-16
**Host:** AWS c7i.8xlarge — Xeon Platinum 8488C, 16 physical cores, THP off
(`raw/2026-08-16-sharded-ladder-skew/host.txt`). Rig: `bench-infra/`. The dev
VM was saturated by an unrelated job all session, so no VM point was measured.
**Method:** 3 rounds interleaved at two topology points — `full` (16 physical
cores) and `smt2x2` (4 CPUs on 2 physical cores, the dev VM's exact shape).
Groups: `sharded_shard_ladder`, `sharded_skew`, `mpsc_producer_ladder`
(same-session comparator), and `bakeoff_sharded_mpsc` as the anchor against
`2026-08-15-bakeoff-rig.md`. Built to completion before measuring. Raw output
in `raw/2026-08-16-sharded-ladder-skew/`.

**Why:** every sharded figure so far — 321.5 Melem/s on the VM (2026-08-07),
5.71–6.20x mpsc on the rig (2026-08-15) — rested on exactly 2 shards under
balanced load. The two costs that could erode it grow with shard count: the
consumer's O(n) empty sweep, and per-shard buffers shrinking at a fixed total.
This session measures both. Values are medians of the 3 round-mids, ranges
alongside.

## Anchor: the 2-shard cell replicates yesterday's result

`bakeoff_sharded_mpsc` at `full`: 123.5 Melem/s (118.4–128.6); the ladder's
p2 rung reads 124.4 (123.7–134.1) — same cell, same answer. sharded/mpsc at
p2 is 6.00x at `full` and 6.04x at `smt2x2` against yesterday's 6.20x and
5.71x. Replicated within round noise.

## 1. The shard ladder: sharded holds while mpsc collapses

Balanced load, fixed 1024 total slots in both ladders, same producer counts.

`full` (16 physical cores):

| producers | sharded (Melem/s) | mpsc (Melem/s) | ratio |
|---:|---:|---:|---:|
| 2 | 124.4 (123.7–134.1) | 20.7 (20.7–20.9) | **6.0x** |
| 4 | 123.7 (118.4–127.7) | 15.9 (15.6–18.0) | **7.8x** |
| 8 | 120.6 (115.4–125.2) | 8.7 (8.3–12.9) | **13.8x** |
| 16 | 118.2 (110.6–119.0) | 2.4 (2.3–7.2) ± | **~16–49x** ± |
| 32 | 111.2 (108.1–113.4) | 1.8 (1.6–2.6) ± | **~44–60x** ± |
| 64 | 91.0 (89.1–91.7) | 2.3 (1.3–2.8) ± | **~32–40x** ± |

± the mpsc p16–p64 cells carry up to 3x cross-round spread (p16 read 2.31,
2.41, and 7.18 across the three rounds), so those rows support an order of
magnitude, not a point ratio. The conservative bound quoted first is against
mpsc's **best** round.

From 2 to 64 shards, sharded loses 27% (124.4 → 91.0). Over the same range
mpsc loses ~89% (20.7 → ~2.3): every added producer feeds the shared claim
CAS, and past the core count the collapse is total. Sharded has no shared
cursor, so there is nothing to collapse — its slope is the consumer's O(n)
sweep plus oversubscription, and at 64 producers on 16 cores (4x
oversubscribed, all spinning) it still moves 91 Melem/s.

`smt2x2` (2 physical cores — the dev-VM shape):

| producers | sharded (Melem/s) | mpsc (Melem/s) | ratio |
|---:|---:|---:|---:|
| 2 | 210.8 (188.3–219.8) | 34.9 (34.8–34.9) | **6.0x** |
| 4 | 106.0 (93.9–120.6) | 30.8 (29.9–31.0) | **3.4x** |
| 8 | 59.7 (56.3–66.0) | 22.8 (22.7–23.1) | **2.6x** |
| 16 | 54.9 (52.6–60.6) | 24.6 (24.3–25.3) | **2.2x** |
| 32 | 46.2 (44.6–47.0) | 21.7 (21.0–22.0) | **2.1x** |
| 64 | 27.0 (26.4–28.6) | 17.4 (16.2–19.2) | **1.6x** |

On 2 physical cores the picture inverts in shape but not in direction: mpsc
is nearly flat (34.9 → 17.4, bounded by how few threads can run at once)
while sharded pays for oversubscribing spinners that never yield. The lead
narrows from 6.0x to 1.6x and never inverts. The crate's stated target —
pinned producers, cores to run them on — is the left side of the `full`
table, not this one.

## 2. Skewed load: 15 idle shards cost 11–14%

One hot producer sends the whole batch; the other n−1 senders stay alive and
idle, so every `VISIT_BUDGET` (32) items the consumer sweeps n−1 empty rings.

`cap512each` (hot shard fixed at 512 slots — the sweep cost in isolation):

| n_shards | `full` (Melem/s) | `smt2x2` (Melem/s) |
|---:|---:|---:|
| 1 (baseline) | 124.7 (114.4–132.9) | 134.6 (126.2–148.7) |
| 2 | 131.1 (120.8–149.6) | 135.9 (124.6–143.0) |
| 4 | 121.5 (117.2–146.0) | 130.1 (120.3–139.0) |
| 8 | 120.7 (111.4–138.2) | 124.6 (111.1–135.2) |
| 16 | 106.9 (91.7–132.7) | 120.2 (108.1–130.0) |

n=1 → n=16 is −14% at `full` and −11% at `smt2x2`. The visit budget does its
job: 32 items amortize the sweep well enough that 15 permanently-empty rings
cost about a seventh of throughput, not a multiple of it. The cross-round
ranges are wide (rounds drift together by up to ±15%, a session effect, not a
cell effect — round 3 at `full` ran globally high), which is why the medians
are the quoted quantity.

`cap1024total` (fixed budget, hot shard shrinks to 1024/n):

| n_shards | hot-shard slots | `full` (Melem/s) | `smt2x2` (Melem/s) |
|---:|---:|---:|---:|
| 2 | 512 | 130.1 (112.1–134.4) | 134.8 (124.5–142.5) |
| 4 | 256 | 123.9 (101.2–155.5) | 129.0 (119.1–137.1) |
| 8 | 128 | 121.4 (108.1–135.2) | 124.0 (111.4–131.5) |
| 16 | 64 | 108.2 (104.2–137.0) | 119.1 (106.9–128.2) |

At every n the fixed-budget cell matches the fixed-per-shard cell within the
round spread (n=16 at `full`: 108.2 vs 106.9). Shrinking the hot shard 8x,
from 512 slots to 64, costs nothing measurable **while the consumer keeps
up** — the ring never fills deep, so its depth doesn't matter. The per-shard
backpressure contract (`Full` at total/n) remains a real semantic footgun for
bursty or consumer-stalled workloads, but it is not a steady-state throughput
cliff.

## 3. What this settles for graduation

The two open performance questions from the graduation review are answered:

- **Scaling holds.** The 2-shard result was not a small-n artifact: 8–16
  shards on 16 cores keep 95–97% of the p2 figure, and the design's advantage
  over the shared-claim mpsc *widens* with producer count rather than
  narrowing.
- **Skew is mild.** The O(n) sweep is an 11–14% tax at n=16 with a single hot
  producer — the worst case for the sticky cursor — not a structural penalty.

One number worth keeping for the wait-strategy work: sharded throughput at
`full` sits in a 91–124 band across every balanced and skewed cell, about the
single-consumer drain rate through `spsc::try_recv` on this box — consistent
with the consumer, not the producers, being the bound everywhere. That is the
argument for a sharded-layer `drain` next.

## Limits

- **No VM point this session** — both topology points come from one rig host,
  same session. `smt2x2` stands in for the VM's shape, and its p2/anchor
  cells replicate yesterday's rig numbers, but no fresh dev-box figure exists.
- **Thread spawn is inside the measured region** and grows with producer
  count, identically in both ladders. Compare across ladders at one producer
  count; do not compare rungs within a ladder to each other for absolutes.
- **mpsc p16–p64 at `full` support direction only** (up to 3x cross-round
  spread); the sharded cells at the same points hold 1–8% spread.
- **The skew harness never stalls the consumer**, so it cannot see the
  per-shard backpressure footgun's semantic cost — only its throughput cost.
- **Absolutes do not transfer across machines**; ratios and slopes are the
  finding, per `2026-08-15-bakeoff-rig.md`.
