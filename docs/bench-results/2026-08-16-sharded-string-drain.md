# Sharded MPSC: the String cell, and what a sharded `drain` is worth

**Date:** 2026-08-16 (same day as `2026-08-16-sharded-ladder-skew.md`, but a
**different rig instance** — same c7i.8xlarge type, Xeon Platinum 8488C, THP
off; absolutes should not be compared across the two hosts, ratios only).
**Method:** two filtered sweeps on one host, 3 rounds each at `full` (16
physical cores) and `smt2x2` (4 CPUs on 2 cores, the dev-VM shape). Sweep 1:
`bakeoff_sharded_string` plus all of `bakeoff_mpsc_string` (crossbeam,
thingbuf both ways) as same-session comparators. Sweep 2, after landing
`sharded::Receiver::drain`: both cells of `bakeoff_sharded_mpsc` — per-item
`try_recv` anchor and the new `drain` consumer. Values are medians of the 3
round-mids. Raw output in `raw/2026-08-16-sharded-string-drain/`.

## 1. String payload: the sharded lead survives, compressed

The cell `2026-08-15-bakeoff-rig.md` flagged as missing. 64-byte `String`
per element (one allocation + one free per element on the move-based crates),
2 producers, 1024 total slots — harness identical to `bakeoff_mpsc_string`.

| cell | `full` (Melem/s) | `smt2x2` (Melem/s) |
|---|---:|---:|
| **sharded** | **10.34** (10.17–10.95) | **9.98** (9.69–10.34) |
| mpsc (ultima) | 7.44 (7.09–7.86) | 8.62 (7.92–8.65) |
| crossbeam | 4.66 (4.36–5.27) | 4.63 (3.75–4.75) |
| thingbuf (value) | 3.82 (3.63–3.85) | 3.99 (3.81–4.64) |
| thingbuf (ref) | 3.40 (3.19–3.47) | 3.78 (3.63–4.36) |

sharded / mpsc: **1.39x at `full`, 1.16x at `smt2x2`** — against 6.0x on the
u64 cells. The compression is expected and diagnostic: with an allocation and
a free per element, the allocator, not the channel, is most of the per-item
cost, so deleting the claim CAS moves a smaller slice. What the cell settles
is direction: sharded stays the fastest MPSC in the group on a payload with a
destructor, at both placements, and this crate's own `mpsc` remains second
(1.6–1.9x crossbeam here, consistent with the standing MPSC-String result).

## 2. `drain`: the consumer bound moves by 6x

The ladder/skew session put every sharded `full` cell in a 91–124 Melem/s
band — the single consumer's per-item `try_recv` (sweep bookkeeping + one
`Release` head store per item) was the bound everywhere. The new
`Receiver::drain` visits each shard at most once per call and publishes each
shard's head once per batch. Same harness, u64, 2 producers, 1024 total:

| consumer path | `full` (Melem/s) | `smt2x2` (Melem/s) |
|---|---:|---:|
| per-item `try_recv` | 127.1 (123.1–138.6) | 209.2 (192.9–213.5) |
| **`drain`** | **775.7** (757.3–828.5) | **684.4** (633.9–701.9) |
| ratio | **6.10x** | **3.27x** |

Two readings:

- **The consumer-bound hypothesis is confirmed.** Batching the head
  publication and looping inside one shard's ring takes the same producers
  from 127 to 776 Melem/s at 16 cores. The producers were never the limit.
- **This inverts the spsc finding.** On a saturated spsc pipeline, `drain`
  measured ~10% *slower* than `try_recv` (`2026-08-14-bakeoff-v4.md`), and
  the how-to warns against reaching for it as a speedup. For sharded the
  advice flips: the per-item path pays sweep bookkeeping per element, and
  batching amortizes it. A saturated sharded consumer should drain.

For scale: 776 Melem/s of u64 is ~6.2 GB/s through one consumer thread, and
sits far above every per-item cell measured on either rig host.

## Limits

- **`drain`'s figure is the friendliest case**: u64 payload, empty closure,
  producers saturating both shards, so calls run long inside one ring. A
  skewed or trickling workload pays the same per-call sweep as `try_recv`
  with none of the batch to amortize it. No String or skewed drain cell was
  measured this session.
- **The String comparators inherit the standing thingbuf caveat** — three
  prior configurations produced three different ultima-mpsc-vs-thingbuf_ref
  answers on String; this session's 2.19x (`full`) and 2.28x (`smt2x2`) are
  one more configuration, not a settled ratio.
- **Same-day, different host** than the ladder/skew doc: the per-item anchor
  read 123.5 there and 127.1 here (±3%), which is the cross-instance drift to
  keep in mind before quoting any absolute.
- 3 rounds per point, not 5; ranges are quoted beside every median.
