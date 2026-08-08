# Sharded MPSC prototype vs. shared-claim MPSC and crossbeam-channel

**Date:** 2026-08-07
**Hardware:** 4-core box, 15 GiB RAM, no swap; built to completion before measuring
**Feature:** `experimental-sharded`
**Spec:** `docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md`

## Methodology

2 producers, 1 consumer, `BusySpin`, barrier-released, 100k elements per batch,
**1024 total buffered slots in every cell** (sharded: 2 shards x 512;
`mpsc` and crossbeam: one 1024 ring), so no cell wins on buffer size.
All three cells re-measured in a single session.

Commands run, in order:

```
cargo bench --features experimental-sharded --no-run
uptime && free -h
cargo bench --features experimental-sharded -- "bakeoff_sharded_mpsc|bakeoff_mpsc/crossbeam|mpsc/busy_spin_2_producers"
```

Box state immediately before measuring: load average 0.18, 0.48, 0.42
(well under the 4.0 core count), 8.5 GiB available memory, no swap. The
`--no-run` build finished before any measurement began (it was already
up to date from Task 3, 0.04s), so no compilation overlapped the
measured run.

## Results

| Cell | Melem/s (mid) | Melem/s (range) | vs. crossbeam |
|---|---:|---|---:|
| sharded (2 shards x 512) | 321.52 | 317.52 – 324.63 | 6.23x |
| crossbeam-channel | 51.649 | 50.616 – 52.622 | 1.00x |
| mpsc (shared bounded-CAS claim) | 29.308 | 29.080 – 29.557 | 0.57x |

v1 reference (`docs/bench-results/2026-08-06-bakeoff.md`, different session):
mpsc 29.9, crossbeam 71.0.

This session's `mpsc` mid (29.308) lines up closely with the v1 reference
(29.9). This session's `crossbeam` mid (51.649) does not — it is
noticeably lower than the v1 reference (71.0), a ~27% difference between
two sessions on the same box. Criterion's own within-run analysis for
`bakeoff_mpsc/crossbeam` is tight (range spans ~4%, one high-severe
outlier out of 100 samples), so this is not run-to-run noise within the
session; it looks like session-to-session variance in crossbeam's
measured throughput specifically. This does not change the gate outcome
below — the sharded cell clears the decisive threshold by more than 2x
against either crossbeam figure (51.649 or 71.0) — but it is a reason to
treat any *absolute* crossbeam number from a single session with some
caution, and it is the reason the brief requires re-measuring crossbeam
in the same session as the cell being judged against it rather than
reusing the v1 figure.

## Verdict

**Decisive: sharded measured 321.52 Melem/s, ≥ 142 Melem/s (2x crossbeam).**

The sharded prototype's mid (321.52 Melem/s) clears the fixed 142 Melem/s
decisive threshold by more than 2x, and is 6.23x this session's own
crossbeam mid (51.649 Melem/s) — the relevant same-session comparison.
Both the fixed threshold and the same-session ratio land in the first
gate branch. This also corroborates Task 3's `--quick` smoke figure
(~320 Melem/s), though that number carried no evidentiary weight on its
own; this controlled run is what makes the result trustworthy, and it
comes out at essentially the same throughput.

Next round designs the dynamic shard registry and the production type.

Separately, the wide gap between `mpsc` (29.308, 0.57x crossbeam) and
`sharded` (321.52, 6.23x crossbeam) — a ~11x difference between two
designs that both use the same underlying SPSC ring primitive, differing
only in whether the claim is a shared CAS or a private per-producer
slot — is itself evidence for the crate's account in `docs/design.md`
§7 and §8 of where the v1 MPSC's cost lives (CAS contention and
availability-array false sharing under the shared claim). Removing both
by sharding recovers the large majority of the gap to crossbeam and then
some.

## What this does and does not show

The sharded cell provides **per-producer FIFO only** and per-shard
backpressure; `mpsc` and crossbeam both provide global FIFO and a global
bound. This is not a like-for-like replacement, and the number should not be
read as one.
