# MPSC colocated slot: measurement

**Date:** 2026-08-09
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; box verified quiet by `vmstat`
(80-92% idle, 0 runnable throughout), built to completion before each measurement block
**Spec:** `docs/superpowers/specs/2026-08-09-colocated-slot-design.md`

## What changed

`buf` (the payload array) and `avail` (the per-slot availability-round array) became one
`slots: Box<[Slot<T>]>`, so a publish writes the payload and its round into one cache line
instead of two. Layout only — orderings, claim protocol and API unchanged. Landed as
`cf74e97`.

## Method

Interleaved A-B-A blocks against `mpsc_layout_probe`'s three configurations
(`cap1024_p2`, `cap4096_p2`, `cap1024_p4`), three runs per block: colocated (A1) →
baseline (B) → colocated (A2). `src/mpsc.rs`'s Task-1 change is committed (`cf74e97`), not
uncommitted, so `git stash` had nothing to stash for it (`git stash push -- src/mpsc.rs`
reported "No local changes to save"). Block B was produced instead by writing the
pre-Task-1 content of `src/mpsc.rs` directly into the working tree
(`git show cf74e97~1:src/mpsc.rs > src/mpsc.rs`, verified with
`grep -c "struct Slot<T>" src/mpsc.rs` printing `0`), rebuilding, measuring, then restoring
the colocated file with `git checkout -- src/mpsc.rs` (verified printing `1`) and
rebuilding again for A2. No other file was touched at any point; `benches/throughput.rs`
was committed and identical across all nine runs. The tree was clean (`git status`) both
before Block B's substitution and after A2's restoration.

Box quietness (`vmstat 2 3`, checked before each block, never overlapping a build):

| Before | idle % | runnable |
|---|---:|---:|
| build (initial) | 91-92 | 0 |
| A1 | 88-90 | 0 |
| B | 85-87 | 0 |
| A2 | 80-85 | 0 |

The A2 window ran a few points lower (80-85% idle) than A1/B, from other unrelated
background sessions on this shared box (`ps aux` showed several other `claude` processes,
not a build of this crate overlapping the run) — `r` (runnable) stayed at 0 throughout, and
the A1-vs-A2 stability check below confirms this did not distort the comparison.

## Raw per-cell numbers (Melem/s)

| Block | run | cap1024_p2 | cap4096_p2 | cap1024_p4 |
|---|---:|---:|---:|---:|
| A1 (colocated) | 1 | 34.94 | 36.69 | 27.66 |
| A1 (colocated) | 2 | 35.74 | 36.85 | 25.61 |
| A1 (colocated) | 3 | 35.66 | 37.48 | 25.82 |
| B (baseline) | 1 | 30.41 | 31.99 | 24.53 |
| B (baseline) | 2 | 30.27 | 32.31 | 24.69 |
| B (baseline) | 3 | 30.40 | 31.59 | 23.06 |
| A2 (colocated) | 1 | 34.53 | 36.80 | 28.06 |
| A2 (colocated) | 2 | 34.74 | 35.73 | 27.35 |
| A2 (colocated) | 3 | 34.70 | 36.21 | 27.31 |

## A1-vs-A2 stability check

| Cell | A1 mean | A2 mean | A1→A2 change | A1 spread | A2 spread | Verdict |
|---|---:|---:|---:|---:|---:|---|
| cap1024_p2 | 35.447 | 34.657 | -2.23% | 2.26% | 0.61% | within spread — stable |
| cap4096_p2 | 37.007 | 36.247 | -2.05% | 2.13% | 2.95% | within spread — stable |
| cap1024_p4 | 26.363 | 27.573 | +4.59% | 7.78% | 2.72% | within spread — stable |

In every cell the A1-to-A2 change is smaller than at least one of the two blocks' own
run-to-run spread, so no monotonic drift is masquerading as (or masking) an effect. The box
was stable enough for the comparison to be valid.

## Results

Mean of six colocated runs (A1+A2) against mean of three baseline runs (B), cell spread as
max−min over the six colocated runs as a percentage of their mean:

| Cell | baseline (mean) | colocated (mean) | delta | cell spread |
|---|---:|---:|---:|---:|
| cap1024_p2 | 30.36 | 35.05 | +15.45% | 3.45% |
| cap4096_p2 | 31.96 | 36.63 | +14.59% | 4.78% |
| cap1024_p4 | 24.09 | 26.97 | +11.93% | 9.08% |

## Gate arithmetic

Gate: keep only if colocation improves all three cells by more than that cell's own
run-to-run spread.

- cap1024_p2: +15.45% delta vs. 3.45% spread — **passes**, delta is ~4.5x the spread.
- cap4096_p2: +14.59% delta vs. 4.78% spread — **passes**, delta is ~3x the spread.
- cap1024_p4: +11.93% delta vs. 9.08% spread — **passes**, but this is the tightest
  margin of the three: cap1024_p4 also has the noisiest block-level spread (up to
  ~10% within a single block, consistent with the 4-producer contention shape noted in
  `docs/bench-results/2026-08-09-mpsc-perf-v2.md`). Even taking the more conservative
  baseline spread (6.77%) or the larger of baseline/colocated spread (9.08%) as the
  noise floor, the 11.93% delta still clears it.

Every baseline run in every cell is lower than every colocated run in that same cell — the
two distributions do not overlap at all, unlike the padding round's single-cell +3.5% that
was inside noise once a second configuration was tried. All three cells pass by a wide,
unambiguous margin.

## Verdict

**KEPT.** All three `mpsc_layout_probe` configurations improved by more than their own
run-to-run spread: cap1024_p2 +15.45%, cap4096_p2 +14.59%, cap1024_p4 +11.93%. This is the
first of the three MPSC hot-path hypotheses tried on this branch (division removal, avail
padding, colocation) that clears the all-three-cells gate.

## What this shows about where the MPSC cost is

The cache-line hypothesis from the hot-path analysis is supported, and by a large margin:
merging the payload and its availability round into one `Slot<T>` — so a publish or
consume touches one cache line instead of two — buys roughly 12-15% more throughput across
capacity (1024 vs 4096) and producer count (2 vs 4). This is a materially different result
from both prior levers on this same gap:

- Removing the runtime division (`seq / cap` → `seq >> shift`, v2) produced no measurable
  change — the ALU cost was never the bottleneck.
- Padding the avail array to one cache line per entry produced +3.5% in one cell and then
  -0.1% at a second capacity — a real effect at one point that did not generalize, because
  padding traded false sharing for lost cache residency in the wrong direction as capacity
  grew.

Colocation avoids that residency trade because it does not add memory — it removes a
*second* array's cache lines from the hot path rather than spreading one array's entries
across more lines. The result is consistent, same-direction improvement at every
capacity/producer-count combination tested, which is exactly the shape the residency
argument in `design.md` §8 predicts should hold when a layout change reduces total cache
traffic instead of trading one cost for another. This does not resolve the MPSC-vs-
crossbeam bake-off gap on its own (the CAS-retry claim cost from §7 is untouched), but it
is now the first of the layout/arithmetic hypotheses on this path to measure as a real,
reproducible win.
