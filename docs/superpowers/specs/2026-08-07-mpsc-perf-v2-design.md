# ultima_rings v2 — MPSC perf round (shift + avail padding + Park bench)

**Date:** 2026-08-07
**Revised:** 2026-08-09 — purpose, gate, and the padding lever's status all
re-aimed after the sharded round. See "What changed on revision" below.
**Status:** Approved (revised scope)

## Purpose

Make `src/mpsc.rs` faster, because it is the crate's **global-FIFO** MPSC and
will remain the default for callers who need ordering.

This is a narrower purpose than the original. As first written, this spec aimed
to "close the v1 bake-off gap" — MPSC BusySpin at 29.9 Melem/s against
crossbeam-channel's 71.0. That gap has since been answered a different way:
`src/sharded.rs` reaches 321.5 Melem/s, 4.51× crossbeam
(`docs/bench-results/2026-08-07-sharded-mpsc.md`). But sharding surrenders
global FIFO, a global bound, and O(1) emptiness (design.md §9), so it does not
replace `mpsc` — it sits beside it. `mpsc` competes with crossbeam-channel,
which also provides global FIFO, and deserves to be fast on its own terms.

Batched claim (the Disruptor-grade lever) remains deferred: it is a protocol/API
change with a real correctness surface.

## What changed on revision

1. **Purpose**, as above: "close the gap" → "make the global-FIFO option fast".
2. **The gate was unreachable and is rewritten.** The original demanded MPSC
   BusySpin ≥2× crossbeam, i.e. ≥142 Melem/s, against a measured ~30. A shift
   and some padding cannot deliver 4.7×; the spec would have failed its own
   gate by construction. The new gate asks whether each lever pays for itself.
3. **The padding lever is now measure-then-decide, not a commitment.** It
   targets false sharing because design.md §7/§8 attribute the gap there — but
   that attribution is documented as *consistent with* the evidence rather than
   established by it (`docs/bench-results/2026-08-07-sharded-mpsc.md`, finding
   3). 64 KiB per channel is a real cost for an unisolated benefit, so it must
   earn its place on a measurement or be reverted.
4. **Stale references corrected**: the suite is 50 tests, not 32; the results
   file is dated on the day it is produced.

## Changes (API-unchanged; `src/mpsc.rs` + `benches/throughput.rs` only)

1. **Shift, not divide.** `Shared<T>` gains `shift: u32`, set in `channel()`
   to `cap.trailing_zeros()` with `debug_assert!(1usize << shift == cap)`
   (`assert_cap` already guarantees power-of-two). Every `seq / cap`
   availability-round computation — `try_send`'s publish store,
   `slot_published`'s expected-round check, `Shared::drop`'s prefix walk, and
   any blocking-path re-check — becomes `(seq >> sh.shift) as i64`. Slot
   indexing stays `& mask`. This removes a runtime hardware division from
   every publish and every consumer poll (design.md §7's documented v1 cost).

2. **Padded availability array — an experiment, kept only if it measures.**
   `avail: Box<[AtomicI64]>` becomes `Box<[CachePadded<AtomicI64>]>` (the
   existing 64-byte-aligned wrapper); access sites become `sh.avail[idx].0`.
   Today 8 round entries share each 64-byte line, so producers and the consumer
   ping-pong every line ~8× per ring wrap; padding gives each entry its own
   line. Atomics and orderings are unchanged — the loom/miri verification story
   is shape-identical.

   **This one is not a commitment.** Memory cost is `cap × 64 B` — 64 KiB at
   the default 1024, an 8× blow-up on that array — for a benefit that no
   measurement has isolated. Implement it, measure `mpsc/busy_spin_2_producers`
   with and without on the same quiet box, and **revert it unless the gain is
   larger than the run-to-run spread of that cell.** Whichever way it lands is
   recorded; a padding that does not pay is a finding, not a failure.

3. **Park-mode bench.** New criterion group `bakeoff_park_mpsc`: ultima_rings
   `WaitStrategy::Park` blocking `send`/`recv` vs crossbeam-channel's blocking
   `send`/`recv`, same 2-producer barrier-released harness and BATCH/cap as
   the existing MPSC bake-off. Fills the gate's unmeasured Park-parity cell.

## Verification (all lanes re-run — the levers touch verified hot-path code)

Full test suite (50 tests, both feature configurations), the 5-model loom lane
(`RUSTFLAGS="--cfg loom" cargo test --test loom --release`), miri
(`cargo +nightly miri test --all-features`), clippy
`--all-features --all-targets -D warnings`, fmt, and `cargo doc` with no
warnings. The tests, loom models, and miri lane require zero changes — both
levers are internal layout / arithmetic changes with identical orderings and
API.

## Measurement, docs, and the gate

- Measure on a box verified quiet by `vmstat` (85%+ idle, 0 runnable), not by
  load average, which lags badly on this machine
  (`docs/bench-results/2026-08-07-sharded-mpsc.md`, "Follow-up"). Build to
  completion before measuring.
- Record `docs/bench-results/<date>-mpsc-perf-v2.md`, with the pre-change
  figures measured **in the same session** rather than quoted from an earlier
  file. Prior sessions' absolute figures on this box are not comparable.
- Update design.md §7 (the division cost paragraph — resolved by the shift) and
  §8 (padding: whichever way the measurement lands).

**Gate — per lever, not one pass/fail:**

- **Shift:** keep unconditionally if correct. It removes a hardware division
  from two hot paths at zero semantic risk; it does not need to win a benchmark
  to be right. Record the delta whatever it is, including "no measurable
  change".
- **Padding:** keep only if the throughput gain exceeds the run-to-run spread
  of `mpsc/busy_spin_2_producers` on the same box. Otherwise revert the code
  and record that 64 KiB per channel bought nothing measurable — which is
  itself useful evidence about where the MPSC cost is *not*.
- **Park bench:** no gate; it is a measurement filling a documented gap. Record
  where `mpsc` Park lands against crossbeam's blocking path.

The batched-claim decision returns to the human regardless of outcome.

## Out of scope

Batched claim; any API or semantics change; SPSC changes (its hot path is
already division-free and wins the bake-off); aarch64 128-byte padding
(deferred with the existing `CachePadded` note); uc2 integration.
