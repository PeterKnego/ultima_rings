# ultima_rings v2 — MPSC perf round (shift + avail padding + Park bench)

**Date:** 2026-08-07
**Status:** Approved

## Purpose

Close (or honestly re-measure) the v1 bake-off gap: MPSC BusySpin throughput
was 29.9 Melem/s vs crossbeam-channel's 71.0 (~0.42×), while SPSC leads ~15×
(`docs/bench-results/2026-08-06-bakeoff.md`). The final v1 review identified
two semantics-preserving levers plus one measurement gap; this round takes
exactly those. Batched claim (the Disruptor-grade lever) is deliberately
deferred: it is a protocol/API change with a real correctness surface, and it
only earns that cost if this round's gap survives.

## Changes (API-unchanged; `src/mpsc.rs` + `benches/throughput.rs` only)

1. **Shift, not divide.** `Shared<T>` gains `shift: u32`, set in `channel()`
   to `cap.trailing_zeros()` with `debug_assert!(1usize << shift == cap)`
   (`assert_cap` already guarantees power-of-two). Every `seq / cap`
   availability-round computation — `try_send`'s publish store,
   `slot_published`'s expected-round check, `Shared::drop`'s prefix walk, and
   any blocking-path re-check — becomes `(seq >> sh.shift) as i64`. Slot
   indexing stays `& mask`. This removes a runtime hardware division from
   every publish and every consumer poll (design.md §7's documented v1 cost).

2. **Padded availability array.** `avail: Box<[AtomicI64]>` becomes
   `Box<[CachePadded<AtomicI64>]>` (the existing 64-byte-aligned wrapper);
   access sites become `sh.avail[idx].0`. Today 8 round entries share each
   64-byte line, so producers and the consumer ping-pong every line ~8× per
   ring wrap; padding gives each entry its own line. Memory cost `cap × 64 B`
   (64 KiB at the default 1024), documented in design.md §8. Atomics and
   orderings are unchanged — the loom/miri verification story is
   shape-identical.

3. **Park-mode bench.** New criterion group `bakeoff_park_mpsc`: ultima_rings
   `WaitStrategy::Park` blocking `send`/`recv` vs crossbeam-channel's blocking
   `send`/`recv`, same 2-producer barrier-released harness and BATCH/cap as
   the existing MPSC bake-off. Fills the gate's unmeasured Park-parity cell.

## Verification (all lanes re-run — the levers touch verified hot-path code)

Full test suite (32 tests), the 5-model loom lane
(`RUSTFLAGS="--cfg loom" cargo test --test loom --release`), miri
(`cargo +nightly miri test`), clippy `-D warnings`, fmt. The tests, loom
models, and miri lane require zero changes — both levers are internal layout /
arithmetic changes with identical orderings and API.

## Measurement, docs, and the gate

- Re-run the complete bake-off (regression groups + all competitor groups +
  the new Park group) on a quiet box.
- Record `docs/bench-results/2026-08-07-bakeoff-v2.md` (v1 file kept for
  history), with the v1 numbers alongside for the delta.
- Update README's numbers section and design.md §7 (division cost: resolved by
  shift — rewrite the "known v1 cost" paragraph) and §8 (add the padding
  memory trade).
- **Gate re-evaluation, recorded explicitly:** pass = MPSC BusySpin ≥2×
  crossbeam-channel throughput AND Park-mode within ~1.5× of crossbeam's
  blocking throughput. Otherwise: record the honest ratios; the batched-claim
  decision returns to the human.

## Out of scope

Batched claim; any API or semantics change; SPSC changes (its hot path is
already division-free and wins the bake-off); aarch64 128-byte padding
(deferred with the existing `CachePadded` note); uc2 integration.
