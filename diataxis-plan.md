# Diátaxis documentation plan — ultima_rings

Last run: 2026-08-16 (first run; full set written and committed). This file
is the durable record for future runs.

## Durable answers (2026-08-16)

- **Audience:** external crate users — Rust developers arriving via
  crates.io/GitHub with no ultima-project context. Internal lab material
  (bench-results log, superpowers records) stays linked but secondary.
- **README "Measured numbers":** stays in the README (user decision).
- **Reference scope (approved):** public API only; rustdoc is the canonical
  per-item reference; Markdown reference carries only cross-cutting pages.
  Internal modules (`notify`, `atomic`) stay code-only.
- **User goals confirmed:** pipeline handoff, topology/strategy choice,
  clean shutdown, backpressure, batch drain, thread placement.

## Standing decisions

- `docs/design.md` stays at that path — it is the explanation cornerstone
  and ~30 files link to it by path. Do not move it under
  `docs/explanation/`.
- `docs/bench-results/` and `docs/superpowers/` are internal records, kept
  in place, outside the user-facing set; user docs cite into them.
- `bench-infra/README.md` is internal tooling documentation; keep in place.

## Documents (title — need — source)

Reference (`docs/reference/`):
- `channels.md` — SPSC/MPSC/sharded semantics matrix, capacity rules,
  handle rules — `src/{lib,spsc,mpsc,sharded}.rs`, design.md §1/§9
- `wait-strategies.md` — per-strategy behavior with measured costs and
  provenance — `src/wait.rs`, `2026-08-12-cpu-cost-and-heap-payload.md`,
  `2026-08-09-wake-latency.md`, `2026-08-08-wait-strategies.md`
- `errors-and-disconnect.md` — error meanings, disconnect matrix,
  drop-drain — `src/lib.rs`, `tests/close_semantics.rs`, design.md §3/§5/§6

How-to (`docs/how-to/`): `choose-a-topology-and-wait-strategy.md`,
`handle-backpressure.md`, `shut-down-a-pipeline.md`,
`batch-consume-with-drain.md`, `pin-threads-for-placement.md` — the five
confirmed goals; sources: public API, close-semantics tests,
`2026-08-14-backoff-cells.md`, `2026-08-14-bakeoff-v4.md`,
`2026-08-15-thread-placement.md`.

Explanation (`docs/explanation/`): `reading-the-benchmarks.md` — why every
ratio carries machine/cores/placement — rig, placement, and bake-off docs.
Cornerstone: `docs/design.md` (kept in place).

Tutorial (`docs/tutorials/`): `your-first-pipeline.md` — first working
pipeline — README API snippet, `tests/spsc_blocking.rs`.

## Not created (reason → remedy)

- **`docs/reference/benchmarks.md`** — content lives in the README's
  "Measured numbers" by user decision (2026-08-16). Remedy: none needed
  unless that decision is revisited; the reference landing links the README
  section and `docs/bench-results/`.
- **Architecture explanation page (C4 System Context / Container)** —
  library crate: single compilation unit, no deployable units, no external
  systems; both levels would depict one box. Remedy: if the crate is
  embedded in a published multi-component system (the ultima SMR project),
  document that system's context there, or add a Context diagram here once
  a real deployment exists to depict.

## Tutorial verification status

`your-first-pipeline.md`: **verified 2026-08-16** — all three programs
executed in a sandbox project against the local checkout (crate source at
commit `c3dfa25`); published outputs are the captured outputs.
**Exception, still unverified:** the dependency step
(`ultima_rings = "0.1"`) cannot work yet — the crate is not on crates.io
and the GitHub repo is private. Discharge by publishing the crate (or
making the repo public and switching the tutorial to a git dependency),
then re-running step 1.

## Standing notes

- The ~27 ns spin-iteration granularity quoted in `src/wait.rs` doc
  comments has **no dated artifact** in `docs/bench-results/`; it was left
  out of `docs/reference/wait-strategies.md`. Remedy: measure and record it
  in a dated bench-results doc, then add it to the reference table.
