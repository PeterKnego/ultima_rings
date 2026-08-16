# Diátaxis documentation plan — ultima_rings

Runs: 2026-08-16 (first run, full set) and 2026-08-16 (update run —
reconciled the set with v0.2.0, in which `sharded` graduated from a gated
prototype to a stable flavor; added the fan-in how-to). Both compass passes
found every page correctly classified; no page has ever needed a move or a
split. This file is the durable record for future runs.

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
- **Fan-in from a fixed producer set (2026-08-16 update run):** confirmed
  as a real user goal in its own right, warranting its own how-to rather
  than a pointer from the topology guide.
- **Tutorials stay minimal (2026-08-16 update run):** the tutorial teaches
  one path to one working result. `sharded` is deliberately absent from it
  and stays discoverable through how-to and reference.
- **Publication status (2026-08-16):** the crate is still unpublished and
  the repo private. The tutorial's dependency line is therefore aspirational
  by design; bump its version with releases, but the verification exception
  below stands until publication.

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
  `sharded`'s per-shard `Full`/`Disconnected` terms, drop-drain —
  `src/lib.rs`, `src/sharded.rs`, `tests/close_semantics.rs`,
  `tests/loom.rs::sharded_composition`, design.md §3/§5/§6

How-to (`docs/how-to/`): `choose-a-topology-and-wait-strategy.md`,
`fan-in-from-a-fixed-producer-set.md`, `handle-backpressure.md`,
`shut-down-a-pipeline.md`, `batch-consume-with-drain.md`,
`pin-threads-for-placement.md` — the six confirmed goals; sources: public
API, close-semantics and sharded tests, `2026-08-14-backoff-cells.md`,
`2026-08-14-bakeoff-v4.md`, `2026-08-15-thread-placement.md`,
`2026-08-16-sharded-ladder-skew.md`.

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
- **A dedicated explanation page for the sharded contract** ("why the
  producer set is fixed") — the rationale lives in `docs/design.md` §9,
  which the standing decisions keep as the explanation cornerstone, and it
  was rewritten this week to record the graduation. A second page would
  duplicate it. Remedy: if §9 grows unwieldy, split the sharded rationale
  out to `docs/explanation/why-a-fixed-producer-set.md` and leave a pointer.
- **`sharded` content in the tutorial (a section, or a second tutorial)** —
  user decision, 2026-08-16: a tutorial teaches one path to one working
  result, and a third flavor competes with that lesson. Remedy: none
  planned; revisit only if `sharded` becomes the flavor most new users
  reach for first, in which case write a separate tutorial rather than
  extending `your-first-pipeline.md`.
- **Architecture explanation page (C4 System Context / Container)** —
  library crate: single compilation unit, no deployable units, no external
  systems; both levels would depict one box. Remedy: if the crate is
  embedded in a published multi-component system (the ultima SMR project),
  document that system's context there, or add a Context diagram here once
  a real deployment exists to depict.

## Tutorial verification status

`your-first-pipeline.md`: **verified 2026-08-16, re-verified after v0.2.0**
— all three programs executed in a sandbox project against the local
checkout (first run at `c3dfa25`; re-run at `e5fcb95`). Published outputs
are the captured outputs and were unchanged by v0.2.0, which touched no
`spsc` API the tutorial uses.

The fan-in how-to's code was likewise executed against `e5fcb95` (sandbox
project, `sharded::channel` with four producers) rather than only
type-checked.
**Exception, still unverified (reconfirmed 2026-08-16):** the dependency
step cannot work for a reader — the crate is not on crates.io and the
GitHub repo is private. The line tracks the crate version (now `"0.2"`)
but remains aspirational. Discharge by publishing the crate (or making the
repo public and switching to a git dependency), then re-running step 1.

## Standing notes

- The ~27 ns spin-iteration granularity quoted in `src/wait.rs` doc
  comments has **no dated artifact** in `docs/bench-results/`; it was left
  out of `docs/reference/wait-strategies.md`. Remedy: measure and record it
  in a dated bench-results doc, then add it to the reference table.
