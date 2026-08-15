# Thread placement decides which crate wins

**Date:** 2026-08-15
**Hardware:** the 4-vCPU VM — 2 physical cores, CPUs 0/1 are SMT siblings of
core 0 and share L1d + L2; CPUs 0 and 2 are different physical cores and share
only L3
**Method:** `spsc_placement` group, 5 rounds, median with range. Two threads,
same CPU count, same code — the only variable is whether the handoff stays in L2
or goes out to L3.

## Result

| crate | `siblings` (L2) | `cross_core` (L3) | penalty |
|---|---:|---:|---:|
| ultima_rings | 469.01 [467.9–481.8] | 250.04 [247.3–263.0] | **1.88x** |
| rtrb | 404.34 [400.8–407.0] | 286.53 [194.5–291.6] | **1.41x** |
| crossbeam-channel | 55.97 [53.6–57.3] | 10.12 [9.0–10.7] | **5.53x** |

Moving two threads from one physical core to two costs between 1.41x and 5.53x
**depending on the crate**. Nothing about any implementation changed.

## The ordering inverts

| pair | `siblings` | `cross_core` |
|---|---:|---:|
| ultima / rtrb | **1.16x** (ultima ahead) | **0.87x** (rtrb ahead) |
| ultima / crossbeam | 8.38x | 24.70x |
| rtrb / crossbeam | 7.22x | 28.30x |

**Whether this crate beats rtrb depends on where the operating system happened to
put two threads.** Close together we lead by 1.16x; far apart rtrb leads by
1.15x. Both are true, and neither is a fact about the two ring buffers.

## What this explains

Five sessions measured this crate's SPSC against rtrb and produced 0.99x, 0.86x,
0.91x, 1.03x, then 1.67–1.80x on the 16-core rig. `2026-08-15-bakeoff-rig.md`
concluded that parity "was a property of the VM" and treated it as a machine
effect. That was at best half right: **placement is an uncontrolled variable that
by itself spans the entire range of results that document was trying to explain.**

The same mechanism accounts for `ultima / crossbeam` swinging between 0.95x and
1.88x across configurations. crossbeam is by far the most placement-sensitive
crate here at 5.53x, so any comparison against it moves with placement even when
neither side changes.

It is also consistent with the earlier `taskset` observation that every MPSC cell
ran faster on 2 packed cores than on 16 spread ones (crossbeam 2.86x, ultima
1.69x, thingbuf 1.10x at constant CPU count). This group isolates the same effect
with two threads instead of three, so no scheduling behaviour is mixed in.

## Why the effect exists

These benchmarks are **handoff-bound, not compute-bound**. Thread count is fixed
at 2–3 regardless of topology, so additional cores add no parallelism — they only
lengthen the path each cache line travels. SMT siblings share L1d and L2, so a
handoff between them never leaves L2. Separate physical cores share only L3, so
every handoff pays a round trip to L3 and the coherence protocol.

Why the crates differ is not measured here. A plausible reading is that it tracks
how many distinct cache lines each design touches per element — a design that
ping-pongs more lines pays the distance penalty more times — but no line-level
counters were collected, so treat that as a hypothesis.

## Consequences for this directory

**Every MPSC and SPSC number recorded before today has an uncontrolled placement
variable in it.** The harness never pinned, so placement was whatever the
scheduler chose, and it varies run to run and machine to machine.

This does not invalidate the measurements — they are what that code did on that
box that day — but it does mean:

- **Competitor ratios are the least reliable numbers here**, more so than the
  resolution budgets in `README.md` §1 suggest, because those budgets were
  themselves measured with placement uncontrolled.
- **A ratio needs a placement as well as a machine and a core count.** The rig
  bake-off's `full` column let the scheduler spread three threads across 16
  cores; its `smt2x2` column forced them together. That difference alone is
  worth up to 5.53x on one of the cells.
- **Comparisons of this crate against itself are much safer** than comparisons
  against others, because a change measured A/B on one box holds placement
  roughly constant between the arms. The `PARK_SPINS` and pre-park-spin results
  are not threatened by this.

## The first version of this experiment was wrong

Worth recording, because the failure mode is silent and specific to this API.

`core_affinity::get_core_ids()` reports **the caller's current affinity mask**,
not the machine. The first implementation pinned criterion's main thread to CPU 0
in the first cell; every thread spawned afterwards inherited a CPU-0-only mask,
could not see any other core, and its `find()` for the target CPU failed. The
pin result was discarded, so all six cells ran two threads on CPU 0 and reported
identical throughput of 0.17 Melem/s — a plausible-looking table in which every
cell was the same measurement.

Two fixes, both in `benches/throughput.rs`:

1. **Never pin the criterion main thread.** Cells run sequentially in one
   process, so a pin there leaks into every later cell. Placement cells spawn
   both sides and leave main unpinned.
2. **`pin()` returns `bool` and callers assert on it.** A failed pin now aborts
   the bench instead of quietly producing a number.

## Limits

- **One box, 2 physical cores.** `cross_core` here means "adjacent cores sharing
  L3". On a 16-core mesh, or across sockets, the penalty should be larger; that
  is untested.
- **Two threads, SPSC, `u64`, `BusySpin`.** No MPSC placement cell exists yet —
  three threads have more placement permutations than two and want their own
  design.
- **The per-crate mechanism is unmeasured.** No cache-line or coherence counters
  were collected.
- **Pinning is opt-in and not applied to the `bakeoff_*` groups.** Turning it on
  globally would silently change every absolute in this directory and would
  substitute a hidden fixed choice for a visible free one. The `pin()` helper is
  available to any cell that wants it.
