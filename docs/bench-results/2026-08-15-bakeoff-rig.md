# Bake-off on the rig: rtrb parity was a property of the VM

**Date:** 2026-08-15
**Host:** AWS c7i.8xlarge — Xeon Platinum 8488C, 16 physical cores, THP off
**Method:** all six bake-off groups, 5 rounds interleaved, at two topology
points: `full` (16 cores) and `smt2x2` (4 CPUs on 2 physical cores — the VM's
exact shape). Built to completion first. Rig: `bench-infra/`.

Companion to `2026-08-14-bakeoff-v4.md`, which ran the same code on the 4-vCPU
VM. Three configurations lets two questions be separated:

- **VM against `smt2x2`** — same topology, different machine → a *machine* effect
- **`smt2x2` against `full`** — same machine, different topology → a *core count* effect

Ties are comparisons whose ranges overlap.

## The whole result in one table

| comparison | VM | rig `smt2x2` | rig `full` |
|---|---:|---:|---:|
| SPSC ultima / rtrb | 1.03x *(tie)* | **1.67x** | **1.80x** |
| MPSC ultima / crossbeam | **1.88x** | 0.95x *(tie)* | **1.25x** |
| String ultima / crossbeam | 1.35x | 1.39x *(tie)* | 1.00x *(tie)* |
| String ultima / thingbuf_ref | 1.07x *(tie)* | **2.05x** | **2.35x** |
| Park ultima / crossbeam_blocking | 0.41x | 0.18x | 0.24x |
| sharded / mpsc | 1.68x | **5.71x** | **6.20x** |

---

## 1. Parity with rtrb was a property of the VM — and of thread placement

> **Revised the same day.** `2026-08-15-thread-placement.md` shows that moving
> two threads from one physical core to two flips this comparison on a single
> machine: this crate leads rtrb 1.16x when they share a core and trails 0.87x
> when they do not. Placement alone spans the range this section attributes to
> the machine. The measurements below stand; the explanation is at best half of
> the story.


Five sessions have measured this crate's SPSC against rtrb and reported 0.99x,
0.86x, 0.91x, and — on a like-for-like cell at last — 1.03x. The standing summary
was "parity, and rtrb leads in two of three sessions".

On the Xeon this crate leads rtrb by **1.67x at `smt2x2` and 1.80x at `full`**,
ranges separated at both. The two topologies agree, so this is a **machine**
difference, not a core-count one: every one of those five parity measurements was
taken on the same AMD EPYC-Milan VM.

The README's softened rtrb claim was the right call on the evidence available.
The evidence has now doubled and points the other way on different silicon.
Neither machine's answer generalises, which is the finding.

## 2. The MPSC lead does not replicate at matched topology

1.88x on the VM, and a **tie** at `smt2x2` — the same topology on a different
machine. At `full` it is 1.25x, separated.

So the headline number varies from 0.95x to 1.88x depending on the box, and the
disagreement is between two configurations that differ only in silicon. The
direction survives everywhere except `smt2x2`, where it is a tie rather than a
loss; the magnitude does not travel at all.

This retires, for good, any statement of the form "N times crossbeam" without a
machine attached. `README.md` already required a core count; a machine is
needed too.

## 3. The String cell finally replicates — and it is topological

`ultima / crossbeam` reads **1.35x on the VM and 1.39x at `smt2x2`** — the first
time this cell has agreed across machines at matched topology. At `full` it is a
tie.

So the String advantage is a small-machine effect that reproduces on two
different processors, rather than the machine-dependent noise it looked like on
2026-08-12. That earlier disagreement was about `thingbuf_ref`, and this run
suggests why: see below.

## 4. `thingbuf_ref` on String — the VM is the outlier

| | ultima / thingbuf_ref |
|---|---|
| VM, 2026-08-12 | **0.55x** (thingbuf 1.82x ahead) |
| VM, 2026-08-14 (v4) | 1.07x *(tie)* |
| rig `smt2x2` | **2.05x** |
| rig `full` | **2.35x** |

The rig gives a consistent answer at both topologies: this crate is 2.05–2.35x
ahead. The VM has given three different answers on three occasions, including
two on the same box.

The reasonable reading is that the VM's `thingbuf_ref` cell is unstable rather
than that the comparison genuinely reverses, and that the 2026-08-12 result which
prompted "thingbuf's reference API is 1.82x this crate" was that instability at
its extreme. `docs/bench-results/README.md` already says to quote no direction
from this cell; that stands, with the addition that the rig is the more
trustworthy of the two boxes here.

## 5. `Park` is not resolvable on the rig, as predicted

0.18x and 0.24x here against 0.41x on the VM — but `crossbeam_blocking` carries
**31.7% and 62.2% spread** at the two points, and `park_block`'s minimum
detectable effect on this machine is 25–53%
(`2026-08-12-resolution-budgets-rig.md`).

The rig cannot measure this comparison, which is exactly what that document
predicted, and the reason the pre-park-spin gate was deliberately run on the VM.
**Do not read a `Park` regression into these numbers.** The VM's 0.41x remains
the measurement of record.

## 6. The sharded prototype scales, and by a lot

1.68x the production ring on the VM, **5.71x at `smt2x2` and 6.20x at `full`**.

`src/sharded.rs` composes N independent SPSC rings with a round-robin consumer,
so it has no shared claim cursor to contend on. On a machine where the ring's
CAS is not the bottleneck the gap narrowed; on the Xeon it opens back up.

It remains feature-gated, `BusySpin`-only, with no loom models, and is not a
shipping path. But 6.2x is large enough that the composition approach deserves
to be revisited rather than left as a curiosity.

## Cells that cannot support a ratio

| cell | spread | note |
|---|---:|---|
| `bakeoff_spsc/crossbeam` @ `full` | **118.3%** | 27.40–59.81; anything quoted against it is meaningless |
| `bakeoff_park_mpsc/crossbeam_blocking` @ `full` | 62.2% | see §5 |
| `bakeoff_mpsc_string/crossbeam` @ `full` | 50.7% | why §3's `full` column is a tie |
| `bakeoff_mpsc/thingbuf_ref` @ `smt2x2` | 47.2% | see §4 |
| `bakeoff_spsc/kanal` @ `smt2x2` | 48.5% | kanal remains unusable everywhere |

## Limits

- **Two machines is not a survey.** Both are x86-64; no ARM, no multi-socket, no
  NUMA. The lesson here is that one machine was never enough, not that two are.
- **Absolutes do not compare across any of the three configurations**, and the
  VM was measured on a day it ran ~2.4x slow.
- **`disruptor` and `sharded` have no `String` cells**, and the bake-off groups
  still have no `Backoff`/`BackoffYield` cells — those live only in
  `backoff_isolation`.
- **`ultima_drain` remains slower than single-item `try_recv`** at both rig
  points (2.00x and 1.97x crossbeam against 3.53x and 3.85x), reproducing the v4
  finding on a second machine.
