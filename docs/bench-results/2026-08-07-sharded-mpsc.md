# Sharded MPSC prototype vs. shared-claim MPSC and crossbeam-channel

**Date:** 2026-08-07; crossbeam baseline settled 2026-08-08 (see "Follow-up" below)
**Hardware:** 4-core VM, 15 GiB RAM, no swap; built to completion before measuring
**Feature:** `experimental-sharded`
**Spec:** `docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md`

> **Headline:** sharded 321.5 Melem/s vs crossbeam's settled 71.25 = **4.51x**.
> An earlier revision of this document read 6.23x against a crossbeam figure
> since shown to be depressed. The gate outcome is unchanged; the ratio is
> not. See "Follow-up" for how the baseline was settled and why this
> comparison structurally favours `ultima_rings` on a loaded box.

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

| Cell | Melem/s (mid) | Melem/s (range) | vs. crossbeam's settled 71.25 |
|---|---:|---|---:|
| sharded (2 shards x 512) | 321.52 | 317.52 – 324.63 | **4.51x** |
| crossbeam-channel | 51.649 | 50.616 – 52.622 | — (depressed; see below) |
| mpsc (shared bounded-CAS claim) | 29.308 | 29.080 – 29.557 | 0.41x |

**The crossbeam figure in this table (51.649) is depressed and should not be
used.** The follow-up investigation below settles crossbeam's figure on this
box at **71.25 Melem/s**, and every ratio above is computed against that
settled value, not against the depressed same-session figure. Using the
same-session 51.649 would give 6.23x — a materially more flattering number
that this crate declines to headline.

## Follow-up: settling the crossbeam baseline (2026-08-08)

The original run's crossbeam mid (51.649) disagreed with the v1 reference
(71.0, `docs/bench-results/2026-08-06-bakeoff.md`) by ~27%, while `mpsc`
reproduced closely (29.308 vs 29.9). Because crossbeam is the denominator of
this crate's standing exit-ramp gate, that ambiguity was worth closing. Eight
further measurements on a verified-quiet box (84–94% CPU idle, 0 runnable,
0 blocked, no swap, steal 0):

| Condition | crossbeam | sharded | mpsc |
|---|---:|---:|---:|
| isolated run 1 | **71.25** | — | — |
| isolated run 2 | 67.65 | — | — |
| isolated run 3 | 61.86 | — | — |
| isolated run 4 | 30.09 | — | — |
| combined run A | 69.89 | 321.22 | 30.19 |
| combined run B | 67.79 | 321.22 | 31.55 |

**Findings:**

1. **crossbeam's settled figure is ~71 Melem/s** — the v1 reference was right
   and the original run's 51.649 was depressed by transient interference. The
   quiet-box cluster is 67.65–71.25.
2. **`sharded` and `mpsc` are highly reproducible**: `sharded` measured 321.22
   three separate times (spread <0.1%); `mpsc` sits at 29.3–31.6 (~7%).
   crossbeam alone swings 30.09–71.25, a 2.4x spread.
3. **The asymmetry is mechanistic, not noise.** `crossbeam-channel`'s array
   flavor calls `backoff.snooze()` on its send and recv paths
   (`crossbeam-channel-0.5.16/src/flavors/array.rs:207,298,351,411`), and
   `snooze()` calls `std::thread::yield_now()`
   (`crossbeam-utils/src/backoff.rs:218`). `ultima_rings` uses only
   `std::hint::spin_loop()` — a PAUSE, never a yield. So crossbeam hands its
   timeslice to whatever else is runnable and its throughput tracks how busy
   the box is, while `ultima_rings` keeps its timeslice regardless.

Two candidate confounders are now **ruled out**: `Cargo.lock` is tracked and
unchanged since `9acfd76`, so both sessions ran the same `crossbeam-channel
0.5.16`; and the installed toolchain is `rustc 1.96.0 (2026-05-25)`, dated
two and a half months before either run, so a stable-channel release between
2026-08-06 and 2026-08-07 could not have applied.

## What this does and does not show

The sharded cell provides **per-producer FIFO only** and per-shard
backpressure; `mpsc` and crossbeam both provide global FIFO and a global
bound. This is not a like-for-like replacement, and the number should not be
read as one.

- **Per-producer FIFO only.** No cross-producer ordering guarantee — two
  values sent by different producers may be delivered in either order,
  regardless of real time. `mpsc` and crossbeam both provide global FIFO;
  sharded does not.
- **Per-shard backpressure.** `mpsc` and crossbeam both provide a single
  global bound; with `channel(2, 1024)` a sharded producer sees `Full` at 512
  outstanding items even while the other shard sits empty.
- **Fixed producer set / no `Sender: Clone`.** The spec itself names the
  dynamic shard registry as "the expensive part," and none of it is measured
  here. Sharper: if `Sender: Clone` is ever added and two producers land on
  the same shard, the single-writer property that produces this whole result
  is gone and a per-shard claim protocol comes back. 321.52 Melem/s is an
  **upper bound** on what the production type can reach, not a promise of it.
- **`BusySpin` only, no `Park`.** `Park` mode would need a new N-way
  Dekker-style wake protocol at the sharded layer; the ecosystem bake-off's
  Park-parity requirement is untested by this cell.
- **The comparison structurally favours `ultima_rings` on a busy box, and
  this applies to every `ultima_rings`-vs-crossbeam number this crate
  publishes — not just this one.** Per finding 3 above, crossbeam yields its
  timeslice under contention while `ultima_rings` spins without yielding. On
  a machine with other runnable work, crossbeam is penalised and
  `ultima_rings` is not, so the measured ratio inflates with box load: the
  same pair of cells gives 4.5x on a quiet box and would give ~10x on the
  loaded one that produced the 30.09 crossbeam reading. Every ratio in this
  document therefore uses crossbeam's **best** observed figure (71.25), which
  is the least favourable choice for `ultima_rings` and the only defensible
  one. A reader comparing these numbers against a *dedicated* benchmark box
  should expect the gap to narrow, not widen.
  Note this is a real behavioural difference with real consequences, not
  purely a measurement artifact — a spinning consumer that never yields is
  genuinely faster under load and genuinely worse for CPU-sharing
  neighbours. `Backoff` and `Park` exist in this crate precisely for callers
  who do not want that trade; neither is measured here.

## Verdict

**Decisive: sharded measured 321.52 Melem/s, ≥ 142 Melem/s (2x crossbeam).**

The sharded prototype clears the fixed 142 Melem/s decisive threshold by more
than 2x, at **4.51x** crossbeam's settled 71.25 Melem/s. That ratio uses
crossbeam's best observed figure — the least favourable denominator for
`ultima_rings` — rather than the depressed same-session 51.649 that would
have read 6.23x.

The result is robust in a way the absolute crossbeam figure is not: `sharded`
measured 321.22 Melem/s on three separate runs (spread <0.1%), and the gate
clears against every crossbeam figure ever observed on this box, including
the highest (71.25 → 4.51x). This also corroborates Task 3's `--quick` smoke
figure (~320 Melem/s), though that number carried no evidentiary weight on
its own.

Next round designs the dynamic shard registry and the production type. See
"What this does and does not show" above for what this number does not
transfer to that next round.

Separately, the wide gap between `mpsc` (29.308, 0.41x crossbeam's settled
71.25) and `sharded` (321.52, 4.51x) — a ~11x difference between the two
`ultima_rings` designs, which is unaffected by the crossbeam denominator
since neither cell involves crossbeam — is *consistent
with* the crate's account in `docs/design.md` §7 and §8 of where the v1
MPSC's cost lives (CAS contention and availability-array false sharing under
the shared claim). But this is not a controlled comparison, and the premise
under which it might look like one is false: `mpsc` and `sharded` are two
independently written ring implementations, not two configurations of one
shared primitive. `src/mpsc.rs` does not use `src/spsc.rs` — it has its own
`Shared<T>` with a per-slot `avail: Box<[AtomicI64]>` and LMAX-style
round-number publication (`src/mpsc.rs:20-33`), while `sharded` composes N
private `src/spsc.rs` rings, each with its own `head`/`tail` pair
(`src/spsc.rs:17-29`). The two also differ on the consumer side as much as
the producer side: `mpsc::Receiver::try_recv` does an Acquire load of
`avail[slot]` plus a Release store of `head` on every item
(`src/mpsc.rs:205-224`), while each `spsc::Receiver::try_recv` behind
`sharded` caches `tail` and touches a contended atomic only when its own ring
looks empty (`src/spsc.rs:165-194`). That consumer-side difference is a
third uncontrolled variable, alongside claim-CAS-vs-private-slot and
shared-vs-none availability array. The measured gap is consistent with the
design.md account of where v1 MPSC's cost lives; it is not, on its own,
evidence for it. Isolating claim contention from consumer-side cost would
need a controlled variant that holds the consumer path fixed across both
cells — not attempted here.
