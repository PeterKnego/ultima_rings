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

| Cell | Melem/s (mid) | Melem/s (range) | vs. crossbeam (same-session) | vs. crossbeam (v1, 71.0) |
|---|---:|---|---:|---:|
| sharded (2 shards x 512) | 321.52 | 317.52 – 324.63 | 6.23x | 4.53x |
| crossbeam-channel | 51.649 | 50.616 – 52.622 | 1.00x | — |
| mpsc (shared bounded-CAS claim) | 29.308 | 29.080 – 29.557 | 0.57x | — |

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

Two checkable facts bear on what could explain the difference, rather than
leaving it as an assumed "variance": `Cargo.lock` is tracked and unchanged
since `9acfd76` (the v1 bake-off commit), so both sessions ran the same
`crossbeam-channel 0.5.16` (v1 doc, `docs/bench-results/2026-08-06-bakeoff.md`
line 38) — a dependency-version change is ruled out. `rust-toolchain.toml`
pins `channel = "stable"` with no explicit version, so a stable Rust release
landing between 2026-08-06 and 2026-08-07 is **not** ruled out. Separately,
the v1 run measured all groups in a single pass (see
`docs/bench-results/2026-08-06-bakeoff.md`), while this session's `cargo
bench` invocation used a `--` name filter restricted to three groups (see
Commands above); bench ordering and thermal context differed between the two
sessions for reasons independent of any code or dependency change.

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

## Verdict

**Decisive: sharded measured 321.52 Melem/s, ≥ 142 Melem/s (2x crossbeam).**

The sharded prototype's mid (321.52 Melem/s) clears the fixed 142 Melem/s
decisive threshold by more than 2x, and is 6.23x this session's own
crossbeam mid (51.649 Melem/s) — the relevant same-session comparison — or
4.53x against the v1 reference's 71.0 Melem/s crossbeam figure, for a reader
who anchors on that session instead. Every gate branch clears against either
denominator. This also corroborates Task 3's `--quick` smoke figure
(~320 Melem/s), though that number carried no evidentiary weight on its
own; this controlled run is what makes the result trustworthy, and it
comes out at essentially the same throughput.

Next round designs the dynamic shard registry and the production type. See
"What this does and does not show" above for what this number does not
transfer to that next round.

Separately, the wide gap between `mpsc` (29.308, 0.57x crossbeam) and
`sharded` (321.52, 6.23x crossbeam) — a ~11x difference — is *consistent
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
