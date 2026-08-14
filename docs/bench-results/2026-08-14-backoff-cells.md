# All four strategies in `backoff_isolation`: the blocking problem is `Park`'s alone

**Date:** 2026-08-14
**Hardware:** the 4-vCPU VM (2 physical cores + SMT)
**Method:** paired `PARK_SPINS` 0-vs-16 A/B, 3 alignments × 3 rounds, 9 pairs per
cell, `spsc` as control. 2 producers, cap 1024.

`backoff_isolation` covered only `BusySpin` and `Park`, which left the two
self-waking ladders unmeasured against both the claim-CAS backoff and
`PARK_SPINS`. All four strategies × {poll, block} are now cells.

## The strategies at current `main` (`PARK_SPINS = 16`)

| strategy | poll | block |
|---|---:|---:|
| `BusySpin` | 45.21 | 39.15 |
| `Backoff` | 44.49 | 38.41 |
| `BackoffYield` | 45.07 | 39.78 |
| **`Park`** | **10.88** | **12.13** |

## 1. The blocking-path weakness belongs to `Park`, not to blocking

`Backoff` blocks — its ladder climbs to timed parks — and reaches 38.41, within
3.5% of `BusySpin`'s 39.15. `Park` reaches 12.13, **3.2x lower than any other
strategy on the same workload**.

Every previous document here framed this as a property of "the blocking path",
because the only blocking cell was `Park`'s. It is not. Three of the four
strategies block or self-wake at comparable throughput, and one does not.

The difference is the wake protocol. `Backoff` self-wakes from timed parks, so
the producer pays nothing; `Park` makes every publish pay a `SeqCst` fence plus a
`consumer_parker.wake()` (design.md §8). That cost is on the *producer*, which is
why `park_poll` (10.88) is no better than `park_block` (12.13) — `try_send` pays
it whichever API the consumer uses.

**Practical consequence the crate does not currently state:** a caller who wants
a blocking API and cares about throughput should choose `Backoff`, not `Park`.
`Park` earns its place on wake latency (~10 µs against `Backoff`'s ~64 µs floor)
and on idle CPU (3.2% of a core against 5.2%), not on throughput.

## 2. `PARK_SPINS` does not touch the self-waking ladders — confirmed

Ratio at `PARK_SPINS = 16` against 0:

| cell | ratio | 95% CI | |
|---|---:|---|---|
| `backoff_poll` | 0.959x | [0.893, 1.024] | ns |
| `backoff_block` | 1.039x | [0.950, 1.128] | ns |
| `backoffyield_poll` | 1.002x | [0.921, 1.082] | ns |
| `backoffyield_block` | 1.025x | [0.952, 1.098] | ns |
| `busyspin_poll` | 1.038x | [0.964, 1.112] | ns |
| **`busyspin_block`** | **1.092x** | [1.029, 1.156] | 8/9 |
| `park_poll` | 1.042x | [0.952, 1.131] | ns |
| **`park_block`** | **1.510x** | [1.335, 1.685] | 9/9 |

`Backoff` and `BackoffYield` dispatch to `idle.idle()` and never reach the `Park`
arm where the spin lives (`src/mpsc.rs:245`). Both are flat at both APIs, which
turns a code-inspection prediction into a measurement.

`park_block` reproduces for a third independent time — 1.65x, 1.61x, now 1.51x
across three separate sessions and build sets.

## 3. The three self-waking `*_poll` cells are a working consistency check

45.21, 44.49, 45.07 — a 1.6% spread across `BusySpin`, `Backoff` and
`BackoffYield`. These are *identical code paths*: `try_send`/`try_recv` never
consult the wait strategy, and all three are self-waking so the productive side
pays nothing. They should agree, and they do.

That gives the group three mutually-checking cells rather than one control. If
they ever diverge, the harness is measuring something other than what it claims.
`park_poll` at 10.88 is the expected outlier, for the reason in §1.

## 4. The `spsc` control is not a control against layout

`spsc` came out marginally "significant" at 1.022x [1.001, 1.044] — and did so
once before, at `PARK_SPINS = 128` in the earlier sweep. Two marginal hits is
more than multiple-testing luck comfortably explains.

There is a mechanism. `src/spsc.rs` is untouched, so the control is valid against
*behaviour* — but both modules compile into one binary, and changing
`PARK_SPINS` changes the size of the MPSC code, which shifts where the SPSC code
lands. Function-alignment padding does not undo an upstream size change.

So the control rules out behavioural coupling and does **not** rule out layout
coupling. A control immune to layout would have to live in a separate binary.
The observed effect is ~2%, which is below anything this directory acts on, but
the limitation should be stated rather than assumed away.

## Limits

- **2 producers, cap 1024, one machine.** The producer ladder was not re-run with
  the new cells.
- **`Backoff`'s wake latency is not measured here.** The throughput advantage
  over `Park` comes with a ~64 µs OS-timer floor against `Park`'s ~10 µs; the
  choice between them is a latency/throughput trade, and this document only
  measures one side of it.
- **Idle CPU for the new cells** is unmeasured in this run; the figures quoted in
  §1 come from `2026-08-12-cpu-cost-and-heap-payload.md` and
  `2026-08-14-park-spins-sweep.md`.
- **`busyspin_block`'s +9.2%** is the codegen side effect seen twice before
  (+10.4%, +16.1%). Consistent in direction, variable in size, and still not
  explained beyond "adding lines to `recv` changes `recv`".
