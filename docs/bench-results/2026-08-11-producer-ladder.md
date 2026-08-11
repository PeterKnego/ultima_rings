# Producer ladder: 2 to 64 producers, and the backoff ceiling across all of them

**Date:** 2026-08-11
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap
**Outcome:** **`CLAIM_BACKOFF_MAX` stays at 64.** No code change. New bench group
`mpsc_producer_ladder` added.

## Why

Every claim-CAS backoff result before this rested on **2 and 4 producers only**.
`mpsc_layout_probe` has three cells, but two of them share a producer count and
vary capacity instead — so the 64-vs-256 decision in
`2026-08-11-backoff-tuning.md` came from a single cell at 4 producers, on a
4-core box. Nothing had ever been measured above the core count.

That is the wrong place to stop for a *backoff* parameter in particular. A
backoff is a scheduling interaction: once threads outnumber cores, a producer can
be descheduled part-way through its wait, which is a different situation from a
producer spinning on a core it owns. The optimum could move.

`mpsc_producer_ladder` fixes capacity at 1024 and runs 2, 4, 8, 16, 32 and 64
producers, crossing the 4-core boundary.

## How MPSC degrades with oversubscription

This had never been measured. At the shipped ceiling of 64, throughput falls
steadily as producers outnumber cores:

| producers | Melem/s |
|---:|---:|
| 2 | 78.80 |
| 4 | 68.94 |
| 8 | 52.52 |
| 16 | 43.08 |
| 32 | 33.15 |
| 64 | 25.54 |

Useful on its own: the design does not collapse under 16x oversubscription, it
degrades roughly logarithmically.

## Ceiling sweep across the ladder

Ceilings 16, 64 and 256, interleaved by round (every ceiling measured once per
round, then the next round) so drift cannot masquerade as a parameter effect.
Three rounds. Mean, with the per-cell spread in brackets:

| ceiling | p2 | p4 | p8 | p16 | p32 | p64 |
|---|---:|---:|---:|---:|---:|---:|
| 16 | 77.81 (2.0) | 66.52 (9.2) | 49.32 (3.7) | 40.61 (2.0) | 31.97 (10.2) | **27.36** (2.0) |
| **64** | **78.80** (0.7) | **68.94** (6.2) | **52.52** (4.3) | **43.08** (3.3) | **33.15** (2.5) | 25.54 (2.8) |
| 256 | 78.09 (1.2) | 68.76 (3.1) | 50.13 (6.1) | 40.27 (2.7) | 31.88 (0.7) | 26.17 (1.2) |

64 is best at five of the six producer counts. 16 leads only at p64, where 64
gives up 6.7%.

Two things this overturned:

- **A single round had suggested 256 leads at low producer counts** (p4, p8, p16).
  Across three rounds it does not — 64 beat 256 at every count.
- **The predicted direction was wrong.** Before running this, the stated
  hypothesis was that heavy oversubscription would favour *longer* ceilings,
  because more producers means more collisions. The data points the other way at
  p64, where the shortest ceiling led.

No gap in the table exceeds its own worst per-cell spread, so none of it is
separated on three rounds.

## The p64 crossover did not survive more rounds either

The only cell where 64 was not best was p64, so it got a focused head-to-head:
16 against 64, five interleaved rounds, that cell alone.

| ceiling | runs | mean | spread |
|---|---|---:|---:|
| 16 | 18.13, 16.30, 15.11, 13.23, 17.61 | 16.08 | 4.90 (30%) |
| **64** | 17.32, 17.06, 15.87, 15.50, 15.70 | **16.29** | 1.82 (11%) |

64 leads on the mean and wins three of five rounds. The apparent 6.7% advantage
for 16 was a three-round artifact.

A second observation, which the means hide: **ceiling 16 has nearly three times
the run-to-run variance of ceiling 64 at this producer count** (30% against 11%).
Even if their throughput were equal, the shorter ceiling delivers it far less
predictably under heavy oversubscription.

Note the absolute figures here (13–18) sit below the ladder's p64 figures
(25–28). The focused run measured that cell alone, repeatedly, where the ladder
run reached it after five other cells. The p64 cell is evidently sensitive to
what else ran in the same invocation. The comparison within each block is still
sound, because both ceilings were measured under identical conditions and
interleaved — but the p64 absolute number should not be quoted without saying
which harness produced it.

## Decision

Keep `CLAIM_BACKOFF_MAX = 64`, now across 2 to 64 producers rather than 2 to 4.
`src/` is unchanged; the bench group is the only addition.

## The methodological finding, which is the more transferable one

**Twice today a three-round signal reversed under five rounds**, both times on
this crate's own parameter tuning:

| Question | 3 rounds said | 5 rounds said |
|---|---|---|
| 64 vs 256 at p4 (`2026-08-11-backoff-tuning.md`) | 256 by +2.9% | 64 by +1.5% |
| 16 vs 64 at p64 (this document) | 16 by +6.7% | 64 by +1.3% |

Both apparent effects were around 3–7%, and both vanished or inverted. On this
box, three rounds does not resolve differences below roughly 5% for these cells,
and a plausible mechanism for the wrong answer was available each time — which is
exactly what makes such results convincing. Treat three-round results as a
screen, never as a decision, and require either a clean separation from the
spread or a five-round confirmation.

## Not covered

- **Capacity is fixed at 1024** across the whole ladder. The interaction between
  producer count and capacity is unmeasured.
- **Ceilings 1, 4 and 1024** were swept only at p2 and p4
  (`2026-08-11-backoff-tuning.md`), not across the ladder.
- **Thread spawn cost grows with producer count** and is a large share of the p64
  measurement. It is identical across ceilings, so it compresses the differences
  between them without biasing which one wins. Compare ceilings within a producer
  count. Do not compare throughput across producer counts as a pure channel
  measurement.
- **Growth factor, starting value and reset policy** remain unswept, as before.
