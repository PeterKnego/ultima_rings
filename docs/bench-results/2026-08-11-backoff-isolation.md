# Isolating the Park regression: it is neither the strategy nor the path

**Date:** 2026-08-11
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap
**Question:** the claim-CAS backoff is worth +108% to +143% under `BusySpin`
(`2026-08-11-cas-backoff.md`) and costs 23% under `Park`
(`2026-08-11-bakeoff-v3.md`). Those two measurements differ in **two** ways at
once — wait strategy *and* API path — so neither had been shown to carry the
cost.

## Design

A 2x2 holding one axis fixed at a time, in `backoff_isolation`:

| | polling (`try_*`) | blocking (`send`/`recv`) |
|---|---|---|
| **BusySpin** | `busyspin_poll` | `busyspin_block` |
| **Park** | `park_poll` | `park_block` |

Each corner measured with and without the backoff, interleaved, three rounds.
The two source variants differ **only** in the backoff — both carry the
colocated slot.

`park_poll` is the diagnostic corner: `try_send` performs the `Park` fence and
consumer wake whenever the channel's strategy is `Park`, whichever API the
caller used, so that cell pays the per-publish wake cost while neither side ever
parks.

## Result

| Corner | no backoff | with backoff | delta | separated |
|---|---:|---:|---:|---|
| `busyspin_poll` | 34.18 (34.03–34.29) | 78.33 (76.86–79.53) | **+129.2%** | yes |
| `busyspin_block` | 33.59 (33.53–33.65) | 71.70 (70.72–72.60) | **+113.5%** | yes |
| `park_poll` | 10.33 (9.92–10.57) | 17.13 (16.86–17.47) | **+65.8%** | yes |
| `park_block` | 13.90 (13.44–14.18) | 10.56 (9.92–11.19) | **−24.0%** | yes |

Every corner separates cleanly — no overlap between the with- and without-
distributions in any of the four.

**Three of four corners gain substantially, and the fourth is the only
regression.**

## What this rules out

- **Not the wait strategy.** `Park` *gains* 65.8% when both sides poll. If the
  per-publish fence and wake were the problem, `park_poll` — which pays that
  cost on every publish — would regress. It does the opposite.
- **Not the API path.** Blocking `send`/`recv` gains 113.5% under `BusySpin`. If
  the crate's blocking loop were the problem, `busyspin_block` would regress.

It is the **interaction**: only the corner where *both* sides block.

This matters practically, because the obvious fix is wrong. Gating the backoff
on `strategy == Park` would surrender the +65.8% on `park_poll` in order to
rescue `park_block`, and would be aimed at a factor this measurement has just
excluded.

## The likely mechanism, and the evidence for it

`Park` parks on the **first** empty observation. `Receiver::recv`'s `Park` arm
runs `prepare_park`, the `SeqCst` fence, the Dekker re-check, and then `park()` —
there is no spin before it. Contrast `Backoff`, whose `Idle` ladder spends 10
spins and 20 yields before its first timed park.

So the chain is: the backoff spaces publishes slightly, a consumer that parks
immediately therefore observes an empty ring more often, and every such
observation costs a park/unpark syscall pair plus the producer's wake. The
throughput lost to that churn exceeds the throughput the backoff wins on the
claim.

Consistent with this, `park_block` without the backoff (13.90) is *higher* than
`park_poll` without it (10.33): when the consumer parks, it stops burning a core
and the producers' wake is not wasted. Adding the backoff inverts that ordering
(10.56 against 17.13), which is what a park/unpark-churn explanation predicts.

**This mechanism is not itself measured.** It is consistent with all four cells
and with the code, but no counter of park/unpark pairs was collected. Treat it
as the leading hypothesis, not as established.

## Implication for a fix

A fix belongs in the consumer's park decision, not in the claim loop. The
concrete candidate is a short spin before the first park in `Park` mode — the
thing `Backoff`'s ladder already does and `Park` does not. That would absorb the
extra empty observations without touching the claim path, and would leave all
three gaining corners alone.

That change lives in the Dekker wake protocol, which is loom-verified
(design.md §3), so it needs its own gate — and that gate must cover **all four
corners of this 2x2**, which is precisely what the backoff's original gate
failed to do.

**Follow-up (2026-08-12): the spin was implemented and did not pass.** Two of its
three `park_block` runs fall inside the unmodified baseline's range, which was
subsequently characterised over ten runs as 10.00–12.26
(`2026-08-12-layout-sensitivity.md`). `park_block` turns out to be the noisiest
cell in the suite, with a ~11% minimum detectable effect. The branch
`feat/park-prespin` stays unmerged.

## Not covered

- **`Backoff` and `BackoffYield`** have no cell here. `Backoff` parks, so it may
  share `park_block`'s exposure, though its 10 spins and 20 yields before the
  first park may already absorb the effect. Untested.
- **Producer counts other than 2.**
- **The park/unpark churn itself.** See above.
