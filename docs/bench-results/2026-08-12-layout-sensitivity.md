# How much of a benchmark difference is code layout?

> **Still current for the 4-vCPU VM; superseded for the rig.** Budgets for the
> 16-core `bench-infra/` host are in `2026-08-12-resolution-budgets-rig.md`, and
> they differ enough to change which machine a given experiment belongs on.
> Note also that this study used two rounds per alignment where the rig study
> used three, so its separation of layout from noise is the weaker of the two.

**Date:** 2026-08-12
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap
**Method:** the **same source** built five times with different function
alignments, via `RUSTFLAGS="-C llvm-args=-align-all-functions=N"` for
N ∈ {0, 3, 4, 5, 6}. Two rounds per build. Because the source is byte-identical
across all five, **every difference between builds is layout**.

This replaces an inference with a measurement. `docs/bench-results/README.md`
previously asserted a "~10% resolution floor" on the strength of a
misdiagnosed control (see the correction there). This is the direct version.

## Result

| Cell | a0 | a3 | a4 | a5 | a6 | layout spread | intrinsic noise |
|---|---:|---:|---:|---:|---:|---:|---:|
| `busyspin_poll` | 78.69 | 79.34 | 77.41 | 75.58 | 78.19 | **5.0%** | 1.1% |
| `busyspin_block` | 71.97 | 73.41 | 70.49 | 70.16 | 72.19 | **4.6%** | 1.5% |
| `park_poll` | 17.19 | 16.95 | 16.58 | 17.31 | 17.23 | **4.4%** | 1.6% |
| `park_block` | 11.09 | 10.70 | 11.65 | 10.52 | 11.13 | 10.8% | **9.1%** |
| `spsc` | 592.75 | 626.89 | 573.58 | 582.17 | 626.87 | 9.3% | **7.5%** |

*Layout spread* is the range across the five alignment means. *Intrinsic noise*
is the mean absolute difference between the two rounds within one alignment,
which involves no rebuild at all.

## Three findings

**1. Layout is real, and it is about 5% — not 10%.** For the three well-behaved
cells, changing nothing but function alignment moves the number 4.4–5.0%, against
1.1–1.6% run-to-run noise. So layout is a genuine effect roughly three times the
size of measurement noise, and the earlier "~10%" claim was too pessimistic by
half.

**2. `park_block` and `spsc` are not layout-sensitive — they are noisy cells.**
Their layout spread (10.8%, 9.3%) barely exceeds their own within-alignment
spread (9.1%, 7.5%). Almost all of their variance is intrinsic and appears
without any rebuild. Treating their spread as a layout problem would have been
the wrong diagnosis, and rebuilding differently would not fix it.

**3. The two effects need separating per cell.** A single global floor is the
wrong model. What matters is a per-cell budget:

| Cell | approximate minimum detectable effect |
|---|---:|
| `busyspin_poll` | ~6% |
| `busyspin_block` | ~6% |
| `park_poll` | ~6% |
| `spsc` | ~9% |
| `park_block` | ~11% |

`park_block` — the single cell the pre-park-spin question rests on — is the
noisiest in the suite.

## Re-reading the results already recorded

| Result | Effect | Standing against the measured budget |
|---|---:|---|
| CAS backoff (`2026-08-11-cas-backoff.md`) | +108% to +143% | Far above every budget. Unaffected. |
| Sharded MPSC (`2026-08-07-sharded-mpsc.md`) | 4.51x | Far above. Unaffected. |
| Colocated slot (`2026-08-09-colocated-slot.md`) | +12% to +15% | Above the ~6% budget for polling MPSC cells, and it moved three configurations together. Stands. |
| Padding, rejected (`2026-08-09-mpsc-perf-v2.md`) | +3.5%, then −0.1% | Below the ~6% budget. Genuinely unresolvable — and the conclusion drawn was "no reliable effect", which is what that looks like. |
| Ceiling sweep (`2026-08-11-backoff-tuning.md`) | 1% to 3% | Below budget; conclusion was "indistinguishable". Supported. |
| `busyspin_block` +10.4% during the pre-park gate | +10.4% | **Above** its 4.6% layout spread, so not layout. Consistent with a real codegen effect on `recv()`. |

Every conclusion previously drawn from below the floor was a negative one, so
nothing was adopted on evidence the box cannot produce. The budget is now
measured per cell rather than guessed globally.

## Verdict on the pre-park spin

> **Overturned 2026-08-13** (`2026-08-13-park-prespin-gate.md`). A paired gate —
> both arms at the same alignment in the same round, 20 pairs — puts the change
> at **+65% on `park_block`, 20 of 20 pairs**, with a flat control. The verdict
> below is wrong because its baseline came from a *different run*, and
> `park_block` drifts 45% between sessions on identical source. The two "inside
> baseline" values were most likely the base binary; see that document.

`park_block` baseline, ten runs across five alignments, no pre-park spin:

```
10.00  10.13  10.60  10.72  10.80  10.91  11.11  11.46  12.20  12.26
range 10.00–12.26, mean 11.02
```

With the pre-park spin (gate runs): **10.31, 10.92, 16.52**.

Two of three fall inside the baseline range. The change does not reliably move
this cell, and the +16% mean reported by the gate came from one run.

The 16.52 is worth keeping on the record rather than discarding: it is 35% above
anything seen in ten baseline runs, which is a large excursion for a cell whose
baseline spans 10.00–12.26. A plausible reading is bimodality — occasionally the
spin catches the publish and the consumer avoids the park/unpark cycle entirely
for a long stretch. That is a hypothesis with one supporting observation, which
is not enough to ship on, and not enough to dismiss either.

~~**`feat/park-prespin` stays unmerged.**~~ Superseded — see the note above.

## Recipe for a layout-robust comparison

For any future change where the expected effect is near a cell's budget:

1. Build each variant at several function alignments
   (`RUSTFLAGS="-C llvm-args=-align-all-functions=N"`, N ∈ {0, 3, 4, 5, 6}).
2. Measure every alignment, and pool the results per variant. Comparing pooled
   distributions averages over layout instead of sampling one arbitrary layout
   per variant.
3. Interleave alignments within a round, as with any other variable.
4. Include a control cell that calls **no function the change touches** — not
   merely one whose branch is not taken. An SPSC cell is a true control for MPSC
   work, since `src/spsc.rs` is untouched. That distinction is exactly what the
   earlier misdiagnosis got wrong.

## Limits

- Five alignments, two rounds each, is a small sample for a spread statistic.
  The 4.4–5.0% figures should be read as "about 5%", not as precise.
- `-align-all-functions` perturbs function entry alignment only. It does not
  randomise basic-block placement, inlining decisions, or data layout, so it is a
  lower bound on total layout sensitivity rather than a full Stabilizer-style
  randomisation.
- All figures are from one box. Nothing here transfers to other hardware.
