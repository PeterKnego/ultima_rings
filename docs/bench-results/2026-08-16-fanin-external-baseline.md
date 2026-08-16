# Fan-in against an external baseline, and a root-cause hunt that failed

**Date:** 2026-08-16
**Host:** AWS c7i.8xlarge — Xeon Platinum 8488C, 16 physical cores, THP off.
**Three separate rig instances** were used across this session (two bake-off
runs, one ablation run); absolutes do not compare between them, only ratios
within a run. Rig: `bench-infra/`. Raw output in
`raw/2026-08-16-fanin-external-baseline/`.
**Method:** `bakeoff_fanin`, 3 rounds at `full` (16 physical cores) and
`smt2x2` (4 CPUs on 2 cores), equal total buffered capacity (1024 slots split
`n` ways). Medians of the 3 round-mids, ranges in the raw files.

## Why this exists

Every `sharded` figure before today compared against this crate's own `mpsc`.
The prior-art survey (`docs/superpowers/research/2026-08-16-sharded-fanin-survey.md`)
found no Rust crate shipping this shape as a channel, which means the honest
external comparator is not a competitor crate but **what a user would build
instead**: N `rtrb` rings plus a hand-rolled consumer sweep. The survey named
that cell, and `crossbeam::Select` over N bounded channels, as the two
missing numbers.

Four cells, plus one added mid-session (see §3):

- `sharded` — this crate.
- `rtrb_sticky` — N `rtrb` rings, hand-rolled sweep using the same policy
  `sharded` uses (sticky cursor, 32-item visit budget), no disconnect handling.
- `rtrb_sticky_dc` — the same plus the aggregate-disconnect bookkeeping
  `sharded` performs internally, mirroring `Receiver::try_recv` including its
  lack of dead-shard memoization.
- `rtrb_roundrobin` — N rings, naive one-item-per-shard sweep: the loop most
  people write first.
- `crossbeam_select` — N bounded crossbeam channels behind a `Select` built
  once and reused (its most favourable usage), driven with `try_select` so
  both sides poll.

Note one asymmetry in crossbeam's favour: `Select` may take any ready channel,
so it carries no per-producer FIFO obligation, while the three sweep cells do.

## 1. The bake-off

`full` (16 physical cores), Melem/s:

| cell | 2 producers | 4 producers |
|---|---:|---:|
| `rtrb_sticky` | 195.9 | 179.0 |
| `rtrb_sticky_dc` | 182.9 | 191.5 |
| `sharded` | 137.0 | 138.4 |
| `rtrb_roundrobin` | 14.9 | 14.7 |
| `crossbeam_select` | 5.4 | 4.6 |

`smt2x2` (2 physical cores), Melem/s:

| cell | 2 producers | 4 producers |
|---|---:|---:|
| `rtrb_sticky` | 226.2 | 113.0 |
| `rtrb_sticky_dc` | 225.1 | 116.4 |
| `sharded` | 199.0 | 119.5 |
| `rtrb_roundrobin` | 184.2 | 21.5 |
| `crossbeam_select` | 7.1 | 6.6 |

Three results, in descending order of confidence.

**`crossbeam::Select` is 20–30x behind every sweep cell**, at 4.6–7.1 Melem/s
across all four points — the most reproducible cell in the group, and measured
in its best configuration. The survey's §6 argument (an O(n) shuffle plus O(n)
register/unregister per blocked operation, and it parks) now has a figure.

**The sweep policy is worth up to 13x, and its value is a function of thread
placement.** Naive round-robin against the sticky cursor is 14.9 vs 195.9 at
`full` p2 — but 184.2 vs 226.2 at `smt2x2` p2, nearly free. The naive loop
switches rings after every item, so it pays cache-line transport on almost
every element; shrink the distance and the penalty largely disappears. It
returns at four shards on two cores (21.5 vs 113.0). This is the same
mechanism `docs/explanation/reading-the-benchmarks.md` argues for handoff
benchmarks generally, showing up in a consumer loop.

**More cores made every cell slower at 2 producers.** Each cell is faster at
`smt2x2` than at `full` for p2 — `sharded` 137.0 → 199.0, `rtrb_roundrobin`
14.9 → 184.2 — with no overlapping ranges. Two producers and a consumer is
three threads; there is no parallelism to win, so extra cores only lengthen
the path a published cache line travels. It reverses at p4, where five
spinning threads on four CPUs are oversubscribed and the fast cells lose more
to CPU starvation than they gain from locality.

## 2. The gap this exposed

`sharded` trails a hand-rolled sweep doing the identical job. Because
`rtrb_sticky` and `rtrb_sticky_dc` agree within noise (1.07x, 0.93x, 1.01x,
0.97x across the four points), **the aggregate-disconnect semantics are free**
— the gap is not the price of the contract.

Measured `sharded` / `rtrb_sticky_dc`, per-item:

| point | sharded | rtrb_dc | delta |
|---|---:|---:|---:|
| `full` p2 | 7.30 ns | 5.47 | +1.83 |
| `full` p4 | 7.23 | 5.22 | +2.00 |
| `smt2x2` p2 | 5.03 | 4.44 | +0.58 |
| `smt2x2` p4 | 8.37 | 8.59 | −0.22 |

The overhead appeared to scale with inter-core distance, which ruled out a
pure instruction-count explanation and pointed at coherence traffic. That
framing turned out to be over-confident; see §4.

## 3. What was eliminated

Each of these was tested, not argued:

- **The disconnect semantics** — refuted by `rtrb_sticky_dc`, which added them
  to the hand-rolled loop at no measurable cost (above). This cell was added
  mid-session precisely to split "semantics" from "packaging".
- **A non-inlined call per item** — neither `sharded::Receiver::try_recv` nor
  `spsc::Receiver::try_recv` appears as a symbol in the bench binary
  (`nm -C`); both are fully inlined.
- **False sharing inside `Shared`** — `Shared` is `repr(Rust)` and reorders,
  so the real offsets were probed with `offset_of!`: `tail` alone at 0,
  `head` alone at 64, `disconnected`/`strategy` at 208/209. The two hot
  counters own their lines, and the line the consumer additionally reads is
  written by neither side under `BusySpin`.
- **An extra executed atomic** — the disassembly does show one `lock orl` in
  the sharded sweep that the rtrb sweep lacks, but it sits behind the
  `cmpb $0x3` testing `strategy == Park`. Under `BusySpin` that branch is
  never taken. Instruction and memory-operand counts are otherwise within one
  of each other: 96/38 against 97/37.
- **The producer's per-send `disconnected.load(Acquire)`** — the leading
  hypothesis after the above, and refuted by the ablation in §4.

## 4. The ablation, and why it settles nothing

Four builds, each measuring `sharded` and `rtrb_sticky_dc` in the same binary
so the gap is computed within-session, pinned to the 16 physical cores
(`raw/.../ablate.*.txt`). Melem/s:

| variant | sharded p2 | sharded p4 |
|---|---:|---:|
| baseline | 147.4 | 145.6 |
| `disc` — producer's `disconnected` load removed | 148.7 | 142.9 |
| `strategy` — the `strategy == Park` load removed from both paths | 96.5 | 98.0 |
| both | 92.5 | 93.8 |

**The hypothesis is refuted.** Removing the producer's per-send `disconnected`
load moved nothing.

**And the method is confounded.** Removing the `strategy` load made the code
**34% slower**, which cannot be read as that load having been expensive. The
disassembly says why: baseline `sweep_sharded` is 96 instructions and the
ablated variant is **99**. Deleting the branch changed LLVM's inlining and
scheduling, so the variant is not "baseline minus one load" but a differently
compiled function. Micro-ablation is the wrong instrument on code this
sensitive to layout — consistent with `2026-08-12-layout-sensitivity.md`.

**The effect itself is not stable.** The gap measured 1.33x and 1.38x in the
bake-off session and 1.12x and 1.28x in the ablation session's baseline, on
the same nominal configuration and a different instance. A meaningful share of
what §2 called overhead is host-and-session variance, and the distance-scaling
story in §2 rests on four points that this instability does not support as
firmly as it first appeared.

## Conclusion

The external baseline is established and is the useful output of this session:
`sharded` beats `crossbeam::Select` by 20–30x, the sticky sweep beats the
naive one by up to 13x, and the semantics `sharded` adds over raw rings are
free.

`sharded` also trails a hand-rolled sweep by something in the range 1.1–1.4x,
and **this session did not find out why.** Five mechanisms were eliminated,
the sixth test was confounded by codegen sensitivity, and the effect moves
between sessions by nearly as much as the effect itself.

The next step is not another cause hunt. It is a **resolution budget for this
cell** — repeated identical-build runs to establish the minimum detectable
effect, in the manner of `2026-08-12-resolution-budgets-rig.md` — so that a
future investigation knows whether a 1.2x gap is a signal at all before
anyone spends a rig session explaining it.

## Limits

- Three instances across the session; absolutes never compare between them.
- 3 rounds per point, not 5.
- `BusySpin` only, `u64` payload, 2 and 4 producers only.
- `rtrb_roundrobin` at `full` p2 carries a 12.3–21.0 spread, the widest cell
  in the group; its 13x claim is an order of magnitude, not a point ratio.
- The ablation builds changed generated code beyond the intended edit, so no
  ablation row supports a causal claim in either direction.
