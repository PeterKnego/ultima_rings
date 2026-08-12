# Resolution budgets on the 16-core rig: the machines are good at opposite things

**Date:** 2026-08-12
**Host:** AWS c7i.8xlarge — Xeon Platinum 8488C, 16 physical cores, THP off
**Method:** same source built at five function alignments
(`-C llvm-args=-align-all-functions=` ∈ {0,3,4,5,6}), each in its own
`CARGO_TARGET_DIR` so switching costs no rebuild. All five built to completion
first, then **three rounds interleaved across alignments**. Two topology points:
`full` (16 cores) and `smt2x2` (4 CPUs on 2 physical cores, the old VM's shape).
Script: `bench-infra/remote/layout.sh`.

Supersedes `2026-08-12-layout-sensitivity.md` for work done on the rig. That
document's budgets remain correct for the 4-vCPU VM and are still the right ones
to use there.

## Final budgets

`layout` is the range across the five per-alignment means; `noise` is the mean
within-alignment range across rounds, which involves no rebuild. MDE is the
minimum effect worth believing.

### `full` — 16 physical cores

| cell | layout | noise | MDE | |
|---|---:|---:|---:|---|
| `bakeoff_mpsc/ultima` | 2.0% | 1.7% | **2%** | noisy |
| `backoff_isolation/busyspin_block` | 2.1% | 1.7% | **3%** | |
| `backoff_isolation/busyspin_poll` | 2.4% | 1.6% | **3%** | |
| `bakeoff_mpsc/thingbuf` | 8.9% | 7.2% | 11% | noisy |
| `bakeoff_mpsc/thingbuf_ref` | 9.4% | 11.1% | 13% | noisy |
| `spsc/busy_spin_pipelined` | 13.9% | 14.3% | 17% | noisy |
| `bakeoff_mpsc/crossbeam` | 18.1% | 18.3% | 22% | noisy |
| `backoff_isolation/park_block` | 21.1% | 16.6% | 25% | |
| `bakeoff_mpsc/flume` | 28.5% | 36.0% | 43% | noisy |
| `bakeoff_mpsc/kanal` | 34.3% | 14.4% | 41% | |
| `backoff_isolation/park_poll` | 42.0% | 52.0% | **62%** | noisy |

### `smt2x2` — 4 CPUs on 2 physical cores

| cell | layout | noise | MDE | |
|---|---:|---:|---:|---|
| `backoff_isolation/busyspin_poll` | 1.5% | 2.0% | **2%** | noisy |
| `bakeoff_mpsc/ultima` | 2.4% | 1.7% | **3%** | |
| `backoff_isolation/busyspin_block` | 2.5% | 2.8% | **3%** | noisy |
| `bakeoff_mpsc/kanal` | 11.2% | 11.0% | 13% | noisy |
| `bakeoff_mpsc/flume` | 6.2% | 15.5% | 19% | noisy |
| `bakeoff_mpsc/thingbuf_ref` | 16.3% | 17.1% | 21% | noisy |
| `bakeoff_mpsc/thingbuf` | 22.0% | 22.6% | 27% | noisy |
| `spsc/busy_spin_pipelined` | 31.0% | 24.3% | 37% | |
| `bakeoff_mpsc/crossbeam` | 14.5% | 33.6% | 40% | noisy |
| `backoff_isolation/park_block` | 35.6% | 44.0% | 53% | noisy |
| `backoff_isolation/park_poll` | 13.8% | 78.7% | **94%** | noisy |

## 1. Layout is no longer separable from noise

On the VM, three cells had layout spread roughly 3x their own run-to-run
variance, which is what justified calling layout a real ~5% effect. **On the rig
almost every cell is flagged `noisy`** — layout spread and intrinsic noise are
the same size.

That is exactly the pattern expected under a null hypothesis of no layout effect.
Each alignment mean is an average of three samples, so its standard error is
about `noise/√3`; spreading five such means produces roughly noise-scale spread
whether or not code placement matters at all.

**So on this rig, at this sample size, no layout effect is demonstrable for any
cell.** That does not prove layout is irrelevant here — it means the experiment
cannot see it above the machine's own variance, and a bigger one would be needed
to try. Practically it does not matter: what a comparison needs is the MDE
column, and MDE is the larger of the two either way.

This also puts a caveat on the earlier VM study, which used two rounds rather
than three and so had a noisier estimate of exactly this separation.

## 2. The two machines are good at opposite things

| cell | VM MDE | rig `full` | rig `smt2x2` |
|---|---:|---:|---:|
| `busyspin_poll` | ~6% | **3%** | **2%** |
| `busyspin_block` | ~6% | **3%** | **3%** |
| `park_poll` | ~6% | 62% | **94%** |
| `park_block` | ~11% | 25% | 53% |
| `spsc` | ~9% | 17% | 37% |

**Spin-path cells are 2–3x tighter on the rig. Park-path cells are 6–15x
worse.** `park_poll` is the extreme: a clean ~6% cell on the VM and a 62–94%
cell here, meaning it can resolve essentially nothing.

The likely reason is that `Park` is syscall- and scheduler-bound rather than
CPU-bound. Futex wake latency and placement decisions vary far more on a 16-core
shared-tenancy instance than on a 2-core VM, and pinning with `taskset` does not
insulate a workload from the host's scheduling. Not measured directly.

Note the degradation is not a core-count effect: `smt2x2` pins to 4 CPUs and is
*worse* than `full`. It is a property of the machine, not of how much of it the
benchmark is allowed to use.

## 3. Consequences

**Use the VM for `Park` work, and the rig for spin-path work.** This inverts the
assumption behind moving to the rig at all. In particular **task #26 — reopening
the `Park` blocking-path gap — should not run here.** That question depends on
`park_block`, whose budget is 25% on the rig against 11% on the VM. The failed
pre-park-spin gate needs a *lower*-variance machine, not a bigger one.

**Competitor cells cannot support two-decimal ratios.** `crossbeam` has an MDE of
22–40%, `flume` 19–43%, `kanal` 13–41%. Bake-off ratios computed against
crossbeam inherit that.

**One claim is withdrawn as unestablished.** `2026-08-12-topology-sweep.md`
states that this crate's lead over crossbeam "grows with the machine: 1.24x on
the VM, 1.59x at 16 cores". Those differ by 28%, which is inside crossbeam's
22% MDE on the rig. The measurements stand; the *trend* does not. What survives
is the direction — `ultima` leads crossbeam at every topology measured — and
that is well clear of the budget because `ultima`'s own cell is tight at 2–3%.

**The crate's own cells are the good news.** `ultima`, `busyspin_poll` and
`busyspin_block` sit at 2–3% MDE at both topologies, better than anything the VM
offered. A/Bs on the ring's spin path can now resolve effects half the size that
were previously possible, which is directly useful for the CAS-backoff and
colocated-slot class of change.

## Limits

- **Three rounds, five alignments, one host, one run.** The MDE figures are
  themselves estimates from a small sample and should be read as
  "a few percent", "roughly a fifth", "unusable".
- **Shared tenancy.** A dedicated-CPU instance would likely narrow the `Park`
  cells; that hypothesis is untested and would change the conclusion in §3.
- **`-align-all-functions` only** perturbs function entry alignment, not basic
  block placement, inlining or data layout. It remains a lower bound on total
  layout sensitivity.
- **No allocation-bound cell is included.** `bakeoff_mpsc_string` was left out,
  and given the arena confound in `2026-08-12-topology-sweep.md` it would need
  `MALLOC_ARENA_MAX` pinned before its budget meant anything.
