# MPSC Colocated Slot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the MPSC availability round from a parallel `avail` array into the ring slot itself, cutting the data path from two ping-ponging cache lines per element to one — then measure whether it pays, and revert if it does not.

**Architecture:** `Shared<T>`'s `buf: Box<[UnsafeCell<MaybeUninit<T>>]>` and `avail: Box<[AtomicI64]>` become a single `slots: Box<[Slot<T>]>`, where `Slot<T>` is `#[repr(C)] { round: AtomicI64, value: UnsafeCell<MaybeUninit<T>> }`. The claim protocol, round semantics, memory orderings and public API are all unchanged — only the address of the round moves.

**Tech Stack:** Rust edition 2024 (stable), loom 0.7 for model checking, miri for UB detection, criterion 0.5 for benches.

**Spec:** `docs/superpowers/specs/2026-08-09-colocated-slot-design.md`

## Global Constraints

- **This is a refactor, not a feature.** No public API changes, no behaviour changes. **The existing tests and loom models must pass with ZERO edits.** If you find yourself wanting to change a test or a model to make it pass, stop and report BLOCKED — that means an ordering edge moved, which this change must not do.
- **`src/mpsc.rs` only.** `src/spsc.rs`, `src/sharded.rs`, `src/wait.rs`, `src/notify.rs` and all test files are untouched.
- **No `#[repr(align(64))]` on `Slot`.** Aligning each slot to its own cache line is the padding experiment that was already measured and rejected (`docs/bench-results/2026-08-09-mpsc-perf-v2.md`). It would defeat the purpose of this change.
- **`#[repr(C)]` with `round` first is required**, not stylistic. It pins the round at offset 0 so a large `T` still shares its first line with the round.
- **Orderings are unchanged**: publish is slot-write then `Release` store of the round; consume is `Acquire` load of the round then slot read.
- **No new dependencies.**
- `cargo clippy --all-features --all-targets -- -D warnings`, `cargo fmt --check`, and `cargo doc` with no warnings must all pass.

## File Structure

| File | Responsibility |
|---|---|
| `src/mpsc.rs` | **Modify.** All seven change sites: the `Slot` type, `Shared` fields, `channel()`, `Shared::drop`, `try_send`, `slot_published`, `try_recv`, `drain`, plus the module doc and the `unsafe impl` SAFETY comment. |
| `docs/bench-results/2026-08-09-colocated-slot.md` | **Create in Task 2.** Measurement and keep/revert verdict. |
| `docs/design.md` | **Modify in Task 2.** §8 gains the outcome; §9 gains the layout-vs-protocol distinction; §10's stale division row is corrected. |

---

### Task 1: Colocate the round into the slot

**Files:**
- Modify: `src/mpsc.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks (this is the first)
- Produces: private `struct Slot<T> { round: AtomicI64, value: UnsafeCell<MaybeUninit<T>> }`; `Shared<T>` field `slots: Box<[Slot<T>]>` replacing `buf` and `avail`. No public signature changes — `channel`, `Sender::try_send`, `Sender::send`, `Receiver::try_recv`, `Receiver::recv`, `Receiver::drain` all keep their exact current signatures.

> **On TDD for this task:** there is no RED phase, because no behaviour is being added. The guard is the *existing* suite, and in particular `survives_many_ring_wraps` (already in `src/mpsc.rs`'s test module), which drives 50 ring wraps at cap 4 through both the single-item and `drain` consume paths. A mis-wired slot or a wrong round is invisible until the ring wraps and slot indices repeat, which is exactly what that test forces. Run it first so you know it passes before you start.

- [ ] **Step 1: Confirm the guard passes before you change anything**

Run: `cargo test --lib mpsc 2>&1 | tail -20`
Expected: PASS, including `survives_many_ring_wraps`. If anything fails here, stop — the tree was not clean when you started.

- [ ] **Step 2: Add the `Slot` type and change `Shared`'s fields**

In `src/mpsc.rs`, replace the `Shared<T>` struct definition's first three lines. Change:

```rust
pub(crate) struct Shared<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Per-slot published round number (`seq >> shift`); -1 = never published.
    avail: Box<[AtomicI64]>,
    mask: usize,
```

to:

```rust
/// One ring slot: the availability round and its payload in a single struct, so
/// that a publish or a consume touches ONE cache line rather than two.
///
/// `repr(C)` with `round` first is load-bearing, not decoration. It pins the
/// round at offset 0, so for a large `T` whose value spans several lines the
/// round still shares a line with the *start* of the value — which is what the
/// consumer reads first. Reordering these fields, or adding `align(64)`,
/// silently discards the only reason this type exists. (`align(64)` in
/// particular was measured as the separate "padding" lever and rejected:
/// docs/bench-results/2026-08-09-mpsc-perf-v2.md.)
#[repr(C)]
struct Slot<T> {
    /// Published round number (`seq >> shift`); -1 = never published.
    round: AtomicI64,
    value: UnsafeCell<MaybeUninit<T>>,
}

pub(crate) struct Shared<T> {
    slots: Box<[Slot<T>]>,
    mask: usize,
```

- [ ] **Step 3: Update `channel()`'s construction**

Change:

```rust
    let buf = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let avail = (0..cap)
        .map(|_| AtomicI64::new(-1))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        buf,
        avail,
        mask: cap - 1,
```

to:

```rust
    let slots = (0..cap)
        .map(|_| Slot {
            round: AtomicI64::new(-1),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        slots,
        mask: cap - 1,
```

- [ ] **Step 4: Update `Shared::drop`'s prefix walk**

Change:

```rust
        let mut seq = self.head.0.load(Ordering::Acquire);
        loop {
            let slot = seq & self.mask;
            if self.avail[slot].load(Ordering::Acquire) != (seq >> self.shift) as i64 {
                break;
            }
            // SAFETY: published and never consumed.
            self.buf[slot].with_mut(|p| unsafe { (*p).assume_init_drop() });
            seq += 1;
        }
```

to:

```rust
        let mut seq = self.head.0.load(Ordering::Acquire);
        loop {
            let slot = &self.slots[seq & self.mask];
            if slot.round.load(Ordering::Acquire) != (seq >> self.shift) as i64 {
                break;
            }
            // SAFETY: published and never consumed.
            slot.value.with_mut(|p| unsafe { (*p).assume_init_drop() });
            seq += 1;
        }
```

- [ ] **Step 5: Update `try_send`'s write-and-publish**

Change:

```rust
        // SAFETY: bounded claim — this slot's previous occupant was consumed;
        // CAS made us its unique writer for this round.
        sh.buf[seq & sh.mask].with_mut(|p| unsafe {
            (*p).write(v);
        });
        // Release pairs with the consumer's Acquire load of avail.
        sh.avail[seq & sh.mask].store((seq >> sh.shift) as i64, Ordering::Release);
```

to:

```rust
        let slot = &sh.slots[seq & sh.mask];
        // SAFETY: bounded claim — this slot's previous occupant was consumed;
        // CAS made us its unique writer for this round.
        slot.value.with_mut(|p| unsafe {
            (*p).write(v);
        });
        // Release pairs with the consumer's Acquire load of this same slot's
        // round — now one cache line rather than two.
        slot.round.store((seq >> sh.shift) as i64, Ordering::Release);
```

- [ ] **Step 6: Update `slot_published` and `try_recv`'s value read**

Change `slot_published`:

```rust
        sh.avail[seq & sh.mask].load(Ordering::Acquire) == (seq >> sh.shift) as i64
```

to:

```rust
        sh.slots[seq & sh.mask].round.load(Ordering::Acquire) == (seq >> sh.shift) as i64
```

Then, further down in `try_recv`, change:

```rust
        // SAFETY: published (Acquire-observed) and consumed exactly once.
        let v = sh.buf[self.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
```

to:

```rust
        // SAFETY: published (Acquire-observed) and consumed exactly once.
        let v = sh.slots[self.head & sh.mask]
            .value
            .with(|p| unsafe { (*p).assume_init_read() });
```

- [ ] **Step 7: Update `drain`'s hot loop**

Change:

```rust
        let mask = sh.mask;
        let shift = sh.shift;
        let buf = &sh.buf;
        let avail = &sh.avail;
        let mut count = 0usize;
```

to:

```rust
        let mask = sh.mask;
        let shift = sh.shift;
        let slots = &sh.slots;
        let mut count = 0usize;
```

and change:

```rust
        while count < max {
            let seq = *guard.head;
            let slot = seq & mask;
            // SAFETY: slot is within bounds; avail[slot] is initialized.
            if avail[slot].load(Ordering::Acquire) != (seq >> shift) as i64 {
                break;
            }
            // SAFETY: as in try_recv.
            let v = buf[slot].with(|p| unsafe { (*p).assume_init_read() });
```

to:

```rust
        while count < max {
            let seq = *guard.head;
            let slot = &slots[seq & mask];
            // SAFETY: slot is within bounds; its round is initialized.
            if slot.round.load(Ordering::Acquire) != (seq >> shift) as i64 {
                break;
            }
            // SAFETY: as in try_recv.
            let v = slot.value.with(|p| unsafe { (*p).assume_init_read() });
```

- [ ] **Step 8: Update the module doc and the `unsafe impl` SAFETY comment**

Change the module doc line:

```rust
//! Publish: slot write → `avail[slot] = seq >> shift` (Release; -1 = never),
//! where `shift = log2(cap)` — the round number, computed without a division.
```

to:

```rust
//! Publish: slot write → `slots[i].round = seq >> shift` (Release; -1 = never),
//! where `shift = log2(cap)` — the round number, computed without a division.
//! The round lives inside the slot beside its payload, so a publish or consume
//! touches one cache line rather than two (see docs/design.md §8).
```

Change the SAFETY comment above `unsafe impl<T: Send> Send for Shared<T>`:

```rust
// SAFETY: each slot is written by exactly one claimer (CAS gives disjoint
// sequences) before its Release avail-store, and read by the single consumer
// after the matching Acquire load; the bounded claim guarantees the previous
// occupant was consumed before the slot is rewritten. T: Send suffices.
```

to:

```rust
// SAFETY: each slot is written by exactly one claimer (CAS gives disjoint
// sequences) before that slot's Release round-store, and read by the single
// consumer after the matching Acquire load of the same slot's round; the
// bounded claim guarantees the previous occupant was consumed before the slot
// is rewritten. T: Send suffices.
```

- [ ] **Step 9: Verify it compiles and the existing tests pass UNCHANGED**

Run: `cargo test --all-features 2>&1 | grep -E "^test result|^error"`
Expected: 51 passed, 0 failed, across all targets.

**You must not have edited any test.** Run `git diff --stat` and confirm `src/mpsc.rs` is the only file changed. If a test needed editing, stop and report BLOCKED.

- [ ] **Step 10: Run the loom lane**

Run: `RUSTFLAGS="--cfg loom" cargo test --test loom --release 2>&1 | grep -E "^test result|^error"`
Expected: 5 passed, 0 failed.

This is the substantive check, not a formality: the models exercise exactly the Release/Acquire pairing this change relocates. An unchanged model suite passing is the evidence that the ordering edge survived the move.

- [ ] **Step 11: Run miri**

Run: `cargo +nightly miri test --all-features 2>&1 | grep -E "^test result|Undefined Behavior|^error"`
Expected: all lanes pass, 0 UB. One test is expected to show as ignored (`yielding_ladder_does_not_sleep`, which is `cfg_attr(miri, ignore)`d).

- [ ] **Step 12: Lints, formatting and docs**

Run: `cargo clippy --all-features --all-targets -- -D warnings && cargo fmt --check && cargo doc --no-deps --all-features 2>&1 | grep -ci "^warning"`
Expected: clippy silent, fmt clean, `0` rustdoc warnings.

- [ ] **Step 13: Commit**

```bash
git add src/mpsc.rs
git commit -m "perf(mpsc): colocate the availability round with its payload

buf and avail become one slots: Box<[Slot<T>]>, where Slot is
repr(C) { round: AtomicI64, value: UnsafeCell<MaybeUninit<T>> }.

Layout change only: the claim protocol, round semantics, memory orderings,
unsafe operations and public API are all unchanged — only the address of the
round moves. A publish previously wrote buf[i] on one cache line and avail[i]
on another; both now live in one slot, so the producer-to-consumer data path
ping-pongs one line per element instead of two.

The existing 51 tests and 5 loom models pass with zero edits. That is the
substantive evidence: the models drive exactly the Release/Acquire pairing this
change relocates, so a broken edge would surface there.

Not yet measured — the keep/revert decision is gated on improving at all three
mpsc_layout_probe configurations."
```

---

### Task 2: Measure, decide, and record

**Files:**
- Create: `docs/bench-results/2026-08-09-colocated-slot.md`
- Modify: `docs/design.md` (§8, §9, §10)
- Possibly revert: `src/mpsc.rs` (if the gate fails)

**Interfaces:**
- Consumes: the colocated `Slot<T>` layout from Task 1
- Produces: the keep/revert verdict — no code that later tasks depend on

- [ ] **Step 1: Confirm the box is quiet and build to completion**

Run: `vmstat 2 3 | tail -2` then `cargo bench --no-run`
Expected: 85%+ idle with 0 runnable in the `id` and `r` columns, and a completed build.

Do **not** use load average to judge quietness — it lags badly on this machine and has read above 2.0 while `vmstat` showed 90% idle. Do not let a build overlap a measurement run.

- [ ] **Step 2: Measure in interleaved A-B-A blocks, not all-of-one-then-all-of-the-other**

Box conditions drift over the minutes a full comparison takes, so measuring every colocated run and then every baseline run would confound the change with the drift. Measure in three blocks: colocated, baseline, colocated.

Use this helper to read criterion's saved estimates rather than parsing console output — the console form is easy to mis-parse when criterion also prints change percentages:

```bash
read_cells() {
  for c in cap1024_p2 cap4096_p2 cap1024_p4; do
    f="$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"])')/criterion/mpsc_layout_probe/$c/new/estimates.json"
    ns=$(python3 -c "import json;print(json.load(open('$f'))['mean']['point_estimate'])")
    python3 -c "print(f'  $c: {100000/($ns/1e9)/1e6:.2f} Melem/s')"
  done
}
```

**Block A1 (colocated)** — the tree as Task 1 left it. Three runs:

```bash
for i in 1 2 3; do
  echo "colocated-A run $i"
  cargo bench --bench throughput -- 'mpsc_layout_probe' >/dev/null 2>&1
  read_cells
done
```

- [ ] **Step 3: Block B (baseline) — stash ONLY `src/mpsc.rs`**

```bash
git stash push -- src/mpsc.rs
grep -c "struct Slot<T>" src/mpsc.rs   # MUST print 0 before you proceed
```

**Stashing only that path matters.** A plain `git stash` also stashes any uncommitted bench code, which makes the criterion filter match nothing; the run then silently re-reports the *previous* run's saved `estimates.json`, and you get byte-identical numbers that look like a result. **If two runs come back identical to two decimal places, that is the bug, not a measurement** — stop and check what is actually in the tree.

Three runs with the same loop and `read_cells` as Step 2. These are the baseline numbers.

- [ ] **Step 4: Block A2 (colocated again) — restore and re-measure**

```bash
git stash pop
grep -c "struct Slot<T>" src/mpsc.rs   # MUST print 1 before you proceed
```

Three more runs. Combining A1 and A2 gives six colocated samples against three baseline samples, with the baseline block sandwiched — so a monotonic drift in box conditions shows up as a difference between A1 and A2 rather than masquerading as an effect. If A1 and A2 disagree by more than their own spread, the box was not stable enough and the comparison is void; wait and redo.

- [ ] **Step 5: Apply the gate**

Compute, for each of the three cells, the mean of the six colocated runs (blocks A1 + A2) against the mean of the three baseline runs, plus each cell's run-to-run spread (max − min, as a percentage of the mean). Check A1 against A2 first: if they disagree by more than their spread, the box drifted and the comparison is void.

**Keep only if colocation improves all three cells by more than that cell's own spread.**

This all-three requirement is the direct lesson of the padding round, which cleared a single-cell gate at +3.5% with p < 0.01 and then measured −0.1% at cap 4096 (`docs/bench-results/2026-08-09-mpsc-perf-v2.md`). Do not weaken it because two cells out of three look good.

- [ ] **Step 6: If the gate FAILS, revert the code**

```bash
git revert --no-edit <Task 1 commit sha>
cargo test --all-features 2>&1 | grep -E "^test result"
```

Expected: 51 passed after the revert. A failing gate is a result, not a defeat — record it in full in Step 6.

- [ ] **Step 7: Write the results document**

Create `docs/bench-results/2026-08-09-colocated-slot.md` with this structure, filling every `<...>` from your own runs:

```markdown
# MPSC colocated slot: measurement

**Date:** 2026-08-09
**Hardware:** 4-core Linux VM, 15 GiB RAM, no swap; box verified quiet by `vmstat`
(<idle>% idle, 0 runnable), built to completion before measuring
**Spec:** `docs/superpowers/specs/2026-08-09-colocated-slot-design.md`

## What changed

`buf` and `avail` became one `slots: Box<[Slot<T>]>`, so a publish writes the
payload and its round into one cache line instead of two. Layout only —
orderings, claim protocol and API unchanged.

## Results

Interleaved A-B-A blocks, `mpsc_layout_probe` — six colocated samples, three baseline:

| Cell | baseline (mean) | colocated (mean) | delta | cell spread |
|---|---:|---:|---:|---:|
| cap1024_p2 | <n> | <n> | <±x%> | <x%> |
| cap4096_p2 | <n> | <n> | <±x%> | <x%> |
| cap1024_p4 | <n> | <n> | <±x%> | <x%> |

## Verdict

<KEPT or REVERTED, and which cells passed or failed the gate.>

## What this shows about where the MPSC cost is

<If KEPT: the cache-line hypothesis from the hot-path analysis is supported,
and by how much.>

<If REVERTED: with the division removed (no effect), false sharing addressed
(no effect), and colocation rejected too, the gap has now survived every
layout and arithmetic hypothesis this crate has proposed. That points at the
claim protocol itself, leaving the batched claim as the remaining candidate —
and it means design.md §8's account of the cost was wrong in a third way.>
```

- [ ] **Step 8: Update `docs/design.md`**

**If the gate KEPT the change, `docs/design.md` currently describes a layout that no longer exists**, and the module doc added in Task 1 points a reader straight at it. The Task 1 reviewer enumerated the stale sites; all of them must be corrected, not just §8:

| Location | What is stale |
|---|---|
| `design.md:57` (§1) | "a per-slot availability array (`avail: Box<[AtomicI64]>`)" |
| `design.md:81-89` (§1) | the wrap/ABA argument, keyed on the separate array |
| `design.md:106-107` (§2) | **the normative ordering table** — two rows keyed on `mpsc avail[slot]` |
| `design.md:340-352` (§6) | the drop-drain argument |
| `design.md:417`, `:434` (§7) | further `avail` references |
| `benches/throughput.rs` (`mpsc_layout_probe` banner) | comments describe "an `avail`-array layout change" and "an unpadded avail array is 8 KiB" |

§2's table is the authority on the publication edge, so a stale row there is the most consequential of these. Rewrite each to `slots[i].round` / `Slot<T>`.

**If the gate REVERTED the change**, all of the above is already accurate and needs no edit — check rather than assume.

Then, whichever way the gate landed:

1. **§8** — add the colocation outcome next to the padding outcome already recorded there. If reverted, state that a third hypothesis has now failed and the dominant cost remains unidentified. If kept, state the measured gain, and rewrite the "false-sharing reality of the interleaved availability array" framing — that paragraph becomes the *motivation* for the change rather than a standing cost.

2. **§9** — add a sentence to the "Vyukov per-slot stamps" paragraph distinguishing layout from protocol: this crate now colocates (or measured and rejected colocating) the round with the payload, which is a *layout* change, while §9's rejection is of the *protocol* that folds readiness into an atomic also encoding the claim — a coupling this design never adopts.

3. **§10** — the pitfall-checklist row for heapless's division-regression class still reads "MPSC's availability-round computation (`seq / cap`) does execute as a runtime division on the hot publish/consume path (a known v1 cost, noted in §7); the v2 optimization is precomputed shifts." That is stale: the shift shipped in `170318d` and §7 was updated while §10 was missed. Rewrite it to say the round is computed as `seq >> shift` with `shift` cached at construction, so the path is division-free.

- [ ] **Step 9: Final verification**

Run: `cargo test --all-features 2>&1 | grep -E "^test result"` and `cargo fmt --check`
Expected: 51 passed, fmt clean.

- [ ] **Step 10: Commit**

```bash
git add docs/bench-results/2026-08-09-colocated-slot.md docs/design.md
git commit -m "docs: colocated-slot measurement and verdict

<One-line verdict.> Gate required improvement at all three mpsc_layout_probe
configurations; records which passed. design.md §8 gains the outcome, §9 gains
the layout-vs-protocol distinction, §10's stale division row corrected."
```

---

## Notes for the implementer

**Do not add a layout-assertion test.** It was considered and explicitly declined as the verification bar for this change; the existing suite is the agreed check. The `repr(C)` comment in Step 2 carries the warning instead.

**If the gate fails, that is a publishable result, not a failure to hide.** Two levers have already been tried and rejected on this path and both are recorded in full. Write the third the same way.
