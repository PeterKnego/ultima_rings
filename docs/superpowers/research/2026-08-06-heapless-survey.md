# Survey: `heapless::spsc::Queue` — informing `ultima_rings` v1

**Date:** 2026-08-06
**Method:** `git clone --depth 1 https://github.com/rust-embedded/heapless` into scratchpad,
read `src/spsc.rs` + `src/storage.rs` + `CHANGELOG.md` in full; shallow clone has no history,
so soundness/API history was reconstructed from `gh api`/`gh pr view`/`gh issue view` against
`rust-embedded/heapless` (issues, PRs, commit bodies) rather than local `git log`.
**Scope:** read-only. Nothing in `ultima_rings` source was touched. This survey is advisory
input to the already-**Approved** `docs/superpowers/specs/2026-08-06-ultima-rings-v1-design.md`,
which already committed to runtime capacity + `& (cap-1)` masking — the findings below are used
to *validate that choice with evidence* and flag concrete follow-ups, not to relitigate it.

---

## 1. Const-generic capacity (`Queue<T, N>`) — ergonomics vs. cost

heapless's `Queue<T, const N: usize>` is `type Queue<T, const N: usize> = QueueInner<T, OwnedStorage<N>>`
(`src/spsc.rs:133`), where `OwnedStorage<N>` stores `[T; N]` inline (stack/static, zero
allocation, `N` known to the optimizer at the call site). This is genuinely the best case for a
single monomorphic instantiation: `capacity()`, `increment()`, and the mask/modulo all fold to
constants when `N` is a literal.

**But heapless does not actually expose `Queue<T, N>` to most consumer code.** `Queue::split()`
returns `Producer<'a, T>` / `Consumer<'a, T>` — **no `N` parameter at all** (confirmed in the
current source, `src/spsc.rs:761-772`). This is deliberate: `CHANGELOG.md` v0.9.1 records
*"Removed generic from `spsc::Consumer`, `spsc::Producer` and `spsc::Iter`"*, done in
[PR #571/#590 "De-monomorphize spsc consumer and producer"](https://github.com/rust-embedded/heapless/pull/590).
The mechanism (`src/storage.rs`) is a sealed `Storage` trait with two impls: `OwnedStorage<N>`
(`Buffer<T> = [T; N]`) and `ViewStorage` (`Buffer<T> = [T]`, unsized). `Queue<T,N>` coerces/erases
to `QueueView<T> = QueueInner<T, ViewStorage>` via `.as_view()`/`.split()`, and `Producer`/
`Consumer` hold `&QueueView<T>`, i.e. `N` becomes a **runtime** slice length the moment you split.

**Why they erased it:** code-size. Before this, every distinct `N` used in a firmware image
generated a fresh monomorphized copy of every `Producer<T,N>`/`Consumer<T,N>` method — expensive
on flash-constrained embedded targets, which is heapless's primary audience. This is exactly the
"cost propagates through signatures" problem the task asks about: a function generic over
`Producer<T, N>` (or a struct holding one) monomorphizes per `N`, so a router touching several
differently-sized rings (comparable to `uc2_net` routing `NetEvent`/`CtrlMsg`/
`HandshakeDatagram`) either monomorphizes per distinct `(T, N)` pair or has to type-erase `N`
manually before calling shared logic.

**The erasure has a real, measured cost, and heapless had to pay it back explicitly.**
[Issue #650](https://github.com/rust-embedded/heapless/issues/650) ("The %(rem) operation seems
to cause unexpected overhead"): *"an unexpectedly high number of `__aeabi_uidivmod` calls, most
of which originate from `QueueInner::increment` and `QueueInner::len`... after `split`, the
internal storage uses slice types, preventing the compiler from propagating length information
and optimizing away the division operations."* I.e.: erasing `N` from the type didn't just cost
monomorphization savings, it **silently reintroduced a division in the hot path** even when the
caller's `N` was in fact a compile-time constant — the optimizer simply couldn't see through the
`&[T]` any more. Fixed in
[PR #652 "Remove the modulo operations in spsc"](https://github.com/rust-embedded/heapless/pull/652)
(shipped v0.9.3): *"leverages the fact that `head` and `tail` are always kept lower than N to
replace the modulo operations with a simple if, which gets optimized pretty well by the compiler
and no branch is left."* A follow-up commit in the same PR then had to further harden that
branch-rewrite against integer overflow at `N` near `usize::MAX` (see §3).

**Verdict for `ultima_rings` v1: runtime capacity, no const-generic, is correct — and heapless's
own history is the evidence, not just an analogy.** heapless is proof that (a) const-generic
capacity in the *type* of your producer/consumer is an ergonomics and code-size liability the
moment consumer code wants to be generic or handle multiple rings, forcing type erasure anyway;
and (b) that erasure is not free — it can silently regress a masked/constant-folded index update
back into a runtime division unless you go out of your way (as `#652` did) to write the index
math so it can't lower to `div`. `ultima_rings`'s spec already starts from runtime `cap` + `&
(cap-1)` masking (never `%`), so it structurally cannot hit heapless's `#650` regression (`AND`
never lowers to `idiv` the way `%` can), and it never had the const-generic ergonomics problem to
begin with. A const-generic `Queue<T, N>` wrapper is **YAGNI for v1** — it would only make sense
later, and only as a thin newtype over the runtime core for callers who want a compile-time
capacity *and* are willing to accept the signature propagation cost, exactly mirroring heapless's
`OwnedStorage<N>`/`ViewStorage` split. Build that later, if ever, don't design it in now.

## 2. Index handling, atomics, and no_std constraints

- **`spsc::Queue` is `usize`-only.** `CHANGELOG.md` v0.7.0: *"spsc::Queue is now usize only"* and
  *"`MultiCore`/`SingleCore` and `Uxx` is now removed from `spsc::Queue`"* — an earlier version
  let you pick a smaller index type (`u8`/`u16`) to shrink the struct on 8/16-bit MCUs; it was
  removed. Unlike `Vec`/`String` (which still have an opt-in `LenType` for a smaller length), spsc
  indices are always `AtomicUsize` today. `ultima_rings` (std-only, 64-bit-class targets in
  practice) has no reason to chase smaller-atomics; noting it only because it's a capability
  heapless deliberately walked back, i.e. not worth adding speculatively.
- **Only atomic load/store is required, not CAS**, confirmed both by the module doc (`src/spsc.rs:1-5`:
  *"This module requires atomic load and store instructions... emulated by
  [`portable-atomic`]"*) and by [issue #466](https://github.com/rust-embedded/heapless/issues/466),
  where a user was confused finding no CAS in the SPSC source at all — correct, because SPSC only
  ever needs one side to `Release`-store its own index and the other side to `Acquire`-load it,
  never a compare-and-swap. This is a materially lower portability bar than CAS (some embedded
  targets, e.g. ARMv6-M/MSP430/`riscv32i`, lack native CAS but do have load/store, or need
  `portable-atomic`'s critical-section emulation only for the load/store case).
- **Relevant if `ultima_rings` ever wants a `no_std` core:** the v1 spec's `spsc.rs` (cache-padded
  head/tail, Acquire/Release publish) needs *only* load/store — a straightforward `no_std` port.
  The v1 spec's **`mpsc.rs` is different**: the 2026-08-06 design amendment makes the MPSC claim a
  *bounded CAS* (producers CAS `seq` after proving `seq − head < cap`), so `mpsc.rs` genuinely
  needs compare-and-swap, not just load/store. Worth stating explicitly in `docs/design.md`: a
  future `no_std` core would be spsc-first (load/store only, broad target support) with mpsc
  needing the stricter CAS bar (narrower target support, or `portable-atomic`'s CAS emulation) —
  mirroring how heapless itself gates `spsc` on `loadstore`/`target_has_atomic_load_store` and
  `mpmc` on `cas`/`target_has_atomic` separately ([issue #271](https://github.com/rust-embedded/heapless/issues/271),
  [#272](https://github.com/rust-embedded/heapless/pull/272), [#273](https://github.com/rust-embedded/heapless/pull/273)).
- heapless is `no_std` by default (`#![cfg_attr(not(test), no_std)]`, `src/lib.rs:127`) with `std`
  used only for tests — a useful existence proof that a lock-free ring core has no inherent
  dependency on `std`, should `ultima_rings` want to shed its std-only constraint later.

## 3. SPSC soundness history — what changed and why

Chronological, from `CHANGELOG.md` plus `gh issue/pr` bodies (shallow clone has no local
history):

| When | What broke / changed | Why it matters here |
|---|---|---|
| v0.3.6 (2018) | *"The capacity of `RingBuffer`. It should be the requested capacity plus not twice that plus one."* | Earliest capacity-arithmetic bug; establishes capacity math as a recurring soundness-adjacent bug class in this design family. |
| v0.5.2 (2020) | *"Fixed edge case in `mpmc::Queue::dequeue` that led to an infinite loop"*; *"Fixed incorrect overflow behavior in computation of capacities"* | Same bug class recurring in the sibling `mpmc` structure. |
| [Issue #207](https://github.com/rust-embedded/heapless/issues/207) (2021) → v0.7.0 | **Root-cause soundness bug**: head/tail were only `wrapping_add`/`wrapping_sub`'d in the index's own (possibly small) integer width, while array access used a real `% N`. Sound only when `N` divides `2^bitwidth`, i.e. only for power-of-two `N` — arbitrary `N` silently corrupted the ring after wraparound. Fix (their option 3 of 4 listed in the issue): **sacrifice one slot** — `Queue<T,N>` now holds `N-1` usable elements, `head == tail` unambiguously means empty, `increment(tail) == head` means full — and drop the small-index-width generic entirely (`spsc::Queue` becomes `usize`-only, per v0.7.0's changelog). | `ultima_rings` mandates power-of-two `cap` up front, so it never has heapless's original problem (non-power-of-two `N`) *and* has no reason to adopt heapless's fix-of-last-resort (the N−1 slot sacrifice) — see §4/R2. [Issue #214](https://github.com/rust-embedded/heapless/issues/214) ("Queue with capacity 1 is both empty and full") is the sharp edge of that N−1 trick still visible in current heapless (`assert!(N > 1)` in `Queue::new()`, `src/spsc.rs:145`). |
| [Issue #314](https://github.com/rust-embedded/heapless/issues/314) (2021) → [PR #323](https://github.com/rust-embedded/heapless/pull/323) | **Aliasing hazard**: the documented pattern `unsafe { Q.split().1 }` called once in `main` and once in an interrupt handler creates two overlapping `&mut Q` borrows (each `Producer`/`Consumer`'s lifetime is tied to the `&mut` used to create it) — flagged as likely Miri-detectable UB. Initially patched by making the doc example "less unsafe" (#323); the structural fix came later via `split_const()` (`src/spsc.rs:502-504,561-563`) — split **once**, in a `const` context, store the owned `Producer`/`Consumer` into a `critical_section::Mutex` for the interrupt handler to `take()` — eliminating the re-split-per-call footgun entirely (see the current module-doc examples). | `ultima_rings`'s v1 API (`channel()` returns owned `Sender`/`Receiver` once, no re-split entry point) already structurally avoids this class of bug — see §4/R3. |
| [Issue #343](https://github.com/rust-embedded/heapless/issues/343) (2022) | User confusion (not a bug) over why `inner_enqueue`'s load of `tail` (the producer's **own** last-written index) is `Ordering::Relaxed` while the load of `head` (the **consumer's** index) is `Ordering::Acquire`. Answer: program order already guarantees the single owner sees its own last write; `Acquire` is only needed to synchronize-with the *other* thread's `Release` store. | This is exactly `ultima_rings`'s stated ordering discipline ("head loads Acquire / stores Release") — worth citing heapless's answer verbatim in `docs/design.md` since it's the most commonly re-asked question about this exact algorithm shape. |
| v0.9.1 (2025) — [PR #571/#590](https://github.com/rust-embedded/heapless/pull/590) "De-monomorphize spsc consumer and producer"; [PR #485](https://github.com/rust-embedded/heapless/pull/485) `QueueView` | Not a soundness fix — a **deliberate type-erasure refactor** (see §1) trading monomorphized code size for a runtime `N`. | Introduced the regression fixed next. |
| [Issue #650](https://github.com/rust-embedded/heapless/issues/650) (2026) → [PR #652](https://github.com/rust-embedded/heapless/pull/652), shipped v0.9.3 | The `N`-erasure above silently reintroduced a runtime `%`/`__aeabi_uidivmod` in `increment()`/`len()` because the erased `[T]` slice hides `N` from the optimizer even for compile-time-constant `N`. Fixed by rewriting the modulo as a branch (`if val >= n { val - n } else { val }`, current `src/spsc.rs:178-188`) that the compiler turns into a `cmov`/`csel`/`it`, no branch, no division. A same-PR follow-up commit then fixed **integer-overflow edge cases** the branch-rewrite introduced: possible panic in `len()` at `N == usize::MAX`, and iterator index overflow (`self.head + self.index`) at `N` near `usize::MAX` — both fixed in v0.9.3 (*"Fix integer overflow leading to a panic in `len` when N == usize::MAX"*, *"Fix integer overflow in iterators when N > usize::MAX/2 and the queue loops"*), with regression tests added directly exercising `Queue::<(), { usize::MAX }>` (`src/spsc.rs:1339-1408` in current source). | Direct precedent for §4/R1: rewriting index math for performance is itself a source of new overflow bugs at the extremes — `ultima_rings` should test its `& (cap-1)` masking at capacity/head/tail values near `usize::MAX`, not just typical small caps. |

## 4. Top 5 recommendations for `ultima_rings`

1. **Keep runtime `cap` + `& (cap-1)` masking, no const-generic `Queue<T, N>`, as already
   decided** — but add a codegen-level regression test/bench asserting the enqueue/dequeue hot
   path never lowers to `idiv`/`__aeabi_uidivmod`. This is not hypothetical caution: it is the
   *exact* bug class heapless shipped and had to patch out in
   [issue #650 / PR #652](https://github.com/rust-embedded/heapless/pull/652), and `ultima_rings`
   starting from a mask instead of a modulo is what prevents it structurally — verify that
   assumption stays true as the code evolves, rather than trusting it by inspection.
2. **Do not adopt heapless's "sacrifice one slot" (`N-1` usable capacity) trick from
   [issue #207](https://github.com/rust-embedded/heapless/issues/207)/v0.7.0** — that trick exists
   only because heapless must support arbitrary, non-power-of-two `N` and needs head==tail to
   unambiguously mean "empty." Since `ultima_rings` mandates power-of-two `cap`, use full-range
   monotonic head/tail counters (`wrapping_add` over the whole `usize` range, masked only at
   buffer-index time) so `len = tail.wrapping_sub(head)` disambiguates empty (`0`) from full
   (`cap`) without wasting a slot — the v1 spec's design already implies this; make it an explicit,
   tested invariant (including the `cap == 1`/`cap == 2` edge cases that bit heapless in
   [issue #214](https://github.com/rust-embedded/heapless/issues/214)).
3. **Treat "no re-split/reacquire" as a load-bearing soundness invariant, not just an API
   choice** — heapless's [issue #314](https://github.com/rust-embedded/heapless/issues/314)
   (repeated `unsafe { Q.split() }` calls from an ISR pattern produced overlapping `&mut Q`
   borrows) took two rounds to fully close (doc patch in
   [#323](https://github.com/rust-embedded/heapless/pull/323), structural fix later via
   `split_const()`). `ultima_rings`'s `channel()` returning owned `Sender`/`Receiver` exactly once
   already avoids this bug class by construction; add a doc comment and/or a `compile_fail`
   doctest recording *why* a "re-split" or "reacquire producer" API must never be added.
4. **Document the load/store-vs-CAS asymmetry between `spsc.rs` and `mpsc.rs` now, even though v1
   is std-only** — per heapless's own module-doc convention and
   [issue #466](https://github.com/rust-embedded/heapless/issues/466): SPSC only needs atomic
   load/store; `ultima_rings`'s MPSC (per the 2026-08-06 design amendment) needs a bounded CAS.
   One line in `docs/design.md` future-proofs any later `no_std` core decision (spsc first, mpsc
   has the stricter target-support bar) the same way heapless gates `spsc` on `loadstore` and
   `mpmc` on `cas` separately.
5. **Reuse heapless's public answer to [issue #343](https://github.com/rust-embedded/heapless/issues/343)
   verbatim as the canonical explanation in `docs/design.md`**: the same-owner index load is
   `Relaxed` (program order already guarantees a single writer sees its own last write), only the
   cross-thread load of the *other* side's index needs `Acquire` to synchronize-with the
   `Release` store — it's already `ultima_rings`'s stated discipline, but it's also the single
   most-repeated point of confusion heapless's own users raised about this exact algorithm shape,
   so pre-empting it in the docs is cheap insurance.

---

*Provenance note: shallow clone (`--depth 1`) has no commit history; all issue/PR-sourced claims
above were fetched live via `gh api`/`gh pr view`/`gh issue view` against
`github.com/rust-embedded/heapless` on 2026-08-06 and are current as of that date.*
