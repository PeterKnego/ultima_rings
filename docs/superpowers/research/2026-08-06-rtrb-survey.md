# rtrb survey — informing ultima_rings

**Date:** 2026-08-06
**Method:** cloned `mgeier/rtrb` (`3d324125`, 2026-07-12) into scratch, read
`src/lib.rs`, `src/arc_ring_buffer.rs`, `src/chunks.rs`, `.github/workflows/main.yml`,
`README.md`; queried GitHub issues/PRs via `gh api` for soundness history.
Cargo.toml still says `0.3.4` but `main` already contains the unreleased 0.4.0
`is_abandoned()` fix (PR #176) — the crates.io release lags `main`.

## 1. The chunk API (`read_chunk`/`write_chunk`/uninit variants)

Shape (`src/chunks.rs`):

- `Producer::write_chunk(n) -> Result<WriteChunk<'_, T>, ChunkError>` — safe,
  requires `T: Default`; slots are pre-filled with `Default::default()`, so
  `as_mut_slices() -> (&mut [T], &mut [T])` is safe to use directly.
- `Producer::write_chunk_uninit(n) -> Result<WriteChunkUninit<'_, T>, ChunkError>`
  — no `Default` bound; `as_mut_slices() -> (&mut [MaybeUninit<T>], &mut [MaybeUninit<T>])`.
  Caller must init before `commit`/`commit_all` (both `unsafe fn`, since
  committing uninitialized memory is UB on read). `fill_from_iter(iter)` is the
  safe on-ramp: moves items from an iterator into the slots and auto-commits
  exactly what was written — the recommended path when `T` isn't `Copy`/`Default`.
- `Consumer::read_chunk(n) -> Result<ReadChunk<'_, T>, ChunkError>` — always
  safe; `as_slices()`/`as_mut_slices() -> (&[T]/&mut [T], &[T]/&mut [T])`;
  `commit(n)`/`commit_all()` drop the committed prefix and advance head.
  `ReadChunk: IntoIterator<Item = T>` — the iterator moves values out and, on
  `Drop`, advances head by however many were actually iterated (partial
  iteration is safe and leaves the rest in the queue).
- `T: Copy` convenience wrappers built on the above: `push_partial_slice`,
  `push_entire_slice`, `pop_partial_slice[_uninit]`, `pop_entire_slice[_uninit]`
  — `memcpy`-shaped, no per-item loop.
- The "two slices, not one" shape is the ring-wraparound artifact: `n` slots
  may straddle the physical buffer end, so every chunk API returns
  `(first, second)` where `second` is empty unless the request wrapped.
  `CopyToUninit` (`src/lib.rs:783`) is a small extension trait so users can
  safely `copy_to_uninit()` a `Copy` slice into the uninit halves without
  hand-rolling `copy_from_slice`-on-`MaybeUninit`.

Why users like it: it turns the ring into a `memcpy`-class batch API instead of
a per-item `push`/`pop` loop — the single biggest throughput lever for `T: Copy`
payloads (bulk network/audio buffers, the crate's original use case), and it
gives an escape hatch (`_uninit` + `unsafe commit`) for zero-copy in-place
construction when `T` doesn't implement `Default`/`Copy`. The `commit(n)` /
`commit_all()` split (vs. always consuming everything) lets a caller request an
optimistic large chunk and give back the unused tail cheaply.

**What it would take for ultima_rings to add this later (v2 notes):** the v1
design doc already earmarks `batch_publish` as a v2 candidate for MPSC
throughput — the SPSC side is the easier port. Concretely:
- Needs the same `(first, second)`-slice split logic as `write_chunk_uninit`/
  `read_chunk`, i.e. exposing `collapse_position`-equivalent math publicly
  inside the crate (ultima_rings' SPSC core already has cached indices, so this
  is mostly threading `n` through the existing `next_tail`/`next_head`
  fast-path checks rather than new algorithm work).
  - MPSC is harder: a chunk write there must reserve a *contiguous* run of `n`
    slots under the bounded-CAS claim (design doc's amendment) — that's a
    multi-slot claim, not `rtrb`'s single-writer case, and needs its own loom
    model before it's trusted.
- The `unsafe fn commit`/`commit_all` two-step (commit less than requested,
  drop the rest via `WriteChunkUninit::drop_suffix`) is worth copying nearly
  verbatim — it is exactly the "partial batch, give back the remainder" shape
  that `drain()` already wants on the consumer side.
- Budget for the panic-safety bug below (issue #185) *before* porting
  `commit_unchecked`'s drop-then-advance-head order — don't reproduce it.

## 2. Cached-index and abandoned-peer design

**Cached index split.** `Producer`/`Consumer` each hold `cached_head` and
`cached_tail: Cell<usize>` (`src/lib.rs:296-306`, `521-531`). For the producer,
`cached_tail` is authoritative (only the producer writes tail) and
`cached_head` is a stale snapshot refreshed only when the fast check
(`next_tail`, `src/lib.rs:477-492`) thinks the ring might be full — i.e. an
`Acquire` load of the real `head` is skipped whenever the cached slack already
proves there's room. Consumer is the mirror image. This is the standard
false-sharing-avoidance/hot-atomic-avoidance trick; PR #48 first added caching
both indices on both sides, PR #132 reverted-then-reconsidered it after
observing it regressed AMD vs. the upstream crossbeam PR (issue #39) —
i.e. even this "obviously correct" optimization needed empirical
per-microarchitecture validation, not just landed as A Cost Model, and both
`head`/`tail` are `CachePadded` (`src/lib.rs:104,109`) so producer-local and
consumer-local traffic don't share a line. **Recommendation implication:**
ultima_rings should re-run this exact experiment (cache both vs. one) on its
own AWS grid instead of assuming rtrb's current answer transfers.

**Position range trick.** Head/tail live in `0 .. 2*capacity`, not
`0 .. capacity` — `collapse_position()` maps down only when dereferencing a
slot (`src/lib.rs:174-181`). This lets `distance()`/`increment()` avoid the
classic "is it full or empty" ambiguity of a plain modulo ring without a
separate "full" flag or a `capacity+1`-sized buffer, at the cost of one extra
branch in `collapse_position`/`increment`. Worth considering for ultima_rings'
SPSC core if it isn't already doing the power-of-two-mask equivalent
(ultima_rings uses `& (cap-1)` per the design doc, which sidesteps this
entirely since masking a monotonic counter has no ambiguity — so this
particular trick is likely N/A, but worth confirming the drop/`Drain` loop
doesn't accidentally assume the `0..cap` range).

**Abandoned-peer (`is_abandoned`) and drop.** Both `Producer`/`Consumer` share
one `AtomicU8 flags` with a single `IS_ABANDONED` bit (`src/lib.rs:82,111`).
`is_abandoned()` (`src/lib.rs:464-466`, `741-743`) is just
`flags.load(Acquire) & IS_ABANDONED != 0` — cheap, and *the same flag* answers
both directions (no separate producer-dead/consumer-dead bits needed because
only one side can observe "am I abandoned"). The real logic lives in
`ArcRingBuffer::drop()` (`src/arc_ring_buffer.rs:46-76`), a hand-rolled
2-reference "arc": `fetch_or(IS_ABANDONED, Release)` — if the bit was *not*
already set, this side is first-to-drop and does nothing further (buffer
memory stays alive, still readable/writable by the surviving side); if it
*was* set, this side is second-to-drop, so it does an extra `Acquire` load
(explained in-code as a stand-in for a fence, because ThreadSanitizer doesn't
support bare fences) and then deallocates via `drop_slow` → `Box::from_raw`.
Items still in the ring when both sides are gone are dropped by
`RingBuffer::drop()` (`src/lib.rs:230-247`), which walks `head..tail` and
`drop_in_place`s each live slot before deallocating the raw `Vec` shell.
**So:** dropping one side never touches queued items (the other side can still
drain them); only dropping *both* sides drops undrained items, and that drop
walk is a fully sequential `head..tail` sweep, not a call into per-slot
consumer/producer commit paths.

This exact mechanism (`fetch_or` + conditional second-drop deallocation) is a
clean minimal pattern ultima_rings' own SPSC/MPSC `Drop` + disconnect design
should compare itself against — it's essentially a manual `Arc<T>` restricted
to a 2-holder count, which is exactly what the design doc's `AtomicUsize`
sender-count + `disconnected` flag scheme is doing for MPSC, just generalized
to N producers instead of hardcoded to 2.

## 3. Soundness pitfalls (fixed and open)

- **Issue #114 / PR #176 — `is_abandoned()` silently broke on stable Rust
  1.74.0 (closed, fixed in unreleased 0.4.0).** rtrb originally used
  `Arc::strong_count()`'s *undocumented* synchronizing behavior to implement
  `is_abandoned()`. rust-lang/rust#115546 removed that undocumented
  synchronization, so `is_abandoned()` stopped being a reliable
  happens-before edge — a caller could observe `is_abandoned() == true` without
  a guarantee that all of the other side's prior writes were visible. Fix
  (PR #176, "Replace Arc with ArcRingBuffer"): drop `Arc` entirely, hand-roll a
  2-holder reference count in `AtomicU8` with explicit `Release`/`Acquire`
  ordering documented in-code (`src/arc_ring_buffer.rs:50-69`) rather than
  relying on another type's unstated guarantees. **Lesson: never depend on a
  library type's incidental/undocumented ordering behavior for your own
  correctness — own the atomic op and its ordering explicitly.** rtrb also
  added a dedicated CI lane for this: `cargo miri test no_race_with_is_abandoned`
  run twice, once with `MIRIFLAGS="-Zmiri-preemption-rate=0"` specifically to
  stress this race (`.github/workflows/main.yml:105-125`).
- **Issue #26 — public `buffer: Arc<RingBuffer<T>>` field was unsound
  (closed, fixed by making it a method).** Exposing the field let downstream
  code *overwrite* `Producer`/`Consumer`'s buffer reference without updating
  the cached `head`/`tail`, producing reads of uninitialized memory reachable
  from 100% safe code. Fix: `buffer()` accessor returning `&RingBuffer<T>`
  instead of a public field. **Lesson: never expose a mutable/replaceable
  handle to shared state when cached indices must stay in lockstep with it —
  ultima_rings' design already does `&self.buffer` privately with a `buffer()`
  accessor per the spec, so this is confirmation, not a gap.**
  Both this and #114 substantiate `docs/design.md`'s memory-ordering write-up
  as the important artifact, not just the code.
- **Issue #185 — `ReadChunk::commit`/`commit_all` are NOT panic-safe (OPEN,
  unfixed as of survey date).** `commit_unchecked` (`src/chunks.rs:977-993`)
  drops the committed slots *before* advancing/storing the new `head`. If a
  `T::drop()` panics mid-loop, `head` is never advanced, so those same slots
  are later dropped again — once by a subsequent `read_chunk`/`RingBuffer::drop`
  walk over the (stale) `head..tail` range — a double-drop/use-after-free
  (CWE-415/416) reachable from fully safe Rust (a panicking `Drop` impl is
  legal Rust, if inadvisable). **Direct, actionable lesson for ultima_rings:**
  in any chunk/batch API, advance the head/tail index (publish) **before**
  running per-item drop logic, or wrap the drop loop in a guard that advances
  the index on unwind (`scopeguard`-style), so a panicking element `Drop`
  can't leave the ring's index and its slot-liveness bookkeeping out of sync.
  This directly informs the `batch_publish`/chunk-API v2 work flagged above —
  don't copy `commit_unchecked`'s ordering as-is.
- **CI verification stack actually used: Miri + ThreadSanitizer, not loom**
  (`.github/workflows/main.yml`). rtrb has no loom dependency at all — it
  relies on `cargo miri test` (including the preemption-rate=0 variant for the
  abandoned-race test) plus a dedicated TSan job (`RUSTFLAGS="-Z
  sanitizer=thread"`), with the `no_race_with_is_abandoned` test explicitly
  *skipped* under TSan because TSan false-positives on the standalone
  `Acquire`-load-as-fence workaround (linked to
  google/sanitizers#1415, noted in-code at `src/arc_ring_buffer.rs:65-68`).
  This is a useful counter-data-point for ultima_rings' "loom + miri"
  verification bar: loom is stronger for *exhaustively* enumerating small
  interleavings (rtrb's own PR #176 discussion implies they'd have liked
  exhaustive coverage of the abandon race, and settled for two Miri runs +
  TSan instead), but TSan/Miri catch real-world scheduler and allocator
  interaction that loom's synthetic model doesn't. **Recommendation: keep
  loom as designed, but also add a Miri preemption-rate=0 lane and consider a
  TSan lane for anything not already loom-modeled** (e.g. full end-to-end
  stress tests loom is too slow to run).

## 4. API comparison vs. the ultima_rings surface

| Capability | rtrb | ultima_rings (per v1 design) |
|---|---|---|
| Topology | SPSC only | SPSC *and* MPSC (LMAX-style bounded-CAS claim) |
| Waiting | None — caller must busy-retry `push`/`pop`; crate docs say so explicitly (`src/lib.rs:16-19`) | First-class `WaitStrategy`: `BusySpin`/`Backoff`/`Park`, chosen at `channel()` construction |
| Disconnect signal | `is_abandoned()` bool, caller must poll; queued items remain readable/writable after abandonment | `Disconnected` variant baked into every `Result` (`TrySendError`/`SendError`/`TryRecvError`/`RecvError`), std-`mpsc`-shaped |
| Batch I/O | Rich: `read_chunk`/`write_chunk[_uninit]`, `Copy`-slice fast paths, iterator fill/drain | Only `drain(max, f)` in v1; chunk/`batch_publish` explicitly deferred to v2 |
| Item type bound | Any `T` (no `Send` required on the type itself — only `Producer<T>: Send` needs `T: Send`) | `T: Send` only, same shape |
| `no_std` | Yes (`alloc`-only, `#![no_std]` behind a feature) | Not stated in v1 design; likely std-only given `thread::park` in the notify layer |
| `Read`/`Write` impls | `Producer<u8>: std::io::Write`, `Consumer<u8>: std::io::Read` | Not mentioned — a cheap, low-risk addition if a byte-oriented channel ever shows up |
| Peek | `Consumer::peek() -> Result<&T, PeekError>` (shared-ref, non-removing) | Not in the v1 surface |
| Verification | Miri + ThreadSanitizer (no loom) | loom + miri + ARM CI (stronger bar) |

**What an rtrb user moving to ultima_rings would gain:** MPSC, built-in
blocking/wait strategies (rtrb forces you to hand-roll your own spin/park loop
around `push`/`pop`), and a disconnect signal integrated into the `Result`
type instead of a polled bool that leaves stale-but-readable data behind.

**What they'd miss (v1):** the chunk/batch API (rtrb's core differentiator for
bulk `Copy` payloads — audio buffers, network frames), `peek()`,
`std::io::Read`/`Write` impls, and `no_std` support. All are reasonable v2/v3
candidates except `no_std`, which conflicts with the notify layer's
`thread::park` unless gated behind a feature the way rtrb gates `std`.

## 5. Top 5 recommendations for ultima_rings

1. **Don't let a batch/chunk commit drop items before publishing the new
   index** — copy the shape of rtrb's `commit_unchecked` but flip the order
   (advance head/tail first, or guard the drop loop against unwind) to avoid
   the open double-drop bug in rtrb issue #185 (`src/chunks.rs:977-993`)
   before any v2 `batch_publish`/chunk API lands.
2. **Never rely on another type's undocumented synchronization for your own
   correctness invariant** — rtrb's `is_abandoned()` broke on stable Rust
   1.74 because it leaned on `Arc::strong_count()`'s incidental ordering
   (issue #114, fixed by PR #176's hand-rolled `ArcRingBuffer`); ultima_rings'
   `AtomicUsize` sender-count + `disconnected` flag design should keep every
   ordering explicit and documented in `docs/design.md`, the way
   `src/arc_ring_buffer.rs:50-69` does.
3. **Never expose a raw, overwritable handle to the shared ring** — keep
   `buffer`-equivalent internals private with an accessor method, per rtrb
   issue #26 (a public `Arc<RingBuffer<T>>` field let safe code desync cached
   indices from the real buffer and read uninitialized memory); the v1 design
   doc already does this, so treat this as a regression test to keep, not new
   work.
4. **Re-measure the "cache both head and tail" cost model on your own
   hardware grid instead of assuming it's settled** — rtrb added dual-index
   caching in PR #48, then had to revert-and-re-litigate it in #132 after
   observing an AMD regression relative to the upstream crossbeam PR
   (issue #39); ultima_rings' AWS bench-infra grid (already used for the
   ported SPSC/MPSC cores) is the right place to validate this per-microarch,
   not assume rtrb's current answer transfers.
5. **Pair loom with a Miri preemption-rate=0 lane (and consider ThreadSanitizer
   for stress tests loom can't afford)** — rtrb ships no loom at all, relying
   instead on `cargo miri test` twice (once with
   `MIRIFLAGS=-Zmiri-preemption-rate=0` targeting exactly the abandon-race
   test) plus a TSan job that explicitly skips that same test due to a known
   TSan/fence false-positive (google/sanitizers#1415); ultima_rings' loom
   models cover the exhaustive small-interleaving space loom is built for, but
   a Miri/TSan lane over the full stress tests catches allocator- and
   scheduler-shaped bugs loom's synthetic model won't reach.

## Sources

- Repo: https://github.com/mgeier/rtrb (cloned at `3d324125`, 2026-07-12)
- Issue #114 — `is_abandoned()` broken since Rust 1.74.0:
  https://github.com/mgeier/rtrb/issues/114
- PR #176 — Replace Arc with ArcRingBuffer (fix for #114):
  https://github.com/mgeier/rtrb/pull/176
- Issue #26 — public `buffer` field unsound:
  https://github.com/mgeier/rtrb/issues/26
- Issue #185 — `ReadChunk::commit`/`commit_all` panic-safety double-free (open):
  https://github.com/mgeier/rtrb/issues/185
- PR #132 / Issue #39 — cache-both-indices AMD regression vs. crossbeam PR 338:
  https://github.com/mgeier/rtrb/pull/132, https://github.com/mgeier/rtrb/issues/39
- Issues #83, #90 — Miri CI setup: https://github.com/mgeier/rtrb/issues/83,
  https://github.com/mgeier/rtrb/issues/90
- `.github/workflows/main.yml` (Miri + ThreadSanitizer jobs, as of `3d324125`)
