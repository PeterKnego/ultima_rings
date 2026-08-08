# Sharded MPSC Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a feature-gated sharded MPSC (one SPSC ring per producer, consumer round-robins) and measure whether it closes the v1 bake-off gap against crossbeam-channel.

**Architecture:** `src/sharded.rs` composes `N` independent `spsc::channel`s — one per producer — behind a `Sender`/`Receiver` pair. Each producer writes only to its own ring, so the send path is a single Release store with no CAS; the consumer sweeps shards with a sticky cursor and a bounded per-shard visit budget. The layer declares no atomics and contains no `unsafe`: every ordering edge belongs to the already loom/miri-verified `src/spsc.rs`.

**Tech Stack:** Rust edition 2024 (stable toolchain), criterion 0.5 for benches, crossbeam-channel as the comparison baseline (dev-dependency only).

**Spec:** `docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md`

## Global Constraints

- **No `unsafe` anywhere in `src/sharded.rs`.** If a task seems to need it, stop — the design is wrong, not the constraint.
- **No new production dependencies.** `[dependencies]` stays empty; competitor crates are `[dev-dependencies]` only.
- **Feature-gated:** all sharded code sits behind `experimental-sharded`, default off. A default `cargo build`/`cargo test` must not compile it.
- **`#![warn(missing_docs)]` is on** (`src/lib.rs:8`) — every public item needs a doc comment.
- **`cargo clippy -- -D warnings` and `cargo fmt --check` must pass.**
- **`BusySpin` only.** No `Park`, no `Backoff`, no blocking `send`/`recv`, no `drain`, no `Sender: Clone`. These are out of scope per the spec.
- **Capacity is total, not per-shard:** `channel(2, 1024)` means 1024 slots across 2 shards (512 each), matching `crossbeam_channel::bounded(1024)`.
- **Ordering contract is per-producer FIFO only.** No test may assert cross-producer ordering.
- **`VISIT_BUDGET = 32`** — the number of items taken from one shard before the cursor advances.

## File Structure

| File | Responsibility |
|---|---|
| `Cargo.toml` | Declares the `experimental-sharded` feature (modify) |
| `src/lib.rs` | Registers the feature-gated `sharded` module (modify, one line) |
| `src/sharded.rs` | **Create.** The whole prototype: `channel()`, `Sender`, `Receiver`, unit tests. ~150 lines. |
| `tests/sharded_stress.rs` | **Create.** Per-producer FIFO stress + drop accounting. |
| `benches/throughput.rs` | Adds the `bakeoff_sharded_mpsc` group and feature-gated `criterion_main!` (modify) |
| `docs/bench-results/2026-08-07-sharded-mpsc.md` | **Create in Task 4.** The measurement and the gate verdict. |

---

### Task 1: The sharded channel — construction, send, and the consumer sweep

**Files:**
- Modify: `Cargo.toml` (add `[features]` section after `[dev-dependencies]`)
- Modify: `src/lib.rs:10-14` (module declarations)
- Create: `src/sharded.rs`

**Interfaces:**
- Consumes: `crate::spsc::channel(cap: usize, strategy: WaitStrategy) -> (spsc::Sender<T>, spsc::Receiver<T>)`; `spsc::Sender::try_send`; `spsc::Receiver::try_recv`; `crate::wait::WaitStrategy`; `crate::TrySendError<T>`; `crate::TryRecvError`
- Produces: `sharded::channel<T: Send>(n_shards: usize, total_cap: usize, strategy: WaitStrategy) -> (Vec<Sender<T>>, Receiver<T>)`; `Sender<T>::try_send(&mut self, v: T) -> Result<(), TrySendError<T>>`; `Receiver<T>::try_recv(&mut self) -> Result<T, TryRecvError>`; module-private `const VISIT_BUDGET: usize = 32`; module-private `Receiver<T>::advance(&mut self)`

> **Why this task is not split further:** an earlier draft separated construction/send from the consumer sweep. That split cannot satisfy the Global Constraint `cargo clippy -- -D warnings`, because `VISIT_BUDGET` and `Receiver`'s three fields are dead code until `try_recv` exists. Keep them together; do not reintroduce the split, and do not paper over it with `#[allow(dead_code)]`.

- [ ] **Step 1: Add the feature to `Cargo.toml`**

Insert after the `[dev-dependencies]` block, before `[target.'cfg(loom)'.dependencies]`:

```toml
[features]
# Experimental sharded MPSC prototype (N x SPSC + round-robin consumer).
# Default off: not part of the stable API, BusySpin-only, no loom models.
experimental-sharded = []
```

- [ ] **Step 2: Register the module in `src/lib.rs`**

Change the module block at `src/lib.rs:10-14` from:

```rust
mod atomic;
pub mod mpsc;
mod notify;
pub mod spsc;
mod wait;
```

to:

```rust
mod atomic;
pub mod mpsc;
mod notify;
#[cfg(feature = "experimental-sharded")]
pub mod sharded;
pub mod spsc;
mod wait;
```

- [ ] **Step 3: Write the failing tests**

Create `src/sharded.rs` containing ONLY this test module for now (the code above it comes in Step 5):

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::{TryRecvError, TrySendError, WaitStrategy};

    #[test]
    #[should_panic(expected = "must divide evenly")]
    fn rejects_uneven_split() {
        let _ = channel::<u64>(3, 1024, WaitStrategy::BusySpin);
    }

    #[test]
    #[should_panic(expected = "must be a positive power of two")]
    fn rejects_non_power_of_two_per_shard() {
        // 100 / 4 = 25: divides evenly, but 25 is not a power of two.
        let _ = channel::<u64>(4, 100, WaitStrategy::BusySpin);
    }

    #[test]
    fn full_is_per_shard_not_total() {
        // 2 shards x 512 = 1024 total, but one producer stalls at 512.
        // `_rx` must stay bound: dropping it would make try_send return
        // Disconnected instead of Full.
        let (mut senders, _rx) = channel::<u64>(2, 1024, WaitStrategy::BusySpin);
        assert_eq!(senders.len(), 2);
        for i in 0..512 {
            senders[0].try_send(i).unwrap();
        }
        assert_eq!(senders[0].try_send(512), Err(TrySendError::Full(512)));
        // The other shard is untouched and still accepts its full 512.
        senders[1].try_send(0).unwrap();
    }

    #[test]
    fn sticky_cursor_drains_a_shard_before_advancing() {
        let (mut senders, mut rx) = channel::<u64>(2, 8, WaitStrategy::BusySpin);
        senders[0].try_send(1).unwrap();
        senders[0].try_send(2).unwrap();
        senders[0].try_send(3).unwrap();
        senders[1].try_send(10).unwrap();
        senders[1].try_send(20).unwrap();
        let mut got = Vec::new();
        while let Ok(v) = rx.try_recv() {
            got.push(v);
        }
        // Shard 0 is drained first (sticky), then the cursor advances.
        assert_eq!(got, vec![1, 2, 3, 10, 20]);
    }

    #[test]
    fn visit_budget_advances_cursor_after_32_items() {
        // 2 shards x 64. Shard 0 holds more than VISIT_BUDGET items, so the
        // cursor must move on mid-shard instead of draining it.
        let (mut senders, mut rx) = channel::<u64>(2, 128, WaitStrategy::BusySpin);
        for i in 0..40 {
            senders[0].try_send(i).unwrap();
        }
        senders[1].try_send(999).unwrap();
        let mut got = Vec::new();
        for _ in 0..34 {
            got.push(rx.try_recv().unwrap());
        }
        assert_eq!(got[..32], (0..32).collect::<Vec<u64>>()[..]);
        assert_eq!(got[32], 999, "budget exhausted: cursor must advance");
        assert_eq!(got[33], 32, "shard 1 empty: cursor wraps back to shard 0");
    }

    #[test]
    fn disconnect_requires_every_shard() {
        let (mut senders, mut rx) = channel::<u64>(2, 8, WaitStrategy::BusySpin);
        senders[1].try_send(7).unwrap();
        let s1 = senders.pop().unwrap();
        let s0 = senders.pop().unwrap();
        drop(s0);
        // Shard 0 disconnected+drained, shard 1 still live and holding 7.
        assert_eq!(rx.try_recv(), Ok(7));
        assert_eq!(
            rx.try_recv(),
            Err(TryRecvError::Empty),
            "one live shard means Empty, never Disconnected"
        );
        drop(s1);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn drains_remaining_items_after_all_senders_drop() {
        let (mut senders, mut rx) = channel::<u64>(2, 8, WaitStrategy::BusySpin);
        senders[0].try_send(1).unwrap();
        senders[1].try_send(2).unwrap();
        drop(senders);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn single_shard_degenerates_cleanly() {
        let (mut senders, mut rx) = channel::<u64>(1, 4, WaitStrategy::BusySpin);
        senders[0].try_send(1).unwrap();
        senders[0].try_send(2).unwrap();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn zero_sized_payloads_work() {
        let (mut senders, mut rx) = channel::<()>(2, 8, WaitStrategy::BusySpin);
        senders[0].try_send(()).unwrap();
        assert_eq!(rx.try_recv(), Ok(()));
    }
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test --features experimental-sharded --lib sharded`
Expected: FAIL — compile error, `cannot find function 'channel' in this scope`.

- [ ] **Step 5: Write the implementation**

Prepend this to `src/sharded.rs`, above the test module:

```rust
//! **Experimental.** Sharded MPSC: one SPSC ring per producer, consumer
//! round-robins. Gated behind the `experimental-sharded` feature; not part of
//! the stable API and not loom-modeled (see below).
//!
//! Each producer owns a private [`crate::spsc`] ring, so a send is a single
//! Release store with no CAS and no cross-producer contention — unlike
//! [`crate::mpsc`]'s shared bounded-CAS claim. Two consequences, both
//! deliberate:
//!
//! **Per-producer FIFO only.** There is no cross-producer ordering guarantee:
//! two values sent by different producers may be delivered in either order,
//! regardless of real time. [`crate::mpsc`] provides global FIFO; this does
//! not.
//!
//! **Backpressure is per-shard.** With `channel(2, 1024)` a producer sees
//! `Full` at 512 outstanding items even while the other shard sits empty.
//!
//! This module declares no atomics and contains no `unsafe`; every
//! memory-ordering edge belongs to [`crate::spsc`], which `tests/loom.rs`
//! already models. See
//! `docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md`.

use crate::spsc;
use crate::wait::WaitStrategy;
use crate::{TryRecvError, TrySendError};

/// Items taken from one shard before the cursor advances. Bounds how long a
/// hot producer can starve the others while still amortizing the shard switch
/// over a run of items.
const VISIT_BUDGET: usize = 32;

/// Create a sharded MPSC channel with `n_shards` producer slots holding
/// `total_cap` items **in total** — `total_cap / n_shards` per shard.
///
/// Returns one [`Sender`] per shard; move each into its own producer thread.
/// [`Sender`] is deliberately not `Clone`: the shard set is fixed at
/// construction.
///
/// # Panics
///
/// Panics unless `n_shards > 0`, `total_cap` divides evenly by `n_shards`, and
/// the resulting per-shard capacity is a positive power of two (which, for a
/// power-of-two `total_cap`, means `n_shards` must itself be a power of two).
pub fn channel<T: Send>(
    n_shards: usize,
    total_cap: usize,
    strategy: WaitStrategy,
) -> (Vec<Sender<T>>, Receiver<T>) {
    assert!(n_shards > 0, "n_shards must be positive");
    assert!(
        total_cap % n_shards == 0,
        "total_cap {total_cap} must divide evenly into {n_shards} shards"
    );
    let per_shard = total_cap / n_shards;
    assert!(
        per_shard > 0 && per_shard.is_power_of_two(),
        "per-shard capacity {per_shard} (= {total_cap} / {n_shards}) \
         must be a positive power of two"
    );
    let mut senders = Vec::with_capacity(n_shards);
    let mut shards = Vec::with_capacity(n_shards);
    for _ in 0..n_shards {
        let (tx, rx) = spsc::channel::<T>(per_shard, strategy);
        senders.push(Sender { inner: tx });
        shards.push(rx);
    }
    (
        senders,
        Receiver {
            shards,
            cursor: 0,
            budget: 0,
        },
    )
}

/// One producer's handle, owning a private shard.
///
/// Not `Clone`: the shard set is fixed when [`channel`] is called.
pub struct Sender<T: Send> {
    inner: spsc::Sender<T>,
}

impl<T: Send> Sender<T> {
    /// Push without blocking into this producer's own shard.
    ///
    /// `Full(v)` means **this shard** is full (`total_cap / n_shards` items),
    /// not that the channel as a whole is full — another shard may be empty.
    /// `Disconnected(v)` when the receiver is gone.
    pub fn try_send(&mut self, v: T) -> Result<(), TrySendError<T>> {
        self.inner.try_send(v)
    }
}

/// The single consumer, sweeping all shards with a sticky cursor.
pub struct Receiver<T: Send> {
    shards: Vec<spsc::Receiver<T>>,
    cursor: usize,
    budget: usize,
}

impl<T: Send> Receiver<T> {
    /// Pop without blocking, sweeping shards from the current cursor.
    ///
    /// Stays on one shard for up to `VISIT_BUDGET` consecutive items before
    /// advancing, so the hot path keeps hitting one ring's cache lines while
    /// no single producer can starve the others indefinitely.
    ///
    /// Returns `Disconnected` only when **every** shard is both
    /// sender-dropped and drained; `Empty` while any shard could still
    /// deliver. Note that both outcomes cost a full `n_shards` scan — that is
    /// the structural price of sharding, versus one atomic load for
    /// [`crate::mpsc`].
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if self.budget >= VISIT_BUDGET {
            self.advance();
        }
        let n = self.shards.len();
        let mut disconnected = 0usize;
        for _ in 0..n {
            match self.shards[self.cursor].try_recv() {
                Ok(v) => {
                    self.budget += 1;
                    return Ok(v);
                }
                // A shard reports Disconnected only once its sender is gone
                // AND it is drained, and that state is stable — so counting
                // per sweep is sound without dead-shard bookkeeping.
                Err(TryRecvError::Disconnected) => disconnected += 1,
                Err(TryRecvError::Empty) => {}
            }
            self.advance();
        }
        if disconnected == n {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Move to the next shard, resetting the visit budget. Compare-and-reset
    /// rather than `%`, so no division enters the hot path.
    fn advance(&mut self) {
        self.cursor = if self.cursor + 1 == self.shards.len() {
            0
        } else {
            self.cursor + 1
        };
        self.budget = 0;
    }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test --features experimental-sharded --lib sharded`
Expected: PASS, 9 tests.

- [ ] **Step 7: Verify the default build is untouched**

Run: `cargo build && cargo test --lib`
Expected: PASS, and `src/sharded.rs` is NOT compiled (the feature is off).

- [ ] **Step 8: Check lints and formatting**

Run: `cargo clippy --features experimental-sharded --all-targets -- -D warnings && cargo fmt --check`
Expected: clean, no output from clippy, no diff from fmt.

- [ ] **Step 9: Commit**

```bash
git add Cargo.toml src/lib.rs src/sharded.rs
git commit -m "feat(sharded): N x SPSC channel with sticky round-robin consumer

Composes N independent spsc channels behind a fixed Sender set. Total-cap
semantics (total_cap / n_shards per shard) so the bake-off compares equal
buffer against crossbeam::bounded. try_recv stays on a shard for up to 32
consecutive items, then advances; Disconnected only when every shard is
drained and sender-dropped. No unsafe, default-off feature."
```

---

### Task 2: Stress and drop-accounting tests

**Files:**
- Create: `tests/sharded_stress.rs`

**Interfaces:**
- Consumes: `ultima_rings::sharded::channel(n_shards, total_cap, strategy)`, `Sender::try_send`, `Receiver::try_recv` from Task 1
- Produces: nothing consumed by later tasks

- [ ] **Step 1: Write the failing test file**

Create `tests/sharded_stress.rs`:

```rust
//! Per-producer FIFO + no-loss/no-dup for the sharded MPSC prototype, plus
//! drop accounting. Mirrors `tests/mpsc_stress.rs`, but asserts the weaker
//! ordering contract this type actually promises: per-producer FIFO, NOT the
//! global FIFO `mpsc` provides.
#![cfg(all(not(loom), feature = "experimental-sharded"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, sharded};

/// Values are `(tag << 32) | seq`, so checking that each tag's sequences
/// arrive as 0, 1, 2, ... catches ordering violations, loss, AND duplication
/// in a single assertion.
fn run_stress(producers: usize, per: usize, total_cap: usize) {
    let (senders, mut rx) =
        sharded::channel::<u64>(producers, total_cap, WaitStrategy::BusySpin);
    let mut handles = Vec::new();
    for (tag, mut tx) in senders.into_iter().enumerate() {
        handles.push(thread::spawn(move || {
            for i in 0..per {
                let mut v = ((tag as u64) << 32) | i as u64;
                loop {
                    match tx.try_send(v) {
                        Ok(()) => break,
                        Err(TrySendError::Full(back)) => {
                            v = back;
                            std::hint::spin_loop();
                        }
                        Err(TrySendError::Disconnected(_)) => panic!("rx died"),
                    }
                }
            }
        }));
    }
    let mut next = vec![0u64; producers];
    let mut got = 0usize;
    loop {
        match rx.try_recv() {
            Ok(v) => {
                let tag = (v >> 32) as usize;
                let seq = v & 0xffff_ffff;
                assert_eq!(
                    seq, next[tag],
                    "per-producer FIFO violated on tag {tag}"
                );
                next[tag] += 1;
                got += 1;
            }
            Err(TryRecvError::Empty) => std::hint::spin_loop(),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(got, producers * per, "loss or duplication");
    for (tag, n) in next.iter().enumerate() {
        assert_eq!(*n as usize, per, "producer {tag} delivered short");
    }
}

#[test]
fn sharded_per_producer_fifo_2_producers() {
    let per = if cfg!(miri) { 200 } else { 30_000 };
    run_stress(2, per, 256);
}

#[test]
fn sharded_per_producer_fifo_4_producers() {
    let per = if cfg!(miri) { 200 } else { 30_000 };
    run_stress(4, per, 256);
}

#[derive(Debug)]
struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn every_value_dropped_exactly_once_including_ring_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut senders, mut rx) =
        sharded::channel::<Counted>(2, 16, WaitStrategy::BusySpin);
    for _ in 0..3 {
        senders[0].try_send(Counted(Arc::clone(&drops))).unwrap();
        senders[1].try_send(Counted(Arc::clone(&drops))).unwrap();
    }
    for _ in 0..2 {
        drop(rx.try_recv().unwrap());
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(senders);
    drop(rx);
    assert_eq!(
        drops.load(Ordering::Relaxed),
        6,
        "leak or double-drop across shard drop-drain"
    );
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --features experimental-sharded --test sharded_stress`
Expected: PASS, 3 tests. (These pass immediately — Task 1 already implements the behavior. Their job is to catch regressions and to verify the ordering contract under real contention, not to drive new code.)

- [ ] **Step 3: Run the whole suite to confirm nothing regressed**

Run: `cargo test --features experimental-sharded`
Expected: PASS — the pre-existing 32 tests plus the new sharded ones.

- [ ] **Step 4: Run miri over the sharded tests**

Run: `cargo +nightly miri test --features experimental-sharded --test sharded_stress`
Expected: PASS, 0 UB reports. (Miri is slow; the `cfg!(miri)` branches cut the item counts to 200.)

- [ ] **Step 5: Commit**

```bash
git add tests/sharded_stress.rs
git commit -m "test(sharded): per-producer FIFO stress + drop accounting

Tagged (tag<<32|seq) values let one assertion catch ordering violations,
loss, and duplication together. Asserts only per-producer FIFO — the
contract this type provides — never global order."
```

---

### Task 3: Bake-off bench group

**Files:**
- Modify: `benches/throughput.rs` (add the group after `bakeoff_mpsc` ends at line 396; replace the `criterion_main!` at line 399)

**Interfaces:**
- Consumes: `ultima_rings::sharded::channel`, `Sender::try_send`, `Receiver::try_recv` from Task 1; existing bench constants `BATCH` (line 12) and the already-imported `Arc`, `Barrier`, `thread`, `Instant`, `Criterion`, `Throughput`, `TryRecvError`, `TrySendError`, `WaitStrategy`
- Produces: criterion group `bakeoff_sharded_mpsc` with bench function `ultima_sharded_2_producers`

- [ ] **Step 1: Add the bench group**

Insert into `benches/throughput.rs` after the closing brace of `bakeoff_mpsc` (line 396), before the `criterion_group!(bakeoff, ...)` line:

```rust
// ---------------------------------------------------------------------------
// Sharded MPSC prototype (feature `experimental-sharded`). Same harness shape,
// BATCH, and total buffered capacity as `bakeoff_mpsc` above, so the two are
// directly comparable: 2 shards x 512 = 1024 slots, matching
// crossbeam_channel::bounded(1024). See
// docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md.
// ---------------------------------------------------------------------------

#[cfg(feature = "experimental-sharded")]
fn bakeoff_sharded_mpsc(c: &mut Criterion) {
    use ultima_rings::sharded;
    let mut g = c.benchmark_group("bakeoff_sharded_mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("ultima_sharded_2_producers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (senders, mut rx) =
                    sharded::channel::<u64>(2, 1024, WaitStrategy::BusySpin);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                // Every sender is moved into a thread, so there is no
                // leftover handle to drop (unlike the mpsc groups, where the
                // original `tx` must be dropped for the consumer to finish).
                for mut tx in senders {
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let mut v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(TrySendError::Disconnected(_)) => return,
                                }
                            }
                        }
                    }));
                }
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(_) => got += 1,
                        Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(TryRecvError::Disconnected) => break,
                    }
                    if got == BATCH {
                        break;
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.finish();
}
```

- [ ] **Step 2: Replace the `criterion_main!` line**

Change `benches/throughput.rs:398-399` from:

```rust
criterion_group!(bakeoff, bakeoff_spsc, bakeoff_mpsc);
criterion_main!(benches, bakeoff);
```

to:

```rust
criterion_group!(bakeoff, bakeoff_spsc, bakeoff_mpsc);

#[cfg(feature = "experimental-sharded")]
criterion_group!(bakeoff_sharded, bakeoff_sharded_mpsc);

#[cfg(feature = "experimental-sharded")]
criterion_main!(benches, bakeoff, bakeoff_sharded);

#[cfg(not(feature = "experimental-sharded"))]
criterion_main!(benches, bakeoff);
```

- [ ] **Step 3: Verify both feature configurations compile**

Run: `cargo bench --no-run && cargo bench --features experimental-sharded --no-run`
Expected: both succeed. The first must NOT contain the sharded group.

- [ ] **Step 4: Smoke-test the new group**

Run: `cargo bench --features experimental-sharded -- --quick bakeoff_sharded_mpsc`
Expected: completes and prints a `thrpt` figure in Melem/s. This is a smoke test for wiring only — do not record this number; Task 4 does the real measurement.

- [ ] **Step 5: Check lints and formatting**

Run: `cargo clippy --features experimental-sharded --all-targets -- -D warnings && cargo fmt --check`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add benches/throughput.rs
git commit -m "bench(sharded): bakeoff_sharded_mpsc group, feature-gated

2 shards x 512 = 1024 total slots, matching crossbeam::bounded(1024), with
the same barrier-released 2-producer harness and BATCH as bakeoff_mpsc."
```

---

### Task 4: Measure, record, and evaluate the gate

**Files:**
- Create: `docs/bench-results/2026-08-07-sharded-mpsc.md`

**Interfaces:**
- Consumes: the `bakeoff_sharded_mpsc` group from Task 3; existing groups `mpsc/busy_spin_2_producers` and `bakeoff_mpsc/crossbeam`
- Produces: the measurement and the go/no-go verdict — no code

- [ ] **Step 1: Build everything to completion first**

Run: `cargo bench --features experimental-sharded --no-run`
Expected: builds fully. **Do not skip this.** The box has 4 cores, ~4.6 GiB available RAM and no swap, and `/tmp` is a tmpfs charged against RAM — compiling during measurement would put reclaim noise straight into the numbers.

- [ ] **Step 2: Confirm the box is quiet**

Run: `uptime && free -h`
Expected: load average well under 4.0, and available memory not near zero. If load is high, wait for it to settle before measuring.

- [ ] **Step 3: Run the three comparable cells in one session**

Run: `cargo bench --features experimental-sharded -- "bakeoff_sharded_mpsc|bakeoff_mpsc/crossbeam|mpsc/busy_spin_2_producers"`
Expected: three groups report, each with a `thrpt` mid value and range in Melem/s.

Record all three from THIS run — do not quote crossbeam's or mpsc's figures from the v1 file. Re-measuring in the same session on the same box is what makes the delta trustworthy.

- [ ] **Step 4: Write the results document**

Create `docs/bench-results/2026-08-07-sharded-mpsc.md` using this structure, filling every `<...>` from Step 3's actual output:

```markdown
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

## Results

| Cell | Melem/s (mid) | Melem/s (range) | vs. crossbeam |
|---|---:|---|---:|
| sharded (2 shards x 512) | <mid> | <lo> – <hi> | <ratio>x |
| crossbeam-channel | <mid> | <lo> – <hi> | 1.00x |
| mpsc (shared bounded-CAS claim) | <mid> | <lo> – <hi> | <ratio>x |

v1 reference (`docs/bench-results/2026-08-06-bakeoff.md`, different session):
mpsc 29.9, crossbeam 71.0.

## Verdict

<One of the three gate branches below, with the ratio that triggered it.>

## What this does and does not show

The sharded cell provides **per-producer FIFO only** and per-shard
backpressure; `mpsc` and crossbeam both provide global FIFO and a global
bound. This is not a like-for-like replacement, and the number should not be
read as one.
```

- [ ] **Step 5: Evaluate the gate and write the verdict**

Pick the branch the measurement actually lands in and write it into the Verdict section:

- **≥ 142 Melem/s (2x crossbeam):** decisive. Next round designs the dynamic shard registry and the production type.
- **> crossbeam's measured mid, but < 142:** sharding wins. Worth designing the registry, weighing the contract costs (no global FIFO, per-shard bound, O(n) emptiness) explicitly against the gain.
- **≤ crossbeam's measured mid:** a real finding, not a failure. With zero CAS contention and zero availability-array false sharing, a loss means the dominant cost is NOT where `docs/design.md` §7 and §8 assume. Record that the v2 shift/padding levers (`docs/superpowers/specs/2026-08-07-mpsc-perf-v2-design.md`) return to the table, and that §7/§8 need correcting to say the dominant cost is unidentified.

Write the branch that the data supports. Do not soften a losing result.

- [ ] **Step 6: Commit**

```bash
git add docs/bench-results/2026-08-07-sharded-mpsc.md
git commit -m "bench(sharded): measured results and gate verdict

All three cells re-measured in one session at equal total buffered
capacity. <one-line verdict>"
```

---

## Follow-ups (explicitly NOT part of this plan)

These are informed by Task 4's result and must not be started before it:

- `docs/design.md` §9 gains a "sharded SPSC" entry under Alternatives considered — a real gap (§9 covers Vyukov stamps, kanal's stack transfer, and flume's lock, but not the most obvious alternative to a shared-claim MPSC). Writing it before the number exists means guessing the conclusion.
- If the gate's third branch lands, §7 and §8 need correcting to state the dominant MPSC cost is unidentified.
- Dynamic producers (`Sender: Clone` + shard registry), `Park` mode with the N-way Dekker protocol, `drain`, loom models, and the single-allocation shard array all remain out of scope.
