# ultima_rings v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A production-grade Rust crate of bounded lock-free generic-`T` SPSC and MPSC rings with pluggable wait strategies (BusySpin / Backoff / Park), std-shaped API, and a loom+miri+ARM verification bar.

**Architecture:** Layered core + notify (spec Approach A): the lock-free cores are generified ports of hi-perf-cmp's proven `thread-handoff-{ring,mpsc_ring}` algorithms; all parking lives in a separate notify layer (single-slot `Parker` for the single consumer / SPSC producer, cold-path `WaiterList` for MPSC producers) driven by a per-channel `WaitStrategy`. Wake correctness uses the Dekker store-fence-load protocol; loom model-checks it.

**Tech Stack:** Rust stable (edition 2024), zero runtime deps. Dev-deps: `loom` (cfg-gated), `criterion`. CI: GitHub Actions x86 + ARM (`ubuntu-24.04-arm`) + miri (nightly) + loom lanes.

**Spec:** `docs/superpowers/specs/2026-08-06-ultima-rings-v1-design.md` (including the CAS-claim amendment — see Global Constraints).

## Global Constraints

- Repo: `/home/claude/ultima/ultima_rings`. All work on branch `feat/v1` (create in Task 1). Every implementer verifies `git rev-parse --show-toplevel` prints this repo before committing.
- API names are std-shaped and EXACT: `spsc::channel<T>(cap, strategy)`, `mpsc::channel<T>(cap, strategy)` → `(Sender<T>, Receiver<T>)`; methods `try_send`, `send`, `try_recv`, `recv`, `drain`; errors `TrySendError::{Full(T), Disconnected(T)}`, `SendError<T>(pub T)`, `TryRecvError::{Empty, Disconnected}`, `RecvError`.
- `cap` must be a positive power of two → panic `"capacity must be a positive power of two"` at construction. Slot indexing uses `& (cap-1)` (mask), a documented deviation from the bench cells' `%`.
- **MPSC claim is a bounded CAS** (spec amendment): a sequence is claimed only when `claim − head < cap`, so a claimed slot is always already consumed — no in-publish backpressure spin, exact `try_send`, parked senders never hold unfilled slots. Publish/consume orderings are unchanged from the proven cells: slot write → `avail` store Release (round `seq / cap`, `-1` sentinel); consumer `avail` load Acquire; head store Release once per drain; head loads Acquire.
- Disconnect semantics mirror std: all senders dropped → receiver drains remaining then `Disconnected`; receiver dropped → sends fail returning the value; published messages are never lost; every disconnect wakes all parked threads.
- Park-mode wake protocol is Dekker: waiter does flag-store → `fence(SeqCst)` → re-check → park; waker does publish → `fence(SeqCst)` → flag-check → wake. The fence runs ONLY when `strategy == Park`.
- `T: Send` is the only bound. Handles are `Send`, not `Sync`. Zero per-op allocation.
- Zero runtime dependencies. `loom` and `criterion` are dev-deps only; loom code is behind `cfg(loom)` (RUSTFLAGS).
- Keep the crate rustfmt- and clippy-clean (`cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`) at every commit.
- Unsafe code carries a `// SAFETY:` comment stating the invariant it relies on.

---

### Task 1: Scaffold — Cargo, toolchain, errors, wait strategies, atomic facade

**Files:**
- Create: `Cargo.toml`, `rust-toolchain.toml`, `.gitignore`, `LICENSE`
- Create: `src/lib.rs`, `src/wait.rs`, `src/atomic.rs`

**Interfaces:**
- Produces: error types above; `WaitStrategy::{BusySpin, Backoff, Park}` (`Copy + Clone + Debug + PartialEq + Eq`); `wait::Idle` (per-blocking-op backoff state: `Idle::new()`, `idle(&mut self)`); `atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering, fence, UnsafeCell, CachePadded}`. All later tasks consume these exact paths.

- [ ] **Step 1: Create the branch and scaffold files**

```bash
cd /home/claude/ultima/ultima_rings && git checkout -b feat/v1
cp /home/claude/ultima/ultima_cluster/LICENSE LICENSE
printf 'target/\n' > .gitignore
```

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

`Cargo.toml`:

```toml
[package]
name = "ultima_rings"
version = "0.1.0"
edition = "2024"
license = "Apache-2.0"
authors = ["Peter Knego <peter@knego.net>"]
description = "Bounded lock-free SPSC/MPSC rings with pluggable wait strategies"
repository = "https://github.com/pknego/ultima_rings"

[dependencies]

[dev-dependencies]
criterion = "0.5"

[target.'cfg(loom)'.dev-dependencies]
loom = "0.7"

[[bench]]
name = "throughput"
harness = false

[lints.rust]
unexpected_cfgs = { level = "warn", check-cfg = ["cfg(loom)"] }
```

- [ ] **Step 2: Write `src/atomic.rs`** (the loom facade; the loom side is completed in Task 6 — the cfg structure lands now so all core code is written against it)

```rust
//! Facade over `std` vs `loom` sync primitives so the cores can be
//! model-checked. Everything in the crate uses these re-exports, never
//! `std::sync::atomic` directly.

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering, fence};

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering, fence};

/// `UnsafeCell` with loom's closure API in both builds.
#[cfg(not(loom))]
#[derive(Debug)]
pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub(crate) fn new(v: T) -> Self {
        Self(std::cell::UnsafeCell::new(v))
    }
    pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }
    pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}

#[cfg(loom)]
pub(crate) use loom::cell::UnsafeCell;

/// Pins a value to its own 64-byte cache line (same trick as the bench cells).
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct CachePadded<T>(pub(crate) T);
```

- [ ] **Step 3: Write the failing test for the Backoff ladder, inside `src/wait.rs`**

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn idle_ladder_spins_then_yields_then_parks_bounded() {
        let mut idle = Idle::new();
        // The first SPINS + YIELDS steps must not sleep (fast).
        let t = Instant::now();
        for _ in 0..(SPINS + YIELDS) {
            idle.idle();
        }
        assert!(t.elapsed().as_millis() < 100, "spin/yield rungs slept");
        // Subsequent steps park with doubling, capped at PARK_MAX.
        let t = Instant::now();
        idle.idle(); // first park: 1 µs floor
        assert!(t.elapsed().as_nanos() >= 1_000, "first park under 1 µs");
        for _ in 0..30 {
            idle.idle(); // doubling must cap, not overflow
        }
    }
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cd /home/claude/ultima/ultima_rings && cargo test`
Expected: COMPILE ERROR — `Idle`, `SPINS`, `YIELDS` not found.

- [ ] **Step 5: Implement `src/wait.rs`**

```rust
//! Wait strategies. `BusySpin` and `Backoff` are self-waking; `Park` blocks
//! via the notify layer (`crate::notify`) and needs the Dekker wake protocol.

use std::time::Duration;

/// How a blocked side (consumer-on-empty, producer-on-full) waits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// `spin_loop()` until progress: lowest latency, one core per blocked side.
    BusySpin,
    /// Aeron-style idle ladder: spins, then yields, then timed parks doubling
    /// 1 µs → 1 ms. Self-waking — the other side never needs to notify.
    Backoff,
    /// Fully blocking park/wake via the notify layer: zero idle CPU,
    /// ~1–5 µs wake latency.
    Park,
}

pub(crate) const SPINS: u32 = 10;
pub(crate) const YIELDS: u32 = 20;
const PARK_MIN: Duration = Duration::from_micros(1);
const PARK_MAX: Duration = Duration::from_millis(1);

/// Per-blocking-operation ladder state (the `backoff` bench cell's ladder).
#[derive(Debug)]
pub(crate) struct Idle {
    step: u32,
    park: Duration,
}

impl Idle {
    pub(crate) fn new() -> Self {
        Idle { step: 0, park: PARK_MIN }
    }

    /// One rung of the ladder. Timed parks self-wake, so callers re-check
    /// their condition after every call.
    pub(crate) fn idle(&mut self) {
        if self.step < SPINS {
            std::hint::spin_loop();
        } else if self.step < SPINS + YIELDS {
            std::thread::yield_now();
        } else {
            std::thread::park_timeout(self.park);
            self.park = (self.park * 2).min(PARK_MAX);
        }
        self.step = self.step.saturating_add(1);
    }
}
```

- [ ] **Step 6: Write `src/lib.rs`** (crate docs, errors, module wiring — `spsc`/`mpsc`/`notify` modules are added by their own tasks)

```rust
//! Bounded lock-free SPSC and MPSC rings with pluggable wait strategies.
//!
//! Extracted from the `hi-perf-cmp` thread-handoff benchmarks and hardened
//! for production use: generic payloads, blocking and non-blocking APIs,
//! close/disconnect semantics, and a loom/miri-verified concurrency core.
//! See `docs/design.md` for the memory-ordering invariants.

mod atomic;
mod wait;

pub use wait::WaitStrategy;

use std::fmt;

/// Error for [`try_send`]: the value is handed back in both cases.
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The ring is full.
    Full(T),
    /// The receiver was dropped.
    Disconnected(T),
}

/// Error for blocking `send`: the receiver was dropped; the value is returned.
#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

/// Error for `try_recv`.
#[derive(Debug, PartialEq, Eq)]
pub enum TryRecvError {
    /// The ring is empty (senders still live).
    Empty,
    /// All senders dropped and the ring is drained.
    Disconnected,
}

/// Error for blocking `recv`: all senders dropped and the ring is drained.
#[derive(Debug, PartialEq, Eq)]
pub struct RecvError;

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrySendError::Full(_) => write!(f, "ring is full"),
            TrySendError::Disconnected(_) => write!(f, "receiver disconnected"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiver disconnected")
    }
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "ring is empty"),
            TryRecvError::Disconnected => write!(f, "senders disconnected"),
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "senders disconnected")
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}
impl<T: fmt::Debug> std::error::Error for SendError<T> {}
impl std::error::Error for TryRecvError {}
impl std::error::Error for RecvError {}

pub(crate) fn assert_cap(cap: usize) {
    assert!(
        cap > 0 && cap.is_power_of_two(),
        "capacity must be a positive power of two"
    );
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: ladder test PASSES; clippy clean. (`atomic.rs` items are unused until Task 2 — if clippy flags dead code, add `#![allow(dead_code)]` at the top of `atomic.rs` with a `// removed in Task 2` note.)

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat: scaffold — errors, WaitStrategy + Idle ladder, atomic facade"
```

---

### Task 2: Generic SPSC core — `try_send`/`try_recv`/`drain` + drop-drain

**Files:**
- Create: `src/spsc.rs`
- Modify: `src/lib.rs` (add `pub mod spsc;`)
- Test: inline `mod tests` + `tests/spsc_stress.rs`

**Interfaces:**
- Consumes: Task 1's `atomic::*`, errors, `assert_cap`, `WaitStrategy`.
- Produces: `spsc::channel<T>(cap: usize, strategy: WaitStrategy) -> (Sender<T>, Receiver<T>)`; `Sender::try_send(&mut self, v: T) -> Result<(), TrySendError<T>>`; `Receiver::{try_recv(&mut self) -> Result<T, TryRecvError>, drain(&mut self, max: usize, f: impl FnMut(T)) -> usize}`. Internal fields Task 3 extends: `Shared<T> { tail, head, disconnected, consumer_parker, producer_parker, strategy, .. }` — in THIS task the parker fields do not exist yet; Task 3 adds them. Blocking `send`/`recv` are Task 3.

- [ ] **Step 1: Write the failing tests**

Inline in `src/spsc.rs` (bottom):

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::{TryRecvError, TrySendError, WaitStrategy};

    #[test]
    #[should_panic(expected = "positive power of two")]
    fn rejects_non_power_of_two_capacity() {
        let _ = channel::<u64>(100, WaitStrategy::BusySpin);
    }

    #[test]
    fn full_and_empty_are_reported() {
        let (mut tx, mut rx) = channel::<u32>(2, WaitStrategy::BusySpin);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
        assert_eq!(rx.try_recv(), Ok(1));
        tx.try_send(3).unwrap(); // wrap after space frees
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(3));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn disconnect_semantics_try_paths() {
        // Sender dropped: drain remaining, then Disconnected.
        let (mut tx, mut rx) = channel::<String>(4, WaitStrategy::BusySpin);
        tx.try_send("a".into()).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok("a".to_string()));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        // Receiver dropped: send fails returning the value.
        let (mut tx, rx) = channel::<String>(4, WaitStrategy::BusySpin);
        drop(rx);
        assert_eq!(
            tx.try_send("b".into()),
            Err(TrySendError::Disconnected("b".to_string()))
        );
    }

    #[test]
    fn drain_consumes_batch() {
        let (mut tx, mut rx) = channel::<u64>(8, WaitStrategy::BusySpin);
        for i in 0..5 {
            tx.try_send(i).unwrap();
        }
        let mut got = Vec::new();
        assert_eq!(rx.drain(3, |v| got.push(v)), 3);
        assert_eq!(rx.drain(usize::MAX, |v| got.push(v)), 2);
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
        assert_eq!(rx.drain(usize::MAX, |_| {}), 0);
    }

    #[test]
    fn zero_sized_payloads_work() {
        let (mut tx, mut rx) = channel::<()>(2, WaitStrategy::BusySpin);
        tx.try_send(()).unwrap();
        assert_eq!(rx.try_recv(), Ok(()));
    }
}
```

`tests/spsc_stress.rs`:

```rust
//! Cross-thread order/count stress and drop-accounting for the SPSC ring.
#![cfg(not(loom))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, spsc};

#[test]
fn spsc_preserves_order_and_count_across_threads() {
    let n: u64 = if cfg!(miri) { 2_000 } else { 100_000 };
    let (mut tx, mut rx) = spsc::channel::<u64>(64, WaitStrategy::BusySpin);
    let consumer = thread::spawn(move || {
        let mut got = Vec::with_capacity(n as usize);
        while (got.len() as u64) < n {
            match rx.try_recv() {
                Ok(v) => got.push(v),
                Err(TryRecvError::Empty) => std::hint::spin_loop(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        got
    });
    for i in 0..n {
        loop {
            match tx.try_send(i) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => std::hint::spin_loop(),
                Err(TrySendError::Disconnected(_)) => panic!("receiver died"),
            }
        }
    }
    let got = consumer.join().unwrap();
    assert_eq!(got.len() as u64, n);
    for (i, v) in got.iter().enumerate() {
        assert_eq!(*v, i as u64, "token {i} out of order");
    }
}

/// Payload that counts drops: proves no leak and no double-drop, including
/// values still in the ring when it is dropped.
struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn every_value_dropped_exactly_once_including_ring_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = spsc::channel::<Counted>(8, WaitStrategy::BusySpin);
    for _ in 0..6 {
        tx.try_send(Counted(Arc::clone(&drops))).unwrap();
    }
    // Consume 2 (dropped by us), leave 4 in the ring for drop-drain.
    for _ in 0..2 {
        drop(rx.try_recv().unwrap());
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(tx);
    drop(rx); // ring drop must drain the remaining 4
    assert_eq!(drops.load(Ordering::Relaxed), 6, "leak or double-drop");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test`
Expected: COMPILE ERROR — module `spsc` not found.

- [ ] **Step 3: Implement `src/spsc.rs`** (and add `pub mod spsc;` after `mod atomic;` in `lib.rs`; remove any Task-1 `allow(dead_code)` from `atomic.rs`)

```rust
//! Bounded single-producer single-consumer ring, generic payload.
//!
//! Generified port of hi-perf-cmp's `thread-handoff-ring` SPSC: `head`/`tail`
//! monotonic counters on separate cache lines, each side caching the opposite
//! index and re-loading the contended atomic only when the ring appears
//! full/empty. Slots are `MaybeUninit<T>`; the index Release/Acquire edges
//! publish the slot writes exactly as they published `u64` stores in the
//! bench cell.

use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::atomic::{AtomicBool, AtomicUsize, CachePadded, Ordering, UnsafeCell};
use crate::wait::WaitStrategy;
use crate::{TryRecvError, TrySendError, assert_cap};

pub(crate) struct Shared<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    mask: usize,
    cap: usize,
    pub(crate) tail: CachePadded<AtomicUsize>, // total pushed (producer writes)
    pub(crate) head: CachePadded<AtomicUsize>, // total popped (consumer writes)
    /// Either handle dropped. One flag serves both directions: a live side
    /// can only ever observe the *other* side's disconnect.
    pub(crate) disconnected: AtomicBool,
    pub(crate) strategy: WaitStrategy,
}

// SAFETY: the single-producer/single-consumer discipline (enforced by the
// unique, !Sync handles taking &mut self) guarantees each slot is written by
// at most one thread before its Release-publish and read by at most one
// thread after the matching Acquire. T: Send suffices.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Both handles are gone; head..tail is the initialized, unconsumed
        // range. Plain loads are fine (we have &mut self).
        let head = self.head.0.load(Ordering::Acquire);
        let tail = self.tail.0.load(Ordering::Acquire);
        for seq in head..tail {
            // SAFETY: slots in head..tail were written and never read.
            self.buf[seq & self.mask].with_mut(|p| unsafe { (*p).assume_init_drop() });
        }
    }
}

/// Create a bounded SPSC ring. `cap` must be a positive power of two.
pub fn channel<T: Send>(cap: usize, strategy: WaitStrategy) -> (Sender<T>, Receiver<T>) {
    assert_cap(cap);
    let buf = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let shared = Arc::new(Shared {
        buf,
        mask: cap - 1,
        cap,
        tail: CachePadded(AtomicUsize::new(0)),
        head: CachePadded(AtomicUsize::new(0)),
        disconnected: AtomicBool::new(false),
        strategy,
    });
    (
        Sender { shared: Arc::clone(&shared), tail: 0, cached_head: 0 },
        Receiver { shared, head: 0, cached_tail: 0 },
    )
}

/// The single producer. Owns the authoritative tail mirror and a cached head.
pub struct Sender<T: Send> {
    pub(crate) shared: Arc<Shared<T>>,
    tail: usize,
    cached_head: usize,
}

/// The single consumer. Owns the authoritative head mirror and a cached tail.
pub struct Receiver<T: Send> {
    pub(crate) shared: Arc<Shared<T>>,
    head: usize,
    cached_tail: usize,
}

impl<T: Send> Sender<T> {
    /// Push without blocking. `Full(v)` when the ring is full,
    /// `Disconnected(v)` when the receiver is gone.
    pub fn try_send(&mut self, v: T) -> Result<(), TrySendError<T>> {
        let sh = &*self.shared;
        if sh.disconnected.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(v));
        }
        if self.tail - self.cached_head == sh.cap {
            self.cached_head = sh.head.0.load(Ordering::Acquire);
            if self.tail - self.cached_head == sh.cap {
                return Err(TrySendError::Full(v));
            }
        }
        // SAFETY: tail - head < cap, so this slot's previous occupant (if
        // any) was consumed; we are the only writer.
        sh.buf[self.tail & sh.mask].with_mut(|p| unsafe {
            (*p).write(v);
        });
        self.tail += 1;
        // Release publishes the slot write above.
        sh.tail.0.store(self.tail, Ordering::Release);
        Ok(())
    }
}

impl<T: Send> Receiver<T> {
    /// Pop without blocking. Drains remaining items after a sender disconnect
    /// before reporting `Disconnected`.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let sh = &*self.shared;
        if self.head == self.cached_tail {
            self.cached_tail = sh.tail.0.load(Ordering::Acquire);
            if self.head == self.cached_tail {
                if sh.disconnected.load(Ordering::Acquire) {
                    // The sender's final publish happens-before its disconnect
                    // store; one more tail read decides drained-vs-remaining.
                    let t = sh.tail.0.load(Ordering::Acquire);
                    if t == self.head {
                        return Err(TryRecvError::Disconnected);
                    }
                    self.cached_tail = t;
                } else {
                    return Err(TryRecvError::Empty);
                }
            }
        }
        // SAFETY: head < cached_tail <= published tail; the slot was written
        // before the Acquire-observed tail store, and is read exactly once.
        let v = sh.buf[self.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
        self.head += 1;
        // Release publishes the slot as reusable before exposing the new head.
        sh.head.0.store(self.head, Ordering::Release);
        Ok(v)
    }

    /// Consume up to `max` currently-available items, advancing the shared
    /// head once at the end. Returns the count consumed.
    pub fn drain(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        let sh = &*self.shared;
        let mut count = 0usize;
        while count < max {
            if self.head == self.cached_tail {
                self.cached_tail = sh.tail.0.load(Ordering::Acquire);
                if self.head == self.cached_tail {
                    break;
                }
            }
            // SAFETY: as in try_recv.
            let v = sh.buf[self.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
            self.head += 1;
            count += 1;
            f(v);
        }
        if count > 0 {
            sh.head.0.store(self.head, Ordering::Release);
        }
        count
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
    }
}

impl<T: Send> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: all Task 1+2 tests PASS (stress ~1–2 s), clippy clean. Note `drain` publishes head once per batch while `try_recv` publishes per pop — both are per the bench cells.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(spsc): generic SPSC core — try paths, drain, drop-drain"
```

---

### Task 3: Notify layer + SPSC blocking `send`/`recv` + close wakes

**Files:**
- Create: `src/notify.rs`
- Modify: `src/lib.rs` (add `mod notify;`), `src/spsc.rs` (parker fields + blocking methods + drop wakes)
- Test: `tests/spsc_blocking.rs`

**Interfaces:**
- Consumes: Task 2's `spsc` internals; Task 1's `wait::Idle`, `atomic::fence`.
- Produces: `notify::Parker` — `new()`, `prepare_park(&self)`, `cancel(&self)`, `park(&self)`, `wake(&self)`; `notify::WaiterList` — `new()`, `prepare_wait(&self)`, `park(&self)`, `wake_all(&self)` (WaiterList is consumed by Task 5; its loom twin arrives in Task 6). `spsc::Sender::send(&mut self, v: T) -> Result<(), SendError<T>>`, `spsc::Receiver::recv(&mut self) -> Result<T, RecvError>`.

- [ ] **Step 1: Write the failing tests** — `tests/spsc_blocking.rs`

```rust
//! Blocking send/recv across all three wait strategies + close semantics.
#![cfg(not(loom))]

use std::thread;
use std::time::Duration;
use ultima_rings::{RecvError, SendError, WaitStrategy, spsc};

fn roundtrip(strategy: WaitStrategy) {
    let n: u64 = if cfg!(miri) { 500 } else { 20_000 };
    // Capacity 4 forces the producer to block regularly.
    let (mut tx, mut rx) = spsc::channel::<u64>(4, strategy);
    let consumer = thread::spawn(move || {
        let mut got = Vec::new();
        while let Ok(v) = rx.recv() {
            got.push(v);
        }
        got
    });
    for i in 0..n {
        tx.send(i).unwrap(); // must block (not error) when full
    }
    drop(tx); // consumer's recv() returns Err after draining
    let got = consumer.join().unwrap();
    assert_eq!(got.len() as u64, n);
    for (i, v) in got.iter().enumerate() {
        assert_eq!(*v, i as u64);
    }
}

#[test]
fn roundtrip_busy_spin() {
    roundtrip(WaitStrategy::BusySpin);
}

#[test]
fn roundtrip_backoff() {
    roundtrip(WaitStrategy::Backoff);
}

#[test]
fn roundtrip_park() {
    roundtrip(WaitStrategy::Park);
}

#[test]
fn parked_recv_wakes_on_send() {
    let (mut tx, mut rx) = spsc::channel::<u64>(4, WaitStrategy::Park);
    let consumer = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(50)); // let it park
    tx.send(7).unwrap();
    assert_eq!(consumer.join().unwrap(), Ok(7));
}

#[test]
fn parked_recv_wakes_on_disconnect() {
    let (tx, mut rx) = spsc::channel::<u64>(4, WaitStrategy::Park);
    let consumer = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(50));
    drop(tx);
    assert_eq!(consumer.join().unwrap(), Err(RecvError));
}

#[test]
fn parked_send_wakes_on_disconnect_and_returns_value() {
    let (mut tx, rx) = spsc::channel::<u64>(1, WaitStrategy::Park);
    tx.send(1).unwrap(); // fill
    let producer = thread::spawn(move || tx.send(2)); // parks on full
    thread::sleep(Duration::from_millis(50));
    drop(rx);
    assert_eq!(producer.join().unwrap(), Err(SendError(2)));
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test spsc_blocking`
Expected: COMPILE ERROR — no method `send`/`recv`.

- [ ] **Step 3: Implement `src/notify.rs`** (std side; the `cfg(loom)` twin is Task 6)

```rust
//! The notify layer: all parking lives here, none in the lock-free cores.
//!
//! Wake correctness is the Dekker protocol (see docs/design.md): the waiter
//! stores its flag, fences SeqCst, re-checks the ring, then parks; the waker
//! publishes, fences SeqCst, then checks the flag. `std::thread::park`'s
//! token makes a wake that races ahead of the park harmless.

#[cfg(not(loom))]
mod imp {
    use crate::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;
    use std::thread::{self, Thread};

    /// Single-waiter parker (the single consumer, or the SPSC producer).
    #[derive(Debug)]
    pub(crate) struct Parker {
        parked: AtomicBool,
        slot: Mutex<Option<Thread>>, // cold path only (park/wake transitions)
    }

    impl Parker {
        pub(crate) fn new() -> Self {
            Parker { parked: AtomicBool::new(false), slot: Mutex::new(None) }
        }

        /// Register intent to park. Caller MUST fence(SeqCst) and re-check
        /// its wait condition before calling `park`.
        pub(crate) fn prepare_park(&self) {
            *self.slot.lock().unwrap() = Some(thread::current());
            self.parked.store(true, Ordering::Relaxed);
        }

        /// Withdraw after a failed re-check.
        pub(crate) fn cancel(&self) {
            self.parked.store(false, Ordering::Relaxed);
        }

        /// Block until woken (or spuriously). Always re-check after return.
        pub(crate) fn park(&self) {
            thread::park();
            self.parked.store(false, Ordering::Relaxed);
        }

        /// Wake the registered waiter if one is parked. Caller MUST have
        /// fenced SeqCst after its publish.
        pub(crate) fn wake(&self) {
            if self.parked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                if let Some(t) = self.slot.lock().unwrap().take() {
                    t.unpark();
                }
            }
        }
    }

    /// Multi-waiter list (MPSC producers blocked on a full ring). Cold path
    /// by construction: it only runs once a sender has decided to park.
    #[derive(Debug)]
    pub(crate) struct WaiterList {
        waiting: AtomicBool,
        list: Mutex<Vec<Thread>>,
    }

    impl WaiterList {
        pub(crate) fn new() -> Self {
            WaiterList { waiting: AtomicBool::new(false), list: Mutex::new(Vec::new()) }
        }

        /// Register the current thread. Caller MUST fence(SeqCst) and
        /// re-check before `park`.
        pub(crate) fn prepare_wait(&self) {
            self.list.lock().unwrap().push(std::thread::current());
            self.waiting.store(true, Ordering::Relaxed);
        }

        /// Block until woken (or spuriously). Always re-check after return.
        pub(crate) fn park(&self) {
            std::thread::park();
        }

        /// Wake every registered waiter (each re-checks its own condition).
        /// Caller MUST have fenced SeqCst after advancing head/disconnecting.
        pub(crate) fn wake_all(&self) {
            if self.waiting.swap(false, Ordering::Relaxed) {
                for t in self.list.lock().unwrap().drain(..) {
                    t.unpark();
                }
            }
        }
    }
}

pub(crate) use imp::{Parker, WaiterList};
```

- [ ] **Step 4: Extend `src/spsc.rs`**

Add to `Shared<T>` (after `disconnected`):

```rust
    pub(crate) consumer_parker: crate::notify::Parker,
    pub(crate) producer_parker: crate::notify::Parker,
```

and to the constructor struct literal:

```rust
        consumer_parker: crate::notify::Parker::new(),
        producer_parker: crate::notify::Parker::new(),
```

At the END of `try_send`'s success path, before `Ok(())` — wake a parked consumer (Park mode only):

```rust
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.consumer_parker.wake();
        }
```

Symmetrically in `try_recv` and in `drain` (after the `count > 0` head store), wake a parked producer:

```rust
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.producer_parker.wake();
        }
```

Add the blocking methods:

```rust
impl<T: Send> Sender<T> {
    /// Push, blocking per the channel's wait strategy while the ring is full.
    /// Fails only when the receiver disconnects, returning the value.
    pub fn send(&mut self, v: T) -> Result<(), crate::SendError<T>> {
        use crate::wait::Idle;
        let mut v = v;
        let mut idle = Idle::new();
        loop {
            match self.try_send(v) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(back)) => return Err(crate::SendError(back)),
                Err(TrySendError::Full(back)) => {
                    v = back;
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Backoff => idle.idle(),
                        WaitStrategy::Park => {
                            sh.producer_parker.prepare_park();
                            crate::atomic::fence(Ordering::SeqCst);
                            let head = sh.head.0.load(Ordering::Acquire);
                            if self.tail - head < sh.cap
                                || sh.disconnected.load(Ordering::Acquire)
                            {
                                sh.producer_parker.cancel();
                            } else {
                                sh.producer_parker.park();
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<T: Send> Receiver<T> {
    /// Pop, blocking per the channel's wait strategy while the ring is empty.
    /// Fails only when all senders are gone and the ring is drained.
    pub fn recv(&mut self) -> Result<T, crate::RecvError> {
        use crate::wait::Idle;
        let mut idle = Idle::new();
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(crate::RecvError),
                Err(TryRecvError::Empty) => {
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Backoff => idle.idle(),
                        WaitStrategy::Park => {
                            sh.consumer_parker.prepare_park();
                            crate::atomic::fence(Ordering::SeqCst);
                            if sh.tail.0.load(Ordering::Acquire) != self.head
                                || sh.disconnected.load(Ordering::Acquire)
                            {
                                sh.consumer_parker.cancel();
                            } else {
                                sh.consumer_parker.park();
                            }
                        }
                    }
                }
            }
        }
    }
}
```

Replace both handle `Drop` impls so every disconnect wakes both sides:

```rust
impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
        crate::atomic::fence(Ordering::SeqCst);
        self.shared.consumer_parker.wake();
        self.shared.producer_parker.wake();
    }
}

impl<T: Send> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.disconnected.store(true, Ordering::Release);
        crate::atomic::fence(Ordering::SeqCst);
        self.shared.consumer_parker.wake();
        self.shared.producer_parker.wake();
    }
}
```

Add `mod notify;` to `lib.rs` (after `mod atomic;`). `WaiterList` is unused until Task 5 — put `#[allow(dead_code)]` on `WaiterList` with a `// used by mpsc (Task 5)` note.

- [ ] **Step 5: Run tests**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: all PASS, including the three roundtrips and the three park/wake tests. If `parked_recv_wakes_on_send` ever hangs, that is a lost wakeup — fix the protocol, never the test.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(notify+spsc): parker/waiter-list layer, blocking send/recv, disconnect wakes"
```

---

### Task 4: Generic MPSC core — bounded-CAS claim, `try_send`/`try_recv`/`drain` + drop-drain

**Files:**
- Create: `src/mpsc.rs`
- Modify: `src/lib.rs` (add `pub mod mpsc;`)
- Test: inline `mod tests` + `tests/mpsc_stress.rs`

**Interfaces:**
- Consumes: Task 1's facade/errors, Task 3's `notify::{Parker, WaiterList}` (fields land now; blocking logic is Task 5).
- Produces: `mpsc::channel<T>(cap, strategy) -> (Sender<T>, Receiver<T>)`; `Sender<T>: Clone` with `try_send(&mut self, v: T)`; `Receiver::{try_recv, drain}` with the same signatures as spsc. Task 5 adds `send`/`recv`.

- [ ] **Step 1: Write the failing tests**

Inline in `src/mpsc.rs`:

```rust
#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use crate::{TryRecvError, TrySendError, WaitStrategy};

    #[test]
    #[should_panic(expected = "positive power of two")]
    fn rejects_non_power_of_two_capacity() {
        let _ = channel::<u64>(100, WaitStrategy::BusySpin);
    }

    #[test]
    fn try_send_reports_full_without_claiming() {
        let (mut tx, mut rx) = channel::<u32>(2, WaitStrategy::BusySpin);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        // A failed try_send must NOT consume a sequence: after one recv,
        // exactly one more send fits.
        assert_eq!(tx.try_send(3), Err(TrySendError::Full(3)));
        assert_eq!(tx.try_send(4), Err(TrySendError::Full(4)));
        assert_eq!(rx.try_recv(), Ok(1));
        tx.try_send(5).unwrap();
        assert_eq!(tx.try_send(6), Err(TrySendError::Full(6)));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Ok(5));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn disconnect_semantics_try_paths() {
        // All senders dropped: drain remaining, then Disconnected.
        let (mut tx, mut rx) = channel::<String>(4, WaitStrategy::BusySpin);
        let mut tx2 = tx.clone();
        tx.try_send("a".into()).unwrap();
        drop(tx);
        assert_eq!(rx.try_recv(), Ok("a".to_string())); // tx2 still live
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        drop(tx2);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
        // Receiver dropped: send fails returning the value.
        let (mut tx, rx) = channel::<String>(4, WaitStrategy::BusySpin);
        drop(rx);
        assert_eq!(
            tx.try_send("b".into()),
            Err(TrySendError::Disconnected("b".to_string()))
        );
    }

    #[test]
    fn drain_consumes_contiguous_prefix() {
        let (mut tx, mut rx) = channel::<u64>(8, WaitStrategy::BusySpin);
        for i in 0..5 {
            tx.try_send(i).unwrap();
        }
        let mut got = Vec::new();
        assert_eq!(rx.drain(usize::MAX, |v| got.push(v)), 5);
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
    }
}
```

`tests/mpsc_stress.rs`:

```rust
//! No-loss/no-dup under contention (the comparability-critical stress from
//! the bench cells) + drop-accounting.
#![cfg(not(loom))]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, mpsc};

fn run_stress(producers: usize, per: usize, cap: usize) {
    let total = producers * per;
    let (tx, mut rx) = mpsc::channel::<u64>(cap, WaitStrategy::BusySpin);
    let mut handles = Vec::new();
    for p in 0..producers {
        let mut tx = tx.clone();
        handles.push(thread::spawn(move || {
            let base = (p * per) as u64; // unique range per producer
            for i in 0..per {
                let mut v = base + i as u64;
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
    drop(tx);
    let mut seen: HashSet<u64> = HashSet::with_capacity(total);
    let mut dups = 0usize;
    loop {
        match rx.try_recv() {
            Ok(v) => {
                if !seen.insert(v) {
                    dups += 1;
                }
            }
            Err(TryRecvError::Empty) => std::hint::spin_loop(),
            Err(TryRecvError::Disconnected) => break,
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(dups, 0, "duplicate delivery");
    assert_eq!(seen.len(), total, "loss");
}

#[test]
fn mpsc_no_loss_no_dup_under_contention() {
    let (reps, per) = if cfg!(miri) { (1, 500) } else { (5, 30_000) };
    for _ in 0..reps {
        run_stress(4, per, 256);
    }
}

struct Counted(Arc<AtomicUsize>);
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn every_value_dropped_exactly_once_including_ring_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = mpsc::channel::<Counted>(8, WaitStrategy::BusySpin);
    for _ in 0..6 {
        tx.try_send(Counted(Arc::clone(&drops))).unwrap();
    }
    for _ in 0..2 {
        drop(rx.try_recv().unwrap());
    }
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    drop(tx);
    drop(rx);
    assert_eq!(drops.load(Ordering::Relaxed), 6, "leak or double-drop");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test`
Expected: COMPILE ERROR — module `mpsc` not found.

- [ ] **Step 3: Implement `src/mpsc.rs`** (add `pub mod mpsc;` to `lib.rs`)

```rust
//! Bounded multi-producer single-consumer ring, generic payload.
//!
//! LMAX-style availability publication ported from hi-perf-cmp's
//! `thread-handoff-mpsc_ring`, with one amendment (see docs/design.md):
//! sequence claim is a **bounded CAS** — a producer claims `seq` only after
//! proving `seq - head < cap` — so a claimed slot is always already free,
//! `try_send` can fail without claiming, and a blocked sender holds nothing.
//! Publish: slot write → `avail[slot] = seq / cap` (Release; -1 = never).
//! The single consumer drains the contiguous published prefix and stores the
//! shared head once per drain (Release).

use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::atomic::{AtomicBool, AtomicI64, AtomicUsize, CachePadded, Ordering, UnsafeCell};
use crate::notify::{Parker, WaiterList};
use crate::wait::WaitStrategy;
use crate::{TryRecvError, TrySendError, assert_cap};

pub(crate) struct Shared<T> {
    buf: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// Per-slot published round number (`seq / cap`); -1 = never published.
    avail: Box<[AtomicI64]>,
    mask: usize,
    cap: usize,
    pub(crate) claim: CachePadded<AtomicUsize>, // next sequence to claim
    pub(crate) head: CachePadded<AtomicUsize>,  // total consumed
    pub(crate) senders: AtomicUsize,
    pub(crate) rx_dropped: AtomicBool,
    pub(crate) consumer_parker: Parker,
    pub(crate) prod_waiters: WaiterList,
    pub(crate) strategy: WaitStrategy,
}

// SAFETY: each slot is written by exactly one claimer (CAS gives disjoint
// sequences) before its Release avail-store, and read by the single consumer
// after the matching Acquire load; the bounded claim guarantees the previous
// occupant was consumed before the slot is rewritten. T: Send suffices.
unsafe impl<T: Send> Send for Shared<T> {}
unsafe impl<T: Send> Sync for Shared<T> {}

impl<T> Drop for Shared<T> {
    fn drop(&mut self) {
        // Both sides are gone. Initialized-but-unconsumed values form the
        // contiguous published prefix from head (a claimed-but-unpublished
        // hole ends it; nothing beyond a hole is reachable).
        let mut seq = self.head.0.load(Ordering::Acquire);
        loop {
            let slot = seq & self.mask;
            if self.avail[slot].load(Ordering::Acquire) != (seq / self.cap) as i64 {
                break;
            }
            // SAFETY: published and never consumed.
            self.buf[slot].with_mut(|p| unsafe { (*p).assume_init_drop() });
            seq += 1;
        }
    }
}

/// Create a bounded MPSC ring. `cap` must be a positive power of two.
pub fn channel<T: Send>(cap: usize, strategy: WaitStrategy) -> (Sender<T>, Receiver<T>) {
    assert_cap(cap);
    let buf = (0..cap)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let avail = (0..cap).map(|_| AtomicI64::new(-1)).collect::<Vec<_>>().into_boxed_slice();
    let shared = Arc::new(Shared {
        buf,
        avail,
        mask: cap - 1,
        cap,
        claim: CachePadded(AtomicUsize::new(0)),
        head: CachePadded(AtomicUsize::new(0)),
        senders: AtomicUsize::new(1),
        rx_dropped: AtomicBool::new(false),
        consumer_parker: Parker::new(),
        prod_waiters: WaiterList::new(),
        strategy,
    });
    (
        Sender { shared: Arc::clone(&shared), cached_head: 0 },
        Receiver { shared, head: 0 },
    )
}

/// A producer handle. `clone()` it once per producer thread.
pub struct Sender<T: Send> {
    pub(crate) shared: Arc<Shared<T>>,
    cached_head: usize, // per-producer cached consumer head
}

/// The single consumer.
pub struct Receiver<T: Send> {
    pub(crate) shared: Arc<Shared<T>>,
    head: usize, // consumer-private cursor (mirrors shared head)
}

impl<T: Send> Sender<T> {
    /// Push without blocking. A failed attempt claims nothing.
    pub fn try_send(&mut self, v: T) -> Result<(), TrySendError<T>> {
        let sh = &*self.shared;
        if sh.rx_dropped.load(Ordering::Acquire) {
            return Err(TrySendError::Disconnected(v));
        }
        // Bounded CAS claim: only claim a sequence whose slot is provably
        // consumed (seq - head < cap). Head only advances, so a successful
        // CAS keeps the bound.
        let mut seq = sh.claim.0.load(Ordering::Relaxed);
        loop {
            if seq - self.cached_head >= sh.cap {
                self.cached_head = sh.head.0.load(Ordering::Acquire);
                if seq - self.cached_head >= sh.cap {
                    return Err(TrySendError::Full(v));
                }
            }
            match sh.claim.0.compare_exchange_weak(
                seq,
                seq + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(cur) => seq = cur,
            }
        }
        // SAFETY: bounded claim — this slot's previous occupant was consumed;
        // CAS made us its unique writer for this round.
        sh.buf[seq & sh.mask].with_mut(|p| unsafe {
            (*p).write(v);
        });
        // Release pairs with the consumer's Acquire load of avail.
        sh.avail[seq & sh.mask].store((seq / sh.cap) as i64, Ordering::Release);
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.consumer_parker.wake();
        }
        Ok(())
    }
}

impl<T: Send> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Sender { shared: Arc::clone(&self.shared), cached_head: 0 }
    }
}

impl<T: Send> Receiver<T> {
    fn slot_published(&self, seq: usize) -> bool {
        let sh = &*self.shared;
        sh.avail[seq & sh.mask].load(Ordering::Acquire) == (seq / sh.cap) as i64
    }

    /// Pop without blocking. Drains remaining items after all senders
    /// disconnect before reporting `Disconnected`.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let sh = &*self.shared;
        if !self.slot_published(self.head) {
            if sh.senders.load(Ordering::Acquire) == 0 {
                // Final publishes happen-before the last sender-count
                // decrement; one more availability check decides.
                if !self.slot_published(self.head) {
                    return Err(TryRecvError::Disconnected);
                }
            } else {
                return Err(TryRecvError::Empty);
            }
        }
        // SAFETY: published (Acquire-observed) and consumed exactly once.
        let v = sh.buf[self.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
        self.head += 1;
        sh.head.0.store(self.head, Ordering::Release);
        self.wake_producers();
        Ok(v)
    }

    /// Consume up to `max` items of the contiguous published prefix,
    /// advancing the shared head once at the end.
    pub fn drain(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        let sh = &*self.shared;
        let mut count = 0usize;
        while count < max && self.slot_published(self.head) {
            // SAFETY: as in try_recv.
            let v = sh.buf[self.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
            self.head += 1;
            count += 1;
            f(v);
        }
        if count > 0 {
            sh.head.0.store(self.head, Ordering::Release);
            self.wake_producers();
        }
        count
    }

    fn wake_producers(&self) {
        let sh = &*self.shared;
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.prod_waiters.wake_all();
        }
    }
}

impl<T: Send> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            crate::atomic::fence(Ordering::SeqCst);
            self.shared.consumer_parker.wake();
        }
    }
}

impl<T: Send> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.rx_dropped.store(true, Ordering::Release);
        crate::atomic::fence(Ordering::SeqCst);
        self.shared.prod_waiters.wake_all();
    }
}
```

Remove the `#[allow(dead_code)]` from `WaiterList` (now used).

- [ ] **Step 4: Run tests**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: all PASS. The stress test runs ~5–15 s. The `try_send_reports_full_without_claiming` test is the CAS-amendment regression guard — a `fetch_add` implementation cannot pass it.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(mpsc): generic MPSC core — bounded-CAS claim, availability publish, drop-drain"
```

---

### Task 5: MPSC blocking `send`/`recv` + close-under-load

**Files:**
- Modify: `src/mpsc.rs`
- Test: `tests/mpsc_blocking.rs`

**Interfaces:**
- Consumes: Task 4's `mpsc` internals; `notify::{Parker, WaiterList}`; `wait::Idle`.
- Produces: `mpsc::Sender::send(&mut self, v: T) -> Result<(), SendError<T>>`; `mpsc::Receiver::recv(&mut self) -> Result<T, RecvError>`.

- [ ] **Step 1: Write the failing tests** — `tests/mpsc_blocking.rs`

```rust
//! Blocking MPSC across strategies + close-under-load races.
#![cfg(not(loom))]

use std::thread;
use std::time::Duration;
use ultima_rings::{RecvError, SendError, WaitStrategy, mpsc};

fn roundtrip(strategy: WaitStrategy) {
    let producers = 4usize;
    let per: u64 = if cfg!(miri) { 300 } else { 10_000 };
    let (tx, mut rx) = mpsc::channel::<u64>(8, strategy); // tiny cap forces blocking
    let mut handles = Vec::new();
    for p in 0..producers {
        let mut tx = tx.clone();
        handles.push(thread::spawn(move || {
            let base = p as u64 * per;
            for i in 0..per {
                tx.send(base + i).unwrap();
            }
        }));
    }
    drop(tx);
    let mut got = Vec::new();
    while let Ok(v) = rx.recv() {
        got.push(v);
    }
    for h in handles {
        h.join().unwrap();
    }
    got.sort_unstable();
    assert_eq!(got.len() as u64, producers as u64 * per);
    for (i, v) in got.iter().enumerate() {
        assert_eq!(*v, i as u64, "loss or duplication");
    }
}

#[test]
fn roundtrip_busy_spin() {
    roundtrip(WaitStrategy::BusySpin);
}

#[test]
fn roundtrip_backoff() {
    roundtrip(WaitStrategy::Backoff);
}

#[test]
fn roundtrip_park() {
    roundtrip(WaitStrategy::Park);
}

#[test]
fn parked_recv_wakes_on_last_sender_drop() {
    let (tx, mut rx) = mpsc::channel::<u64>(4, WaitStrategy::Park);
    let tx2 = tx.clone();
    let consumer = thread::spawn(move || rx.recv());
    thread::sleep(Duration::from_millis(50));
    drop(tx);
    thread::sleep(Duration::from_millis(20)); // one sender left: still parked
    drop(tx2);
    assert_eq!(consumer.join().unwrap(), Err(RecvError));
}

#[test]
fn parked_senders_wake_on_receiver_drop_and_return_values() {
    let (tx, rx) = mpsc::channel::<u64>(1, WaitStrategy::Park);
    let mut tx0 = tx.clone();
    tx0.send(0).unwrap(); // fill the 1-slot ring
    let mut handles = Vec::new();
    for i in 1..=2u64 {
        let mut tx = tx.clone();
        handles.push(thread::spawn(move || tx.send(i)));
    }
    drop(tx);
    thread::sleep(Duration::from_millis(50)); // both park on full
    drop(rx);
    let mut returned: Vec<u64> = handles
        .into_iter()
        .map(|h| match h.join().unwrap() {
            Err(SendError(v)) => v,
            Ok(()) => panic!("send succeeded after receiver drop"),
        })
        .collect();
    returned.sort_unstable();
    assert_eq!(returned, vec![1, 2]);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --test mpsc_blocking`
Expected: COMPILE ERROR — no method `send`/`recv` on mpsc handles.

- [ ] **Step 3: Implement in `src/mpsc.rs`**

```rust
impl<T: Send> Sender<T> {
    /// Push, blocking per the wait strategy while the ring is full. Because
    /// the claim is bounded-CAS, a blocked sender holds no sequence.
    pub fn send(&mut self, v: T) -> Result<(), crate::SendError<T>> {
        use crate::wait::Idle;
        let mut v = v;
        let mut idle = Idle::new();
        loop {
            match self.try_send(v) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(back)) => return Err(crate::SendError(back)),
                Err(TrySendError::Full(back)) => {
                    v = back;
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Backoff => idle.idle(),
                        WaitStrategy::Park => {
                            sh.prod_waiters.prepare_wait();
                            crate::atomic::fence(Ordering::SeqCst);
                            let claim = sh.claim.0.load(Ordering::Relaxed);
                            let head = sh.head.0.load(Ordering::Acquire);
                            if claim - head < sh.cap || sh.rx_dropped.load(Ordering::Acquire) {
                                // Space appeared or disconnected: skip the
                                // park; our registration is consumed by the
                                // next wake_all as a harmless spurious unpark.
                            } else {
                                sh.prod_waiters.park();
                            }
                        }
                    }
                }
            }
        }
    }
}

impl<T: Send> Receiver<T> {
    /// Pop, blocking per the wait strategy while the ring is empty.
    pub fn recv(&mut self) -> Result<T, crate::RecvError> {
        use crate::wait::Idle;
        let mut idle = Idle::new();
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(crate::RecvError),
                Err(TryRecvError::Empty) => {
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Backoff => idle.idle(),
                        WaitStrategy::Park => {
                            sh.consumer_parker.prepare_park();
                            crate::atomic::fence(Ordering::SeqCst);
                            if self.slot_published(self.head)
                                || sh.senders.load(Ordering::Acquire) == 0
                            {
                                sh.consumer_parker.cancel();
                            } else {
                                sh.consumer_parker.park();
                            }
                        }
                    }
                }
            }
        }
    }
}
```

- [ ] **Step 4: Run tests**

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt`
Expected: all PASS. `parked_senders_wake_on_receiver_drop_and_return_values` is the deadlock guard for the waiter list — if it hangs, the Dekker pairing on the producer side is broken.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(mpsc): blocking send/recv with parker/waiter-list, close-under-load"
```

---

### Task 6: loom lane — modeled parker + the four models

**Files:**
- Modify: `src/notify.rs` (add the `cfg(loom)` impl of `Parker`/`WaiterList`)
- Create: `tests/loom.rs`

**Interfaces:**
- Consumes: everything; the `atomic.rs` facade already switches on `cfg(loom)`.
- Produces: the loom verification lane: `RUSTFLAGS="--cfg loom" cargo test --test loom --release`.

- [ ] **Step 1: Add the loom twin to `src/notify.rs`** (below the `#[cfg(not(loom))] mod imp`, same `pub(crate) use imp::…` tail works for both)

```rust
#[cfg(loom)]
mod imp {
    //! Loom-modeled parking: Mutex+Condvar so loom explores wakeups and
    //! detects deadlocks (a lost wakeup = all-threads-blocked = loom failure).
    use crate::atomic::{AtomicBool, Ordering};
    use loom::sync::{Condvar, Mutex};

    #[derive(Debug)]
    pub(crate) struct Parker {
        parked: AtomicBool,
        state: Mutex<bool>, // token
        cv: Condvar,
    }

    impl Parker {
        pub(crate) fn new() -> Self {
            Parker { parked: AtomicBool::new(false), state: Mutex::new(false), cv: Condvar::new() }
        }
        pub(crate) fn prepare_park(&self) {
            self.parked.store(true, Ordering::Relaxed);
        }
        pub(crate) fn cancel(&self) {
            self.parked.store(false, Ordering::Relaxed);
        }
        pub(crate) fn park(&self) {
            let mut token = self.state.lock().unwrap();
            while !*token {
                token = self.cv.wait(token).unwrap();
            }
            *token = false;
            drop(token);
            self.parked.store(false, Ordering::Relaxed);
        }
        pub(crate) fn wake(&self) {
            if self.parked.load(Ordering::Relaxed) {
                self.parked.store(false, Ordering::Relaxed);
                *self.state.lock().unwrap() = true;
                self.cv.notify_one();
            }
        }
    }

    #[derive(Debug)]
    pub(crate) struct WaiterList {
        waiting: AtomicBool,
        gen: Mutex<usize>,
        cv: Condvar,
    }

    impl WaiterList {
        pub(crate) fn new() -> Self {
            WaiterList { waiting: AtomicBool::new(false), gen: Mutex::new(0), cv: Condvar::new() }
        }
        pub(crate) fn prepare_wait(&self) {
            self.waiting.store(true, Ordering::Relaxed);
        }
        pub(crate) fn park(&self) {
            let mut g = self.gen.lock().unwrap();
            let g0 = *g;
            while *g == g0 && self.waiting.load(Ordering::Relaxed) {
                g = self.cv.wait(g).unwrap();
            }
        }
        pub(crate) fn wake_all(&self) {
            if self.waiting.swap(false, Ordering::Relaxed) {
                *self.gen.lock().unwrap() += 1;
                self.cv.notify_all();
            }
        }
    }
}
```

- [ ] **Step 2: Write `tests/loom.rs`**

```rust
//! Loom model-checking: small caps/counts, all interleavings + orderings.
//! Run: RUSTFLAGS="--cfg loom" cargo test --test loom --release
#![cfg(loom)]

use loom::thread;
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, mpsc, spsc};

/// (1) SPSC publish/consume with wrap: order and count under all orderings.
#[test]
fn loom_spsc_publish_consume() {
    loom::model(|| {
        let (mut tx, mut rx) = spsc::channel::<u64>(2, WaitStrategy::BusySpin);
        let producer = thread::spawn(move || {
            for i in 0..3u64 {
                // cap 2, 3 items => exercises wrap + full
                let mut v = i;
                loop {
                    match tx.try_send(v) {
                        Ok(()) => break,
                        Err(TrySendError::Full(b)) => {
                            v = b;
                            thread::yield_now();
                        }
                        Err(TrySendError::Disconnected(_)) => unreachable!(),
                    }
                }
            }
        });
        let mut got = Vec::new();
        while got.len() < 3 {
            match rx.try_recv() {
                Ok(v) => got.push(v),
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        producer.join().unwrap();
        assert_eq!(got, vec![0, 1, 2]);
    });
}

/// (2) MPSC two producers, claim/publish/drain with wrap: exact multiset.
#[test]
fn loom_mpsc_two_producers() {
    loom::model(|| {
        let (tx, mut rx) = mpsc::channel::<u64>(2, WaitStrategy::BusySpin);
        let mut handles = Vec::new();
        for p in 0..2u64 {
            let mut tx = tx.clone();
            handles.push(thread::spawn(move || {
                let mut v = p; // one unique item per producer
                loop {
                    match tx.try_send(v) {
                        Ok(()) => break,
                        Err(TrySendError::Full(b)) => {
                            v = b;
                            thread::yield_now();
                        }
                        Err(TrySendError::Disconnected(_)) => unreachable!(),
                    }
                }
            }));
        }
        drop(tx);
        let mut got = Vec::new();
        while got.len() < 2 {
            match rx.try_recv() {
                Ok(v) => got.push(v),
                Err(TryRecvError::Empty) => thread::yield_now(),
                Err(TryRecvError::Disconnected) => break,
            }
        }
        for h in handles {
            h.join().unwrap();
        }
        got.sort_unstable();
        assert_eq!(got, vec![0, 1]);
    });
}

/// (3) Park/wake lost-wakeup: a parked consumer must always see the send.
/// A lost wakeup deadlocks -> loom reports all-threads-blocked.
#[test]
fn loom_park_no_lost_wakeup() {
    loom::model(|| {
        let (mut tx, mut rx) = spsc::channel::<u64>(2, WaitStrategy::Park);
        let consumer = thread::spawn(move || rx.recv().unwrap());
        tx.send(7).unwrap();
        assert_eq!(consumer.join().unwrap(), 7);
    });
}

/// (4) Close-vs-park: sender drop must wake a parked consumer.
#[test]
fn loom_close_wakes_parked_consumer() {
    loom::model(|| {
        let (tx, mut rx) = spsc::channel::<u64>(2, WaitStrategy::Park);
        let consumer = thread::spawn(move || rx.recv());
        drop(tx);
        assert!(consumer.join().unwrap().is_err());
    });
}
```

- [ ] **Step 3: Run the loom lane**

Run: `RUSTFLAGS="--cfg loom" cargo test --test loom --release`
Expected: all four models PASS (minutes, not hours — caps and counts are tiny). If loom reports a panic or deadlock, it prints the failing interleaving — fix the protocol, never shrink the model. Also confirm the normal lane still passes: `cargo test`.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(loom): modeled parker + spsc/mpsc/park/close models"
```

---

### Task 7: miri lane

**Files:**
- Modify (only if miri flags issues): `src/spsc.rs`, `src/mpsc.rs`, `src/atomic.rs`

**Interfaces:** none new — this task validates the unsafe surface.

- [ ] **Step 1: Install and run miri**

```bash
rustup toolchain install nightly --component miri
cd /home/claude/ultima/ultima_rings && cargo +nightly miri test
```

The test files already shrink under miri (`cfg!(miri)` counts from Tasks 2–5). Expected: PASS with no undefined-behavior reports. Miri is slow (minutes) — that is normal.

- [ ] **Step 2: Fix anything miri reports**

Typical findings and their legitimate fixes: stacked-borrows violations from holding a `*mut` across a publish (keep raw-pointer scopes inside single `with`/`with_mut` closures); reading `MaybeUninit` slots without the Acquire edge (never bypass `slot_published`/tail checks). Do NOT silence miri with `-Zmiri-disable-*` flags.

- [ ] **Step 3: Commit** (only if fixes were needed; otherwise record the clean run in the task report)

```bash
git add -A && git commit -m "fix: miri findings on the unsafe slot surface"
```

---

### Task 8: Criterion benches — regression guard

**Files:**
- Create: `benches/throughput.rs`

**Interfaces:**
- Consumes: public API only.

- [ ] **Step 1: Write `benches/throughput.rs`**

```rust
//! Regression-guard benches (single machine, indicative only — the
//! cross-language rig lives in hi-perf-cmp). Persistent producer threads,
//! barrier-released batches, wall-clock per batch.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, mpsc, spsc};

const BATCH: u64 = 100_000;

fn spsc_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("spsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("busy_spin_pipelined", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut tx, mut rx) = spsc::channel::<u64>(1024, WaitStrategy::BusySpin);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        got += rx.drain(usize::MAX, |_| {}) as u64;
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let mut v = i;
                    loop {
                        match tx.try_send(v) {
                            Ok(()) => break,
                            Err(TrySendError::Full(b)) => {
                                v = b;
                                std::hint::spin_loop();
                            }
                            Err(TrySendError::Disconnected(_)) => unreachable!(),
                        }
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.finish();
}

fn mpsc_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("busy_spin_2_producers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, mut rx) = mpsc::channel::<u64>(1024, WaitStrategy::BusySpin);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let mut tx = tx.clone();
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
                drop(tx);
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

criterion_group!(benches, spsc_throughput, mpsc_throughput);
criterion_main!(benches);
```

- [ ] **Step 2: Smoke-run**

Run: `cargo bench -- --quick 2>&1 | tail -20`
Expected: both benches complete with positive throughput; SPSC in the hundreds of M elem/s on this box, MPSC in the tens. Numbers are indicative only — do not tune against them here.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "bench: criterion throughput regression guards (spsc, mpsc)"
```

---

### Task 9: `docs/design.md` + README + rustdoc pass

**Files:**
- Create: `docs/design.md`, `README.md`
- Modify: doc comments where the pass below finds gaps

- [ ] **Step 1: Write `docs/design.md`** with exactly these sections, each making the stated argument (write full prose — this is the reference documentation, not an outline):

1. **Rings and invariants.** SPSC: monotonic head/tail, `tail - head ∈ [0, cap]`; every sequence written once before its Release-publish, read once after the Acquire-observe. MPSC: bounded-CAS claim invariant — a CAS claiming `seq` succeeds only after observing `seq - head < cap`, and head is monotonic, so the slot's previous occupant (sequence `seq - cap`) is consumed before any write; availability round `seq / cap` distinguishes rounds so a slot published in round `r` is never confused with round `r-1` (the wrap/ABA argument: rounds strictly increase per slot, and the consumer only accepts the exact expected round).
2. **Ordering table.** Every atomic with its ordering and its pairing: spsc `tail` store Release ↔ consumer `tail` load Acquire (publishes slot write); spsc `head` store Release ↔ producer `head` load Acquire (publishes slot reuse); mpsc `claim` CAS Relaxed (ordering rides entirely on `avail`); `avail` store Release ↔ load Acquire; mpsc `head` store Release ↔ producer/cleanup loads Acquire; `senders` fetch_sub AcqRel (last-decrement observes all prior publishes); disconnect flags store Release / load Acquire; why Relaxed is sufficient wherever it appears.
3. **The Dekker wake protocol.** The two racing sequences (waiter: flag-store → fence → re-check → park; waker: publish → fence → flag-check → wake), the two SeqCst fences' total-order argument (either the waker sees the flag or the waiter's re-check sees the publish), why `std::thread::park`'s token absorbs the unpark-before-park race, and why the fence is only paid in `Park` mode.
4. **Waiter list (MPSC producers).** Cold-path-by-construction argument; wake-all with per-waiter re-check; spurious unparks are harmless.
5. **Disconnect.** Publishes-happen-before-disconnect argument for both directions (SPSC: final tail store precedes the `disconnected` Release store, so the consumer's re-read of tail after observing the flag sees everything; MPSC: final `avail` stores precede the last `senders` decrement); why published messages are never lost; why a parked thread cannot sleep through a close (every disconnect transition fences and wakes).
6. **Drop-drain.** SPSC `head..tail`; MPSC contiguous published prefix; why a claimed-but-unpublished hole (a sender that bailed on `Disconnected`) safely terminates the drain.
7. **Deviations from the bench cells.** Bounded-CAS claim vs fetch_add (and what it buys); `&mask` vs `%`; what stayed byte-equivalent (the publish/consume edges).
8. **Costs.** Park-mode's one SeqCst fence per operation on each side; Backoff's zero cross-side cost; the false-sharing reality of the interleaved availability array (with the measured AWS numbers).

- [ ] **Step 2: Write `README.md`**: what it is (one paragraph, provenance from hi-perf-cmp), the API example below, the wait-strategy table with the guidance (BusySpin = latency at any CPU cost; Backoff = balanced; Park = idle-efficient), the measured numbers (AWS c6id.2xlarge, run `20260806T053918Z`: SPSC 387 M ops/s Rust pipelined, one-way handoff p50 ~200–300 ns; MPSC 2-producer 9.4 M ops/s, p50 277 ns) with the caveat that they are the bench-cell (u64, `%`-indexed, fetch_add) numbers, verification story (loom, miri, ARM CI, stress), license.

```rust
use ultima_rings::{WaitStrategy, mpsc};

let (tx, mut rx) = mpsc::channel::<Event>(1024, WaitStrategy::Park);
let tx2 = tx.clone();
// producers:            consumers:
tx.send(event)?;         while let Ok(ev) = rx.recv() { handle(ev); }
```

- [ ] **Step 3: rustdoc pass**

Run: `cargo doc --no-deps 2>&1 | grep -i warning`
Expected: no warnings; every public item documented (spot-check the rendered docs list). Fix gaps.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "docs: design.md (invariants + ordering arguments), README with measured numbers"
```

---

### Task 10: CI — x86, ARM, miri, loom lanes

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  test-x86:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: rustup show active-toolchain
      - run: cargo fmt --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test

  test-arm:
    # Weak-memory hardware lane: same tests on aarch64.
    runs-on: ubuntu-24.04-arm
    steps:
      - uses: actions/checkout@v4
      - run: cargo test

  loom:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: RUSTFLAGS="--cfg loom" cargo test --test loom --release

  miri:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: rustup toolchain install nightly --component miri
      - run: cargo +nightly miri test
```

- [ ] **Step 2: Validate + run the local equivalents one final time**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('ok')"
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
RUSTFLAGS="--cfg loom" cargo test --test loom --release
cargo +nightly miri test
```

Expected: `ok` + all four green locally (ARM lane runs only in CI; note that in the task report).

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "ci: x86 + arm + loom + miri lanes"
```

---

## After the plan

Merge `feat/v1` per the finishing-a-development-branch skill. Follow-ups already out of scope per spec: producer batch API, MPMC, async, Go/Java ports, uc2 integration (blocked on the pending `uc2_net` branch), crates.io publish.
