//! Sharded MPSC: one SPSC ring per producer, consumer round-robins. The
//! producer set is **fixed at construction** — that is the stable contract of
//! this type, not a missing feature (see below). Reach for it when the
//! producer threads are known up front and per-producer FIFO is enough;
//! reach for [`crate::mpsc`] when producers come and go, `Sender: Clone` is
//! needed, or consumers rely on one global order.
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
//! **`Sender` is not `Clone`, and that is the point.** The fixed shard set is
//! what makes one-writer-per-shard structural, and one writer per ring is the
//! entire source of the speed: no CAS, no retry loop, no contended line. This
//! is a precondition of the design, not a missing feature — so adding `Clone`
//! would not be a small extension. It would have to preserve one writer per
//! shard (allocating a shard per clone, with the lifecycle and reaping that
//! implies) or give up the result, because two writers on one ring reintroduce
//! exactly the per-shard claim protocol this design exists to delete. The
//! measured figures in `docs/bench-results/2026-08-07-sharded-mpsc.md` hold for
//! the fixed set only.
//!
//! This module declares no atomics and contains no `unsafe`; every
//! memory-ordering edge belongs to [`crate::spsc`], which `tests/loom.rs`
//! models. The composition on top — the sweep's disconnect counting under
//! concurrent sends and drops — is modeled there too
//! (`sharded_composition`). See
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
///
/// Also panics if `strategy` is [`WaitStrategy::Park`]. The three self-waking
/// strategies (`BusySpin`, `Backoff`, `BackoffYield`) compose for free: a
/// blocked side wakes itself, so N independent rings need no shared plumbing.
/// `Park` does not compose — a consumer parked through one shard's parker
/// would sleep through every other shard's publish, and fixing that would
/// need a cross-shard parker registered with all N rings, putting a `SeqCst`
/// fence plus a wake on every send: exactly the per-publish cost this design
/// exists to delete.
pub fn channel<T: Send>(
    n_shards: usize,
    total_cap: usize,
    strategy: WaitStrategy,
) -> (Vec<Sender<T>>, Receiver<T>) {
    assert!(n_shards > 0, "n_shards must be positive");
    assert!(
        total_cap.is_multiple_of(n_shards),
        "total_cap {total_cap} must divide evenly into {n_shards} shards"
    );
    let per_shard = total_cap / n_shards;
    assert!(
        per_shard > 0 && per_shard.is_power_of_two(),
        "per-shard capacity {per_shard} (= {total_cap} / {n_shards}) \
         must be a positive power of two"
    );
    assert!(
        !matches!(strategy, WaitStrategy::Park),
        "sharded::channel does not support WaitStrategy::Park: there is no \
         cross-shard parker, so a consumer parked on one shard would sleep \
         through the others' publishes. Use BusySpin, Backoff, or \
         BackoffYield (all self-waking)."
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
            strategy,
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

    /// Push, blocking per the channel's wait strategy while **this shard** is
    /// full — other shards' occupancy is irrelevant, the same per-shard bound
    /// as [`Sender::try_send`]. Fails only when the receiver disconnects,
    /// returning the value.
    ///
    /// Delegates to [`crate::spsc::Sender::send`]: the wait ladder is the
    /// shard's own, and with the self-waking strategies this constructor
    /// accepts it costs the consumer nothing.
    pub fn send(&mut self, v: T) -> Result<(), crate::SendError<T>> {
        self.inner.send(v)
    }
}

/// The single consumer, sweeping all shards with a sticky cursor.
pub struct Receiver<T: Send> {
    shards: Vec<spsc::Receiver<T>>,
    cursor: usize,
    budget: usize,
    strategy: WaitStrategy,
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

    /// Consume up to `max` currently-available items across all shards,
    /// visiting each shard at most once, sweeping from the current cursor.
    /// Returns the count consumed.
    ///
    /// Each shard's take goes through [`crate::spsc::Receiver::drain`], so
    /// the shared head is published once per shard, not once per item — the
    /// batched-consume path. "At most once per shard" is the bound that keeps
    /// one call finite while producers keep refilling; loop the call for a
    /// continuous drain.
    ///
    /// Returns 0 both when every shard is empty and after every sender has
    /// disconnected; pair with [`Receiver::try_recv`] to distinguish, same as
    /// the spsc contract.
    pub fn drain(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        let n = self.shards.len();
        let mut count = 0usize;
        for _ in 0..n {
            if count >= max {
                break;
            }
            let want = max - count;
            let took = self.shards[self.cursor].drain(want, &mut f);
            count += took;
            if took == want {
                // `max` was reached mid-shard: hold the cursor here so the
                // next call continues this shard — same sticky-cursor rule
                // as `try_recv`.
                break;
            }
            self.advance();
        }
        count
    }

    /// Pop, blocking per the channel's wait strategy while every shard is
    /// empty. Fails only when all senders are gone and every shard is
    /// drained.
    ///
    /// The wait is self-waking (the constructor rejects `Park`): between
    /// sweeps the consumer spins, or climbs the `Backoff`/`BackoffYield`
    /// ladder, and re-checks — no shard ever needs to notify it.
    pub fn recv(&mut self) -> Result<T, crate::RecvError> {
        use crate::wait::Idle;
        let mut idle = Idle::for_strategy(self.strategy);
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(crate::RecvError),
                Err(TryRecvError::Empty) => match self.strategy {
                    WaitStrategy::BusySpin => std::hint::spin_loop(),
                    WaitStrategy::Backoff | WaitStrategy::BackoffYield => idle.idle(),
                    // The constructor rejects Park.
                    WaitStrategy::Park => unreachable!(),
                },
            }
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
    #[should_panic(expected = "does not support WaitStrategy::Park")]
    fn rejects_park_strategy() {
        let _ = channel::<u64>(2, 1024, WaitStrategy::Park);
    }

    #[test]
    fn accepts_every_self_waking_strategy() {
        for strategy in [
            WaitStrategy::BusySpin,
            WaitStrategy::Backoff,
            WaitStrategy::BackoffYield,
        ] {
            let (mut senders, mut rx) = channel::<u64>(2, 8, strategy);
            senders[0].try_send(1).unwrap();
            assert_eq!(rx.try_recv(), Ok(1));
        }
    }

    #[test]
    fn drain_sweeps_shards_and_respects_max() {
        let (mut senders, mut rx) = channel::<u64>(2, 8, WaitStrategy::BusySpin);
        for v in [1, 2, 3] {
            senders[0].try_send(v).unwrap();
        }
        for v in [10, 20] {
            senders[1].try_send(v).unwrap();
        }
        // max cuts mid-shard: 2 of shard 0's 3 items.
        let mut got = Vec::new();
        assert_eq!(rx.drain(2, |v| got.push(v)), 2);
        assert_eq!(got, vec![1, 2]);
        // Unbounded drain finishes shard 0 and sweeps shard 1.
        got.clear();
        assert_eq!(rx.drain(usize::MAX, |v| got.push(v)), 3);
        assert_eq!(got, vec![3, 10, 20]);
        assert_eq!(rx.drain(usize::MAX, |_| {}), 0, "empty channel drains 0");
    }

    #[test]
    fn drain_returns_zero_after_disconnect_pair_with_try_recv() {
        let (mut senders, mut rx) = channel::<u64>(2, 8, WaitStrategy::BusySpin);
        senders[0].try_send(7).unwrap();
        drop(senders);
        // Remaining items still come out through drain...
        let mut got = Vec::new();
        assert_eq!(rx.drain(usize::MAX, |v| got.push(v)), 1);
        assert_eq!(got, vec![7]);
        // ...then drain reports 0, same as empty; try_recv disambiguates.
        assert_eq!(rx.drain(usize::MAX, |_| {}), 0);
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
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
