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
    let avail = (0..cap)
        .map(|_| AtomicI64::new(-1))
        .collect::<Vec<_>>()
        .into_boxed_slice();
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
        Sender {
            shared: Arc::clone(&shared),
            cached_head: 0,
        },
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
            // Check if ring is full by comparing seq >= cached_head + cap
            // instead of subtracting to avoid underflow in debug mode.
            if seq >= self.cached_head.saturating_add(sh.cap) {
                self.cached_head = sh.head.0.load(Ordering::Acquire);
                if seq >= self.cached_head.saturating_add(sh.cap) {
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
                            if claim < head.saturating_add(sh.cap)
                                || sh.rx_dropped.load(Ordering::Acquire)
                            {
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

impl<T: Send> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Sender {
            shared: Arc::clone(&self.shared),
            cached_head: 0,
        }
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

    /// Consume up to `max` items of the contiguous published prefix,
    /// advancing the shared head once at the end.
    pub fn drain(&mut self, max: usize, mut f: impl FnMut(T)) -> usize {
        struct PublishGuard<'a> {
            head: &'a mut usize, // private cursor (already advanced per item)
            shared: &'a AtomicUsize,
            start: usize,
        }
        impl Drop for PublishGuard<'_> {
            fn drop(&mut self) {
                // Runs on normal exit AND unwind: the shared head always catches
                // up to every item actually moved out of the ring, so Shared::Drop
                // never re-drops a consumed slot (leak-not-double-drop policy).
                if *self.head != self.start {
                    self.shared.store(*self.head, Ordering::Release);
                }
            }
        }

        let sh = &*self.shared;
        let mask = sh.mask;
        let cap = sh.cap;
        let buf = &sh.buf;
        let avail = &sh.avail;
        let mut count = 0usize;
        let start = self.head;
        let guard = PublishGuard {
            head: &mut self.head,
            shared: &sh.head.0,
            start,
        };
        while count < max {
            let seq = *guard.head;
            let slot = seq & mask;
            // SAFETY: slot is within bounds; avail[slot] is initialized.
            if avail[slot].load(Ordering::Acquire) != (seq / cap) as i64 {
                break;
            }
            // SAFETY: as in try_recv.
            let v = buf[slot].with(|p| unsafe { (*p).assume_init_read() });
            *guard.head += 1;
            count += 1;
            f(v);
        }
        drop(guard); // publish once (normal path); unwind publishes via Drop
        if count > 0 {
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
        let tx2 = tx.clone();
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
