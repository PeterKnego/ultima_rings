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
    pub(crate) consumer_parker: crate::notify::Parker,
    pub(crate) producer_parker: crate::notify::Parker,
}

// SAFETY: the single-producer/single-consumer discipline is enforced by the
// API surface: `Sender<T>` and `Receiver<T>` have no `Clone` impl, every
// mutating method takes `&mut self`, and `channel()` hands out exactly one
// `Sender` and one `Receiver`. This guarantees each slot is written by at most
// one thread before its Release-publish and read by at most one thread after
// the matching Acquire. T: Send suffices.
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
        consumer_parker: crate::notify::Parker::new(),
        producer_parker: crate::notify::Parker::new(),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
            tail: 0,
            cached_head: 0,
        },
        Receiver {
            shared,
            head: 0,
            cached_tail: 0,
        },
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
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.consumer_parker.wake();
        }
        Ok(())
    }

    /// Push, blocking per the channel's wait strategy while the ring is full.
    /// Fails only when the receiver disconnects, returning the value.
    pub fn send(&mut self, v: T) -> Result<(), crate::SendError<T>> {
        use crate::wait::Idle;
        let mut v = v;
        let mut idle = Idle::for_strategy(self.shared.strategy);
        loop {
            match self.try_send(v) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Disconnected(back)) => return Err(crate::SendError(back)),
                Err(TrySendError::Full(back)) => {
                    v = back;
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        // Both ladders live in `Idle`; which rungs it climbs
                        // was fixed by `Idle::for_strategy` above.
                        WaitStrategy::Backoff | WaitStrategy::BackoffYield => idle.idle(),
                        WaitStrategy::Park => {
                            sh.producer_parker.prepare_park();
                            crate::atomic::fence(Ordering::SeqCst);
                            let head = sh.head.0.load(Ordering::Acquire);
                            if self.tail - head < sh.cap || sh.disconnected.load(Ordering::Acquire)
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
        if sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.producer_parker.wake();
        }
        Ok(v)
    }

    /// Consume up to `max` currently-available items, advancing the shared
    /// head once at the end. Returns the count consumed. Note: returns 0 both
    /// when the ring is empty and after the sender has disconnected; pair with
    /// `try_recv` to distinguish.
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
        let mut count = 0usize;
        let start = self.head;
        let guard = PublishGuard {
            head: &mut self.head,
            shared: &sh.head.0,
            start,
        };
        while count < max {
            if *guard.head == self.cached_tail {
                self.cached_tail = sh.tail.0.load(Ordering::Acquire);
                if *guard.head == self.cached_tail {
                    break;
                }
            }
            // SAFETY: as in try_recv.
            let v = sh.buf[*guard.head & sh.mask].with(|p| unsafe { (*p).assume_init_read() });
            *guard.head += 1;
            count += 1;
            f(v);
        }
        drop(guard); // publish once (normal path); unwind publishes via Drop
        if count > 0 && sh.strategy == WaitStrategy::Park {
            crate::atomic::fence(Ordering::SeqCst);
            sh.producer_parker.wake();
        }
        count
    }

    /// Pop, blocking per the channel's wait strategy while the ring is empty.
    /// Fails only when all senders are gone and the ring is drained.
    pub fn recv(&mut self) -> Result<T, crate::RecvError> {
        use crate::wait::Idle;
        let mut idle = Idle::for_strategy(self.shared.strategy);
        loop {
            match self.try_recv() {
                Ok(v) => return Ok(v),
                Err(TryRecvError::Disconnected) => return Err(crate::RecvError),
                Err(TryRecvError::Empty) => {
                    let sh = &*self.shared;
                    match sh.strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        // Both ladders live in `Idle`; which rungs it climbs
                        // was fixed by `Idle::for_strategy` above.
                        WaitStrategy::Backoff | WaitStrategy::BackoffYield => idle.idle(),
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
