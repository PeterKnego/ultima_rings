//! Disconnect/drop corner cases ported from crossbeam-channel's array tests
//! (see docs/superpowers/research/2026-08-06-crossbeam-channel-survey.md).
#![cfg(not(loom))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use ultima_rings::{RecvError, SendError, TryRecvError, TrySendError, WaitStrategy, mpsc, spsc};

/// crossbeam `try_recv_closed_with_data`: data survives the disconnect.
#[test]
fn try_recv_closed_with_data_spsc_and_mpsc() {
    let (mut tx, mut rx) = spsc::channel::<u32>(4, WaitStrategy::BusySpin);
    tx.try_send(1).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));

    let (mut tx, mut rx) = mpsc::channel::<u32>(4, WaitStrategy::BusySpin);
    tx.try_send(1).unwrap();
    drop(tx);
    assert_eq!(rx.try_recv(), Ok(1));
    assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
}

/// crossbeam `drop_unreceived` / `drop_full`: values in a dropped ring are
/// dropped exactly once, whether the ring was partly or completely full.
#[test]
fn drop_full_ring_drops_all_values_once() {
    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    for produced in [1usize, 4, 8] {
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, rx) = mpsc::channel::<Counted>(8, WaitStrategy::BusySpin);
        for _ in 0..produced {
            tx.try_send(Counted(Arc::clone(&drops))).unwrap();
        }
        drop(rx);
        drop(tx);
        assert_eq!(drops.load(Ordering::Relaxed), produced);
    }
}

/// crossbeam `send_after_disconnect`: every send flavor fails and returns the
/// value after the receiver is gone.
#[test]
fn send_after_disconnect_returns_value() {
    let (mut tx, rx) = mpsc::channel::<String>(4, WaitStrategy::BusySpin);
    drop(rx);
    assert_eq!(
        tx.try_send("t".into()),
        Err(TrySendError::Disconnected("t".to_string()))
    );
    assert_eq!(tx.send("b".into()), Err(SendError("b".to_string())));
}

/// crossbeam `disconnect_wakes_receiver`, multi-producer variant: the LAST
/// sender's drop (from a different thread each time) wakes a parked receiver.
#[test]
fn last_sender_drop_from_thread_wakes_parked_receiver() {
    for _ in 0..20 {
        let (tx, mut rx) = mpsc::channel::<u32>(4, WaitStrategy::Park);
        let txs: Vec<_> = (0..3).map(|_| tx.clone()).collect();
        drop(tx);
        let consumer = thread::spawn(move || rx.recv());
        let droppers: Vec<_> = txs
            .into_iter()
            .map(|t| thread::spawn(move || drop(t)))
            .collect();
        for d in droppers {
            d.join().unwrap();
        }
        assert_eq!(consumer.join().unwrap(), Err(RecvError));
    }
}

/// The same crossbeam corner cases against the sharded prototype, where they
/// apply. Global-FIFO cases don't (sharded promises per-producer FIFO only),
/// and Park cases don't (sharded is BusySpin-only); the disconnect and drop
/// accounting contracts are identical and are what this module checks.
#[cfg(feature = "experimental-sharded")]
mod sharded {
    use super::*;
    use ultima_rings::sharded;

    /// crossbeam `try_recv_closed_with_data`: data in any shard survives all
    /// senders disconnecting, in whichever order they drop.
    #[test]
    fn try_recv_closed_with_data() {
        let (mut senders, mut rx) = sharded::channel::<u32>(2, 8, WaitStrategy::BusySpin);
        senders[1].try_send(1).unwrap();
        drop(senders);
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Disconnected));
    }

    /// crossbeam `drop_unreceived` / `drop_full`: values left in the shards
    /// are dropped exactly once when the channel dies, spread unevenly across
    /// rings and including a completely full one.
    #[test]
    fn drop_full_rings_drop_all_values_once() {
        #[derive(Debug)]
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        for (in_shard0, in_shard1) in [(1usize, 0usize), (4, 2), (4, 4)] {
            let drops = Arc::new(AtomicUsize::new(0));
            let (mut senders, rx) = sharded::channel::<Counted>(2, 8, WaitStrategy::BusySpin);
            for _ in 0..in_shard0 {
                senders[0].try_send(Counted(Arc::clone(&drops))).unwrap();
            }
            for _ in 0..in_shard1 {
                senders[1].try_send(Counted(Arc::clone(&drops))).unwrap();
            }
            drop(rx);
            drop(senders);
            assert_eq!(drops.load(Ordering::Relaxed), in_shard0 + in_shard1);
        }
    }

    /// crossbeam `send_after_disconnect`: try_send fails and returns the
    /// value once the receiver is gone — from every shard, not just one.
    #[test]
    fn send_after_disconnect_returns_value() {
        let (mut senders, rx) = sharded::channel::<String>(2, 8, WaitStrategy::BusySpin);
        drop(rx);
        for tx in senders.iter_mut() {
            assert_eq!(
                tx.try_send("t".into()),
                Err(TrySendError::Disconnected("t".to_string()))
            );
        }
    }
}

/// crossbeam `drops` fuzz: randomized produce/consume/close with exact
/// drop accounting. Deterministic LCG, no rand dep.
#[test]
fn randomized_drop_accounting_fuzz() {
    #[derive(Debug)]
    struct Counted(Arc<AtomicUsize>);
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let mut seed = 0x9E3779B97F4A7C15u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let rounds = if cfg!(miri) { 20 } else { 500 };
    for _ in 0..rounds {
        let produce = (next() % 40) as usize;
        let consume_max = (next() % 40) as usize;
        let drops = Arc::new(AtomicUsize::new(0));
        let (mut tx, mut rx) = mpsc::channel::<Counted>(16, WaitStrategy::BusySpin);
        let mut sent = 0usize;
        for _ in 0..produce {
            match tx.try_send(Counted(Arc::clone(&drops))) {
                Ok(()) => sent += 1,
                Err(TrySendError::Full(v)) => drop(v),
                Err(TrySendError::Disconnected(_)) => unreachable!(),
            }
        }
        let mut consumed = 0usize;
        for _ in 0..consume_max.min(sent) {
            if rx.try_recv().is_ok() {
                consumed += 1;
            }
        }
        let _ = consumed;
        drop(tx);
        drop(rx);
        // Every constructed value dropped exactly once, regardless of path:
        // rejected-full ones, consumed ones, and ring-drained ones.
        assert_eq!(drops.load(Ordering::Relaxed), produce);
    }
}
