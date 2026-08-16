//! Per-producer FIFO + no-loss/no-dup for the sharded MPSC prototype, plus
//! drop accounting. Mirrors `tests/mpsc_stress.rs`, but asserts the weaker
//! ordering contract this type actually promises: per-producer FIFO, NOT the
//! global FIFO `mpsc` provides.
#![cfg(not(loom))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, sharded};

/// Values are `(tag << 32) | seq`, so checking that each tag's sequences
/// arrive as 0, 1, 2, ... catches ordering violations, loss, AND duplication
/// in a single assertion.
fn run_stress(producers: usize, per: usize, total_cap: usize) {
    let (senders, mut rx) = sharded::channel::<u64>(producers, total_cap, WaitStrategy::BusySpin);
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
                assert_eq!(seq, next[tag], "per-producer FIFO violated on tag {tag}");
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
    let (mut senders, mut rx) = sharded::channel::<Counted>(2, 16, WaitStrategy::BusySpin);
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
