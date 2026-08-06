//! No-loss/no-dup under contention (the comparability-critical stress from
//! the bench cells) + drop-accounting.
#![cfg(not(loom))]

use std::collections::HashSet;
use std::panic;
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

#[test]
fn drain_unwind_safe_no_double_drop_on_panicking_callback() {
    // Regression test: drain must publish head even on panic to avoid double-drop
    // in Shared::Drop. Closure panics on 2nd item of 4-item batch.
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = mpsc::channel::<Counted>(4, WaitStrategy::BusySpin);

    // Send 4 items to drain
    for _ in 0..4 {
        tx.try_send(Counted(Arc::clone(&drops))).unwrap();
    }

    // Try to drain 4 items; closure panics on the 2nd item.
    // The 1st item is moved out and dropped by the panic unwinding.
    // The 2nd item is moved out and dropped by the panic unwinding.
    // Items 3 and 4 stay in the ring and will be drop-drained by Shared::Drop.
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        rx.drain(4, |_counted| {
            let count = drops.load(Ordering::Relaxed);
            if count == 1 {
                // Panic on 2nd item: the 1st has already been dropped
                panic!("drain callback panic test");
            }
        });
    }));

    assert!(result.is_err(), "expected panic");

    // Drop both handles; Shared::Drop will drain the remaining items.
    // The key test: this must NOT double-drop the already-consumed items.
    drop(tx);
    drop(rx);

    // Verify exact drop count: 4 items constructed, 4 items dropped
    // (2 by unwind, 2 by Shared::Drop). No double-drop, no leak.
    assert_eq!(
        drops.load(Ordering::Relaxed),
        4,
        "exact count check: no double-drop, no leak"
    );
}
