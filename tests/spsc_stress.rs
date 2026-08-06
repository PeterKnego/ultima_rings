//! Cross-thread order/count stress and drop-accounting for the SPSC ring.
#![cfg(not(loom))]

use std::panic;
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

#[test]
fn drain_unwind_safe_no_double_drop_on_panicking_callback() {
    // Regression test: drain must publish head even on panic to avoid double-drop
    // in Shared::Drop. Closure panics on 2nd item of 4-item batch.
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut tx, mut rx) = spsc::channel::<Counted>(8, WaitStrategy::BusySpin);

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
    // After panic: 1st item was moved out and dropped, 2nd item was moved out
    // and dropped (by unwind), so we should see 2 drops from the drain plus
    // whatever panic cleanup does.
    let drops_after_panic = drops.load(Ordering::Relaxed);
    assert!(
        drops_after_panic >= 2,
        "should have at least 2 drops after panic"
    );

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
