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

/// (5) WaiterList path (the kanal "cancel races delivery" class): a sender
/// parked on a full MPSC ring must be woken by BOTH possible events —
/// consumer progress and receiver drop — under every interleaving.
#[test]
fn loom_full_parked_sender_vs_recv_and_rx_drop() {
    loom::model(|| {
        let (mut tx, mut rx) = mpsc::channel::<u64>(1, WaitStrategy::Park);
        tx.try_send(0).unwrap(); // fill the 1-slot ring
        let producer = thread::spawn(move || tx.send(1)); // parks on full
        // Consumer frees a slot, then drops: the parked sender must either
        // deliver (send returns Ok) or observe the disconnect (Err) — never
        // hang. Loom's deadlock detection is the assertion.
        let _ = rx.try_recv();
        drop(rx);
        let _ = producer.join().unwrap();
    });
}

// Sharded composition models. `src/sharded.rs` declares no atomics — every
// ordering edge is inside the spsc rings modeled above — but the sweep in
// `Receiver::try_recv` layers a claim on top: Disconnected is returned only
// once EVERY shard is both sender-dropped and drained, counted per sweep with
// no dead-shard bookkeeping, on the argument that a shard's Disconnected
// state is stable. These models check that composition-level claim under all
// interleavings of send, drop, and sweep.
mod sharded_composition {
    use loom::thread;
    use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, sharded};

    /// (6) Two shards, one value each, senders drop at arbitrary points:
    /// the consumer must deliver both values before it ever reports
    /// Disconnected — an early Disconnected exits the loop short and fails
    /// the multiset assertion.
    #[test]
    fn loom_sharded_no_loss_no_early_disconnect() {
        loom::model(|| {
            let (senders, mut rx) = sharded::channel::<u64>(2, 4, WaitStrategy::BusySpin);
            let mut handles = Vec::new();
            for (p, mut tx) in senders.into_iter().enumerate() {
                handles.push(thread::spawn(move || {
                    match tx.try_send(p as u64) {
                        Ok(()) => {}
                        // cap 2 per shard, 1 item: Full is impossible.
                        Err(_) => unreachable!(),
                    }
                    // tx drops here, racing the consumer's sweep.
                }));
            }
            let mut got = Vec::new();
            loop {
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
            assert_eq!(got, vec![0, 1], "lost a value or Disconnected early");
        });
    }

    /// (7) Mixed shard states: shard 0's sender drops without ever sending
    /// while shard 1 sends then drops. The sweep sees one
    /// disconnected+drained shard and one live shard in the same pass, in
    /// either cursor order, and must still deliver the value first.
    #[test]
    fn loom_sharded_staggered_drop() {
        loom::model(|| {
            let (mut senders, mut rx) = sharded::channel::<u64>(2, 4, WaitStrategy::BusySpin);
            let s1 = senders.pop().unwrap();
            let s0 = senders.pop().unwrap();
            let dropper = thread::spawn(move || drop(s0));
            let sender = thread::spawn(move || {
                let mut s1 = s1;
                assert_eq!(s1.try_send(7), Ok(()));
            });
            let mut got = None;
            loop {
                match rx.try_recv() {
                    Ok(v) => got = Some(v),
                    Err(TryRecvError::Empty) => thread::yield_now(),
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            dropper.join().unwrap();
            sender.join().unwrap();
            assert_eq!(got, Some(7), "value lost behind a dead shard");
        });
    }

    /// (8) Receiver drops while a producer is mid-send: the producer must
    /// observe Disconnected (get the value back) or have delivered into the
    /// ring, whose drop then frees it — either way no hang and no leak.
    /// Loom's leak + deadlock detection carries the assertion.
    #[test]
    fn loom_sharded_rx_drop_races_send() {
        loom::model(|| {
            let (mut senders, rx) = sharded::channel::<u64>(2, 4, WaitStrategy::BusySpin);
            let mut tx = senders.pop().unwrap();
            let producer = thread::spawn(move || match tx.try_send(1) {
                Ok(()) | Err(TrySendError::Disconnected(_)) => {}
                Err(TrySendError::Full(_)) => unreachable!(),
            });
            drop(rx);
            producer.join().unwrap();
        });
    }
}
