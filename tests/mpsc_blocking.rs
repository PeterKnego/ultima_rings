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
fn roundtrip_backoff_yield() {
    roundtrip(WaitStrategy::BackoffYield);
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
