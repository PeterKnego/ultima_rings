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
