//! Blocking sharded MPSC across the self-waking strategies. Mirrors
//! `tests/mpsc_blocking.rs`, minus the `Park` cases (sharded rejects `Park`:
//! there is no cross-shard parker) and with the weaker ordering contract this
//! type promises: per-producer FIFO, not global FIFO.
#![cfg(all(not(loom), feature = "experimental-sharded"))]

use std::thread;
use ultima_rings::{RecvError, SendError, WaitStrategy, sharded};

fn roundtrip(strategy: WaitStrategy) {
    let producers = 4usize;
    let per: u64 = if cfg!(miri) { 300 } else { 10_000 };
    // Tiny per-shard cap (8 / 4 = 2) forces every sender to block.
    let (senders, mut rx) = sharded::channel::<u64>(producers, 8, strategy);
    let mut handles = Vec::new();
    for (tag, mut tx) in senders.into_iter().enumerate() {
        handles.push(thread::spawn(move || {
            for i in 0..per {
                tx.send(((tag as u64) << 32) | i).unwrap();
            }
        }));
    }
    let mut next = vec![0u64; producers];
    let mut got = 0u64;
    while let Ok(v) = rx.recv() {
        let tag = (v >> 32) as usize;
        let seq = v & 0xffff_ffff;
        assert_eq!(seq, next[tag], "per-producer FIFO violated on tag {tag}");
        next[tag] += 1;
        got += 1;
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(got, producers as u64 * per, "loss or duplication");
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
fn recv_returns_err_after_all_senders_drop() {
    let (mut senders, mut rx) = sharded::channel::<u64>(2, 8, WaitStrategy::Backoff);
    senders[0].try_send(5).unwrap();
    let consumer = thread::spawn(move || {
        let first = rx.recv();
        let second = rx.recv();
        (first, second)
    });
    drop(senders);
    assert_eq!(consumer.join().unwrap(), (Ok(5), Err(RecvError)));
}

#[test]
fn blocked_send_returns_value_after_receiver_drop() {
    // Per-shard cap 2: fill sender 0's shard, then block on the third send.
    let (mut senders, rx) = sharded::channel::<u64>(2, 4, WaitStrategy::Backoff);
    senders[0].try_send(0).unwrap();
    senders[0].try_send(1).unwrap();
    let mut tx = senders.remove(0);
    let producer = thread::spawn(move || tx.send(2));
    drop(rx);
    assert_eq!(producer.join().unwrap(), Err(SendError(2)));
}

#[test]
fn blocked_send_completes_when_consumer_frees_the_shard() {
    let (mut senders, mut rx) = sharded::channel::<u64>(1, 2, WaitStrategy::Backoff);
    let mut tx = senders.pop().unwrap();
    tx.try_send(0).unwrap();
    tx.try_send(1).unwrap();
    let producer = thread::spawn(move || tx.send(2));
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(rx.recv().unwrap());
    }
    producer.join().unwrap().unwrap();
    assert_eq!(got, vec![0, 1, 2]);
}

/// Blocking consumer against `drain`: producers use blocking `send`, the
/// consumer alternates `drain` batches with a blocking `recv` to make
/// progress guarantees hold across both paths.
#[test]
fn drain_and_recv_interleave_under_backoff() {
    let producers = 2usize;
    let per: u64 = if cfg!(miri) { 200 } else { 5_000 };
    let (senders, mut rx) = sharded::channel::<u64>(producers, 8, WaitStrategy::Backoff);
    let mut handles = Vec::new();
    for (tag, mut tx) in senders.into_iter().enumerate() {
        handles.push(thread::spawn(move || {
            for i in 0..per {
                tx.send(((tag as u64) << 32) | i).unwrap();
            }
        }));
    }
    let mut next = vec![0u64; producers];
    let mut got = 0u64;
    let target = producers as u64 * per;
    while got < target {
        let mut check = |v: u64| {
            let tag = (v >> 32) as usize;
            let seq = v & 0xffff_ffff;
            assert_eq!(seq, next[tag], "per-producer FIFO violated on tag {tag}");
            next[tag] += 1;
        };
        let n = rx.drain(64, &mut check);
        got += n as u64;
        if n == 0 && got < target {
            match rx.recv() {
                Ok(v) => {
                    check(v);
                    got += 1;
                }
                Err(RecvError) => break,
            }
        }
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(got, target, "loss or duplication");
}
