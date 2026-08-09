//! Regression-guard benches (single machine, indicative only — the
//! cross-language rig lives in hi-perf-cmp). Persistent producer threads,
//! barrier-released batches, wall-clock per batch.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, mpsc, spsc};

const BATCH: u64 = 100_000;

fn spsc_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("spsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("busy_spin_pipelined", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut tx, mut rx) = spsc::channel::<u64>(1024, WaitStrategy::BusySpin);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        got += rx.drain(usize::MAX, |_| {}) as u64;
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let mut v = i;
                    loop {
                        match tx.try_send(v) {
                            Ok(()) => break,
                            Err(TrySendError::Full(b)) => {
                                v = b;
                                std::hint::spin_loop();
                            }
                            Err(TrySendError::Disconnected(_)) => unreachable!(),
                        }
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.finish();
}

fn mpsc_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("busy_spin_2_producers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, mut rx) = mpsc::channel::<u64>(1024, WaitStrategy::BusySpin);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let mut tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let mut v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(TrySendError::Disconnected(_)) => return,
                                }
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(_) => got += 1,
                        Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(TryRecvError::Disconnected) => break,
                    }
                    if got == BATCH {
                        break;
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.finish();
}

// ---------------------------------------------------------------------------
// Paced handoff: the only cell that actually exercises the wait-strategy
// ladders. A saturating throughput bench never does — the ring is rarely empty
// long enough for `Idle` to climb past its spin rungs. Pacing the producer
// forces the consumer's ladder to deepen before every wake, which is what
// separates the strategies. Same rationale as hi-perf-cmp's paced ping-pong
// grid (docs/superpowers/specs/2026-08-05-thread-handoff-backoff-design.md
// upstream).
//
// With a 200 us gap the responder's ladder climbs: 10 spins, 20 yields
// (~14 us), then timed parks from PARK_MIN.
//
// It must be a PING-PONG, not a one-way stream. An earlier one-way version of
// this bench measured nothing: the producer never waited on the consumer, so
// the consumer's wake latency was off the critical path and all four
// strategies reported the pacing time and nothing else. Round-tripping puts
// the responder's wake on the requester's critical path, which is the whole
// point. Reported time is PACED_ROUNDS x (gap + RTT); the gap is identical
// across strategies, so the deltas between cells are wake latency.
// ---------------------------------------------------------------------------

const PACED_GAP: std::time::Duration = std::time::Duration::from_micros(200);
const PACED_ROUNDS: u64 = 500;

fn wait_strategy_paced_handoff(c: &mut Criterion) {
    let mut g = c.benchmark_group("wait_strategy_paced_handoff");
    g.throughput(Throughput::Elements(PACED_ROUNDS));
    for (name, strategy) in [
        ("busy_spin", WaitStrategy::BusySpin),
        ("backoff_yield", WaitStrategy::BackoffYield),
        ("backoff", WaitStrategy::Backoff),
        ("park", WaitStrategy::Park),
    ] {
        g.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (mut req_tx, mut req_rx) = spsc::channel::<u64>(1024, strategy);
                    let (mut resp_tx, mut resp_rx) = spsc::channel::<u64>(1024, strategy);
                    let responder = thread::spawn(move || {
                        for _ in 0..PACED_ROUNDS {
                            // Blocks on an empty ring: this is the ladder
                            // under test, deepened by the requester's gap.
                            let v = req_rx.recv().unwrap();
                            resp_tx.send(v).unwrap();
                        }
                    });
                    let t = Instant::now();
                    for i in 0..PACED_ROUNDS {
                        // Spin to the deadline rather than sleeping: the pacing
                        // gap must not inherit park_timeout's ~60 us overshoot,
                        // or it would swamp the wake latency being measured.
                        let deadline = Instant::now() + PACED_GAP;
                        while Instant::now() < deadline {
                            std::hint::spin_loop();
                        }
                        req_tx.send(i).unwrap();
                        resp_rx.recv().unwrap();
                    }
                    responder.join().unwrap();
                    total += t.elapsed();
                }
                total
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    spsc_throughput,
    mpsc_throughput,
    wait_strategy_paced_handoff
);

// ---------------------------------------------------------------------------
// Bake-off: same two workloads (pipelined SPSC 100k through cap-1024;
// 2-producer MPSC 100k through cap-1024) run against the ecosystem's
// idiomatic non-blocking APIs. Bench-only — these crates never appear in
// [dependencies]. This is the standing honesty check for the exit-ramp gate:
// BusySpin >= 2x crossbeam-channel throughput, Park-mode parity, at
// uc2-shaped workloads (see task-9 report for the verdict).
// ---------------------------------------------------------------------------

fn bakeoff_spsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("bakeoff_spsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("crossbeam", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(1024);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        if rx.try_recv().is_ok() {
                            got += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let mut v = i;
                    while let Err(crossbeam_channel::TrySendError::Full(b)) = tx.try_send(v) {
                        v = b;
                        std::hint::spin_loop();
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.bench_function("rtrb", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (mut tx, mut rx) = rtrb::RingBuffer::<u64>::new(1024);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        if rx.pop().is_ok() {
                            got += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let mut v = i;
                    while let Err(rtrb::PushError::Full(b)) = tx.push(v) {
                        v = b;
                        std::hint::spin_loop();
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.bench_function("flume", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = flume::bounded::<u64>(1024);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        if rx.try_recv().is_ok() {
                            got += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let mut v = i;
                    while let Err(flume::TrySendError::Full(b)) = tx.try_send(v) {
                        v = b;
                        std::hint::spin_loop();
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.bench_function("kanal", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = kanal::bounded::<u64>(1024);
                let consumer = thread::spawn(move || {
                    let mut got = 0u64;
                    while got < BATCH {
                        match rx.try_recv() {
                            Ok(Some(_)) => got += 1,
                            Ok(None) => std::hint::spin_loop(),
                            Err(_) => unreachable!(),
                        }
                    }
                });
                let t = Instant::now();
                for i in 0..BATCH {
                    let v = i;
                    // try_send drops `v` on a full/closed channel, but u64 is
                    // Copy so re-sending the same value on retry is fine.
                    loop {
                        match tx.try_send(v) {
                            Ok(true) => break,
                            Ok(false) => std::hint::spin_loop(),
                            Err(_) => unreachable!(),
                        }
                    }
                }
                consumer.join().unwrap();
                total += t.elapsed();
            }
            total
        })
    });
    g.finish();
}

fn bakeoff_mpsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("bakeoff_mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("crossbeam", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let mut v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(crossbeam_channel::TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                                        return;
                                    }
                                }
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(_) => got += 1,
                        Err(crossbeam_channel::TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                    }
                    if got == BATCH {
                        break;
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.bench_function("flume", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = flume::bounded::<u64>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let mut v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(flume::TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(flume::TrySendError::Disconnected(_)) => return,
                                }
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(_) => got += 1,
                        Err(flume::TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(flume::TryRecvError::Disconnected) => break,
                    }
                    if got == BATCH {
                        break;
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.bench_function("kanal", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = kanal::bounded::<u64>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(true) => break,
                                    Ok(false) => std::hint::spin_loop(),
                                    Err(_) => return,
                                }
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(Some(_)) => {
                            got += 1;
                            if got == BATCH {
                                break;
                            }
                        }
                        Ok(None) => std::hint::spin_loop(),
                        Err(_) => break,
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.finish();
}

// ---------------------------------------------------------------------------
// Sharded MPSC prototype (feature `experimental-sharded`). Same harness shape,
// BATCH, and total buffered capacity as `bakeoff_mpsc` above, so the two are
// directly comparable: 2 shards x 512 = 1024 slots, matching
// crossbeam_channel::bounded(1024). See
// docs/superpowers/specs/2026-08-07-sharded-mpsc-design.md.
// ---------------------------------------------------------------------------

#[cfg(feature = "experimental-sharded")]
fn bakeoff_sharded_mpsc(c: &mut Criterion) {
    use ultima_rings::sharded;
    let mut g = c.benchmark_group("bakeoff_sharded_mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("ultima_sharded_2_producers", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (senders, mut rx) = sharded::channel::<u64>(2, 1024, WaitStrategy::BusySpin);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                // Every sender is moved into a thread, so there is no
                // leftover handle to drop (unlike the mpsc groups, where the
                // original `tx` must be dropped for the consumer to finish).
                for mut tx in senders {
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            let mut v = i;
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(TrySendError::Disconnected(_)) => return,
                                }
                            }
                        }
                    }));
                }
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                loop {
                    match rx.try_recv() {
                        Ok(_) => got += 1,
                        Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(TryRecvError::Disconnected) => break,
                    }
                    if got == BATCH {
                        break;
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.finish();
}

// ---------------------------------------------------------------------------
// Park-mode MPSC bake-off. The other MPSC cells all drive the non-blocking
// `try_*` API with a hand-rolled spin, so nothing there exercises either
// crate's blocking path — a gap the v2 spec flagged
// (docs/superpowers/specs/2026-08-07-mpsc-perf-v2-design.md). Here both sides
// use blocking `send`/`recv`: ultima_rings under `WaitStrategy::Park` (which
// parks via the notify layer and pays a SeqCst fence + wake per operation),
// crossbeam-channel under its own blocking path. Same 2-producer
// barrier-released harness, same BATCH, same 1024 capacity as `bakeoff_mpsc`.
// ---------------------------------------------------------------------------

fn bakeoff_park_mpsc(c: &mut Criterion) {
    let mut g = c.benchmark_group("bakeoff_park_mpsc");
    g.throughput(Throughput::Elements(BATCH));
    g.bench_function("ultima_park", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, mut rx) = mpsc::channel::<u64>(1024, WaitStrategy::Park);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let mut tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            if tx.send(i).is_err() {
                                return;
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                while got < BATCH {
                    match rx.recv() {
                        Ok(_) => got += 1,
                        Err(_) => break,
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.bench_function("crossbeam_blocking", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = crossbeam_channel::bounded::<u64>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            if tx.send(i).is_err() {
                                return;
                            }
                        }
                    }));
                }
                drop(tx);
                barrier.wait();
                let t = Instant::now();
                let mut got = 0u64;
                while got < BATCH {
                    match rx.recv() {
                        Ok(_) => got += 1,
                        Err(_) => break,
                    }
                }
                total += t.elapsed();
                for h in handles {
                    h.join().unwrap();
                }
            }
            total
        })
    });
    g.finish();
}

criterion_group!(bakeoff, bakeoff_spsc, bakeoff_mpsc, bakeoff_park_mpsc);

#[cfg(feature = "experimental-sharded")]
criterion_group!(bakeoff_sharded, bakeoff_sharded_mpsc);

#[cfg(feature = "experimental-sharded")]
criterion_main!(benches, bakeoff, bakeoff_sharded);

#[cfg(not(feature = "experimental-sharded"))]
criterion_main!(benches, bakeoff);
