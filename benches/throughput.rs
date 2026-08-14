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
    // This crate's own cell lives in the group rather than in `mpsc` alongside
    // the development benches. It is the same harness shape either way, but
    // keeping it here makes the group self-contained: a filtered run of
    // `bakeoff_mpsc/` yields every number the comparison table needs, and
    // cannot silently omit the baseline it is comparing against.
    g.bench_function("ultima", |b| {
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
    // thingbuf: the closest prior art in the roster — a fixed-capacity
    // MaybeUninit-slot ring with an MPSC channel layer, same shape as
    // src/mpsc.rs. Survey:
    // docs/superpowers/research/2026-08-06-thingbuf-survey.md.
    //
    // TWO CELLS, for the same reason the disruptor group has two: thingbuf's
    // reason for existing is the by-reference API, and its by-value API is
    // sugar over it.
    //   thingbuf     — try_send/try_recv, by value. Like-for-like against
    //                  crossbeam/flume/kanal above.
    //   thingbuf_ref — try_send_ref/try_recv_ref, the crate's natural API.
    //
    // On a u64 payload the ref API cannot show its actual benefit. Slot
    // recycling exists to avoid reallocating heap-owning payloads (String,
    // Vec<u8>), and a u64 owns nothing, so `thingbuf_ref` here measures the
    // cost of the Ref machinery with none of its payoff. Read it as the
    // overhead of the API, never as a verdict on slot recycling — that verdict
    // needs a heap-owning payload, which no cell in this file uses.
    g.bench_function("thingbuf", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = thingbuf::mpsc::blocking::channel::<u64>(1024);
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
                                    Err(thingbuf::mpsc::errors::TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
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
                        Ok(_) => {
                            got += 1;
                            if got == BATCH {
                                break;
                            }
                        }
                        Err(thingbuf::mpsc::errors::TryRecvError::Empty) => std::hint::spin_loop(),
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
    g.bench_function("thingbuf_ref", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = thingbuf::mpsc::blocking::channel::<u64>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for i in 0..BATCH / 2 {
                            loop {
                                match tx.try_send_ref() {
                                    Ok(mut slot) => {
                                        *slot = i;
                                        break;
                                    }
                                    Err(thingbuf::mpsc::errors::TrySendError::Full(())) => {
                                        std::hint::spin_loop();
                                    }
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
                    match rx.try_recv_ref() {
                        Ok(slot) => {
                            std::hint::black_box(*slot);
                            drop(slot);
                            got += 1;
                            if got == BATCH {
                                break;
                            }
                        }
                        Err(thingbuf::mpsc::errors::TryRecvError::Empty) => std::hint::spin_loop(),
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
    // thingbuf's blocking path: send()/recv() park the thread, the same
    // contract as ultima_park and crossbeam_blocking above.
    g.bench_function("thingbuf_blocking", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = thingbuf::mpsc::blocking::channel::<u64>(1024);
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
                        Some(_) => got += 1,
                        None => break,
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
// Disruptor cells. `disruptor` 4.4 is the maintained Rust port of the LMAX
// Disruptor and the closest structural relative of `src/mpsc.rs` — a claim
// cursor plus per-slot availability publication. Every other competitor here is
// a channel; this is the same lineage, so it measures what the design family
// can reach. Survey: docs/superpowers/research/2026-08-10-disruptor-survey.md.
//
// TWO CELLS, because disruptor's consumer can take one event or a whole
// available run, and the difference is the question we are actually asking:
//   batched — `poll()` (limit u64::MAX), all currently-available events per
//             poll. This is disruptor's idiomatic API and what its availability
//             bitmap (one bit per slot, 64 per word) exists to make cheap.
//   single  — `take(1)`, one event per poll.
//
// The `single` cell is NOT a like-for-like single-item comparison against the
// crossbeam/flume/kanal cells, and must never be quoted as one. `take` runs the
// full availability walk BEFORE applying its limit:
//
//     let available = self.dependent_barrier.get_after(sequence);  // full scan
//     ...
//     let available = std::cmp::min(available, max_sequence);      // then capped
//
// and `get_after` walks the contiguous published run bit by bit until it hits a
// parity mismatch. So `take(1)` costs O(backlog) per event and O(backlog^2) to
// drain a backlog — and because the slow consumer lets the backlog grow, the two
// feed each other.
//
// That is a genuine property of the bitmap design rather than an implementation
// slip, and it is exactly the trade this crate faces in reverse: a per-slot round
// number makes the single-item check O(1) and the batch check O(n), while a
// shared bitmap makes the batch check O(n/64) and the single-item check O(n).
// The cell is kept because that trade is the finding, not because the number is
// a fair head-to-head.
//
// NOT LIKE-FOR-LIKE, in two ways that must be stated with any number:
//  1. Disruptor slots are pre-constructed by a factory and mutated in place
//     (`FnOnce(&mut E)`); it never moves a value in and has no `MaybeUninit`
//     and no drop bookkeeping. For a `u64` payload that difference nearly
//     vanishes — writing a u64 into a slot and mutating a u64 in a slot are the
//     same store — which is why `u64` is the payload where this comparison is
//     close to fair. It would NOT be fair for a `String` or any `T` with a
//     destructor.
//  2. Batched *publication* (`try_batch_publish`, the batched claim itself) is
//     not measured. These producers hold one item at a time, which is the
//     workload this crate targets; batching the producer would change the
//     workload, not just the API.
// ---------------------------------------------------------------------------

fn bakeoff_disruptor_mpsc(c: &mut Criterion) {
    use disruptor::{BusySpin, Polling, Producer, build_multi_producer};

    let mut g = c.benchmark_group("bakeoff_disruptor_mpsc");
    g.throughput(Throughput::Elements(BATCH));

    // `limit` is the per-poll event cap: 1 mimics `try_recv`, u64::MAX is
    // disruptor's own `poll()`.
    for (name, limit) in [("disruptor_single", 1u64), ("disruptor_batched", u64::MAX)] {
        g.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let builder = build_multi_producer(1024, || 0u64, BusySpin);
                    let (mut poller, builder) = builder.new_event_poller();
                    let producer = builder.build();
                    let barrier = Arc::new(Barrier::new(3));
                    let mut handles = Vec::new();
                    for _ in 0..2 {
                        let mut tx = producer.clone();
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..BATCH / 2 {
                                // Retry on a full ring, matching every other
                                // producer loop in this file.
                                while tx.try_publish(|e| *e = i).is_err() {
                                    std::hint::spin_loop();
                                }
                            }
                        }));
                    }
                    // Drop the original handle so the last producer's drop
                    // signals shutdown, as `drop(tx)` does for the channels.
                    drop(producer);
                    barrier.wait();
                    let t = Instant::now();
                    let mut got = 0u64;
                    while got < BATCH {
                        match poller.take(limit) {
                            Ok(mut events) => {
                                for _ in &mut events {
                                    got += 1;
                                }
                            }
                            Err(Polling::NoEvents) => std::hint::spin_loop(),
                            Err(Polling::Shutdown) => break,
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
    }
    g.finish();
}

// ---------------------------------------------------------------------------
// Layout probe: the same MPSC workload across capacities and producer counts.
// Exists to test whether an MPSC hot-path layout change generalizes or holds
// only at one point — the recurring lesson on this path is that a single
// favorable cell is not evidence, only all three configurations agreeing is.
//
// Originally built to test padding the (then-separate) `avail` array to one
// cache line per entry: padding trades false sharing for cache residency, and
// the two scale opposite ways — benefit growing with producer contention,
// cost growing with capacity. It held at cap 1024 (+2.0%) and failed at cap
// 4096 (-0.1%), so it was rejected (docs/bench-results/2026-08-09-mpsc-perf-v2.md).
//
// Reused to test colocating the round with its payload (`avail` and the
// payload buffer merged into one `slots: Box<[Slot<T>]>`, see design.md §8)
// — this time it held at all three configurations (+11.9% to +15.5%) and was
// kept (docs/bench-results/2026-08-09-colocated-slot.md).
//
// Filter-only by design: `cargo bench -- mpsc_layout_probe`.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Producer ladder: the same MPSC workload from 2 up to 64 producers, at a fixed
// capacity. `mpsc_layout_probe` covers only 2 and 4 producers, and two of its
// three cells share the same producer count — so every claim-CAS backoff result
// so far rests on at most 4 producers on a 4-core box.
//
// That is the wrong place to stop for a *backoff* parameter specifically. A
// backoff is a scheduling interaction: once threads outnumber cores a producer
// can be descheduled part-way through its wait, and the best ceiling can move.
// This ladder crosses the core count (4) so that region is measured rather than
// assumed.
//
// Note on interpretation: thread spawn cost per iteration grows with the
// producer count, and at 64 producers it is a large share of the measurement.
// That cost is identical across backoff ceilings, so it compresses the relative
// differences between them without biasing which one wins. Compare ceilings
// within a producer count; do not compare throughput across producer counts.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Backoff isolation: a 2x2 of wait strategy against API path.
//
// The claim-CAS backoff is worth +108% to +143% under `BusySpin`
// (2026-08-11-cas-backoff.md) and costs 23% under `Park`
// (2026-08-11-bakeoff-v3.md). Those two measurements differ in TWO ways at
// once: the `BusySpin` cells drive `try_send`/`try_recv` in a harness retry
// loop, while the `Park` cell drives blocking `send`/`recv`. Either the
// strategy or the path could carry the cost.
//
// This group holds one axis fixed at a time:
//
//                  polling (try_*)      blocking (send/recv)
//   BusySpin       busyspin_poll        busyspin_block
//   Park           park_poll            park_block
//
// Run each cell with and without the backoff; the per-cell delta says which
// axis the cost attaches to. If both `Park` cells regress and neither
// `BusySpin` cell does, it is the strategy. If both blocking cells regress and
// neither polling cell does, it is the path. If only `park_block` regresses, it
// is the interaction and neither factor alone explains it.
//
// Note `try_send` performs the `Park` fence and consumer wake whenever the
// channel's strategy is `Park`, whichever path the caller used — so `park_poll`
// pays the per-publish wake cost without either side ever parking.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Heap-owning payload. Every other cell in this file carries a `u64`, which is
// the payload where `thingbuf` and `disruptor` are measured with all of their
// machinery and none of their benefit: both exist so that a slot's existing
// value can be mutated in place instead of reallocated, and a u64 owns nothing
// to reallocate.
//
// This group is the same harness with a `String` payload. Producers build a
// message per element, which is what a logging or serialization pipeline
// actually does, so the move-based crates pay one allocation and one free per
// element and the slot-owning crates pay neither after warm-up. That difference
// is the architectural claim under test, not an unfairness in the harness.
//
// It is also the first cell in this file to exercise this crate's drop
// bookkeeping at all — a `u64` has no destructor, so every prior number was
// measured on a path where `Slot::drop` does nothing.
//
// Read `thingbuf_ref` as the crate's designed configuration and `thingbuf` as
// what a caller gets by reaching for the API that looks like every other
// channel. Per its own docs, mixing by-value with by-reference silently
// discards the pooled allocation, so the by-value cell frees and reallocates
// exactly like the move-based crates.
// ---------------------------------------------------------------------------

/// 64 bytes: a plausible log line, and large enough that the allocation is not
/// lost in the noise of the handoff.
const MSG: &str = "2026-08-12T00:00:00Z INFO request completed status=200 in 4ms";
const STR_BATCH: u64 = 200_000;

fn bakeoff_mpsc_string(c: &mut Criterion) {
    let mut g = c.benchmark_group("bakeoff_mpsc_string");
    g.throughput(Throughput::Elements(STR_BATCH));

    g.bench_function("ultima", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, mut rx) = mpsc::channel::<String>(1024, WaitStrategy::BusySpin);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let mut tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..STR_BATCH / 2 {
                            let mut v = String::from(MSG);
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
                        Ok(v) => {
                            drop(v);
                            got += 1;
                            if got == STR_BATCH {
                                break;
                            }
                        }
                        Err(TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(TryRecvError::Disconnected) => break,
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

    g.bench_function("crossbeam", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = crossbeam_channel::bounded::<String>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..STR_BATCH / 2 {
                            let mut v = String::from(MSG);
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(crossbeam_channel::TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
                                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => return,
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
                        Ok(v) => {
                            drop(v);
                            got += 1;
                            if got == STR_BATCH {
                                break;
                            }
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => std::hint::spin_loop(),
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
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

    g.bench_function("thingbuf", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = thingbuf::mpsc::blocking::channel::<String>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..STR_BATCH / 2 {
                            let mut v = String::from(MSG);
                            loop {
                                match tx.try_send(v) {
                                    Ok(()) => break,
                                    Err(thingbuf::mpsc::errors::TrySendError::Full(b)) => {
                                        v = b;
                                        std::hint::spin_loop();
                                    }
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
                        Ok(v) => {
                            drop(v);
                            got += 1;
                            if got == STR_BATCH {
                                break;
                            }
                        }
                        Err(thingbuf::mpsc::errors::TryRecvError::Empty) => std::hint::spin_loop(),
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

    // The designed configuration: the producer overwrites the slot's existing
    // String in place, so after the first lap the buffer's capacity is reused
    // and the allocator is never touched.
    g.bench_function("thingbuf_ref", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let (tx, rx) = thingbuf::mpsc::blocking::channel::<String>(1024);
                let barrier = Arc::new(Barrier::new(3));
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    handles.push(thread::spawn(move || {
                        barrier.wait();
                        for _ in 0..STR_BATCH / 2 {
                            loop {
                                match tx.try_send_ref() {
                                    Ok(mut slot) => {
                                        slot.clear();
                                        slot.push_str(MSG);
                                        break;
                                    }
                                    Err(thingbuf::mpsc::errors::TrySendError::Full(())) => {
                                        std::hint::spin_loop();
                                    }
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
                    match rx.try_recv_ref() {
                        Ok(slot) => {
                            std::hint::black_box(slot.len());
                            drop(slot);
                            got += 1;
                            if got == STR_BATCH {
                                break;
                            }
                        }
                        Err(thingbuf::mpsc::errors::TryRecvError::Empty) => std::hint::spin_loop(),
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

fn backoff_isolation(c: &mut Criterion) {
    let mut g = c.benchmark_group("backoff_isolation");
    g.throughput(Throughput::Elements(BATCH));
    // All four strategies x {poll, block}. The two self-waking ladders were
    // missing until 2026-08-14, which left both the claim-CAS backoff and
    // PARK_SPINS unmeasured against them.
    //
    // The `*_poll` cells for BusySpin, Backoff and BackoffYield should be
    // IDENTICAL code paths: `try_send`/`try_recv` never consult the wait
    // strategy, and these three are self-waking so the productive side pays
    // nothing for them. They are kept as a standing consistency check — if
    // those three diverge, the harness is measuring something other than what
    // it claims. `park_poll` is the exception and is expected to differ,
    // because `Park`'s `try_send` pays a SeqCst fence plus a consumer wake on
    // every publish whichever API the caller used (design.md §8).
    for (name, strategy, blocking) in [
        ("busyspin_poll", WaitStrategy::BusySpin, false),
        ("busyspin_block", WaitStrategy::BusySpin, true),
        ("backoff_poll", WaitStrategy::Backoff, false),
        ("backoff_block", WaitStrategy::Backoff, true),
        ("backoffyield_poll", WaitStrategy::BackoffYield, false),
        ("backoffyield_block", WaitStrategy::BackoffYield, true),
        ("park_poll", WaitStrategy::Park, false),
        ("park_block", WaitStrategy::Park, true),
    ] {
        g.bench_function(name, |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (tx, mut rx) = mpsc::channel::<u64>(1024, strategy);
                    let barrier = Arc::new(Barrier::new(3));
                    let mut handles = Vec::new();
                    for _ in 0..2 {
                        let mut tx = tx.clone();
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..BATCH / 2 {
                                if blocking {
                                    if tx.send(i).is_err() {
                                        return;
                                    }
                                } else {
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
                            }
                        }));
                    }
                    drop(tx);
                    barrier.wait();
                    let t = Instant::now();
                    let mut got = 0u64;
                    while got < BATCH {
                        if blocking {
                            match rx.recv() {
                                Ok(_) => got += 1,
                                Err(_) => break,
                            }
                        } else {
                            match rx.try_recv() {
                                Ok(_) => got += 1,
                                Err(TryRecvError::Empty) => std::hint::spin_loop(),
                                Err(TryRecvError::Disconnected) => break,
                            }
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
    }
    g.finish();
}

fn mpsc_producer_ladder(c: &mut Criterion) {
    let mut g = c.benchmark_group("mpsc_producer_ladder");
    g.throughput(Throughput::Elements(BATCH));
    for producers in [2usize, 4, 8, 16, 32, 64] {
        g.bench_function(format!("p{producers}"), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (tx, mut rx) = mpsc::channel::<u64>(1024, WaitStrategy::BusySpin);
                    let barrier = Arc::new(Barrier::new(producers + 1));
                    let per = BATCH / producers as u64;
                    let mut handles = Vec::new();
                    for _ in 0..producers {
                        let mut tx = tx.clone();
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..per {
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
                    let target = per * producers as u64;
                    let mut got = 0u64;
                    loop {
                        match rx.try_recv() {
                            Ok(_) => got += 1,
                            Err(TryRecvError::Empty) => std::hint::spin_loop(),
                            Err(TryRecvError::Disconnected) => break,
                        }
                        if got == target {
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
    }
    g.finish();
}

fn mpsc_layout_probe(c: &mut Criterion) {
    let mut g = c.benchmark_group("mpsc_layout_probe");
    g.throughput(Throughput::Elements(BATCH));
    for (cap, producers) in [(1024usize, 2usize), (4096, 2), (1024, 4)] {
        g.bench_function(format!("cap{cap}_p{producers}"), |b| {
            b.iter_custom(|iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let (tx, mut rx) = mpsc::channel::<u64>(cap, WaitStrategy::BusySpin);
                    let barrier = Arc::new(Barrier::new(producers + 1));
                    let per = BATCH / producers as u64;
                    let mut handles = Vec::new();
                    for _ in 0..producers {
                        let mut tx = tx.clone();
                        let barrier = Arc::clone(&barrier);
                        handles.push(thread::spawn(move || {
                            barrier.wait();
                            for i in 0..per {
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
                    let target = per * producers as u64;
                    let mut got = 0u64;
                    loop {
                        match rx.try_recv() {
                            Ok(_) => got += 1,
                            Err(TryRecvError::Empty) => std::hint::spin_loop(),
                            Err(TryRecvError::Disconnected) => break,
                        }
                        if got == target {
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
    }
    g.finish();
}

criterion_group!(
    bakeoff,
    bakeoff_spsc,
    bakeoff_mpsc,
    bakeoff_mpsc_string,
    bakeoff_park_mpsc,
    bakeoff_disruptor_mpsc,
    mpsc_layout_probe,
    mpsc_producer_ladder,
    backoff_isolation
);

#[cfg(feature = "experimental-sharded")]
criterion_group!(bakeoff_sharded, bakeoff_sharded_mpsc);

#[cfg(feature = "experimental-sharded")]
criterion_main!(benches, bakeoff, bakeoff_sharded);

#[cfg(not(feature = "experimental-sharded"))]
criterion_main!(benches, bakeoff);
