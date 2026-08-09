//! Single-wake latency per [`WaitStrategy`]: the time from a producer's
//! publish to a waiting consumer's `recv()` returning.
//!
//! This isolates what `wait_strategy_paced_handoff` in `benches/throughput.rs`
//! cannot. That bench measures a *round trip*, which contains two asymmetric
//! wakes plus two handoffs — the responder wakes from a deepened ladder while
//! the requester's is still in its spin rungs — so halving it does not give a
//! wake latency. Here the producer stamps an `Instant` into the payload and the
//! consumer reads it on receipt, so each sample is one publish-to-delivery
//! path.
//!
//! The gap between sends is spun, not slept, and is long enough that a `Park`
//! consumer is genuinely parked before each publish rather than still spinning
//! through its pre-park re-check.
//!
//! `BusySpin` is the control: it never parks, so its number is handoff plus
//! spin-detect. Any other strategy's excess over it is the cost of its wait
//! mechanism.
//!
//! Run with: `cargo run --release --example wake_latency`

use std::thread;
use std::time::{Duration, Instant};

use ultima_rings::{WaitStrategy, spsc};

const SAMPLES: usize = 1000;
/// Long enough for `Park` to be parked and for `Backoff` to have climbed to its
/// capped 1 ms rung before each publish.
const GAP: Duration = Duration::from_micros(2000);

struct Stats {
    p50: f64,
    p99: f64,
    max: f64,
}

fn measure(strategy: WaitStrategy) -> Stats {
    let (mut tx, mut rx) = spsc::channel::<Instant>(1024, strategy);
    let consumer = thread::spawn(move || {
        let mut lat = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let sent = rx.recv().unwrap();
            // `Instant` is monotonic and comparable across threads on Linux.
            lat.push(sent.elapsed().as_nanos() as u64);
        }
        lat
    });

    for _ in 0..SAMPLES {
        // Spin to the deadline: sleeping here would inherit park_timeout's own
        // ~60 us overshoot and blur the quantity being measured.
        let deadline = Instant::now() + GAP;
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
        tx.send(Instant::now()).unwrap();
    }

    let mut lat = consumer.join().unwrap();
    lat.sort_unstable();
    Stats {
        p50: lat[SAMPLES / 2] as f64 / 1000.0,
        p99: lat[SAMPLES * 99 / 100] as f64 / 1000.0,
        max: lat[SAMPLES - 1] as f64 / 1000.0,
    }
}

fn main() {
    println!(
        "single-wake latency, {SAMPLES} samples, {} us gap between sends\n",
        GAP.as_micros()
    );
    println!(
        "{:<15} {:>10} {:>10} {:>10}",
        "strategy", "p50 (us)", "p99 (us)", "max (us)"
    );

    let baseline = measure(WaitStrategy::BusySpin);
    println!(
        "{:<15} {:>10.2} {:>10.2} {:>10.2}   (control: never parks)",
        "BusySpin", baseline.p50, baseline.p99, baseline.max
    );

    for (name, strategy) in [
        ("BackoffYield", WaitStrategy::BackoffYield),
        ("Park", WaitStrategy::Park),
        ("Backoff", WaitStrategy::Backoff),
    ] {
        let s = measure(strategy);
        println!(
            "{:<15} {:>10.2} {:>10.2} {:>10.2}   (+{:.2} us over control at p50)",
            name,
            s.p50,
            s.p99,
            s.max,
            s.p50 - baseline.p50
        );
    }
}
