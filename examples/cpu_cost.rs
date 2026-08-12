//! What the spinning costs.
//!
//! Every cell in `benches/throughput.rs` reports elements per second of *wall*
//! time. On a 4-core box that flatters a spinning ring against a parking
//! channel, because a spinner converts idle cores into throughput and the
//! throughput number does not show the cores it spent. This crate's pitch is
//! spinning, so that is exactly the number it must not hide.
//!
//! Here each thread reads its own `/proc/thread-self/schedstat` before and
//! after its work. Field 0 of that file is nanoseconds the task has spent on
//! CPU, so the deltas summed across threads give total CPU time with no
//! dependency and nanosecond resolution. Dividing by wall time gives **cores
//! burned**: 1.0 means one core saturated for the whole run, 3.0 means all
//! three threads spun continuously.
//!
//! The figure to compare across crates is **CPU nanoseconds per element**. It
//! is throughput and occupancy in one number, and it is the one a reader
//! deciding whether to spend a core on this crate actually needs.
//!
//! **Two sections, because one workload cannot answer the question.** Under
//! saturation the ring is never empty long enough for anyone to park, so the
//! first table measures every parking mechanism's cost with none of its
//! benefit. The second table paces the producer so the consumer is genuinely
//! idle between elements, which is the only condition under which parking can
//! pay for itself — and the condition under which "busy-spinning burns a whole
//! core" is either true or it is not.
//!
//! Linux only — it reads procfs directly.
//!
//! Run with: `cargo run --release --example cpu_cost`

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use ultima_rings::{TryRecvError, TrySendError, WaitStrategy, mpsc};

const BATCH: u64 = 1_000_000;
const CAP: usize = 1024;
const PRODUCERS: u64 = 2;
/// Iterations per measurement. Large enough that the ~50 us of thread-spawn
/// cost per iteration stays near 1% of the total.
const ITERS: u64 = 5;
const ROUNDS: usize = 3;

/// Gap between sends in the paced section. Long enough that a `Park` consumer
/// is genuinely parked between elements and `Backoff` has climbed off its spin
/// rungs.
const PACED_GAP: std::time::Duration = std::time::Duration::from_micros(200);
const PACED_ELEMS: u64 = 5_000;

/// Nanoseconds this thread has spent on CPU, from `/proc/thread-self/schedstat`.
///
/// Field 0 is `sum_exec_runtime` in nanoseconds — time on CPU, excluding time
/// blocked or runnable-but-not-running. That exclusion is the point: a parked
/// thread accrues wall time and no CPU time, which is the difference this
/// example exists to show.
fn thread_cpu_ns() -> u64 {
    let s = std::fs::read_to_string("/proc/thread-self/schedstat")
        .expect("this example requires Linux procfs");
    s.split_whitespace()
        .next()
        .expect("schedstat field 0")
        .parse()
        .expect("schedstat field 0 is a u64")
}

struct Run {
    wall_ns: u128,
    cpu_ns: u64,
}

impl Run {
    fn elems(&self) -> u64 {
        BATCH * ITERS
    }
    fn melem_per_s(&self) -> f64 {
        self.elems() as f64 / (self.wall_ns as f64 / 1e9) / 1e6
    }
    fn cores(&self) -> f64 {
        self.cpu_ns as f64 / self.wall_ns as f64
    }
    fn cpu_ns_per_elem(&self) -> f64 {
        self.cpu_ns as f64 / self.elems() as f64
    }
}

/// Runs the standard 2-producer handoff and accounts CPU to every thread that
/// takes part, including the consumer running on this thread.
///
/// `send` returns false to abandon the run (channel disconnected); `recv`
/// returns true when it took one element.
fn measure<S, R, FS, FR>(make: impl Fn() -> (S, R), send: FS, mut recv: FR) -> Run
where
    S: Clone + Send + 'static,
    FS: Fn(&mut S, u64) -> bool + Clone + Send + 'static,
    FR: FnMut(&mut R) -> bool,
{
    let mut cpu_ns = 0u64;
    let wall = Instant::now();

    for _ in 0..ITERS {
        let (tx, mut rx) = make();
        let barrier = Arc::new(Barrier::new(PRODUCERS as usize + 1));
        let mut handles = Vec::new();

        for _ in 0..PRODUCERS {
            let mut tx = tx.clone();
            let send = send.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                let t0 = thread_cpu_ns();
                for i in 0..BATCH / PRODUCERS {
                    if !send(&mut tx, i) {
                        break;
                    }
                }
                thread_cpu_ns() - t0
            }));
        }
        drop(tx);

        barrier.wait();
        let t0 = thread_cpu_ns();
        let mut got = 0u64;
        while got < BATCH {
            if !recv(&mut rx) {
                break;
            }
            got += 1;
        }
        cpu_ns += thread_cpu_ns() - t0;

        for h in handles {
            cpu_ns += h.join().unwrap();
        }
    }

    Run {
        wall_ns: wall.elapsed().as_nanos(),
        cpu_ns,
    }
}

struct Paced {
    wall_ns: u128,
    consumer_cpu_ns: u64,
}

impl Paced {
    fn cores(&self) -> f64 {
        self.consumer_cpu_ns as f64 / self.wall_ns as f64
    }
    fn cpu_ns_per_elem(&self) -> f64 {
        self.consumer_cpu_ns as f64 / PACED_ELEMS as f64
    }
}

/// One paced producer, one waiting consumer. Only the consumer's CPU is
/// accounted: the producer sleeps between sends by construction, so its time
/// says nothing about the wait mechanism under test.
fn paced<S, R, FS, FR>(make: impl Fn() -> (S, R), send: FS, mut recv: FR) -> Paced
where
    S: Send + 'static,
    FS: Fn(&mut S, u64) -> bool + Send + 'static,
    FR: FnMut(&mut R) -> bool,
{
    let (tx, mut rx) = make();
    let barrier = Arc::new(Barrier::new(2));
    let b = Arc::clone(&barrier);
    let h = thread::spawn(move || {
        let mut tx = tx;
        b.wait();
        for i in 0..PACED_ELEMS {
            thread::sleep(PACED_GAP);
            if !send(&mut tx, i) {
                break;
            }
        }
    });

    barrier.wait();
    let wall = Instant::now();
    let t0 = thread_cpu_ns();
    let mut got = 0u64;
    while got < PACED_ELEMS {
        if !recv(&mut rx) {
            break;
        }
        got += 1;
    }
    let consumer_cpu_ns = thread_cpu_ns() - t0;
    let wall_ns = wall.elapsed().as_nanos();
    h.join().unwrap();

    Paced {
        wall_ns,
        consumer_cpu_ns,
    }
}

fn report_paced(name: &str, runs: &mut [Paced]) {
    runs.sort_by(|a, b| a.cores().partial_cmp(&b.cores()).unwrap());
    let m = &runs[runs.len() / 2];
    println!(
        "{:<26} {:>9.3} {:>13.1}% {:>12.0}",
        name,
        m.cores(),
        m.cores() * 100.0,
        m.cpu_ns_per_elem(),
    );
}

fn report(name: &str, runs: &mut [Run]) {
    runs.sort_by(|a, b| a.cpu_ns_per_elem().partial_cmp(&b.cpu_ns_per_elem()).unwrap());
    let m = &runs[runs.len() / 2];
    println!(
        "{:<26} {:>9.2} {:>8.2} {:>12.1}",
        name,
        m.melem_per_s(),
        m.cores(),
        m.cpu_ns_per_elem(),
    );
}

fn main() {
    println!(
        "2 producers -> 1 consumer, cap {CAP}, {BATCH} u64 per iteration, \
         {ITERS} iterations per run, median of {ROUNDS}\n"
    );
    println!(
        "{:<26} {:>9} {:>8} {:>12}",
        "config", "Melem/s", "cores", "cpu ns/elem"
    );
    println!("{}", "-".repeat(59));

    macro_rules! bench {
        ($name:literal, $make:expr, $send:expr, $recv:expr) => {{
            let mut runs = Vec::new();
            for _ in 0..ROUNDS {
                runs.push(measure($make, $send, $recv));
            }
            report($name, &mut runs);
        }};
    }

    // ---- ultima_rings ----
    bench!(
        "ultima BusySpin (poll)",
        || mpsc::channel::<u64>(CAP, WaitStrategy::BusySpin),
        |tx: &mut mpsc::Sender<u64>, mut v: u64| {
            loop {
                match tx.try_send(v) {
                    Ok(()) => return true,
                    Err(TrySendError::Full(b)) => {
                        v = b;
                        std::hint::spin_loop();
                    }
                    Err(TrySendError::Disconnected(_)) => return false,
                }
            }
        },
        |rx: &mut mpsc::Receiver<u64>| {
            loop {
                match rx.try_recv() {
                    Ok(_) => return true,
                    Err(TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(TryRecvError::Disconnected) => return false,
                }
            }
        }
    );

    bench!(
        "ultima Park (block)",
        || mpsc::channel::<u64>(CAP, WaitStrategy::Park),
        |tx: &mut mpsc::Sender<u64>, v: u64| tx.send(v).is_ok(),
        |rx: &mut mpsc::Receiver<u64>| rx.recv().is_ok()
    );

    // ---- crossbeam-channel ----
    bench!(
        "crossbeam (poll)",
        || crossbeam_channel::bounded::<u64>(CAP),
        |tx: &mut crossbeam_channel::Sender<u64>, mut v: u64| {
            loop {
                match tx.try_send(v) {
                    Ok(()) => return true,
                    Err(crossbeam_channel::TrySendError::Full(b)) => {
                        v = b;
                        std::hint::spin_loop();
                    }
                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
                }
            }
        },
        |rx: &mut crossbeam_channel::Receiver<u64>| {
            loop {
                match rx.try_recv() {
                    Ok(_) => return true,
                    Err(crossbeam_channel::TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(crossbeam_channel::TryRecvError::Disconnected) => return false,
                }
            }
        }
    );

    bench!(
        "crossbeam (block)",
        || crossbeam_channel::bounded::<u64>(CAP),
        |tx: &mut crossbeam_channel::Sender<u64>, v: u64| tx.send(v).is_ok(),
        |rx: &mut crossbeam_channel::Receiver<u64>| rx.recv().is_ok()
    );

    // ---- thingbuf ----
    bench!(
        "thingbuf (poll)",
        || thingbuf::mpsc::blocking::channel::<u64>(CAP),
        |tx: &mut thingbuf::mpsc::blocking::Sender<u64>, mut v: u64| {
            loop {
                match tx.try_send(v) {
                    Ok(()) => return true,
                    Err(thingbuf::mpsc::errors::TrySendError::Full(b)) => {
                        v = b;
                        std::hint::spin_loop();
                    }
                    Err(_) => return false,
                }
            }
        },
        |rx: &mut thingbuf::mpsc::blocking::Receiver<u64>| {
            loop {
                match rx.try_recv() {
                    Ok(_) => return true,
                    Err(thingbuf::mpsc::errors::TryRecvError::Empty) => std::hint::spin_loop(),
                    Err(_) => return false,
                }
            }
        }
    );

    bench!(
        "thingbuf (block)",
        || thingbuf::mpsc::blocking::channel::<u64>(CAP),
        |tx: &mut thingbuf::mpsc::blocking::Sender<u64>, v: u64| tx.send(v).is_ok(),
        |rx: &mut thingbuf::mpsc::blocking::Receiver<u64>| rx.recv().is_some()
    );

    println!(
        "\ncores       = CPU time / wall time. 3.0 means all three threads spun \
         continuously.\ncpu ns/elem = CPU nanoseconds spent per element \
         delivered; lower is cheaper.\n              It is the reciprocal of \
         throughput-per-core, so the two say the same thing."
    );

    // -----------------------------------------------------------------------
    // Paced section. The table above cannot show what parking is for: at
    // saturation the ring is never empty, so no consumer ever reaches its park.
    // Here one producer sends every PACED_GAP and sleeps in between, so the
    // consumer is idle roughly all the time and its wait mechanism is the only
    // thing running. Producer CPU is excluded — it sleeps by construction, and
    // the question is what the *waiting* side costs.
    //
    // Contrast examples/wake_latency.rs, which spins its gap rather than
    // sleeping it, because there the goal is timing precision. Here the goal is
    // for the producer to be genuinely off-CPU.
    // -----------------------------------------------------------------------
    println!(
        "\n\nPaced: 1 producer sending every {:?}, {PACED_ELEMS} elements, \
         consumer idle between each.\nConsumer CPU only. This is where parking \
         either pays for itself or does not.\n",
        PACED_GAP
    );
    println!(
        "{:<26} {:>9} {:>14} {:>12}",
        "config", "cores", "% of a core", "cpu ns/elem"
    );
    println!("{}", "-".repeat(65));

    for (name, strat) in [
        ("ultima BusySpin", WaitStrategy::BusySpin),
        ("ultima BackoffYield", WaitStrategy::BackoffYield),
        ("ultima Backoff", WaitStrategy::Backoff),
        ("ultima Park", WaitStrategy::Park),
    ] {
        let mut runs: Vec<Paced> = (0..ROUNDS)
            .map(|_| {
                paced(
                    || mpsc::channel::<u64>(CAP, strat),
                    |tx: &mut mpsc::Sender<u64>, v| tx.send(v).is_ok(),
                    |rx: &mut mpsc::Receiver<u64>| rx.recv().is_ok(),
                )
            })
            .collect();
        report_paced(name, &mut runs);
    }

    let mut runs: Vec<Paced> = (0..ROUNDS)
        .map(|_| {
            paced(
                || crossbeam_channel::bounded::<u64>(CAP),
                |tx: &mut crossbeam_channel::Sender<u64>, v| tx.send(v).is_ok(),
                |rx: &mut crossbeam_channel::Receiver<u64>| rx.recv().is_ok(),
            )
        })
        .collect();
    report_paced("crossbeam (block)", &mut runs);

    let mut runs: Vec<Paced> = (0..ROUNDS)
        .map(|_| {
            paced(
                || thingbuf::mpsc::blocking::channel::<u64>(CAP),
                |tx: &mut thingbuf::mpsc::blocking::Sender<u64>, v| tx.send(v).is_ok(),
                |rx: &mut thingbuf::mpsc::blocking::Receiver<u64>| rx.recv().is_some(),
            )
        })
        .collect();
    report_paced("thingbuf (block)", &mut runs);

    println!(
        "\ncores measured on the consumer thread alone. 1.0 means it held a \
         core saturated\nfor the entire run while doing almost nothing — one \
         element every {:?}.",
        PACED_GAP
    );
}
