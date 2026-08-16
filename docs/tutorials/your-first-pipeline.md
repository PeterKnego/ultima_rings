# Your first pipeline

In this tutorial we will build a two-thread pipeline: one thread produces
numbers, another consumes and sums them, and a lock-free ring carries every
value between them. Along the way we will fill the ring to the brim and
watch it push back, and we will shut the pipeline down without losing a
value.

We need a Rust toolchain (`rustup` default stable is fine) and about ten
minutes.

## 1. Create the project

In a terminal, in any directory we like:

```console
$ cargo new ring_pipeline
    Creating binary (application) `ring_pipeline` package
$ cd ring_pipeline
```

Open `Cargo.toml` and add the dependency:

```toml
[dependencies]
ultima_rings = "0.2"
```

## 2. Send one value through a ring

Replace the contents of `src/main.rs` with:

```rust
use ultima_rings::{WaitStrategy, spsc};

fn main() {
    let (mut tx, mut rx) = spsc::channel::<u64>(4, WaitStrategy::Park);

    tx.try_send(7).unwrap();
    let value = rx.try_recv().unwrap();
    println!("received {value}");
}
```

`spsc::channel` gives us a sender and a receiver for a ring holding at most
4 values — capacities must be powers of two, see the
[capacity rules](../reference/channels.md#capacity-rules). We picked
`WaitStrategy::Park` because it costs almost no CPU while a side waits —
see the [wait-strategy reference](../reference/wait-strategies.md) for the
other options.

Run it:

```console
$ cargo run
received 7
```

The value went in on one end and came out the other. Both handles live on
one thread for now; we fix that in step 4.

## 3. Fill the ring and watch it push back

The ring holds 4 values. Let us try to send 5. Replace `main` with:

```rust
use ultima_rings::{WaitStrategy, spsc};

fn main() {
    let (mut tx, mut rx) = spsc::channel::<u64>(4, WaitStrategy::Park);

    for n in 1..=5 {
        match tx.try_send(n) {
            Ok(()) => println!("sent {n}"),
            Err(e) => println!("send {n} failed: {e}"),
        }
    }

    while let Ok(value) = rx.try_recv() {
        println!("received {value}");
    }
}
```

Run it:

```console
$ cargo run
sent 1
sent 2
sent 3
sent 4
send 5 failed: ring is full
received 1
received 2
received 3
received 4
```

Notice that the fifth send failed while the first four succeeded: the ring
is bounded, and a full ring refuses new values instead of growing. Notice
also that nothing was lost — the error handed the rejected value back to
us inside `Full(v)`. This is the
crate's backpressure signal; the
[backpressure guide](../how-to/handle-backpressure.md) covers what real
producers do with it.

## 4. Run producer and consumer on their own threads

Now the real pipeline. Replace `src/main.rs` with:

```rust
use std::thread;
use ultima_rings::{WaitStrategy, spsc};

fn main() {
    let (mut tx, mut rx) = spsc::channel::<u64>(4, WaitStrategy::Park);

    let producer = thread::spawn(move || {
        for n in 1..=10 {
            tx.send(n * n).unwrap();
        }
        // `tx` drops here: the channel closes.
    });

    let mut sum = 0;
    while let Ok(value) = rx.recv() {
        println!("received {value}");
        sum += value;
    }
    println!("sum = {sum}");

    producer.join().unwrap();
}
```

This time the producer runs on its own thread and uses the blocking `send`,
and the consumer uses the blocking `recv`. Run it:

```console
$ cargo run
received 1
received 4
received 9
received 16
received 25
received 36
received 49
received 64
received 81
received 100
sum = 385
```

If your `received` lines appear in a different order, something is wrong —
this ring is strictly first-in, first-out.

Notice what just happened quietly: we pushed **ten** values through a ring
that holds **four**. The producer filled the ring, blocked in `send`, and
was woken each time our consumer freed a slot — backpressure again, this
time absorbed by blocking instead of surfacing as an error.

Notice also how the pipeline ended: no sentinel value, no stop flag. When
the producer finished, its `tx` dropped, and our `recv` loop received the
remaining buffered values and then returned an error — that error is the
close signal, and it fires only after the ring is drained, so shutdown
loses nothing. The
[shutdown guide](../how-to/shut-down-a-pipeline.md) builds on exactly this
mechanism.

## Where we are

We built a bounded, lock-free, FIFO pipeline across two threads; we saw the
ring refuse a value when full, absorb a fast producer by blocking, and shut
down cleanly on drop. From here:

- swap in `mpsc` for many producers, and choose strategies deliberately —
  [How to choose a topology and wait strategy](../how-to/choose-a-topology-and-wait-strategy.md)
- consume in batches — [How to batch-consume with drain](../how-to/batch-consume-with-drain.md)
- read what the performance numbers really mean —
  [About reading this crate's benchmark numbers](../explanation/reading-the-benchmarks.md)
