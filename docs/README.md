# ultima_rings documentation

Documentation for `ultima_rings`, a bounded lock-free SPSC/MPSC ring-channel
crate. It is organized in four kinds — a hands-on lesson, goal-oriented
guides, dry facts, and background reading — so start with the kind that
matches what you need right now.

## Learn it

New to the crate? [Your first pipeline](tutorials/your-first-pipeline.md)
builds a working two-thread pipeline in about ten minutes, and meets
backpressure and clean shutdown along the way. Overview:
[tutorials](tutorials/README.md).

## Get something done

The [how-to guides](how-to/README.md) serve specific jobs: choosing a
topology and wait strategy, handling a full ring, shutting down without
losing values, batching consumption, and placing threads for cache locality.

## Look something up

The [reference](reference/README.md) states the facts: channel types and
their guarantees, the wait-strategy table with measured costs, and the full
error and disconnect semantics. The canonical per-item API reference is the
rustdoc (`cargo doc --open`).

## Understand it

The [explanation section](explanation/README.md) is for reading away from
the keyboard: the design document behind every atomic in the crate, and a
guide to what the benchmark numbers do and do not mean.

## Lab records

Two directories hold the project's internal records rather than user
documentation: [`bench-results/`](bench-results/) is the dated measurement
log (indexed by its own README), and [`superpowers/`](superpowers/) holds
plans, competitor research, and design specs from development.
