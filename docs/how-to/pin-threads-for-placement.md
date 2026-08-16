# How to place threads so the handoff stays fast

Where the OS puts your producer and consumer threads changes handoff
throughput more than most code changes will: on the crate's own measurements,
moving two threads from SMT siblings to separate physical cores cost this
crate 1.88× (and crossbeam-channel 5.53×) with no code change
([`2026-08-15-thread-placement.md`](../bench-results/2026-08-15-thread-placement.md)).

## Decide whether to pin at all

- If the handoff is on your latency budget and the machine is yours to
  partition, pin. An unpinned pipeline's throughput is a scheduler decision
  that varies run to run.
- If the threads do heavy compute besides the handoff, don't co-locate them
  blindly: SMT siblings share one core's execution units, so what the
  handoff gains, the compute can lose. Measure both placements with your
  real workload.

## Pin the two sides

Use any affinity crate; `core_affinity` is what this repo's own benches use
(see `pin()` in `benches/throughput.rs` for an in-repo example):

```rust
// In each thread, before entering the hot loop:
let ok = core_affinity::set_for_current(core_affinity::CoreId { id: CPU });
assert!(ok, "pin failed");
```

- For the tightest handoff, put producer and consumer on SMT siblings of
  one physical core (they share L1d/L2, so the exchanged cache lines never
  leave the core).
- To keep full cores for compute, put them on separate cores and accept the
  L3 round trip per handoff.
- Find sibling pairs in `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list`
  or `lscpu -e` — do not assume CPU 0 and 1 are siblings; numbering varies
  by machine.

## Assert your pins

`core_affinity::get_core_ids()` reports the *calling thread's affinity
mask*, not the machine — a thread that inherited a narrowed mask cannot see
or pin to other cores, and a failed pin silently leaves placement to the
scheduler. Check the return value and fail loudly; this repo's benches abort
on a failed pin for exactly that reason.

## Verify with a measurement

Pinning is a hypothesis until measured. Run your pipeline pinned both ways
and compare; if you have no workload harness yet, `cargo bench` in this repo
sweeps the crate's own cells over placements you can compare against.
