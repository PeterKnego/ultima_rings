Raw output from the topology sweep analysed in
`docs/bench-results/2026-08-12-topology-sweep.md`.

Produced by `bench-infra/remote/sweep.sh` on one AWS c7i.8xlarge (16 physical
cores), one binary built once and pinned with `taskset` at each point.
`host.txt` records the machine, toolchain and `lscpu -e` topology.

Kept in the repo because `bench-infra/bench-out/` is gitignored, so this is the
only durable copy of the numbers behind that document.
