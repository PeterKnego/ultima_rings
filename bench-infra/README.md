# bench-infra — CPU-topology rig for ultima_rings

This module provisions one AWS host with a high core count. It copies this
checkout to the host with `rsync`, uncommitted edits included. Then it runs the wait-strategy
benchmarks, pinned to a sweep of core counts.

It is an adaptation of `ultima_db/bench-infra`, the same terraform module
aimed at a different question. That rig wants NVMe. This one wants only physical
cores and no storage at all.

## Why this exists

Every number in `docs/bench-results/` up to 2026-08-12 came from a virtual
machine (VM) with 4 virtual CPUs (vCPUs). That VM reports 4 CPUs, but it has 2
physical cores with SMT (two hardware threads on each core). CPUs 0 and 1 are
siblings, and CPUs 2 and 3 are siblings.

So a statement of the form '3 threads on 4 cores, not oversubscribed' in fact
describes 2 real cores with 3 runnable threads. When two threads spin on
hyperthreads that are siblings, they contend for the execution units of one
core. They
do not run in parallel.

Every wait-strategy conclusion in that directory is therefore conditional on a
topology that no report stated. The oversubscription results
(`2026-08-12-cpu-cost-and-heap-payload.md`, Part 2) are the most suspect. It
is possible that they measure SMT contention, not the scheduler effect that
they attribute the numbers to.

## Design: one machine, many core counts

The sweep pins the workload with `taskset` on a single host. It does not
provision several instance sizes. This holds the CPU model, the frequency, the
kernel, the memory, and the binary constant. So the only variable is the count
of cores that the workload can see. Different instance sizes, in contrast, would confound the core count with the
processor generation and the NUMA (non-uniform memory access) layout.

The sweep has two axes, and they are different questions:

- `phys2`, `phys4`, `phys8`, and `phys16`: one thread on each physical core.
  This is the honest 'N cores' condition.

- `smt2x2`: both siblings of 2 physical cores. This point reproduces the
  original VM exactly.

- `smt8x2`: both siblings of 8 physical cores.

`smt2x2` is the row that matters most. It says whether the current results are
a property of the crate or a property of that machine.

The sweep passes `URINGS_CORES` to `examples/cpu_cost.rs` at each point. So
the thread counts of that example scale as ratios of the visible cores. Then
'2x oversubscribed' means the same thing at every point, and that is what
makes the rows comparable.

## Control-machine setup

The control machine needs these tools:

- `terraform` >= 1.6. The targets `make init`, `make up`, `make destroy`, and
  `make status` use it.

- `rsync`, `jq`, and `ssh`. They do the source push, the inventory, and the
  result pull.

- An SSH keypair, for access to the host. Its path goes in `terraform.tfvars`.

You do not need Ansible. The remote side is two shell scripts.

## Credentials

This directory never stores credentials. Point `ENV_FILE` at a gitignored file
that holds `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. Or use
`AWS_PROFILE`:

    ENV_FILE=../../ultima_db/bench-infra/.env make up

The `.gitignore` here excludes `.env`, `terraform.tfvars`, all terraform
state, and `bench-out/`.

## Quickstart

Do not leave the host up. Run `make destroy` at the end of each session —
nothing reaps the host for you.

    cp example.tfvars terraform.tfvars   # edit ssh key + allow_ssh_cidr (your IP/32)
    make init
    make up                              # create + install toolchain + tune OS
    make sweep                           # rsync, build once, run every point, fetch
    make destroy                         # ALWAYS — nothing auto-reaps

`SKIP_BAKEOFF=1 make sweep` runs only the `cpu_cost` sweep. That sweep is the
fast part, and it answers the topology question on its own. The full
`criterion` bake-off adds about half an hour.

`make status` prints the instance and its uptime. `ttl_hours` is an advisory
tag only. Nothing reaps the instance for you.

## Instance size

The default is `c7i.8xlarge`: 32 vCPUs, which is 16 physical cores, on a
single socket and a single NUMA node. That is exactly the account's limit of
32 on-demand vCPUs. So no other instance can run at the same time, or the
apply fails. If the quota blocks the default size, fall back to `c7i.4xlarge`
(16 vCPUs, 8 physical cores). The sweep skips the points that are larger than the host.

On-demand price at the default size: about $1.43 an hour in us-east-1.

## Notes

- **Same-host relative only.** Compare the order and the ratios within one
  sweep. Absolute figures do not transfer between machines. The directory
  of results already documents a 20% session-to-session drift on unchanged
  code.

- **No cpufreq driver.** Most EC2 types expose no cpufreq driver, so you
  cannot set the performance governor. The hypervisor manages the frequency.
  Turbo stays on deliberately. Steadier numbers with turbo off would be less
  like the machines that this crate runs on.

- **THP off.** `provision.sh` turns off THP (transparent huge pages). The host
  records the actual state of THP in `host.txt` beside the results. So the
  setting is a recorded fact, not an assumption.
