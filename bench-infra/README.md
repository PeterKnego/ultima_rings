# bench-infra — CPU-topology rig for ultima_rings

Provisions **one** high-core-count AWS host, rsyncs this working tree, and runs
the wait-strategy benchmarks pinned to a sweep of core counts. Adapted from
`ultima_db/bench-infra`, which is the same terraform module aimed at a different
question: that rig wants NVMe, this one wants **physical cores** and no storage
at all.

## Why this exists

Every number in `docs/bench-results/` up to 2026-08-12 was measured on a 4-vCPU
VM. That VM reports 4 CPUs and has **2 physical cores** with SMT — CPUs 0/1 are
siblings, 2/3 are siblings. So statements of the form "3 threads on 4 cores, not
oversubscribed" were made on 2 real cores with 3 runnable threads, and two
spinning threads landing on sibling hyperthreads contend for one core's
execution units rather than running in parallel.

Every wait-strategy conclusion in that directory is therefore conditioned on a
topology that was never stated, and the oversubscription results especially
(`2026-08-12-cpu-cost-and-heap-payload.md`, Part 2) may be measuring SMT
contention rather than the scheduling effect they attribute it to.

## Design: one machine, many core counts

The sweep pins the workload with `taskset` on a single host rather than
provisioning several instance sizes. That holds CPU model, frequency, kernel,
memory and binary constant, so the only thing varying is how many cores the
workload can see. Different instance sizes would confound core count with
processor generation and NUMA layout.

Two axes, and they are different questions:

| point | meaning |
|---|---|
| `phys2`, `phys4`, `phys8`, `phys16` | one thread per physical core; the honest "N cores" condition |
| `smt2x2` | both siblings of 2 physical cores — **reproduces the original VM exactly** |
| `smt8x2` | both siblings of 8 physical cores |

`smt2x2` is the row that matters most: it says whether the existing results are a
property of the crate or of that machine.

`URINGS_CORES` is passed to `examples/cpu_cost.rs` at each point so its thread
counts scale as ratios of the visible cores. "2x oversubscribed" then means the
same thing at every point, which is what makes rows comparable.

## Control-machine setup

| Tool | Used by |
|---|---|
| `terraform` >= 1.6 | `make init/up/destroy/status` |
| `rsync`, `jq`, `ssh` | source push, inventory, result pull |
| an SSH keypair | host access (path in `terraform.tfvars`) |

Ansible is **not** required — the remote side is two shell scripts.

## Credentials

Never stored in this directory. Point `ENV_FILE` at a gitignored file holding
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or use `AWS_PROFILE`:

    ENV_FILE=../../ultima_db/bench-infra/.env make up

`.gitignore` here excludes `.env`, `terraform.tfvars`, all terraform state, and
`bench-out/`.

## Quickstart

    cp example.tfvars terraform.tfvars   # edit ssh key + allow_ssh_cidr (your IP/32)
    make init
    make up                              # create + install toolchain + tune OS
    make sweep                           # rsync, build once, run every point, fetch
    make destroy                         # ALWAYS — nothing auto-reaps

`SKIP_BAKEOFF=1 make sweep` runs only the `cpu_cost` sweep, which is the fast
part and answers the topology question on its own. The criterion bake-off adds
roughly half an hour.

`make status` prints the instance and its uptime. **`ttl_hours` is an advisory
tag only** — nothing reaps it for you.

## Instance sizing

Default is `c7i.8xlarge`: 32 vCPU = 16 physical cores, single socket, single
NUMA node. That is **exactly** the account's 32 on-demand vCPU limit, so nothing
else may be running or the apply fails. Fall back to `c7i.4xlarge` (16 vCPU /
8 physical) if quota bites; the sweep skips points larger than the host.

Roughly $1.43/hr on-demand in us-east-1 at the default size.

## Notes

- **Same-host relative only.** Compare ordering and ratios within one sweep;
  absolute figures do not transfer between machines, and the results directory
  already documents a 20% session-to-session drift on unchanged code.
- **No cpufreq driver** is exposed on most EC2 types, so the performance
  governor cannot be set. The hypervisor manages frequency. Turbo is left on
  deliberately — steadier numbers with it off would be less like the machines
  this crate runs on.
- **THP is disabled** by `provision.sh`; the host records its actual state in
  `host.txt` alongside the results, so the setting is never assumed.
