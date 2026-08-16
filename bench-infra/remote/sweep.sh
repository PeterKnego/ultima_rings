#!/usr/bin/env bash
# Runs on the bench host. Sweeps CPU topology by pinning the workload to a
# chosen set of CPUs with taskset, holding the machine, kernel and binary
# constant so the only variable is how many cores the workload can see.
#
# Two axes, and they are different questions:
#   physical  — one thread per physical core (SMT siblings excluded)
#   smt       — both siblings of each physical core
#
# The distinction is not academic for this crate. The box that produced
# docs/bench-results/2026-08-12-* reports 4 CPUs but has 2 physical cores, so
# every "3 threads on 4 cores, not oversubscribed" statement in that directory
# was measured on 2 real cores with 3 runnable threads.
set -euo pipefail

REPO="${REPO:-$HOME/ultima_rings}"
OUT="${OUT:-$HOME/bench-out}"
mkdir -p "$OUT"
cd "$REPO"

# --- topology -------------------------------------------------------------
# lscpu -p gives CPU,Core,Socket,Node,... ; we want one representative CPU per
# physical core, and the sibling list for the SMT runs.
mapfile -t CORE_FIRST < <(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -k1,1n | awk -F, '!seen[$2]++ {print $1}')
mapfile -t CORE_ALL   < <(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -k1,1n | awk -F, '{a[$2]=a[$2]","$1} END {for (c in a) print substr(a[c],2)}' | sort -t, -k1,1n)
NPHYS=${#CORE_FIRST[@]}
NCPU=$(nproc)

{
  echo "host:      $(hostname)"
  echo "date:      $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpus:      $NCPU"
  echo "physical:  $NPHYS"
  echo "model:     $(lscpu | sed -n 's/^Model name: *//p')"
  echo "governor:  $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
  echo "thp:       $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo unknown)"
  echo "rustc:     $(rustc -V)"
  echo "commit:    $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo
  lscpu -e | head -40
} | tee "$OUT/host.txt"

# --- build once -----------------------------------------------------------
# One binary for every sweep point, so code layout cannot vary between them.
echo "building..."
cargo build --release --example cpu_cost 2>&1 | tail -3
cargo build --release --benches 2>&1 | tail -3

# Join the first N entries of an array with commas.
take() { local n=$1; shift; local IFS=,; echo "${*:1:$n}"; }

run_point() {
  local label="$1" cpulist="$2" ncores="$3"
  local f="$OUT/cpu_cost.${label}.txt"
  echo "=== $label  (cpus=$cpulist, URINGS_CORES=$ncores) ==="
  {
    echo "### label=$label cpulist=$cpulist URINGS_CORES=$ncores"
    URINGS_CORES="$ncores" taskset -c "$cpulist" \
      ./target/release/examples/cpu_cost
  } 2>&1 | tee "$f"
}

# --- physical-core sweep --------------------------------------------------
# 1 thread per core: the honest "N cores" condition.
if [ "${SKIP_CPUCOST:-0}" != "1" ]; then
for n in 2 4 8 16; do
  [ "$n" -le "$NPHYS" ] || continue
  run_point "phys${n}" "$(take "$n" "${CORE_FIRST[@]}")" "$n"
done

# --- SMT control ----------------------------------------------------------
# Both siblings of 2 physical cores = 4 CPUs. This is the exact shape of the
# VPC that produced the existing results, so it is the row that says whether
# those results were a property of the crate or of that machine.
if [ "$NPHYS" -ge 2 ] && [ "$NCPU" -gt "$NPHYS" ]; then
  run_point "smt2x2" "$(take 2 "${CORE_ALL[@]}")" 4
  if [ "$NPHYS" -ge 8 ]; then
    run_point "smt8x2" "$(take 8 "${CORE_ALL[@]}")" 16
  fi
fi
fi

# --- bake-off -------------------------------------------------------------
# Criterion is slow, so the points are explicit rather than the full sweep.
# BAKEOFF_POINTS is a space-separated list of labels understood below; the
# default pair is the full machine and the 2-CPU corner.
#
# The interesting pairs are matched on *CPU count*, not core count:
#   smt2x2 vs phys4  — 4 CPUs either way, 2 physical cores against 4. Isolates
#                      what SMT costs when the runqueue slots are equal.
#   smt2x2           — the shape of the VM that produced the pre-2026-08-12
#                      results, so it is the replication check.
cpus_for() {
  case "$1" in
    full)    take "$NPHYS" "${CORE_FIRST[@]}" ;;
    phys2)   take 2  "${CORE_FIRST[@]}" ;;
    phys4)   take 4  "${CORE_FIRST[@]}" ;;
    phys8)   take 8  "${CORE_FIRST[@]}" ;;
    phys16)  take 16 "${CORE_FIRST[@]}" ;;
    smt2x2)  take 2  "${CORE_ALL[@]}" ;;
    smt8x2)  take 8  "${CORE_ALL[@]}" ;;
    *) echo "unknown bakeoff point: $1" >&2; return 1 ;;
  esac
}

if [ "${SKIP_BAKEOFF:-0}" != "1" ]; then
  for label in ${BAKEOFF_POINTS:-full phys2}; do
    cpus="$(cpus_for "$label")" || continue
    echo "=== bakeoff $label (cpus=$cpus) ==="
    {
      echo "=== bakeoff $label ==="
      for r in $(seq 1 "${BAKEOFF_ROUNDS:-3}"); do
        echo "### round=$r point=$label"
        taskset -c "$cpus" cargo bench \
          --bench throughput -- "${BAKEOFF_FILTER:-^bakeoff_}" 2>&1 \
          | grep -E '^(bakeoff|sharded|mpsc)|thrpt:  \[[0-9]'
      done
    } | tee "$OUT/bakeoff.${label}.txt"
  done
fi

echo
echo "results in $OUT"
ls -la "$OUT"
