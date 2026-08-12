#!/usr/bin/env bash
# Re-derives the resolution budget: how much of a measured difference is code
# layout, and how much is run-to-run noise.
#
# Method is the one from docs/bench-results/2026-08-12-layout-sensitivity.md.
# The SAME SOURCE is built at several function alignments, so every difference
# between builds is layout by construction. Two statistics per cell:
#
#   layout spread   range across the per-alignment means
#   intrinsic noise mean absolute round-to-round difference *within* one
#                   alignment, which involves no rebuild at all
#
# Minimum detectable effect is roughly the larger of the two. A cell whose
# layout spread barely exceeds its intrinsic noise is not layout-sensitive, it
# is just noisy, and rebuilding differently will not help it.
#
# Two rules the original run established and this preserves:
#   - build every variant to completion BEFORE measuring anything
#   - interleave alignments within each round, never all rounds of one build
#
# Each alignment gets its own CARGO_TARGET_DIR precisely so that interleaving
# costs nothing; switching RUSTFLAGS in a shared target dir would rebuild on
# every switch and force a block design.
set -euo pipefail

REPO="${REPO:-$HOME/ultima_rings}"
OUT="${OUT:-$HOME/bench-out}"
ALIGNS="${ALIGNS:-0 3 4 5 6}"
ROUNDS="${LAYOUT_ROUNDS:-3}"
FILTER="${LAYOUT_FILTER:-backoff_isolation/|^spsc/|bakeoff_mpsc/}"
mkdir -p "$OUT"
cd "$REPO"

mapfile -t CORE_FIRST < <(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -k1,1n | awk -F, '!seen[$2]++ {print $1}')
mapfile -t CORE_ALL   < <(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -k1,1n | awk -F, '{a[$2]=a[$2]","$1} END {for (c in a) print substr(a[c],2)}' | sort -t, -k1,1n)
NPHYS=${#CORE_FIRST[@]}
take() { local n=$1; shift; local IFS=,; echo "${*:1:$n}"; }

cpus_for() {
  case "$1" in
    full)   take "$NPHYS" "${CORE_FIRST[@]}" ;;
    smt2x2) take 2 "${CORE_ALL[@]}" ;;
    *) echo "unknown point: $1" >&2; return 1 ;;
  esac
}
POINTS="${LAYOUT_POINTS:-full smt2x2}"

# --- build every alignment to completion ----------------------------------
for a in $ALIGNS; do
  echo "building align=$a ..."
  CARGO_TARGET_DIR="$HOME/t-align$a" \
  RUSTFLAGS="-C llvm-args=-align-all-functions=$a" \
    cargo build --release --benches 2>&1 | tail -2
done
df -h "$HOME" | tail -1

# --- measure, interleaved by round ----------------------------------------
for point in $POINTS; do
  cpus="$(cpus_for "$point")" || continue
  f="$OUT/layout.${point}.txt"
  : > "$f"
  echo "=== layout sweep: $point (cpus=$cpus) ==="
  for r in $(seq 1 "$ROUNDS"); do
    for a in $ALIGNS; do
      echo "### round=$r align=$a point=$point" >> "$f"
      CARGO_TARGET_DIR="$HOME/t-align$a" \
      RUSTFLAGS="-C llvm-args=-align-all-functions=$a" \
        taskset -c "$cpus" cargo bench --bench throughput -- "$FILTER" 2>&1 \
        | grep -E '^(backoff_isolation|spsc|bakeoff_mpsc)|thrpt:  \[[0-9]' >> "$f"
      echo "  round=$r align=$a done"
    done
  done
done

echo "results in $OUT"
ls -la "$OUT"
