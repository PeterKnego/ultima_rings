#!/usr/bin/env bash
# Runs on the bench host, once. Installs the toolchain and tunes the OS for
# low-variance measurement. Idempotent.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

# Ubuntu's unattended-upgrades holds the dpkg lock on a fresh boot.
for _ in $(seq 1 60); do
  sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 || break
  sleep 5
done

sudo apt-get update -qq
sudo apt-get install -y -qq build-essential util-linux linux-tools-common >/dev/null

# --- toolchain ------------------------------------------------------------
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
rustc -V

# --- tuning ---------------------------------------------------------------
# Frequency scaling and THP are the two settings that most reliably turn a
# clean A/B into noise. Both are best-effort: some instance types expose no
# cpufreq driver at all, and that is fine as long as it is reported.
if [ -d /sys/devices/system/cpu/cpu0/cpufreq ]; then
  echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor >/dev/null 2>&1 || true
  echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
else
  echo "governor: no cpufreq driver exposed (normal on many EC2 types)"
fi

echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled >/dev/null 2>&1 || true
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag  >/dev/null 2>&1 || true
echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo unknown)"

# Keep the scheduler from migrating a spinning thread off the CPU taskset gave
# it, and stop watchdogs from stealing slices mid-measurement.
sudo sysctl -qw kernel.numa_balancing=0 2>/dev/null || true
sudo sysctl -qw kernel.watchdog=0 2>/dev/null || true
sudo sysctl -qw kernel.perf_event_paranoid=0 2>/dev/null || true

# Turbo/boost left ON deliberately: switching it off would make the numbers
# steadier but less like the machines this crate actually runs on. Recorded so
# the choice is visible rather than implicit.
echo "provision complete"
