#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash scripts/run_stress_matrix.sh
# Optional:
#   BIN=./build/tests/stress_test TOPIC=stress_topic bash scripts/run_stress_matrix.sh

BIN="${BIN:-./test/stress_test}"
TOPIC="${TOPIC:-stress_topic}"

if [[ ! -x "$BIN" ]]; then
  echo "Binary not found or not executable: $BIN"
  echo "Build it first:"
  echo "  make run-test TEST=stress_test"
  echo "Then re-run this script."
  exit 1
fi

# Matrix: "threads totalMessages partitions"
MATRIX=(
  "10 10000 2"
  "20 50000 2"
  "20 50000 8"
  "50 100000 8"
  "50 100000 16"
)

timestamp="$(date +%Y%m%d_%H%M%S)"
out_dir="build/stress_runs/$timestamp"
mkdir -p "$out_dir"

summary_csv="$out_dir/summary.csv"
echo "threads,totalMessages,partitions,attempted,succeeded,failed,connFail,ioFail,durationSec,throughput,p50us,p95us,p99us" > "$summary_csv"

echo "Running stress matrix..."
echo "Logs: $out_dir"
echo

for row in "${MATRIX[@]}"; do
  read -r threads total partitions <<< "$row"
  log="$out_dir/t${threads}_m${total}_p${partitions}.log"

  echo ">>> threads=$threads totalMessages=$total partitions=$partitions topic=$TOPIC"
  "$BIN" "$threads" "$total" "$partitions" "$TOPIC" | tee "$log"

  attempted=$(awk -F: '/attempted/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  succeeded=$(awk -F: '/succeeded/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  failed=$(awk -F: '/failed/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  connFail=$(awk -F: '/connection failures/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  ioFail=$(awk -F: '/io failures/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  durationSec=$(awk -F: '/duration sec/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  throughput=$(awk -F: '/throughput msg\/sec/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  p50=$(awk -F: '/latency p50/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  p95=$(awk -F: '/latency p95/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)
  p99=$(awk -F: '/latency p99/{gsub(/ /,"",$2); print $2}' "$log" | tail -1)

  echo "${threads},${total},${partitions},${attempted},${succeeded},${failed},${connFail},${ioFail},${durationSec},${throughput},${p50},${p95},${p99}" >> "$summary_csv"
  echo
done

echo "========== SUMMARY =========="
printf "%-8s %-12s %-10s %-10s %-10s %-8s %-10s %-10s %-12s %-12s %-10s %-10s %-10s\n" \
  "threads" "messages" "parts" "attempted" "success" "failed" "connFail" "ioFail" "duration(s)" "throughput" "p50(us)" "p95(us)" "p99(us)"

tail -n +2 "$summary_csv" | while IFS=, read -r threads total parts attempted succeeded failed connFail ioFail duration tp p50 p95 p99; do
  printf "%-8s %-12s %-10s %-10s %-10s %-8s %-10s %-10s %-12s %-12s %-10s %-10s %-10s\n" \
    "$threads" "$total" "$parts" "$attempted" "$succeeded" "$failed" "$connFail" "$ioFail" "$duration" "$tp" "$p50" "$p95" "$p99"
done

echo
echo "CSV written to: $summary_csv"
echo "All logs under: $out_dir"
