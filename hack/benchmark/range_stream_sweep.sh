#!/usr/bin/env bash
# Sweep range_stream.sh across a fixed matrix of per-range sizes and emit a
# single consolidated table. Re-runs range_stream.sh per size since each size
# requires a fresh seed.
#
# Output columns: per_range | mode | avg_lat | rps | client_peak | server_peak
#
# Usage:
#   ./hack/benchmark/range_stream_sweep.sh > /tmp/sweep.log
#
# Per-size knobs come from the matrix below; CLIENTS/PAGE/etc. inherit from
# range_stream.sh defaults but are pinned here to keep numbers comparable.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
DRIVER="$ROOT/hack/benchmark/range_stream.sh"
RAW_LOG=${RAW_LOG:-/tmp/range_stream_sweep_raw.log}

: > "$RAW_LOG"

# Per-range size matrix: label | KEYS | VAL_SIZE | ITERS
MATRIX=(
  "13 MB|100000|100|10"
  "65 MB|500000|100|5"
  "130 MB|1000000|100|3"
  "1 GB|10000|100000|3"
)

declare -a ROWS

for entry in "${MATRIX[@]}"; do
  IFS='|' read -r label keys val iters <<< "$entry"

  echo "############################################################" | tee -a "$RAW_LOG"
  echo "# $label  (KEYS=$keys VAL_SIZE=$val ITERS=$iters)" | tee -a "$RAW_LOG"
  echo "############################################################" | tee -a "$RAW_LOG"

  out=$(KEYS="$keys" VAL_SIZE="$val" ITERS="$iters" CLIENTS=1 PAGE="$keys" \
    "$DRIVER" 2>&1)
  printf '%s\n' "$out" >> "$RAW_LOG"

  # Extract the Summary block lines: "Label  | avg=Xs rps=Y | client_peak=A server_peak=B"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    mode=$(printf '%s\n' "$line" | awk -F' *\\| *' '{print $1}' | sed 's/[[:space:]]*$//')
    avg=$(printf '%s\n' "$line" | sed -n 's/.*avg=\([^ ]*\)s.*/\1/p')
    rps=$(printf '%s\n' "$line" | sed -n 's/.*rps=\([^ ]*\).*/\1/p')
    cpk=$(printf '%s\n' "$line" | sed -n 's/.*client_peak=\([^ ]*\)MB.*/\1/p')
    spk=$(printf '%s\n' "$line" | sed -n 's/.*server_peak=\([^ ]*\)MB.*/\1/p')
    [[ -z "$avg" || -z "$rps" ]] && continue
    ROWS+=("$(printf '%-8s | %-30s | %7ss | %10s | %10s MB | %10s MB' \
      "$label" "$mode" "$avg" "$rps" "$cpk" "$spk")")
  done < <(printf '%s\n' "$out" | awk '/^================ Summary ================$/{flag=1; next} flag')
done

echo
echo "================ Sweep Summary ================"
printf '%-8s | %-30s | %8s | %10s | %13s | %13s\n' \
  "size" "mode" "avg_lat" "rps" "client_peak" "server_peak"
printf '%-8s-+-%-30s-+-%8s-+-%10s-+-%13s-+-%13s\n' \
  "--------" "------------------------------" "--------" "----------" "-------------" "-------------"
for row in "${ROWS[@]}"; do echo "$row"; done

echo
echo "Raw per-size logs: $RAW_LOG"
