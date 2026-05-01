#!/usr/bin/env bash
# Compare unary vs streaming vs paginated-unary Range performance.
# Spins up a local etcd, seeds keys, runs each mode, and scrapes server heap
# from /metrics in parallel.
#
# Defaults are tuned for a quick relative-ratio sample, not absolute numbers.
# Override via env vars:
#   KEYS, KEY_SIZE, VAL_SIZE, ITERS, PAGE, ENDPOINT, DATA_DIR

set -euo pipefail

KEYS=${KEYS:-100000}
KEY_SIZE=${KEY_SIZE:-32}
VAL_SIZE=${VAL_SIZE:-100}
ITERS=${ITERS:-10}
CLIENTS=${CLIENTS:-1}
PAGE=${PAGE:-10000}
ENDPOINT=${ENDPOINT:-127.0.0.1:2379}
DATA_DIR=${DATA_DIR:-/tmp/etcd-range-bench}
METRICS_URL="http://${ENDPOINT}/metrics"
SCRAPE_INTERVAL=${SCRAPE_INTERVAL:-0.01}
CLIENT_SAMPLE_MS=${CLIENT_SAMPLE_MS:-1}

ROOT="$(git rev-parse --show-toplevel)"
ETCD="$ROOT/bin/etcd"
BENCH="$ROOT/bin/benchmark"

if [[ ! -x "$ETCD" || ! -x "$BENCH" ]]; then
  echo "Building etcd + benchmark..."
  (cd "$ROOT" && make build && go build -o bin/benchmark ./tools/benchmark/)
fi

ETCD_PID=
SCRAPE_PID=
cleanup() {
  if [[ -n "$SCRAPE_PID" ]]; then kill "$SCRAPE_PID" 2>/dev/null || true; fi
  if [[ -n "$ETCD_PID" ]]; then kill "$ETCD_PID" 2>/dev/null || true; wait "$ETCD_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

rm -rf "$DATA_DIR"
echo "Starting etcd at $ENDPOINT (data: $DATA_DIR, GOGC=${GOGC:-100})..."
GOGC=${GOGC:-100} "$ETCD" \
  --data-dir "$DATA_DIR" \
  --listen-client-urls "http://$ENDPOINT" \
  --advertise-client-urls "http://$ENDPOINT" \
  --quota-backend-bytes=8589934592 \
  --log-level=warn \
  >/tmp/etcd-range-bench.log 2>&1 &
ETCD_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS "$METRICS_URL" >/dev/null 2>&1; then break; fi
  sleep 0.25
done
if ! curl -fsS "$METRICS_URL" >/dev/null 2>&1; then
  echo "etcd failed to come up; tail of /tmp/etcd-range-bench.log:" >&2
  tail -40 /tmp/etcd-range-bench.log >&2
  exit 1
fi

echo "Seeding $KEYS keys × ${VAL_SIZE}B values..."
"$BENCH" --endpoints="$ENDPOINT" --clients=64 --conns=64 put \
  --sequential-keys --key-space-size="$KEYS" \
  --total="$KEYS" --key-size="$KEY_SIZE" --val-size="$VAL_SIZE" \
  >/dev/null

scrape_server_heap() {
  local out="$1"
  : > "$out"
  while true; do
    curl -fsS "$METRICS_URL" 2>/dev/null \
      | awk '/^go_memstats_heap_inuse_bytes /{print $2}' >> "$out"
    sleep "$SCRAPE_INTERVAL"
  done
}

declare -a SUMMARY

run_mode() {
  local label="$1"; shift
  local heap_file
  heap_file=$(mktemp)
  scrape_server_heap "$heap_file" &
  SCRAPE_PID=$!

  echo
  echo "=== $label ==="
  local out
  out=$("$BENCH" --endpoints="$ENDPOINT" range \
    --total="$ITERS" --conns="$CLIENTS" --clients="$CLIENTS" \
    --from-key --mem-sample-ms="$CLIENT_SAMPLE_MS" \
    "$@" 2>&1)
  printf '%s\n' "$out"

  kill "$SCRAPE_PID" 2>/dev/null || true
  wait "$SCRAPE_PID" 2>/dev/null || true
  SCRAPE_PID=

  local server_peak_mb
  server_peak_mb=$(awk 'BEGIN{m=0} {if($1+0>m+0)m=$1} END{printf "%.2f", m/1024/1024}' "$heap_file")
  rm -f "$heap_file"

  local client_peak_mb avg_lat throughput
  client_peak_mb=$(printf '%s\n' "$out" | awk -F': *' '/CLIENT_PEAK_HEAP_MB/{print $2}')
  avg_lat=$(printf '%s\n' "$out" | awk '/Average:/{print $2; exit}')
  throughput=$(printf '%s\n' "$out" | awk '/Requests\/sec/{print $2}')

  SUMMARY+=("$(printf '%-28s | avg=%ss rps=%s | client_peak=%sMB server_peak=%sMB' \
    "$label" "${avg_lat:-?}" "${throughput:-?}" "${client_peak_mb:-?}" "$server_peak_mb")")
}

run_mode "Single-shot unary"
run_mode "Stream (accumulate)"          --stream
run_mode "Stream (pre-sized accum)"     --stream --stream-accum-cap="$KEYS"
run_mode "Stream (discard)"             --stream --stream-discard
run_mode "Paginated unary (page=$PAGE)" --paginate="$PAGE"

echo
echo "================ Summary ================"
echo "Config: keys=$KEYS key_size=$KEY_SIZE val_size=$VAL_SIZE iters=$ITERS clients=$CLIENTS page=$PAGE"
for line in "${SUMMARY[@]}"; do echo "$line"; done
