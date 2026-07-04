#!/usr/bin/env bash
#
# Downloads real Binance aggTrades data for benchmarking (§5).
#
# Binance publishes daily aggTrades dumps as zipped CSVs at a stable public
# URL pattern — no API key needed. This grabs one day of BTCUSDT by default
# (~15-25M rows, ~1GB uncompressed), which is plenty to exercise realistic
# compression ratios and query latencies; pass --days N for more.
#
# NOTE: `data.binance.vision` was not reachable from the sandboxed
# environment that wrote this script (outside its network allowlist), so
# this has not been run for real — the URL pattern and CSV shape are
# documented and stable (Binance has used this exact layout for years), and
# match the CSV parser in `crates/cli/src/csv.rs` field-for-field, but
# you're the first real execution of this script.
#
# Usage:
#   ./bench/download.sh                       # 1 day of BTCUSDT
#   ./bench/download.sh --symbol ETHUSDT --days 7
#   ./bench/download.sh --date 2024-01-15      # a specific day

set -euo pipefail

SYMBOL="BTCUSDT"
DAYS=1
START_DATE=""
OUT_DIR="$(dirname "$0")/data"

while [ $# -gt 0 ]; do
    case "$1" in
        --symbol) SYMBOL="$2"; shift 2 ;;
        --days) DAYS="$2"; shift 2 ;;
        --date) START_DATE="$2"; shift 2 ;;
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [ -z "$START_DATE" ]; then
    # Default to 3 days ago: Binance sometimes takes a day or two to
    # publish the most recent daily file, so "today" or "yesterday" can
    # 404 depending on when you run this.
    START_DATE=$(date -d "3 days ago" +%Y-%m-%d 2>/dev/null || date -v-3d +%Y-%m-%d)
fi

mkdir -p "$OUT_DIR"

log() { printf '\033[1;36m[download]\033[0m %s\n' "$*"; }

for i in $(seq 0 $((DAYS - 1))); do
    day=$(date -d "$START_DATE + $i days" +%Y-%m-%d 2>/dev/null \
        || date -j -v+"${i}"d -f %Y-%m-%d "$START_DATE" +%Y-%m-%d)

    url="https://data.binance.vision/data/spot/daily/aggTrades/${SYMBOL}/${SYMBOL}-aggTrades-${day}.zip"
    zip_path="${OUT_DIR}/${SYMBOL}-aggTrades-${day}.zip"
    csv_path="${OUT_DIR}/${SYMBOL}-aggTrades-${day}.csv"

    if [ -f "$csv_path" ]; then
        log "already have $csv_path, skipping"
        continue
    fi

    log "downloading $url"
    if ! curl -sf -o "$zip_path" "$url"; then
        log "WARNING: failed to fetch $day (may not be published yet, or the date is too recent) — skipping"
        rm -f "$zip_path"
        continue
    fi

    log "unzipping..."
    unzip -oq "$zip_path" -d "$OUT_DIR"
    rm -f "$zip_path"

    rows=$(wc -l < "$csv_path")
    log "  $csv_path: $rows rows"
done

log "done. CSVs are in $OUT_DIR"
log ""
log "next: ingest into tickdb —"
log "  cargo run -p cli --release -- ingest --data-dir /tmp/tickdb-bench \\"
log "    --symbol $SYMBOL --csv ${OUT_DIR}/${SYMBOL}-aggTrades-${START_DATE}.csv --fsync never"
