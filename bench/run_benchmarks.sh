#!/usr/bin/env bash
#
# Full benchmark suite (§5): tickdb vs DuckDB / Parquet / Polars, same
# dataset on both sides for a rigorous comparison.
#
# Usage:
#   ./bench/run_benchmarks.sh <csv_path> [symbol]
#
# For real numbers, first fetch real Binance data:
#   ./bench/download.sh
#   ./bench/run_benchmarks.sh bench/data/BTCUSDT-aggTrades-<date>.csv BTCUSDT
#
# Or on synthetic data for a quick check:
#   python3 bench/gen_synthetic.py 2000000 42 > /tmp/bench.csv
#   ./bench/run_benchmarks.sh /tmp/bench.csv BTCUSDT

set -euo pipefail

CSV_PATH="${1:?usage: run_benchmarks.sh <csv_path> [symbol]}"
SYMBOL="${2:-BTCUSDT}"
DATA_DIR=$(mktemp -d)

log() { printf '\033[1;36m[bench]\033[0m %s\n' "$*"; }

trap 'rm -rf "$DATA_DIR"' EXIT

log "dataset: $CSV_PATH ($(wc -l < "$CSV_PATH") lines including header)"
log ""

log "── tickdb: ingest via the real CLI ──"
cargo run -p cli --release -- ingest \
    --data-dir "$DATA_DIR" --symbol "$SYMBOL" --csv "$CSV_PATH" --fsync never

log ""
log "── tickdb: query kernels (same on-disk data) ──"
cargo run --manifest-path bench/tickdb_bench_rs/Cargo.toml --release --bin query_only -- "$DATA_DIR" "$SYMBOL"

log ""
log "── DuckDB / Parquet / Polars (same CSV) ──"
python3 bench/compare_duckdb_parquet.py "$CSV_PATH"

log ""
log "done."
