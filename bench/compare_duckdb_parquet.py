#!/usr/bin/env python3
"""
DuckDB / Parquet / Polars comparison for tickdb's benchmark suite (§5).

Runs the same three query shapes as bench/run_tickdb_bench (OHLCV 1-min
bars, VWAP over the full range, top-10 by volume) against the identical
synthetic dataset, measuring in-process query latency the same way — no
network, no serialization overhead on either side, so the numbers are
directly comparable.

Usage:
    python3 bench/compare_duckdb_parquet.py /path/to/binance_aggtrades.csv
"""
import sys
import time
import os

import duckdb
import polars as pl


def human_bytes(n):
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def main():
    if len(sys.argv) < 2:
        print("usage: compare_duckdb_parquet.py <csv_path>", file=sys.stderr)
        sys.exit(1)

    csv_path = sys.argv[1]
    n_rows = sum(1 for _ in open(csv_path)) - 1  # minus header
    print(f"# DuckDB / Parquet / Polars comparison")
    print(f"csv: {csv_path}")
    print(f"rows: {n_rows}")
    print()

    # ── DuckDB: load CSV, write Parquet, measure both ──────────────────────
    con = duckdb.connect(":memory:")

    t0 = time.perf_counter()
    con.execute(f"""
        CREATE TABLE trades AS
        SELECT
            agg_trade_id AS trade_id,
            price,
            qty AS size,
            transact_time * 1000000 AS ts_ns,  -- ms -> ns, matching tickdb's units
            is_buyer_maker
        FROM read_csv_auto('{csv_path}')
    """)
    load_elapsed = time.perf_counter() - t0
    print(f"duckdb_csv_load: {load_elapsed:.2f}s ({n_rows / load_elapsed:.0f} rows/sec)")

    parquet_path = csv_path.replace(".csv", ".parquet")
    t0 = time.perf_counter()
    con.execute(f"COPY trades TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    parquet_write_elapsed = time.perf_counter() - t0
    parquet_size = os.path.getsize(parquet_path)
    bytes_per_trade = parquet_size / n_rows
    print(f"duckdb_parquet_write: {parquet_write_elapsed:.2f}s")
    print(f"parquet_on_disk_bytes: {parquet_size} ({human_bytes(parquet_size)})")
    print(f"parquet_bytes_per_trade: {bytes_per_trade:.2f}")
    print()

    # Re-open fresh against the Parquet file for query timing, so DuckDB's
    # query path (not an in-memory table already warm from the load above)
    # is what gets measured — the fairer comparison to tickdb's on-disk
    # mmap'd segment reads.
    con2 = duckdb.connect(":memory:")
    con2.execute(f"CREATE VIEW trades AS SELECT * FROM read_parquet('{parquet_path}')")

    # ── Query: OHLCV 1-minute bars ──────────────────────────────────────────
    t0 = time.perf_counter()
    bars = con2.execute("""
        SELECT
            time_bucket(INTERVAL '1 minute', to_timestamp(ts_ns / 1e9)) AS bucket,
            first(price ORDER BY ts_ns) AS open,
            max(price) AS high,
            min(price) AS low,
            last(price ORDER BY ts_ns) AS close,
            sum(size) AS volume,
            count(*) AS trade_count
        FROM trades
        GROUP BY bucket
        ORDER BY bucket
    """).fetchall()
    elapsed = time.perf_counter() - t0
    print(f"duckdb_query_ohlcv_1min: {elapsed:.4f}s ({len(bars)} bars, {n_rows / elapsed:.0f} rows/sec)")

    # ── Query: VWAP over full range ─────────────────────────────────────────
    t0 = time.perf_counter()
    vwap = con2.execute("SELECT sum(price * size) / sum(size) FROM trades").fetchone()[0]
    elapsed = time.perf_counter() - t0
    print(f"duckdb_query_vwap_full_range: {elapsed:.4f}s (result={vwap}, {n_rows / elapsed:.0f} rows/sec)")

    # ── Query: Top-10 by volume (size) ──────────────────────────────────────
    t0 = time.perf_counter()
    top = con2.execute("SELECT trade_id, size FROM trades ORDER BY size DESC LIMIT 10").fetchall()
    elapsed = time.perf_counter() - t0
    print(f"duckdb_query_top10_by_volume: {elapsed:.4f}s ({len(top)} results, {n_rows / elapsed:.0f} rows/sec)")
    print()

    # ── Polars (lazy, scanning the same Parquet file) ───────────────────────
    t0 = time.perf_counter()
    lf = pl.scan_parquet(parquet_path)
    vwap_pl = (
        lf.select(
            (pl.col("price") * pl.col("size")).sum().alias("pv"),
            pl.col("size").sum().alias("v"),
        )
        .collect()
    )
    vwap_result = vwap_pl["pv"][0] / vwap_pl["v"][0]
    elapsed = time.perf_counter() - t0
    print(f"polars_query_vwap_full_range: {elapsed:.4f}s (result={vwap_result}, {n_rows / elapsed:.0f} rows/sec)")

    os.remove(parquet_path)


if __name__ == "__main__":
    main()
