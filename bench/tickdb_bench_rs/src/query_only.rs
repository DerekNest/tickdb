use std::time::Instant;

use query::builder::{Db, Interval};
use storage::{Engine, FsyncPolicy};

/// Runs OHLCV/VWAP/top-N queries against an ALREADY-INGESTED tickdb data
/// directory (no re-ingest here) — for a same-dataset comparison against
/// DuckDB/Parquet, this must open the exact directory the `tickdb` CLI
/// wrote from the same CSV those tools read.
fn main() {
    let data_dir = std::env::args().nth(1)
        .unwrap_or_else(|| "/tmp/tickdb-bench-real".to_string());
    let symbol = std::env::args().nth(2).unwrap_or_else(|| "BTCUSDT".to_string());

    let engine = Engine::open(&data_dir, FsyncPolicy::Never).unwrap();
    let db = Db::new(&engine);

    let total_rows: u64 = engine.manifest().segments.iter().map(|s| s.row_count).sum();
    let total_bytes: u64 = engine.manifest().segments.iter()
        .map(|s| std::fs::metadata(std::path::Path::new(&data_dir).join(&s.path)).map(|m| m.len()).unwrap_or(0))
        .sum();

    println!("# tickdb query-only benchmark (same on-disk data as the DuckDB comparison)");
    println!("data_dir: {data_dir}");
    println!("rows: {total_rows}");
    println!("on_disk_bytes: {total_bytes}");
    println!("bytes_per_trade: {:.2}", total_bytes as f64 / total_rows as f64);
    println!();

    let range_start = i64::MIN;
    let range_end = i64::MAX;

    // ── OHLCV 1-minute bars ──
    let t0 = Instant::now();
    let bars = db.scan(&symbol).range(range_start, range_end).bars(Interval::Min1).collect().unwrap();
    let elapsed = t0.elapsed();
    println!("query_ohlcv_1min: {:.4}s ({} bars, {:.0} rows/sec)",
        elapsed.as_secs_f64(), bars.len(), total_rows as f64 / elapsed.as_secs_f64());

    // ── VWAP over full range ──
    let t0 = Instant::now();
    let vwap = db.scan(&symbol).range(range_start, range_end).vwap().unwrap();
    let elapsed = t0.elapsed();
    println!("query_vwap_full_range: {:.4}s (result={:?}, {:.0} rows/sec)",
        elapsed.as_secs_f64(), vwap, total_rows as f64 / elapsed.as_secs_f64());

    // ── Top-10 by volume ──
    let t0 = Instant::now();
    let top = db.scan(&symbol).range(range_start, range_end).top_n(10).unwrap();
    let elapsed = t0.elapsed();
    println!("query_top10_by_volume: {:.4}s ({} results, {:.0} rows/sec)",
        elapsed.as_secs_f64(), top.len(), total_rows as f64 / elapsed.as_secs_f64());
}
