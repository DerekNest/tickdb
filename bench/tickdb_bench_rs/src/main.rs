use std::time::Instant;

use format::dz::Decimal64;
use format::types::{Side, Trade};
use query::builder::{Db, Interval};
use storage::{Engine, FsyncPolicy};

fn main() {
    let n: usize = std::env::args().nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);

    let data_dir = std::env::temp_dir().join(format!("tickdb_bench_{}", std::process::id()));
    std::fs::create_dir_all(&data_dir).unwrap();

    println!("# tickdb in-process benchmark");
    println!("rows: {n}");
    println!("data_dir: {}", data_dir.display());
    println!();

    // ── Generate synthetic trades (same model as bench/gen_synthetic.py's "tight" mode) ──
    let base_ts = 1_705_276_800_000_000_000i64;
    let mut price: i64 = 4_325_100;
    let mut rng_state: u64 = 0x2545F4914F6CDD1D;
    let mut next_rand = || {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    let gen_start = Instant::now();
    let mut trades = Vec::with_capacity(n);
    let mut ts = base_ts;
    for i in 0..n {
        ts += 950_000_000 + (next_rand() % 100_000_000) as i64; // ~1s apart
        let tick = (next_rand() % 4) as i64 - 1; // -1, 0, 1, 2 -ish
        price += tick.clamp(-1, 1);
        let size = 1 + (next_rand() % 500);
        let side = if next_rand() % 2 == 0 { Side::Buy } else { Side::Sell };
        trades.push(Trade {
            symbol: 0,
            ts,
            price: Decimal64 { mantissa: price, exponent: -2 },
            size,
            side,
            trade_id: i as u64,
        });
    }
    println!("generation: {:.2}s", gen_start.elapsed().as_secs_f64());

    // ── Ingest ──
    let mut engine = Engine::open(&data_dir, FsyncPolicy::Never).unwrap();
    let symbol_id = engine.intern_symbol("BTCUSDT").unwrap();
    for t in trades.iter_mut() {
        t.symbol = symbol_id;
    }

    let ingest_start = Instant::now();
    const BATCH: usize = 4096;
    for chunk in trades.chunks(BATCH) {
        engine.ingest(chunk).unwrap();
    }
    let ingest_elapsed = ingest_start.elapsed();
    let ingest_rows_per_sec = n as f64 / ingest_elapsed.as_secs_f64();
    println!("ingest: {:.2}s ({:.0} rows/sec)", ingest_elapsed.as_secs_f64(), ingest_rows_per_sec);

    let flush_start = Instant::now();
    engine.flush().unwrap();
    println!("flush: {:.2}s", flush_start.elapsed().as_secs_f64());

    // ── On-disk size ──
    let mut total_bytes: u64 = 0;
    for seg in &engine.manifest().segments {
        let path = data_dir.join(&seg.path);
        if let Ok(meta) = std::fs::metadata(&path) {
            total_bytes += meta.len();
        }
    }
    let bytes_per_trade = total_bytes as f64 / n as f64;
    println!("on_disk_bytes: {total_bytes}");
    println!("bytes_per_trade: {bytes_per_trade:.2}");
    println!();

    // ── Query: OHLCV bars (1-minute) ──
    let range_start = base_ts;
    let range_end = trades.last().unwrap().ts + 1;
    let db = Db::new(&engine);

    let ohlcv_start = Instant::now();
    let bars = db.scan("BTCUSDT")
        .range(range_start, range_end)
        .bars(Interval::Min1)
        .collect()
        .unwrap();
    let ohlcv_elapsed = ohlcv_start.elapsed();
    println!("query_ohlcv_1min: {:.4}s ({} bars, {:.0} rows/sec)",
        ohlcv_elapsed.as_secs_f64(), bars.len(), n as f64 / ohlcv_elapsed.as_secs_f64());

    // ── Query: VWAP over full range ──
    let vwap_start = Instant::now();
    let vwap = db.scan("BTCUSDT").range(range_start, range_end).vwap().unwrap();
    let vwap_elapsed = vwap_start.elapsed();
    println!("query_vwap_full_range: {:.4}s (result={:?}, {:.0} rows/sec)",
        vwap_elapsed.as_secs_f64(), vwap, n as f64 / vwap_elapsed.as_secs_f64());

    // ── Query: Top-N by volume ──
    let topn_start = Instant::now();
    let top = db.scan("BTCUSDT").range(range_start, range_end).top_n(10).unwrap();
    let topn_elapsed = topn_start.elapsed();
    println!("query_top10_by_volume: {:.4}s ({} results, {:.0} rows/sec)",
        topn_elapsed.as_secs_f64(), top.len(), n as f64 / topn_elapsed.as_secs_f64());

    // Cleanup.
    let _ = std::fs::remove_dir_all(&data_dir);
}
