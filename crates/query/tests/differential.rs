/// Differential tests (§3.4).
///
/// Every operator's output equals a naive `Vec<Trade>` reference
/// implementation on randomly generated data.  This is the correctness
/// story stated in the README.
///
/// The test strategy:
///   1. Generate random trades using `rand::SmallRng` seeded deterministically.
///   2. Ingest into a real `Engine` (in-memory, no flush so it's fast).
///   3. Run the optimised operator (scan + kernel).
///   4. Run the naive reference over the same raw Vec.
///   5. Assert exact equality (or float equality within epsilon for prices).
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use rand::{rngs::SmallRng, Rng, SeedableRng};

use format::dz::Decimal64;
use format::types::{Side, SymbolId, Trade};
use query::agg::{ohlcv_ref, top_n_ref, vwap_ref};
use query::asof::{asof_join, asof_join_ref, FlatTrade};
use query::builder::{Db, Interval};
use storage::{Engine, FsyncPolicy};

static DIR_CTR: AtomicU64 = AtomicU64::new(0);

// ── Helpers ───────────────────────────────────────────────────────────────────

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let n = DIR_CTR.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("tickdb_diff_test_{n}"));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Generates `n` random trades for `symbol_id` with ts in [base_ts, base_ts + n * step].
fn gen_trades(
    rng: &mut SmallRng,
    symbol_id: SymbolId,
    n: usize,
    base_ts: i64,
    step: i64,
) -> Vec<Trade> {
    let mut trades = Vec::with_capacity(n);
    let mut ts = base_ts;
    for i in 0..n {
        ts += rng.gen_range(1..=step * 2); // jitter around step
        trades.push(Trade {
            symbol: symbol_id,
            ts,
            price: Decimal64 {
                mantissa: 5_000_000 + rng.gen_range(-50i64..=50),
                exponent: -2,
            },
            size: rng.gen_range(1u64..=1000),
            side: if rng.gen_bool(0.5) {
                Side::Buy
            } else {
                Side::Sell
            },
            trade_id: i as u64,
        });
    }
    trades.sort_by_key(|t| (t.ts, t.trade_id));
    trades
}

fn open_engine(dir: &TempDir) -> Engine {
    Engine::open(dir.path(), FsyncPolicy::Never).unwrap()
}

const EPSILON: f64 = 1e-6;

fn approx_eq(a: f64, b: f64) -> bool {
    if a == b {
        return true;
    }
    (a - b).abs() / a.abs().max(b.abs()).max(1.0) < EPSILON
}

// ── OHLCV differential tests ──────────────────────────────────────────────────

/// Runs OHLCV on `n_trades` ingested trades and compares with the reference.
fn diff_ohlcv(seed: u64, n_trades: usize, interval_ns: i64) {
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("DIFF_SYM").unwrap();

    let mut rng = SmallRng::seed_from_u64(seed);
    let base_ts = 1_705_276_800_000_000_000i64;
    let step = interval_ns / 10; // ~10 trades per bucket on average
    let trades = gen_trades(&mut rng, sym, n_trades, base_ts, step);

    engine.ingest(&trades).unwrap();

    let range_start = base_ts;
    let range_end = trades.last().map(|t| t.ts + 1).unwrap_or(base_ts + 1);

    // Optimised path.
    let db = Db::new(&engine);
    let fast_bars = db
        .scan("DIFF_SYM")
        .range(range_start, range_end)
        .bars(Interval::Custom(interval_ns))
        .collect()
        .unwrap();

    // Reference path: flat Vec of (ts, price_f64, size).
    let ref_input: Vec<(i64, f64, u64)> = trades
        .iter()
        .map(|t| (t.ts, t.price.mantissa as f64 * 0.01, t.size))
        .collect();
    let ref_bars = ohlcv_ref(&ref_input, range_start, range_end, interval_ns);

    assert_eq!(
        fast_bars.len(),
        ref_bars.len(),
        "seed={seed} n={n_trades}: bar count mismatch"
    );

    for (i, (f, r)) in fast_bars.iter().zip(ref_bars.iter()).enumerate() {
        assert_eq!(f.ts, r.ts, "seed={seed} bar[{i}] ts mismatch");
        assert!(
            approx_eq(f.open, r.open),
            "seed={seed} bar[{i}] open  {:.4} ≠ {:.4}",
            f.open,
            r.open
        );
        assert!(
            approx_eq(f.high, r.high),
            "seed={seed} bar[{i}] high  {:.4} ≠ {:.4}",
            f.high,
            r.high
        );
        assert!(
            approx_eq(f.low, r.low),
            "seed={seed} bar[{i}] low   {:.4} ≠ {:.4}",
            f.low,
            r.low
        );
        assert!(
            approx_eq(f.close, r.close),
            "seed={seed} bar[{i}] close {:.4} ≠ {:.4}",
            f.close,
            r.close
        );
        assert_eq!(f.volume, r.volume, "seed={seed} bar[{i}] volume");
        assert_eq!(
            f.trade_count, r.trade_count,
            "seed={seed} bar[{i}] trade_count"
        );
        assert!(
            approx_eq(f.vwap(), r.vwap()),
            "seed={seed} bar[{i}] vwap {:.4} ≠ {:.4}",
            f.vwap(),
            r.vwap()
        );
    }
}

#[test]
fn ohlcv_differential_50_seeds() {
    for seed in 0..50 {
        diff_ohlcv(seed, 1000, 60_000_000_000); // 1-min bars, 1000 trades
    }
}

#[test]
fn ohlcv_differential_large() {
    diff_ohlcv(99, 10_000, 60_000_000_000);
}

#[test]
fn ohlcv_single_bucket() {
    // Degenerate case: entire range fits in one bucket.
    diff_ohlcv(7, 500, 3_600_000_000_000); // 1-hour bucket
}

#[test]
fn ohlcv_empty_range_returns_empty() {
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let _sym = engine.intern_symbol("EMPTY").unwrap();
    let db = Db::new(&engine);
    let bars = db
        .scan("EMPTY")
        .range(0, 1000)
        .bars(Interval::Min1)
        .collect()
        .unwrap();
    assert!(bars.is_empty());
}

// ── VWAP differential tests ───────────────────────────────────────────────────

fn diff_vwap(seed: u64, n_trades: usize) {
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("VWAP_SYM").unwrap();

    let mut rng = SmallRng::seed_from_u64(seed);
    let base_ts = 1_705_276_800_000_000_000i64;
    let trades = gen_trades(&mut rng, sym, n_trades, base_ts, 100_000);

    engine.ingest(&trades).unwrap();

    let ts_start = base_ts;
    let ts_end = trades.last().map(|t| t.ts).unwrap_or(base_ts);

    let fast = Db::new(&engine)
        .scan("VWAP_SYM")
        .range(ts_start, ts_end)
        .vwap()
        .unwrap();

    let ref_input: Vec<(i64, f64, u64)> = trades
        .iter()
        .map(|t| (t.ts, t.price.mantissa as f64 * 0.01, t.size))
        .collect();
    let slow = vwap_ref(&ref_input, ts_start, ts_end);

    match (fast, slow) {
        (Some(f), Some(s)) => assert!(approx_eq(f, s), "seed={seed} vwap {f:.6} ≠ {s:.6}"),
        (None, None) => {}
        (f, s) => panic!("seed={seed} vwap mismatch: {f:?} vs {s:?}"),
    }
}

#[test]
fn vwap_differential_50_seeds() {
    for seed in 0..50 {
        diff_vwap(seed, 500);
    }
}

#[test]
fn vwap_empty_returns_none() {
    let dir = TempDir::new();
    let engine = open_engine(&dir);
    let _ = engine.intern_symbol_ro("NODATA"); // ensure symbol doesn't exist
                                               // scan on unknown symbol returns empty → vwap = None
    let dir2 = TempDir::new();
    let mut e2 = open_engine(&dir2);
    let _sym = e2.intern_symbol("NODATA").unwrap();
    let db = Db::new(&e2);
    let v = db.scan("NODATA").range(0, 1000).vwap().unwrap();
    assert!(v.is_none());
}

// ── Top-N differential tests ──────────────────────────────────────────────────

fn diff_top_n(seed: u64, n_trades: usize, top_n: usize) {
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("TOPN").unwrap();

    let mut rng = SmallRng::seed_from_u64(seed);
    let base_ts = 1_705_276_800_000_000_000i64;
    let trades = gen_trades(&mut rng, sym, n_trades, base_ts, 100_000);

    engine.ingest(&trades).unwrap();

    let ts_start = base_ts;
    let ts_end = trades.last().map(|t| t.ts).unwrap_or(base_ts);

    let fast = Db::new(&engine)
        .scan("TOPN")
        .range(ts_start, ts_end)
        .top_n(top_n)
        .unwrap();

    let ref_input: Vec<(i64, u64, u64)> =
        trades.iter().map(|t| (t.ts, t.size, t.trade_id)).collect();
    let slow = top_n_ref(&ref_input, ts_start, ts_end, top_n);

    assert_eq!(fast.len(), slow.len(), "seed={seed} top_n count");
    for (i, (f, s)) in fast.iter().zip(slow.iter()).enumerate() {
        assert_eq!(f.volume, s.volume, "seed={seed} top_n[{i}] volume");
        assert_eq!(f.trade_id, s.trade_id, "seed={seed} top_n[{i}] trade_id");
    }
}

#[test]
fn top_n_differential_50_seeds() {
    for seed in 0..50 {
        diff_top_n(seed, 500, 10);
    }
}

#[test]
fn top_n_fewer_than_n_trades() {
    // When there are fewer trades than N, return all of them.
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("SMALL").unwrap();
    let mut rng = SmallRng::seed_from_u64(0);
    let trades = gen_trades(&mut rng, sym, 5, 0, 100_000);
    engine.ingest(&trades).unwrap();
    let result = Db::new(&engine)
        .scan("SMALL")
        .range(0, i64::MAX)
        .top_n(100)
        .unwrap();
    assert_eq!(result.len(), 5);
}

// ── As-of join differential tests ─────────────────────────────────────────────

fn diff_asof(seed: u64, n_left: usize, n_right: usize, tolerance_ns: i64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let base_ts = 1_705_276_800_000_000_000i64;

    let mut make_flat = |n: usize, step: i64| -> Vec<FlatTrade> {
        let mut ts = base_ts;
        (0..n)
            .map(|i| {
                ts += rng.gen_range(1..=step * 2);
                FlatTrade {
                    ts,
                    price: 50000.0 + rng.gen_range(-10.0..10.0),
                    size: 1,
                    trade_id: i as u64,
                }
            })
            .collect()
    };

    let left = make_flat(n_left, 100_000);
    let right = make_flat(n_right, 150_000);

    // Ensure both are ts-sorted (gen_trades already sorts).
    let mut left_s = left.clone();
    left_s.sort_by_key(|r| r.ts);
    let mut right_s = right.clone();
    right_s.sort_by_key(|r| r.ts);

    let fast = asof_join(&left_s, &right_s, tolerance_ns);
    let slow = asof_join_ref(&left_s, &right_s, tolerance_ns);

    assert_eq!(fast.len(), slow.len(), "seed={seed} asof row count");
    for (i, (f, s)) in fast.iter().zip(slow.iter()).enumerate() {
        assert_eq!(f.right_ts, s.right_ts, "seed={seed} asof[{i}] right_ts");
        assert_eq!(
            f.right_trade_id, s.right_trade_id,
            "seed={seed} asof[{i}] right_trade_id"
        );
        assert_eq!(f.lag_ns, s.lag_ns, "seed={seed} asof[{i}] lag");
    }
}

#[test]
fn asof_differential_50_seeds() {
    for seed in 0..50 {
        diff_asof(seed, 200, 300, 500_000_000); // 500ms tolerance
    }
}

#[test]
fn asof_zero_tolerance_exact_only() {
    diff_asof(42, 100, 100, 0);
}

#[test]
fn asof_infinite_tolerance_always_matches() {
    // With infinite tolerance, every left row with any prior right row matches.
    let base = 1_000_000_000i64;
    let left: Vec<FlatTrade> = (10..20)
        .map(|i| FlatTrade {
            ts: base + i * 100,
            price: 1.0,
            size: 1,
            trade_id: i as u64,
        })
        .collect();
    let right: Vec<FlatTrade> = (0..10)
        .map(|i| FlatTrade {
            ts: base + i * 100,
            price: 1.0,
            size: 1,
            trade_id: 100 + i as u64,
        })
        .collect();

    let result = asof_join(&left, &right, i64::MAX);
    for row in &result {
        assert!(row.right_ts.is_some(), "expected match for all left rows");
    }
}

// ── Robustness: broad/unbounded ranges must not attempt a catastrophic allocation ──
//
// Found via the benchmark suite (§5): querying `.range(i64::MIN, i64::MAX)`
// with a small interval used to try to pre-allocate a bucket array sized
// by `(range_end - range_start) / interval_ns` — quintillions of slots,
// aborting the process with an allocation failure. `ohlcv`/`ohlcv_ref`
// were rewritten to accumulate into a sparse BTreeMap keyed by bucket
// timestamp instead, so memory scales with the data, not the query's
// nominal span. This test is the regression guard for that fix.

#[test]
fn ohlcv_unbounded_range_does_not_allocate_catastrophically() {
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("HUGE_RANGE").unwrap();

    let mut rng = SmallRng::seed_from_u64(1);
    let base_ts = 1_705_276_800_000_000_000i64;
    let trades = gen_trades(&mut rng, sym, 500, base_ts, 100_000);
    engine.ingest(&trades).unwrap();

    // The exact case that used to crash: effectively "unbounded" range
    // with a small (1-second) interval.
    let db = Db::new(&engine);
    let bars = db
        .scan("HUGE_RANGE")
        .range(i64::MIN, i64::MAX)
        .bars(Interval::Sec1)
        .collect()
        .unwrap();

    // Only buckets touched by actual data should exist — nowhere near the
    // (i64::MAX - i64::MIN) / 1s buckets the old dense-array version would
    // have tried to allocate.
    assert!(
        bars.len() <= 500,
        "expected at most one bucket per trade, got {}",
        bars.len()
    );
    assert!(!bars.is_empty());

    // And it must still agree with the naive reference on this pathological range.
    let ref_input: Vec<(i64, f64, u64)> = trades
        .iter()
        .map(|t| (t.ts, t.price.mantissa as f64 * 0.01, t.size))
        .collect();
    let ref_bars = ohlcv_ref(&ref_input, i64::MIN, i64::MAX, 1_000_000_000);
    assert_eq!(bars.len(), ref_bars.len());
}

#[test]
fn ohlcv_sparse_data_over_huge_interval_span() {
    // A different flavor of the same class of bug: a huge interval (years)
    // over a huge range, which the old dense array would ALSO have
    // struggled with if interval were small relative to range — here we
    // instead confirm sparse accumulation handles a wide relative span
    // (many buckets nominally possible, very few actually populated)
    // without issue, and that results are correct, not just non-crashing.
    let dir = TempDir::new();
    let mut engine = open_engine(&dir);
    let sym = engine.intern_symbol("SPARSE").unwrap();

    let mut rng = SmallRng::seed_from_u64(2);
    let base_ts = 1_705_276_800_000_000_000i64;
    // Trades spread across a wide span with large gaps.
    let trades = gen_trades(
        &mut rng,
        sym,
        20,
        base_ts,
        30 * 24 * 3600 * 1_000_000_000i64 / 20,
    );
    engine.ingest(&trades).unwrap();

    let db = Db::new(&engine);
    let bars = db
        .scan("SPARSE")
        .range(i64::MIN, i64::MAX)
        .bars(Interval::Min1) // fine-grained bucket over a month-wide span
        .collect()
        .unwrap();

    assert!(
        bars.len() <= 20,
        "at most one bucket per trade for widely-spaced data"
    );
    let total_trade_count: u64 = bars.iter().map(|b| b.trade_count).sum();
    assert_eq!(
        total_trade_count, 20,
        "every trade must be accounted for exactly once"
    );
}
