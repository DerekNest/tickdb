/// Fluent query builder (§3.3).
///
/// ```ignore
/// let bars = db.scan("BTCUSDT")
///              .range(t0, t1)
///              .bars(Interval::Min1)
///              .collect()?;
///
/// let v = db.scan("BTCUSDT").range(t0, t1).vwap()?;
///
/// let joined = db.asof("ETHUSDT", "BTCUSDT")
///                .range(t0, t1)
///                .tolerance_ms(100)
///                .collect()?;
/// ```
use format::types::SymbolId;
use storage::Engine;

use crate::agg::{ohlcv, top_n_by_volume, vwap, Bar, TopNEntry};
use crate::asof::{asof_join, AsOfRow, FlatTrade};
use crate::error::Error;
use crate::scan::{collect_all, Scan};

// ── Interval ──────────────────────────────────────────────────────────────────

/// Canonical bar intervals in nanoseconds.
pub enum Interval {
    Sec1,
    Sec10,
    Min1,
    Min5,
    Min15,
    Hour1,
    Custom(i64),
}

impl Interval {
    pub fn as_ns(&self) -> i64 {
        match self {
            Interval::Sec1 => 1_000_000_000,
            Interval::Sec10 => 10_000_000_000,
            Interval::Min1 => 60_000_000_000,
            Interval::Min5 => 5 * 60_000_000_000,
            Interval::Min15 => 15 * 60_000_000_000,
            Interval::Hour1 => 3600 * 1_000_000_000,
            Interval::Custom(ns) => *ns,
        }
    }
}

// ── ScanBuilder ───────────────────────────────────────────────────────────────

/// Entry point: resolves symbol → id and chains to a `RangeBuilder`.
pub struct ScanBuilder<'e> {
    engine: &'e Engine,
    symbol: String,
}

impl<'e> ScanBuilder<'e> {
    pub fn new(engine: &'e Engine, symbol: impl Into<String>) -> Self {
        ScanBuilder {
            engine,
            symbol: symbol.into(),
        }
    }

    /// Sets the time range and returns a `RangeBuilder`.
    pub fn range(self, ts_start: i64, ts_end: i64) -> RangeBuilder<'e> {
        RangeBuilder {
            engine: self.engine,
            symbol: self.symbol,
            ts_start,
            ts_end,
        }
    }
}

// ── RangeBuilder ──────────────────────────────────────────────────────────────

pub struct RangeBuilder<'e> {
    engine: &'e Engine,
    symbol: String,
    ts_start: i64,
    ts_end: i64,
}

impl<'e> RangeBuilder<'e> {
    fn resolve_symbol(&self) -> Result<SymbolId, Error> {
        self.engine
            .intern_symbol_ro(&self.symbol)
            .ok_or_else(|| Error::SymbolNotFound(self.symbol.clone()))
    }

    /// Returns OHLCV bars for the given interval.
    pub fn bars(self, interval: Interval) -> BarsBuilder<'e> {
        BarsBuilder {
            range: self,
            interval_ns: interval.as_ns(),
        }
    }

    /// Computes VWAP over the full range.
    pub fn vwap(self) -> Result<Option<f64>, Error> {
        let symbol_id = self.resolve_symbol()?;
        let batches = Scan::new(self.engine, symbol_id, self.ts_start, self.ts_end);
        vwap(batches, symbol_id, self.ts_start, self.ts_end)
    }

    /// Returns the top N trades by volume.
    pub fn top_n(self, n: usize) -> Result<Vec<TopNEntry>, Error> {
        let symbol_id = self.resolve_symbol()?;
        let batches = Scan::new(self.engine, symbol_id, self.ts_start, self.ts_end);
        top_n_by_volume(batches, symbol_id, self.ts_start, self.ts_end, n)
    }

    /// Collects all trades as a flat sorted vec of `FlatTrade`.
    pub fn collect_trades(self) -> Result<Vec<FlatTrade>, Error> {
        let symbol_id = self.resolve_symbol()?;
        let raw = collect_all(self.engine, symbol_id, self.ts_start, self.ts_end)?;
        Ok(raw
            .into_iter()
            .map(|row| FlatTrade {
                ts: row.ts,
                price: row.price_mantissa as f64 * 10f64.powi(row.exponent as i32),
                size: row.size,
                trade_id: row.trade_id,
            })
            .collect())
    }
}

// ── BarsBuilder ───────────────────────────────────────────────────────────────

pub struct BarsBuilder<'e> {
    range: RangeBuilder<'e>,
    interval_ns: i64,
}

impl<'e> BarsBuilder<'e> {
    /// Executes the OHLCV resample and returns the bars.
    pub fn collect(self) -> Result<Vec<Bar>, Error> {
        let symbol_id = self.range.resolve_symbol()?;
        let batches = Scan::new(
            self.range.engine,
            symbol_id,
            self.range.ts_start,
            self.range.ts_end,
        );
        ohlcv(
            batches,
            symbol_id,
            self.range.ts_start,
            self.range.ts_end,
            self.interval_ns,
        )
    }
}

// ── AsOfBuilder ───────────────────────────────────────────────────────────────

/// Entry point for as-of join queries.
pub struct AsOfBuilder<'e> {
    engine: &'e Engine,
    left_symbol: String,
    right_symbol: String,
    ts_start: i64,
    ts_end: i64,
    tolerance_ns: i64,
}

impl<'e> AsOfBuilder<'e> {
    pub fn new(
        engine: &'e Engine,
        left_symbol: impl Into<String>,
        right_symbol: impl Into<String>,
    ) -> Self {
        AsOfBuilder {
            engine,
            left_symbol: left_symbol.into(),
            right_symbol: right_symbol.into(),
            ts_start: i64::MIN,
            ts_end: i64::MAX,
            tolerance_ns: i64::MAX,
        }
    }

    pub fn range(mut self, ts_start: i64, ts_end: i64) -> Self {
        self.ts_start = ts_start;
        self.ts_end = ts_end;
        self
    }

    pub fn tolerance_ns(mut self, ns: i64) -> Self {
        self.tolerance_ns = ns;
        self
    }

    pub fn tolerance_ms(mut self, ms: i64) -> Self {
        self.tolerance_ns = ms * 1_000_000;
        self
    }

    /// Executes the as-of join and returns one row per left trade.
    pub fn collect(self) -> Result<Vec<AsOfRow>, Error> {
        let left_id = self
            .engine
            .intern_symbol_ro(&self.left_symbol)
            .ok_or_else(|| Error::SymbolNotFound(self.left_symbol.clone()))?;
        let right_id = self
            .engine
            .intern_symbol_ro(&self.right_symbol)
            .ok_or_else(|| Error::SymbolNotFound(self.right_symbol.clone()))?;

        if self.tolerance_ns < 0 {
            return Err(Error::InvalidTolerance(self.tolerance_ns));
        }

        // Collect both streams sorted.
        let left_raw = collect_all(self.engine, left_id, self.ts_start, self.ts_end)?;
        let right_raw = collect_all(self.engine, right_id, self.ts_start, self.ts_end)?;

        let left: Vec<FlatTrade> = left_raw
            .into_iter()
            .map(|row| FlatTrade {
                ts: row.ts,
                price: row.price_mantissa as f64 * 10f64.powi(row.exponent as i32),
                size: row.size,
                trade_id: row.trade_id,
            })
            .collect();

        let right: Vec<FlatTrade> = right_raw
            .into_iter()
            .map(|row| FlatTrade {
                ts: row.ts,
                price: row.price_mantissa as f64 * 10f64.powi(row.exponent as i32),
                size: row.size,
                trade_id: row.trade_id,
            })
            .collect();

        Ok(asof_join(&left, &right, self.tolerance_ns))
    }
}

// ── Db entry point ────────────────────────────────────────────────────────────

/// Thin wrapper around `Engine` that provides the fluent query API.
///
/// ```ignore
/// let db = Db::new(&engine);
/// let bars = db.scan("BTCUSDT").range(t0, t1).bars(Interval::Min1).collect()?;
/// ```
pub struct Db<'e> {
    engine: &'e Engine,
}

impl<'e> Db<'e> {
    pub fn new(engine: &'e Engine) -> Self {
        Db { engine }
    }

    pub fn scan(&self, symbol: impl Into<String>) -> ScanBuilder<'_> {
        ScanBuilder::new(self.engine, symbol)
    }

    pub fn asof(
        &self,
        left_symbol: impl Into<String>,
        right_symbol: impl Into<String>,
    ) -> AsOfBuilder<'_> {
        AsOfBuilder::new(self.engine, left_symbol, right_symbol)
    }
}
