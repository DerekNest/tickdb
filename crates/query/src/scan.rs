use crate::error::Error;
use format::types::SymbolId;
/// Range scan operator.
///
/// Yields `ColumnBatch`es for `(symbol_id, ts_start..=ts_end)`.
/// Sources: all open segments (zone-map pruned) merged with the memtable.
///
/// # Output ordering
///
/// Batches from segments are already sorted by (ts, trade_id) within each
/// block.  The memtable batch is sorted on the fly inside `Engine::scan`.
/// Because segments are written in ts-order and the memtable only contains
/// data newer than the newest flushed segment (in the common case), the
/// output is approximately ts-sorted across batches.
///
/// The OHLCV and as-of operators rely on ts-sorted input.  `SortedScan`
/// wraps this scan with a merge-sort when strict ordering is required
/// (e.g., when the query range straddles a flush boundary).
use storage::{ColumnBatch, Engine};

/// A lazy iterator over `ColumnBatch`es from the engine.
pub struct Scan<'e> {
    inner: Box<dyn Iterator<Item = Result<ColumnBatch, storage::Error>> + 'e>,
}

impl<'e> Scan<'e> {
    pub fn new(engine: &'e Engine, symbol_id: SymbolId, ts_start: i64, ts_end: i64) -> Self {
        Scan {
            inner: Box::new(engine.scan(symbol_id, ts_start, ts_end)),
        }
    }
}

impl<'e> Iterator for Scan<'e> {
    type Item = Result<ColumnBatch, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.map_err(Error::Storage))
    }
}

/// Range filter applied within a batch.
///
/// Chops batches that straddle `ts_start` or `ts_end` so callers see only
/// rows strictly within the requested range.  Zone-map pruning at the block
/// level already eliminates most batches; this handles the edge blocks.
pub fn filter_range(batch: ColumnBatch, ts_start: i64, ts_end: i64) -> Option<ColumnBatch> {
    if batch.ts.is_empty() {
        return None;
    }

    // Find the subrange [lo, hi) within the batch that falls in [ts_start, ts_end].
    // ts is sorted ascending within a batch.
    let lo = batch.ts.partition_point(|&t| t < ts_start);
    let hi = batch.ts.partition_point(|&t| t <= ts_end);

    if lo >= hi {
        return None;
    }

    if lo == 0 && hi == batch.ts.len() {
        // Entire batch is within range — return as-is (no copy).
        return Some(batch);
    }

    // Slice the batch to [lo, hi).
    Some(ColumnBatch {
        symbol_id: batch.symbol_id,
        ts: batch.ts[lo..hi].to_vec(),
        price_mantissa: batch.price_mantissa[lo..hi].to_vec(),
        exponent: batch.exponent,
        size: batch.size[lo..hi].to_vec(),
        side: batch.side[lo..hi].to_vec(),
        trade_id: batch.trade_id[lo..hi].to_vec(),
    })
}

/// Collects all batches from a scan into a single flat vec of trades.
///
/// Used by the reference implementations in differential tests — not on the
/// hot path.
/// a single flattened trade row, as produced by `collect_all` for use in
/// differential-test reference implementations.
pub struct ScanRow {
    pub ts: i64,
    pub price_mantissa: i64,
    pub exponent: i8,
    pub size: u64,
    pub side: u8,
    pub trade_id: u64,
}
pub fn collect_all(
    engine: &Engine,
    symbol_id: SymbolId,
    ts_start: i64,
    ts_end: i64,
) -> Result<Vec<ScanRow>, Error> {
    let mut out = Vec::new();
    for batch in Scan::new(engine, symbol_id, ts_start, ts_end) {
        let batch = batch?;
        for i in 0..batch.ts.len() {
            out.push(ScanRow {
                ts: batch.ts[i],
                price_mantissa: batch.price_mantissa[i],
                exponent: batch.exponent,
                size: batch.size[i],
                side: batch.side[i],
                trade_id: batch.trade_id[i],
            });
        }
    }
    out.sort_unstable_by_key(|r| (r.ts, r.trade_id));
    out.dedup_by_key(|r| r.trade_id);
    Ok(out)
}
