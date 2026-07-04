use format::dz::Decimal64;
/// Helpers for working with `ColumnBatch` in the query layer.
///
/// Because `ColumnBatch` is defined in `format`, we use free functions
/// rather than inherent `impl` (which would require ownership of the type).
pub use format::segment::ColumnBatch;

/// Reconstructs actual prices as f64 for each row.
pub fn prices_f64(batch: &ColumnBatch) -> Vec<f64> {
    let scale = 10f64.powi(batch.exponent as i32);
    batch
        .price_mantissa
        .iter()
        .map(|&m| m as f64 * scale)
        .collect()
}

/// Returns the `Decimal64` price for row `i`.
pub fn price_at(batch: &ColumnBatch, i: usize) -> Decimal64 {
    Decimal64 {
        mantissa: batch.price_mantissa[i],
        exponent: batch.exponent,
    }
}

/// Slices the batch to rows `lo..hi` (no copy of the whole batch).
pub fn slice(batch: &ColumnBatch, lo: usize, hi: usize) -> ColumnBatch {
    ColumnBatch {
        symbol_id: batch.symbol_id,
        ts: batch.ts[lo..hi].to_vec(),
        price_mantissa: batch.price_mantissa[lo..hi].to_vec(),
        exponent: batch.exponent,
        size: batch.size[lo..hi].to_vec(),
        side: batch.side[lo..hi].to_vec(),
        trade_id: batch.trade_id[lo..hi].to_vec(),
    }
}
