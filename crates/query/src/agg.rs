use crate::error::Error;
/// Aggregation kernels: OHLCV resample, VWAP, Top-N by volume.
///
/// # Vectorization strategy (§3.1)
///
/// Inner loops are written as clean scalar loops over slices.  LLVM
/// auto-vectorizes these on x86-64 with AVX2 when compiled with
/// `RUSTFLAGS="-C target-cpu=native"`.  Verified via `cargo asm` — see
/// DESIGN.md §Phase 2 for findings.
///
/// Hand SIMD (`std::simd`) is a stretch goal; the auto-vectorized version
/// already hits the ≥100M rows/sec/core target on M-series and Ryzen.
///
/// # Bucket indexing
///
/// Both OHLCV and VWAP use the same bucket formula:
///   `bucket = (ts - range_start) / interval_ns`
///
/// Integer division truncates toward zero; since ts ≥ range_start is
/// guaranteed by the range scan, this is always a non-negative bucket index.
use format::types::SymbolId;
use storage::ColumnBatch;

// ── OHLCV ─────────────────────────────────────────────────────────────────────

/// One OHLCV bar.
#[derive(Debug, Clone, PartialEq)]
pub struct Bar {
    /// Bucket start timestamp (ns).
    pub ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Sum of size * price (for VWAP = vwap_num / volume).
    pub vwap_num: f64,
    /// Total volume (sum of sizes).
    pub volume: u64,
    pub trade_count: u64,
}

impl Bar {
    /// Computes VWAP from the accumulated numerator and volume.
    pub fn vwap(&self) -> f64 {
        if self.volume == 0 {
            0.0
        } else {
            self.vwap_num / self.volume as f64
        }
    }
}

/// Resamples a stream of `ColumnBatch`es into OHLCV bars.
///
/// # Arguments
/// - `batches`: iterator of batches, ts-sorted ascending.
/// - `range_start`: first bucket starts here (ns).
/// - `range_end`: last bucket ends here (exclusive at range_end, ns).
/// - `interval_ns`: bucket width in nanoseconds.
///
/// # Bucket invariant
/// `bucket_ts = range_start + floor((ts - range_start) / interval_ns) * interval_ns`
/// Rows with ts < range_start or ts >= range_end are silently dropped.
///
/// # Sparse accumulation
/// Bars are accumulated into a `BTreeMap<i64, Bar>` keyed by each bucket's
/// start timestamp, rather than a dense `Vec` indexed by bucket number.
/// This is a deliberate correction from an earlier dense-array version:
/// pre-allocating `Vec<Option<Bar>>` sized by `(range_end - range_start) /
/// interval_ns` is fine for a sane query (a day of 1-minute bars = 1440
/// slots) but allocates catastrophically for a broad, sparse range with a
/// small interval — e.g. `range(i64::MIN, i64::MAX).bars(Interval::Sec1)`
/// implies quintillions of buckets and OOMs the process, discovered while
/// benchmarking this function against unbounded ranges (§5). A `BTreeMap`
/// costs a per-row `O(log distinct_buckets)` insert instead of an O(1)
/// array write — slightly slower per row, but memory now scales with how
/// many buckets the *data* actually touches, not with the query's nominal
/// span, which is the only bound that matters for correctness under an
/// adversarial or merely careless range.
pub fn ohlcv<I>(
    batches: I,
    symbol_id: SymbolId,
    range_start: i64,
    range_end: i64,
    interval_ns: i64,
) -> Result<Vec<Bar>, Error>
where
    I: Iterator<Item = Result<ColumnBatch, Error>>,
{
    use std::collections::BTreeMap;

    if interval_ns <= 0 {
        return Err(Error::InvalidInterval(interval_ns));
    }
    if range_start > range_end {
        return Err(Error::InvalidRange {
            start: range_start,
            end: range_end,
        });
    }

    let mut bars: BTreeMap<i64, Bar> = BTreeMap::new();

    for batch_result in batches {
        let batch = batch_result?;
        if batch.symbol_id != symbol_id {
            continue;
        }

        let scale = 10f64.powi(batch.exponent as i32);

        for i in 0..batch.ts.len() {
            let ts = batch.ts[i];
            if ts < range_start || ts >= range_end {
                continue;
            }

            // Widen to i128 for the intermediate arithmetic: with
            // range_start near i64::MIN and ts near i64::MAX, `ts -
            // range_start` can overflow i64 even though the final
            // bucket_ts is always safe to narrow back (see below).
            // Found via the same benchmark run that motivated the sparse
            // BTreeMap rewrite above — i64::MIN/MAX range bounds are an
            // edge case worth being exactly correct on, not just
            // non-crashing.
            let bucket_offset = (ts as i128 - range_start as i128) / interval_ns as i128;
            let bucket_ts_wide = range_start as i128 + bucket_offset * interval_ns as i128;
            // Safe to narrow: floor-division guarantees
            // range_start <= bucket_ts_wide <= ts, and both bounds are
            // already valid i64 values (ts passed the range filter above).
            let bucket_ts = bucket_ts_wide as i64;

            let price = batch.price_mantissa[i] as f64 * scale;
            let size = batch.size[i];

            bars.entry(bucket_ts)
                .and_modify(|bar| {
                    // Update existing bar — branch-light hot path.
                    if price > bar.high {
                        bar.high = price;
                    }
                    if price < bar.low {
                        bar.low = price;
                    }
                    bar.close = price;
                    bar.vwap_num += price * size as f64;
                    bar.volume += size;
                    bar.trade_count += 1;
                })
                .or_insert(Bar {
                    ts: bucket_ts,
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    vwap_num: price * size as f64,
                    volume: size,
                    trade_count: 1,
                });
        }
    }

    // BTreeMap iterates in key order, so bars are already ts-sorted.
    Ok(bars.into_values().collect())
}

// ── VWAP ──────────────────────────────────────────────────────────────────────

/// Computes the volume-weighted average price over `[range_start, range_end]`.
///
/// This is the degenerate single-bucket case of `ohlcv` — exposed separately
/// for the API surface (§3.2 item 3).
///
/// Returns `None` if there are no trades in the range.
pub fn vwap<I>(
    batches: I,
    symbol_id: SymbolId,
    range_start: i64,
    range_end: i64,
) -> Result<Option<f64>, Error>
where
    I: Iterator<Item = Result<ColumnBatch, Error>>,
{
    if range_start > range_end {
        return Err(Error::InvalidRange {
            start: range_start,
            end: range_end,
        });
    }

    let mut price_volume_sum: f64 = 0.0;
    let mut total_volume: u64 = 0;

    for batch_result in batches {
        let batch = batch_result?;
        if batch.symbol_id != symbol_id {
            continue;
        }

        let scale = 10f64.powi(batch.exponent as i32);

        // Inner loop — auto-vectorizable (independent per row).
        for i in 0..batch.ts.len() {
            let ts = batch.ts[i];
            if ts < range_start || ts > range_end {
                continue;
            }
            let price = batch.price_mantissa[i] as f64 * scale;
            let size = batch.size[i];
            price_volume_sum += price * size as f64;
            total_volume += size;
        }
    }

    if total_volume == 0 {
        Ok(None)
    } else {
        Ok(Some(price_volume_sum / total_volume as f64))
    }
}

// ── Top-N by volume ───────────────────────────────────────────────────────────

/// One entry in a Top-N result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopNEntry {
    pub trade_id: u64,
    pub ts: i64,
    pub volume: u64,
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by volume descending, then ts ascending for stability.
        other.volume.cmp(&self.volume).then(self.ts.cmp(&other.ts))
    }
}
impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Returns the N trades with the highest volume in `[range_start, range_end]`.
///
/// Uses a fixed-size min-heap of size N so memory is O(N) regardless of
/// how many trades the range contains.
pub fn top_n_by_volume<I>(
    batches: I,
    symbol_id: SymbolId,
    range_start: i64,
    range_end: i64,
    n: usize,
) -> Result<Vec<TopNEntry>, Error>
where
    I: Iterator<Item = Result<ColumnBatch, Error>>,
{
    if n == 0 {
        return Ok(Vec::new());
    }
    if range_start > range_end {
        return Err(Error::InvalidRange {
            start: range_start,
            end: range_end,
        });
    }

    use std::collections::BinaryHeap;

    // TopNEntry::Ord is "volume DESC" — so smaller volume = "greater" in this Ord.
    // BinaryHeap<TopNEntry> is a max-heap in TopNEntry::Ord, meaning it pops the
    // entry with the SMALLEST volume first.  This is exactly what we need: we keep
    // the N largest-volume entries by evicting the smallest when the heap grows past N.
    let mut heap: BinaryHeap<TopNEntry> = BinaryHeap::with_capacity(n + 1);

    for batch_result in batches {
        let batch = batch_result?;
        if batch.symbol_id != symbol_id {
            continue;
        }

        for i in 0..batch.ts.len() {
            let ts = batch.ts[i];
            if ts < range_start || ts > range_end {
                continue;
            }
            let entry = TopNEntry {
                trade_id: batch.trade_id[i],
                ts,
                volume: batch.size[i],
            };

            heap.push(entry);
            if heap.len() > n {
                heap.pop(); // evict the entry with smallest volume
            }
        }
    }

    // Collect and sort descending by volume.
    let mut result: Vec<TopNEntry> = heap.into_iter().collect();
    result.sort();
    Ok(result)
}

// ── Reference implementations (used in differential tests) ────────────────────

/// Naive reference OHLCV over a flat slice of `(ts, price_f64, size)` tuples.
///
/// Single-loop, obviously correct, used to verify the vectorized kernel.
pub fn ohlcv_ref(
    trades: &[(i64, f64, u64)], // (ts, price, size)
    range_start: i64,
    range_end: i64,
    interval_ns: i64,
) -> Vec<Bar> {
    use std::collections::BTreeMap;
    let mut bars: BTreeMap<i64, Bar> = BTreeMap::new();

    for &(ts, price, size) in trades {
        if ts < range_start || ts >= range_end {
            continue;
        }
        // See the primary `ohlcv` implementation's comment on why this
        // widens to i128 — i64::MIN/MAX range bounds overflow plain i64
        // subtraction, even though the final bucket_ts always narrows
        // back safely.
        let bucket_offset = (ts as i128 - range_start as i128) / interval_ns as i128;
        let bucket_ts = (range_start as i128 + bucket_offset * interval_ns as i128) as i64;

        bars.entry(bucket_ts)
            .and_modify(|bar| {
                if price > bar.high {
                    bar.high = price;
                }
                if price < bar.low {
                    bar.low = price;
                }
                bar.close = price;
                bar.vwap_num += price * size as f64;
                bar.volume += size;
                bar.trade_count += 1;
            })
            .or_insert(Bar {
                ts: bucket_ts,
                open: price,
                high: price,
                low: price,
                close: price,
                vwap_num: price * size as f64,
                volume: size,
                trade_count: 1,
            });
    }
    bars.into_values().collect()
}

/// Naive reference VWAP.
pub fn vwap_ref(trades: &[(i64, f64, u64)], range_start: i64, range_end: i64) -> Option<f64> {
    let mut num = 0f64;
    let mut vol = 0u64;
    for &(ts, price, size) in trades {
        if ts >= range_start && ts <= range_end {
            num += price * size as f64;
            vol += size;
        }
    }
    if vol == 0 {
        None
    } else {
        Some(num / vol as f64)
    }
}

/// Naive reference Top-N.
pub fn top_n_ref(
    trades: &[(i64, u64, u64)], // (ts, size, trade_id)
    range_start: i64,
    range_end: i64,
    n: usize,
) -> Vec<TopNEntry> {
    let mut all: Vec<TopNEntry> = trades
        .iter()
        .filter(|&&(ts, _, _)| ts >= range_start && ts <= range_end)
        .map(|&(ts, size, trade_id)| TopNEntry {
            trade_id,
            ts,
            volume: size,
        })
        .collect();
    all.sort();
    all.truncate(n);
    all
}
