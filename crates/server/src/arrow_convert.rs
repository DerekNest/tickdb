/// Converts query-layer result types (`Bar`, `FlatTrade`, `AsOfRow`) into
/// Arrow `RecordBatch`es and serializes them as Arrow IPC streams.
///
/// # Why Arrow IPC
///
/// Arrow IPC stream format is self-describing (schema + record batches in
/// one byte stream) and has first-class support in pandas, polars, and
/// DuckDB (`read_arrow`).  This lets the demo notebook (`bench/demo.ipynb`)
/// load query results directly into a DataFrame with zero custom parsing.
///
/// Content-Type: `application/vnd.apache.arrow.stream`
use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::writer::StreamWriter;

use query::asof::FlatTrade;
use query::{AsOfRow, Bar};

use crate::error::Error;

/// Serializes a `RecordBatch` to an Arrow IPC stream (schema + one batch + EOS).
pub fn to_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

// ── Bar → RecordBatch ───────────────────────────────────────────────────────

/// Schema: ts(i64), open/high/low/close/vwap(f64), volume(u64), trade_count(u64)
pub fn bars_to_record_batch(bars: &[Bar]) -> Result<RecordBatch, Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("vwap", DataType::Float64, false),
        Field::new("volume", DataType::UInt64, false),
        Field::new("trade_count", DataType::UInt64, false),
    ]));

    let ts: Int64Array = bars.iter().map(|b| b.ts).collect();
    let open: Float64Array = bars.iter().map(|b| b.open).collect();
    let high: Float64Array = bars.iter().map(|b| b.high).collect();
    let low: Float64Array = bars.iter().map(|b| b.low).collect();
    let close: Float64Array = bars.iter().map(|b| b.close).collect();
    let vwap: Float64Array = bars.iter().map(|b| b.vwap()).collect();
    let volume: UInt64Array = bars.iter().map(|b| b.volume).collect();
    let trade_count: UInt64Array = bars.iter().map(|b| b.trade_count).collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(vwap),
            Arc::new(volume),
            Arc::new(trade_count),
        ],
    )?)
}

// ── FlatTrade → RecordBatch ─────────────────────────────────────────────────

/// Schema: ts(i64), price(f64), size(u64), trade_id(u64)
pub fn trades_to_record_batch(trades: &[FlatTrade]) -> Result<RecordBatch, Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::UInt64, false),
        Field::new("trade_id", DataType::UInt64, false),
    ]));

    let ts: Int64Array = trades.iter().map(|t| t.ts).collect();
    let price: Float64Array = trades.iter().map(|t| t.price).collect();
    let size: UInt64Array = trades.iter().map(|t| t.size).collect();
    let trade_id: UInt64Array = trades.iter().map(|t| t.trade_id).collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ts),
            Arc::new(price),
            Arc::new(size),
            Arc::new(trade_id),
        ],
    )?)
}

// ── AsOfRow → RecordBatch ────────────────────────────────────────────────────

/// Schema: left_ts(i64), left_price(f64), left_size(u64), left_trade_id(u64),
///         right_ts(i64, nullable), right_price(f64, nullable),
///         right_trade_id(u64, nullable), lag_ns(i64, nullable)
pub fn asof_to_record_batch(rows: &[AsOfRow]) -> Result<RecordBatch, Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("left_ts", DataType::Int64, false),
        Field::new("left_price", DataType::Float64, false),
        Field::new("left_size", DataType::UInt64, false),
        Field::new("left_trade_id", DataType::UInt64, false),
        Field::new("right_ts", DataType::Int64, true),
        Field::new("right_price", DataType::Float64, true),
        Field::new("right_trade_id", DataType::UInt64, true),
        Field::new("lag_ns", DataType::Int64, true),
    ]));

    let left_ts: Int64Array = rows.iter().map(|r| r.left_ts).collect();
    let left_price: Float64Array = rows.iter().map(|r| r.left_price).collect();
    let left_size: UInt64Array = rows.iter().map(|r| r.left_size).collect();
    let left_trade_id: UInt64Array = rows.iter().map(|r| r.left_trade_id).collect();
    let right_ts: Int64Array = rows.iter().map(|r| r.right_ts).collect();
    let right_price: Float64Array = rows.iter().map(|r| r.right_price).collect();
    let right_trade_id: UInt64Array = rows.iter().map(|r| r.right_trade_id).collect();
    let lag_ns: Int64Array = rows.iter().map(|r| r.lag_ns).collect();

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(left_ts),
            Arc::new(left_price),
            Arc::new(left_size),
            Arc::new(left_trade_id),
            Arc::new(right_ts),
            Arc::new(right_price),
            Arc::new(right_trade_id),
            Arc::new(lag_ns),
        ],
    )?)
}

// ── Unused-import silencer for UInt8Array (reserved for `side` column, M7+) ───
#[allow(dead_code)]
fn _reserved_side_array(sides: &[u8]) -> UInt8Array {
    sides.iter().copied().collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use query::Bar;

    #[test]
    fn bars_roundtrip_ipc() {
        let bars = vec![Bar {
            ts: 1000,
            open: 1.0,
            high: 2.0,
            low: 0.5,
            close: 1.5,
            vwap_num: 3.0,
            volume: 2,
            trade_count: 2,
        }];
        let batch = bars_to_record_batch(&bars).unwrap();
        let bytes = to_ipc_bytes(&batch).unwrap();
        assert!(!bytes.is_empty());

        // Decode back via arrow_ipc reader to verify the bytes are valid Arrow IPC.
        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn empty_bars_produces_valid_ipc() {
        let batch = bars_to_record_batch(&[]).unwrap();
        let bytes = to_ipc_bytes(&batch).unwrap();
        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn asof_with_nulls_roundtrip() {
        let rows = vec![AsOfRow {
            left_ts: 100,
            left_price: 1.0,
            left_size: 1,
            left_trade_id: 1,
            right_ts: None,
            right_price: None,
            right_trade_id: None,
            lag_ns: None,
        }];
        let batch = asof_to_record_batch(&rows).unwrap();
        let bytes = to_ipc_bytes(&batch).unwrap();
        let cursor = std::io::Cursor::new(bytes);
        let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert!(batches[0].column(4).is_null(0)); // right_ts is null
    }
}
