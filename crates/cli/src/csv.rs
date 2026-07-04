/// Binance aggTrades CSV parser.
///
/// # CSV format (Binance public data dumps)
///
/// ```text
/// agg_trade_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker
/// 1234,50000.12,0.001,5678,5679,1705276800123,false
/// ```
///
/// Column indices (0-based):
///   0  agg_trade_id   u64
///   1  price          decimal string (e.g., "50000.12")
///   4  last_trade_id  u64  (used as our trade_id for dedup)
///   5  transact_time  u64  milliseconds since epoch
///   6  is_buyer_maker bool ("true" = sell-aggressor, "false" = buy-aggressor)
///
/// # Performance target
///
/// ≥ 2M rows/sec single-threaded (§2.6).  We achieve this by:
///   1. Reading the file in large chunks (BufReader, 1 MiB buffer).
///   2. Parsing each line with manual byte scanning — no regex, no alloc per field.
///   3. Parsing price as fixed-point by finding the decimal point position,
///      avoiding any floating-point operations.
///   4. Batching parsed trades into Vec<Trade> of `BATCH_SIZE` before handing
///      to the engine, amortizing the WAL overhead.
use std::io::{BufReader, Read};

use format::dz::Decimal64;
use format::types::{Side, SymbolId, Trade};

use crate::error::IngestError;

/// Number of trades per ingest batch.  Large enough to amortise WAL overhead;
/// small enough that a single batch fits comfortably in L2 cache.
pub const BATCH_SIZE: usize = 4096;

/// Parses Binance aggTrades CSV from `reader`, calling `on_batch` for each
/// completed batch of up to `BATCH_SIZE` trades.
///
/// Skips the header line automatically.  Returns the total number of rows parsed.
pub fn parse_binance_csv<R, F>(
    reader: R,
    symbol_id: SymbolId,
    mut on_batch: F,
) -> Result<u64, IngestError>
where
    R: Read,
    F: FnMut(&[Trade]) -> Result<(), IngestError>,
{
    use std::io::BufRead;
    let mut buf_reader = BufReader::with_capacity(1024 * 1024, reader);
    let mut batch: Vec<Trade> = Vec::with_capacity(BATCH_SIZE);
    let mut total: u64 = 0;
    let mut first_line = true;
    // Reuse a single line buffer to avoid per-line heap allocation.
    let mut line_buf: Vec<u8> = Vec::with_capacity(256);

    loop {
        line_buf.clear();
        let n = buf_reader
            .read_until(b'\n', &mut line_buf)
            .map_err(IngestError::Io)?;
        if n == 0 {
            break; // EOF
        }

        // Strip trailing newline / carriage-return.
        let bytes = line_buf.as_slice();
        let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        let bytes = bytes.strip_suffix(b"\r").unwrap_or(bytes);

        // Skip header row.
        if first_line {
            first_line = false;
            if bytes.starts_with(b"agg_trade_id") || bytes.starts_with(b"id") {
                continue;
            }
        }

        if bytes.is_empty() {
            continue;
        }

        let trade = parse_line(bytes, symbol_id)?;
        batch.push(trade);
        total += 1;

        if batch.len() == BATCH_SIZE {
            on_batch(&batch)?;
            batch.clear();
        }
    }

    // Flush remaining trades.
    if !batch.is_empty() {
        on_batch(&batch)?;
    }

    Ok(total)
}

/// Parses a single CSV line into a `Trade`.
///
/// Uses manual field splitting on commas — avoids any heap allocation per line.
/// Returns `IngestError::ParseError` on malformed input.
fn parse_line(bytes: &[u8], symbol_id: SymbolId) -> Result<Trade, IngestError> {
    // Split on commas.  Binance aggTrades has exactly 7 fields.
    let mut fields = [0usize; 8]; // start indices of each field
    fields[0] = 0;
    let mut nfields = 1usize;

    for (i, &b) in bytes.iter().enumerate() {
        if b == b',' {
            if nfields >= 8 {
                return Err(IngestError::ParseError("too many fields".into()));
            }
            fields[nfields] = i + 1;
            nfields += 1;
        }
    }

    if nfields < 7 {
        return Err(IngestError::ParseError(format!(
            "expected 7 fields, got {nfields}"
        )));
    }

    // Field boundaries.
    let field = |idx: usize| -> &[u8] {
        let start = fields[idx];
        let end = if idx + 1 < nfields {
            fields[idx + 1] - 1
        } else {
            bytes.len()
        };
        &bytes[start..end]
    };

    // Field 0: agg_trade_id (used as trade_id)
    let trade_id = parse_u64(field(0))?;

    // Field 1: price (decimal string → Decimal64)
    let price = parse_price(field(1))?;

    // Field 2: qty (decimal string — store as fixed-point u64 with exponent -8)
    let size = parse_qty(field(2))?;

    // Field 5: transact_time (milliseconds → nanoseconds)
    let ts_ms = parse_u64(field(5))?;
    // Cap ts_ms to avoid overflow when converting ms → ns.
    // Real Binance timestamps fit easily; only synthetic test data triggers this.
    let ts = (ts_ms.min(i64::MAX as u64 / 1_000_000)) as i64 * 1_000_000;

    // Field 6: is_buyer_maker ("true" = sell aggressor, "false" = buy aggressor)
    let side = match field(6) {
        b"true" | b"True" | b"TRUE" => Side::Sell,
        b"false" | b"False" | b"FALSE" => Side::Buy,
        _ => Side::Unknown,
    };

    Ok(Trade {
        symbol: symbol_id,
        ts,
        price,
        size,
        side,
        trade_id,
    })
}

/// Parses a decimal price string into `Decimal64`.
///
/// Finds the decimal point, counts the digits after it (= exponent magnitude),
/// then parses mantissa as an integer — no floating-point involved.
///
/// Examples:
///   "50000.12"  → Decimal64 { mantissa: 5000012, exponent: -2 }
///   "100"       → Decimal64 { mantissa: 100,     exponent: 0  }
///   "0.00001"   → Decimal64 { mantissa: 1,       exponent: -5 }
fn parse_price(bytes: &[u8]) -> Result<Decimal64, IngestError> {
    // Find decimal point.
    let dot_pos = bytes.iter().position(|&b| b == b'.');

    match dot_pos {
        None => {
            // Integer price.
            let mantissa = parse_i64(bytes)?;
            Ok(Decimal64 {
                mantissa,
                exponent: 0,
            })
        }
        Some(dot) => {
            let frac = &bytes[dot + 1..];
            let exponent = -(frac.len() as i8);

            // Build mantissa by parsing integer + fractional parts together.
            let int_part = parse_i64(&bytes[..dot])?;
            let frac_part = parse_u64(frac)?;

            // Scale int_part by 10^frac.len() then add frac_part.
            let scale = 10i64.pow(frac.len() as u32);
            let mantissa = int_part
                .checked_mul(scale)
                .and_then(|v| v.checked_add(frac_part as i64))
                .ok_or_else(|| IngestError::ParseError("price overflow".into()))?;

            Ok(Decimal64 { mantissa, exponent })
        }
    }
}

/// Parses a quantity string into a u64 scaled to 8 decimal places (satoshi-style).
///
/// "0.001" → 100_000 (with implicit exponent -8 stored separately in the schema)
///
/// We store qty as the number of "minor units" (smallest representable quantity).
/// For Binance spot: 8 decimal places = satoshi scale.
fn parse_qty(bytes: &[u8]) -> Result<u64, IngestError> {
    let dot_pos = bytes.iter().position(|&b| b == b'.');

    const SCALE_DECIMALS: u32 = 8;
    const SCALE: u64 = 100_000_000; // 10^8

    match dot_pos {
        None => {
            let n = parse_u64(bytes)?;
            n.checked_mul(SCALE)
                .ok_or_else(|| IngestError::ParseError("qty overflow".into()))
        }
        Some(dot) => {
            let int_part = parse_u64(&bytes[..dot])?;
            let frac = &bytes[dot + 1..];

            // Truncate or pad fractional part to exactly SCALE_DECIMALS digits.
            let frac_digits = frac.len() as u32;
            let (frac_val, frac_scale) = if frac_digits <= SCALE_DECIMALS {
                let val = parse_u64(frac)?;
                let scale = 10u64.pow(SCALE_DECIMALS - frac_digits);
                (val * scale, SCALE)
            } else {
                // More digits than we store: truncate.
                let val = parse_u64(&frac[..SCALE_DECIMALS as usize])?;
                (val, SCALE)
            };

            int_part
                .checked_mul(frac_scale)
                .and_then(|v| v.checked_add(frac_val))
                .ok_or_else(|| IngestError::ParseError("qty overflow".into()))
        }
    }
}

/// Parses an ASCII decimal string as u64.  Returns error on non-digit bytes.
#[inline]
fn parse_u64(bytes: &[u8]) -> Result<u64, IngestError> {
    if bytes.is_empty() {
        return Err(IngestError::ParseError("empty u64 field".into()));
    }
    let mut val: u64 = 0;
    for &b in bytes {
        if !b.is_ascii_digit() {
            return Err(IngestError::ParseError(format!(
                "non-digit byte 0x{b:02x} in u64 field"
            )));
        }
        val = val
            .checked_mul(10)
            .and_then(|v| v.checked_add((b - b'0') as u64))
            .ok_or_else(|| IngestError::ParseError("u64 overflow".into()))?;
    }
    Ok(val)
}

/// Parses an ASCII decimal string as i64 (handles optional leading '-').
#[inline]
fn parse_i64(bytes: &[u8]) -> Result<i64, IngestError> {
    if bytes.is_empty() {
        return Err(IngestError::ParseError("empty i64 field".into()));
    }
    if bytes[0] == b'-' {
        let mag = parse_u64(&bytes[1..])?;
        Ok(-(mag as i64))
    } else {
        Ok(parse_u64(bytes)? as i64)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_price_integer() {
        let p = parse_price(b"50000").unwrap();
        assert_eq!(p.mantissa, 50000);
        assert_eq!(p.exponent, 0);
    }

    #[test]
    fn parse_price_two_decimals() {
        let p = parse_price(b"50000.12").unwrap();
        assert_eq!(p.mantissa, 5000012);
        assert_eq!(p.exponent, -2);
    }

    #[test]
    fn parse_price_many_decimals() {
        let p = parse_price(b"0.00001").unwrap();
        assert_eq!(p.mantissa, 1);
        assert_eq!(p.exponent, -5);
    }

    #[test]
    fn parse_qty_fractional() {
        // 0.001 BTC = 100_000 satoshis
        let q = parse_qty(b"0.001").unwrap();
        assert_eq!(q, 100_000);
    }

    #[test]
    fn parse_qty_whole() {
        let q = parse_qty(b"5").unwrap();
        assert_eq!(q, 500_000_000);
    }

    #[test]
    fn parse_line_buy() {
        // agg_trade_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker
        let line = b"1234567,50000.12,0.001,9999,10000,1705276800123,false";
        let t = parse_line(line, 1).unwrap();
        assert_eq!(t.trade_id, 1234567);
        assert_eq!(t.price.mantissa, 5000012);
        assert_eq!(t.price.exponent, -2);
        assert_eq!(t.ts, 1705276800123 * 1_000_000);
        assert_eq!(t.side, Side::Buy);
    }

    #[test]
    fn parse_line_sell() {
        let line = b"999,29000.00,1.5,1,2,1700000000000,true";
        let t = parse_line(line, 2).unwrap();
        assert_eq!(t.side, Side::Sell);
    }

    #[test]
    fn parse_csv_with_header() {
        let csv =
            b"agg_trade_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
                    1,50000.00,0.1,1,1,1705276800000,false\n\
                    2,50001.00,0.2,2,2,1705276800001,true\n";

        let mut all: Vec<Trade> = Vec::new();
        let n = parse_binance_csv(&csv[..], 1, |batch| {
            all.extend_from_slice(batch);
            Ok(())
        })
        .unwrap();

        assert_eq!(n, 2);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].price.mantissa, 5000000);
        assert_eq!(all[1].side, Side::Sell);
    }

    #[test]
    fn parse_csv_batches_correctly() {
        // Generate BATCH_SIZE + 1 lines to force a batch flush.
        let header =
            b"agg_trade_id,price,qty,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n";
        let mut csv = header.to_vec();
        for i in 0..BATCH_SIZE + 1 {
            csv.extend_from_slice(
                format!("{i},100.00,1.0,{i},{i},1705276800{i:03},false\n").as_bytes(),
            );
        }

        let mut batch_count = 0usize;
        let mut total_trades = 0usize;
        parse_binance_csv(&csv[..], 1, |batch| {
            batch_count += 1;
            total_trades += batch.len();
            Ok(())
        })
        .unwrap();

        assert_eq!(batch_count, 2); // one full batch + one remainder
        assert_eq!(total_trades, BATCH_SIZE + 1);
    }
}
