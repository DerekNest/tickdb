/// Delta + zigzag + varint encoding for price mantissas.
///
/// # Representation
///
/// Prices are stored as `Decimal64 { mantissa: i64, exponent: i8 }`.
/// The actual price is `mantissa * 10^exponent`.
///
/// Within a (segment, symbol) block all trades typically share the same
/// exponent (e.g., BTCUSDT with 2 decimal places → exponent = -2).
/// When that holds, the deltas between successive mantissas are tiny
/// (a few ticks), compressing extremely well with zigzag + varint.
///
/// # Block-level exponent handling
///
/// All `Decimal64` entries in a block must be reduced to a single common
/// exponent before encoding their mantissas.
///
/// Algorithm:
/// 1. Find `min_exp = min(entry.exponent for entry in block)`.
/// 2. For each entry: `scaled_mantissa = entry.mantissa * 10^(entry.exponent - min_exp)`.
///    (Because `min_exp` is the most negative, the scale factor is ≥ 1.)
/// 3. If any rescale would overflow `i64`, fall back to the raw fallback path
///    (XOR-with-previous on the raw IEEE-754 f64 bit pattern, then varint of XOR).
/// 4. Encode deltas of the scaled mantissas with zigzag + varint.
///
/// The block header records `min_exp` and a flag bit distinguishing the two paths.
///
/// # Wire format per block
///
/// ```text
/// [flags: u8]            bit 0 = 0 → delta path, 1 → raw f64-XOR fallback
/// [exponent: i8]         common exponent (only meaningful on delta path)
/// [first_mantissa or first_f64_bits: i64 LE, 8 bytes]
/// [successive delta/xor values: zigzag+varint i64]*
/// ```
use crate::{varint, Error};

const FLAG_FALLBACK: u8 = 0x01;

/// A price value stored as fixed-point decimal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Decimal64 {
    pub mantissa: i64,
    pub exponent: i8,
}

/// Encodes a slice of `Decimal64` prices into `buf`.
///
/// Returns the common exponent used (valid only on the delta path; on
/// the fallback path the exponent field in the header is set to 0 and
/// should be ignored by the reader).
pub fn encode(prices: &[Decimal64], buf: &mut Vec<u8>) {
    if prices.is_empty() {
        return;
    }

    // Determine the minimum (most negative) exponent in the block.
    // prices is non-empty (checked by caller); fold is infallible.
    let min_exp = prices.iter().map(|p| p.exponent).fold(i8::MAX, i8::min);

    // Attempt to rescale every price to min_exp.  If any rescale overflows
    // i64, we fall back to the f64-XOR path.
    match try_rescale(prices, min_exp) {
        Some(mantissas) => encode_delta_path(&mantissas, min_exp, buf),
        None => encode_fallback_path(prices, buf),
    }
}

/// Decodes exactly `n_rows` prices from `src`.
///
/// Never panics; returns an error on malformed input.
pub fn decode(src: &[u8], n_rows: usize) -> Result<Vec<Decimal64>, Error> {
    if n_rows == 0 {
        return Ok(Vec::new());
    }

    // We need at least the 2-byte header + 8-byte first value.
    if src.len() < 10 {
        return Err(Error::Truncated {
            expected: 10 - src.len().min(10),
        });
    }

    // Allocation guard: after the 10-byte header, each subsequent value is at
    // least 1 varint byte.
    let min_needed = 10 + n_rows.saturating_sub(1);
    if n_rows > 1 && src.len() < min_needed {
        return Err(Error::AllocationGuard {
            declared: n_rows,
            remaining: src.len(),
        });
    }

    let flags = src[0];
    let exponent = src[1] as i8;
    let s = &src[2..10];
    let first_raw = i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]);

    if flags & FLAG_FALLBACK != 0 {
        decode_fallback_path(src, n_rows)
    } else {
        decode_delta_path(src, n_rows, exponent, first_raw)
    }
}

// ── Delta path ────────────────────────────────────────────────────────────────

/// Tries to rescale all mantissas to `min_exp`.
///
/// Returns `None` if any rescale would overflow `i64`.
fn try_rescale(prices: &[Decimal64], min_exp: i8) -> Option<Vec<i64>> {
    let mut out = Vec::with_capacity(prices.len());
    for p in prices {
        // scale_shift ≥ 0 because min_exp ≤ p.exponent.
        let scale_shift = (p.exponent - min_exp) as u32;
        let scale = 10i64.checked_pow(scale_shift)?;
        let scaled = p.mantissa.checked_mul(scale)?;
        out.push(scaled);
    }
    Some(out)
}

fn encode_delta_path(mantissas: &[i64], exponent: i8, buf: &mut Vec<u8>) {
    buf.push(0x00); // flags: delta path
    buf.push(exponent as u8);

    // First mantissa stored verbatim.
    buf.extend_from_slice(&mantissas[0].to_le_bytes());

    // Subsequent values encoded as zigzag+varint deltas.
    // Mantissa deltas for market prices are typically small (1–10 ticks).
    //
    // Uses wrapping arithmetic: real prices never overflow, but the encoder
    // must be panic-free on arbitrary i64 mantissas (proptest generates
    // i64::MIN/MAX extremes), and wrapping_sub/wrapping_add form a group
    // under mod 2^64 — so `a.wrapping_add(b.wrapping_sub(a)) == b` holds
    // EXACTLY for every i64, making the roundtrip exact even when
    // intermediate deltas "overflow" by wrapping. Same reasoning as
    // `dod::encode` for timestamp deltas.
    let mut prev = mantissas[0];
    for &m in &mantissas[1..] {
        let delta = m.wrapping_sub(prev);
        varint::encode_i64(delta, buf);
        prev = m;
    }
}

fn decode_delta_path(
    src: &[u8],
    n_rows: usize,
    exponent: i8,
    first_mantissa: i64,
) -> Result<Vec<Decimal64>, Error> {
    let mut out = Vec::with_capacity(n_rows);
    out.push(Decimal64 {
        mantissa: first_mantissa,
        exponent,
    });

    let mut pos = 10usize; // past flags(1) + exp(1) + first(8)
    let mut prev = first_mantissa;

    for _ in 1..n_rows {
        let (delta, consumed) = varint::decode_i64(&src[pos..])?;
        pos += consumed;
        // wrapping_add is the exact inverse of encode's wrapping_sub — see
        // the comment in encode_delta_path.
        let m = prev.wrapping_add(delta);
        out.push(Decimal64 {
            mantissa: m,
            exponent,
        });
        prev = m;
    }

    Ok(out)
}

// ── Fallback path (f64 XOR) ───────────────────────────────────────────────────
//
// When mantissas cannot be rescaled without overflow, we convert each price to
// f64, store the first as raw bits, then store successive XOR-with-previous
// values encoded as zigzag+varint.  XOR of close IEEE-754 doubles shares many
// high bits, so varint compression still helps.

fn encode_fallback_path(prices: &[Decimal64], buf: &mut Vec<u8>) {
    buf.push(FLAG_FALLBACK);
    buf.push(0x00); // exponent field unused in fallback; reserved as 0

    let to_f64 = |p: &Decimal64| -> f64 { (p.mantissa as f64) * 10f64.powi(p.exponent as i32) };

    let first_bits = to_f64(&prices[0]).to_bits() as i64;
    buf.extend_from_slice(&first_bits.to_le_bytes());

    let mut prev_bits = first_bits as u64;
    for p in &prices[1..] {
        let bits = to_f64(p).to_bits();
        let xor = (bits ^ prev_bits) as i64;
        varint::encode_i64(xor, buf);
        prev_bits = bits;
    }
}

fn decode_fallback_path(src: &[u8], n_rows: usize) -> Result<Vec<Decimal64>, Error> {
    // Fallback path loses the exact exponent; we normalize all values to
    // exponent = 0 with the f64 value rounded to 8 significant decimal digits.
    // This is a lossy representation — callers should avoid triggering the
    // fallback by ensuring mantissas don't overflow at encode time.
    let s = &src[2..10];
    let first_bits = i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]) as u64;
    let first_f64 = f64::from_bits(first_bits);

    // Use exponent = -8 as a canonical representation (8 decimal places).
    const CANONICAL_EXP: i8 = -8;
    let scale = 10i64.pow(8);

    let f64_to_decimal = |f: f64| -> Decimal64 {
        Decimal64 {
            mantissa: (f * scale as f64).round() as i64,
            exponent: CANONICAL_EXP,
        }
    };

    let mut out = Vec::with_capacity(n_rows);
    out.push(f64_to_decimal(first_f64));

    let mut pos = 10usize;
    let mut prev_bits = first_bits;

    for _ in 1..n_rows {
        let (xor_signed, consumed) = varint::decode_i64(&src[pos..])?;
        pos += consumed;
        let bits = (xor_signed as u64) ^ prev_bits;
        let f = f64::from_bits(bits);
        out.push(f64_to_decimal(f));
        prev_bits = bits;
    }

    Ok(out)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn d(mantissa: i64, exponent: i8) -> Decimal64 {
        Decimal64 { mantissa, exponent }
    }

    #[test]
    fn empty() {
        let mut buf = Vec::new();
        encode(&[], &mut buf);
        assert!(buf.is_empty());
        assert_eq!(decode(&[], 0).unwrap(), vec![]);
    }

    #[test]
    fn single_price() {
        let prices = vec![d(5432100, -2)];
        let mut buf = Vec::new();
        encode(&prices, &mut buf);
        let decoded = decode(&buf, 1).unwrap();
        assert_eq!(decoded, prices);
    }

    #[test]
    fn same_exponent_delta_path() {
        // BTC prices near 50000.00 with exponent -2.
        let prices = vec![
            d(5000000, -2),
            d(5000001, -2),
            d(4999999, -2),
            d(5000050, -2),
        ];
        let mut buf = Vec::new();
        encode(&prices, &mut buf);
        assert_eq!(buf[0], 0x00, "expected delta-path flag");
        let decoded = decode(&buf, 4).unwrap();
        assert_eq!(decoded, prices);
    }

    #[test]
    fn mixed_exponents_rescaled() {
        // exponents -2 and -3 → rescale to -3.
        let prices = vec![d(50000, -2), d(500010, -3), d(50002, -2)];
        let mut buf = Vec::new();
        encode(&prices, &mut buf);
        assert_eq!(buf[0], 0x00, "expected delta-path flag after rescale");
        let decoded = decode(&buf, 3).unwrap();
        // After rescaling to exponent -3, the original values should be preserved.
        for (orig, dec) in prices.iter().zip(decoded.iter()) {
            let orig_val = orig.mantissa as f64 * 10f64.powi(orig.exponent as i32);
            let dec_val = dec.mantissa as f64 * 10f64.powi(dec.exponent as i32);
            let diff = (orig_val - dec_val).abs();
            assert!(diff < 1e-6, "orig={orig_val} dec={dec_val}");
        }
    }

    #[test]
    fn decode_truncated_no_panic() {
        let prices = vec![d(100, -2), d(101, -2), d(102, -2)];
        let mut buf = Vec::new();
        encode(&prices, &mut buf);
        // Give only the header — should error, not panic.
        let _ = decode(&buf[..5], 3);
    }

    #[test]
    fn decode_arbitrary_no_panic() {
        let _ = decode(b"\x00\xfe\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", 4);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_same_exponent_roundtrip(
            mantissas in proptest::collection::vec(-1_000_000_000i64..1_000_000_000, 1..256),
            exponent in -8i8..=0,
        ) {
            let prices: Vec<Decimal64> = mantissas.iter()
                .map(|&m| Decimal64 { mantissa: m, exponent })
                .collect();
            let mut buf = Vec::new();
            encode(&prices, &mut buf);
            let decoded = decode(&buf, prices.len()).unwrap();
            prop_assert_eq!(decoded, prices);
        }

        #[test]
        fn prop_decode_no_panic(data: Vec<u8>, n in 0usize..512) {
            let _ = decode(&data, n);
        }
    }
}
