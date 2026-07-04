/// Run-length encoding (RLE) for the `side` column.
///
/// The side column contains values `{0, 1, 2}` (buy, sell, unknown).  In
/// real order-flow data, aggressor side often persists for many consecutive
/// trades (e.g., a momentum burst of 50 buys).  RLE exploits this structure.
///
/// # Wire format
///
/// A sequence of `(value: u8, run_length: varint u64)` pairs.
///
/// ```text
/// [value: u8][run: LEB128 u64][value: u8][run: LEB128 u64] …
/// ```
///
/// Encoding the entire column as RLE pairs (rather than mixed raw/run) keeps
/// the decoder simple: it just fills `run` output slots with `value`.
///
/// An empty input encodes to 0 bytes.
use crate::{varint, Error};

/// Encodes a slice of `u8` side values using run-length encoding.
pub fn encode(sides: &[u8], buf: &mut Vec<u8>) {
    if sides.is_empty() {
        return;
    }

    // sides[0] is valid: guarded by is_empty() above.
    let mut current = sides[0];
    let mut run: u64 = 1;

    for &v in &sides[1..] {
        if v == current {
            run += 1;
        } else {
            // Flush the current run.
            buf.push(current);
            varint::encode_u64(run, buf);
            current = v;
            run = 1;
        }
    }
    // Flush final run.
    buf.push(current);
    varint::encode_u64(run, buf);
}

/// Decodes exactly `n_rows` side values from `src`.
///
/// Allocation guard: refuses to allocate more than `n_rows` entries regardless
/// of what the encoded run lengths claim.  This prevents OOM on malformed input.
///
/// Returns `Error::Truncated` if the stream ends before `n_rows` are decoded.
pub fn decode(src: &[u8], n_rows: usize) -> Result<Vec<u8>, Error> {
    if n_rows == 0 {
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(n_rows);
    let mut pos = 0usize;

    while out.len() < n_rows {
        // Need at least 2 bytes: value byte + at least 1 varint byte.
        if pos >= src.len() {
            return Err(Error::Truncated { expected: 2 });
        }

        let value = src[pos];
        pos += 1;

        let (run, consumed) = varint::decode_u64(&src[pos..])?;
        pos += consumed;

        // Allocation guard: clamp the run to the remaining needed rows so
        // we never allocate more than n_rows regardless of the encoded value.
        let remaining = n_rows - out.len();
        let run_clamped = (run as usize).min(remaining);

        if run_clamped == 0 {
            // run = 0 in the stream means something is malformed.
            return Err(Error::Truncated { expected: 1 });
        }

        out.extend(std::iter::repeat_n(value, run_clamped));
        // If the encoded run exceeded what we needed, the stream is technically
        // malformed (encoder should never produce this), but we consumed the
        // right number of rows — we'll just stop.
    }

    Ok(out)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let mut buf = Vec::new();
        encode(&[], &mut buf);
        assert!(buf.is_empty());
        assert_eq!(decode(&[], 0).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn single_value() {
        let sides = vec![1u8];
        let mut buf = Vec::new();
        encode(&sides, &mut buf);
        // Encodes as: value=1 (1 byte) + run=1 varint (1 byte) = 2 bytes.
        assert_eq!(buf.len(), 2);
        assert_eq!(decode(&buf, 1).unwrap(), sides);
    }

    #[test]
    fn all_same() {
        let sides = vec![0u8; 128];
        let mut buf = Vec::new();
        encode(&sides, &mut buf);
        // One run of 128: value(1) + varint(128) = 1+2 = 3 bytes.
        assert_eq!(buf.len(), 3);
        assert_eq!(decode(&buf, 128).unwrap(), sides);
    }

    #[test]
    fn alternating_values() {
        let sides = vec![0u8, 1, 0, 1, 0];
        let mut buf = Vec::new();
        encode(&sides, &mut buf);
        assert_eq!(decode(&buf, 5).unwrap(), sides);
    }

    #[test]
    fn realistic_burst() {
        // 50 buys, 20 sells, 5 unknowns.
        let mut sides: Vec<u8> = vec![0; 50];
        sides.extend(vec![1; 20]);
        sides.extend(vec![2; 5]);
        let mut buf = Vec::new();
        encode(&sides, &mut buf);
        // 3 runs × (value + varint) = compact.
        assert!(buf.len() < 10);
        assert_eq!(decode(&buf, 75).unwrap(), sides);
    }

    #[test]
    fn decode_truncated_returns_err() {
        let sides = vec![0u8; 10];
        let mut buf = Vec::new();
        encode(&sides, &mut buf);
        // Give only the value byte, no run length.
        assert!(decode(&buf[..1], 10).is_err());
    }

    #[test]
    fn decode_arbitrary_no_panic() {
        let _ = decode(b"\x00\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", 5);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_roundtrip(sides in proptest::collection::vec(0u8..3, 0..512)) {
            let mut buf = Vec::new();
            encode(&sides, &mut buf);
            let decoded = decode(&buf, sides.len()).unwrap();
            prop_assert_eq!(decoded, sides);
        }

        #[test]
        fn prop_decode_no_panic(data: Vec<u8>, n in 0usize..1024) {
            // Must never panic regardless of input.
            let _ = decode(&data, n);
        }
    }
}
