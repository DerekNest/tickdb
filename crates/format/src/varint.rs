/// LEB128 unsigned varint encoding / decoding, plus zigzag mapping for signed integers.
///
/// All other encoders in this crate build on top of these primitives.
use crate::Error;

// ── Unsigned LEB128 ──────────────────────────────────────────────────────────

/// Appends the LEB128 encoding of `value` to `buf`.
///
/// LEB128: emit 7 bits per byte, LSB first.  Set the high bit on every byte
/// except the last.  A u64 fits in at most 10 bytes (⌈64/7⌉ = 10).
pub fn encode_u64(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        // Take the lowest 7 bits.
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            // More bytes follow — set the continuation bit.
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decodes one LEB128 u64 from `src`, returning `(value, bytes_consumed)`.
///
/// Returns `Error::Truncated` if `src` ends mid-varint, or
/// `Error::VarintOverflow` if the value exceeds 64 bits.
pub fn decode_u64(src: &[u8]) -> Result<(u64, usize), Error> {
    let mut result: u64 = 0;
    let mut shift = 0u32;

    for (i, &byte) in src.iter().enumerate() {
        if shift >= 64 {
            // A 10-byte varint is valid (max u64), but 11+ bytes means the
            // value cannot fit in u64.
            return Err(Error::VarintOverflow);
        }
        // The bottom 7 bits are data.
        let low7 = (byte & 0x7F) as u64;
        result |= low7 << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            // Continuation bit clear → this is the final byte.
            return Ok((result, i + 1));
        }
    }

    Err(Error::Truncated { expected: 1 })
}

// ── Zigzag mapping (signed ↔ unsigned) ──────────────────────────────────────

/// Maps a signed i64 to an unsigned u64 using zigzag encoding.
///
/// Zigzag maps small-magnitude signed values to small unsigned values so that
/// LEB128 compression is effective on negative numbers:
///   0 → 0,  -1 → 1,  1 → 2,  -2 → 3,  2 → 4, …
///
/// Formula: `(n << 1) ^ (n >> 63)`.
/// The arithmetic right-shift fills the upper bits with the sign bit, giving
/// an all-ones mask for negative numbers.
#[inline]
pub fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// Inverts zigzag encoding.
///
/// Formula: `(u >> 1) ^ -(u & 1)`.
/// The mask `-(u & 1)` is all-ones when the LSB is 1 (was negative), all-zeros
/// otherwise — this is the arithmetic XOR complement of the right-shift.
#[inline]
pub fn zigzag_decode(u: u64) -> i64 {
    ((u >> 1) as i64) ^ -((u & 1) as i64)
}

// ── Signed convenience wrappers ──────────────────────────────────────────────

/// Zigzag-encodes `value` then appends its LEB128 form to `buf`.
#[inline]
pub fn encode_i64(value: i64, buf: &mut Vec<u8>) {
    encode_u64(zigzag_encode(value), buf);
}

/// Decodes a zigzag+LEB128 i64 from `src`, returning `(value, bytes_consumed)`.
#[inline]
pub fn decode_i64(src: &[u8]) -> Result<(i64, usize), Error> {
    let (u, n) = decode_u64(src)?;
    Ok((zigzag_decode(u), n))
}

// ── Batch helpers used by other encoder modules ──────────────────────────────

/// Encodes a slice of u64s as successive LEB128 varints into `buf`.
pub fn encode_u64_slice(values: &[u64], buf: &mut Vec<u8>) {
    for &v in values {
        encode_u64(v, buf);
    }
}

/// Decodes exactly `n_rows` LEB128 u64 values from `src`.
pub fn decode_u64_slice(src: &[u8], n_rows: usize) -> Result<Vec<u64>, Error> {
    // Allocation guard: each varint is at least 1 byte.  If n_rows > src.len()
    // the decode would fail anyway, but we check upfront to avoid allocating a
    // huge Vec before we even start reading.
    if n_rows > src.len() {
        return Err(Error::AllocationGuard {
            declared: n_rows,
            remaining: src.len(),
        });
    }

    let mut out = Vec::with_capacity(n_rows);
    let mut pos = 0;
    for _ in 0..n_rows {
        let (v, consumed) = decode_u64(&src[pos..])?;
        out.push(v);
        pos += consumed;
    }
    Ok(out)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── zigzag round-trip ────────────────────────────────────────────────────

    #[test]
    fn zigzag_zero() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_decode(0), 0);
    }

    #[test]
    fn zigzag_positive() {
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(i64::MAX), u64::MAX - 1);
    }

    #[test]
    fn zigzag_negative() {
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(i64::MIN), u64::MAX);
    }

    #[test]
    fn zigzag_roundtrip_edges() {
        for v in [0i64, 1, -1, i64::MAX, i64::MIN, 127, -128] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    // ── u64 varint ──────────────────────────────────────────────────────────

    #[test]
    fn u64_single_byte() {
        let mut buf = Vec::new();
        encode_u64(0, &mut buf);
        assert_eq!(buf, &[0x00]);
        let (v, n) = decode_u64(&buf).unwrap();
        assert_eq!((v, n), (0, 1));
    }

    #[test]
    fn u64_multibyte() {
        // 300 = 0x12C.  LEB128: 0xAC 0x02
        let mut buf = Vec::new();
        encode_u64(300, &mut buf);
        assert_eq!(buf, &[0xAC, 0x02]);
        let (v, n) = decode_u64(&buf).unwrap();
        assert_eq!((v, n), (300, 2));
    }

    #[test]
    fn u64_max() {
        let mut buf = Vec::new();
        encode_u64(u64::MAX, &mut buf);
        assert_eq!(buf.len(), 10); // u64::MAX needs exactly 10 bytes
        let (v, _) = decode_u64(&buf).unwrap();
        assert_eq!(v, u64::MAX);
    }

    #[test]
    fn decode_truncated() {
        // High bit set but no following byte — truncated stream.
        assert!(matches!(decode_u64(&[0x80]), Err(Error::Truncated { .. })));
    }

    #[test]
    fn decode_overflow() {
        // 11 bytes, all with continuation bits — value > 2^70, overflows u64.
        let bad = [0x80u8; 11];
        assert!(matches!(decode_u64(&bad), Err(Error::VarintOverflow)));
    }

    // ── i64 round-trip ──────────────────────────────────────────────────────

    #[test]
    fn i64_roundtrip_edges() {
        for v in [0i64, 1, -1, 127, -128, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            encode_i64(v, &mut buf);
            let (decoded, _) = decode_i64(&buf).unwrap();
            assert_eq!(decoded, v, "failed for {v}");
        }
    }

    // ── proptest round-trips ─────────────────────────────────────────────────

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_u64_roundtrip(v: u64) {
            let mut buf = Vec::new();
            encode_u64(v, &mut buf);
            let (decoded, consumed) = decode_u64(&buf).unwrap();
            prop_assert_eq!(decoded, v);
            prop_assert_eq!(consumed, buf.len());
        }

        #[test]
        fn prop_i64_roundtrip(v: i64) {
            let mut buf = Vec::new();
            encode_i64(v, &mut buf);
            let (decoded, consumed) = decode_i64(&buf).unwrap();
            prop_assert_eq!(decoded, v);
            prop_assert_eq!(consumed, buf.len());
        }

        #[test]
        fn prop_u64_slice_roundtrip(values in proptest::collection::vec(any::<u64>(), 0..512)) {
            let mut buf = Vec::new();
            encode_u64_slice(&values, &mut buf);
            let decoded = decode_u64_slice(&buf, values.len()).unwrap();
            prop_assert_eq!(decoded, values);
        }

        #[test]
        fn prop_decode_never_panics(data: Vec<u8>) {
            // The decoder must never panic on arbitrary bytes — it may return Err.
            let _ = decode_u64(&data);
        }
    }
}
