/// Delta-of-delta (DoD) encoding for nanosecond timestamps.
///
/// # Why delta-of-delta works for tick data
///
/// For regularly-arriving trades, timestamps form a near-arithmetic sequence.
/// If ticks arrive every ~100 µs, then:
///   delta[i]     = ts[i] - ts[i-1]      ≈ constant C
///   dod[i]       = delta[i] - delta[i-1] ≈ 0
///
/// Most DoD values cluster tightly around zero, so zigzag + LEB128 encodes
/// them in 1–2 bytes rather than the 8 bytes a raw i64 would require.
///
/// # Encoding layout
///
/// ```text
/// [first_ts: i64 LE 8 bytes]
/// [first_delta: zigzag+varint i64]   // ts[1] - ts[0]; needed to prime the recursion
/// [dod[2]: zigzag+varint i64]
/// [dod[3]: zigzag+varint i64]
/// ...
/// [dod[n-1]: zigzag+varint i64]
/// ```
///
/// A slice of length 0 encodes to 0 bytes.  Length 1 encodes to 8 bytes.
/// Length 2 encodes to 8 + varint(delta) bytes.
///
/// # Trade-off note (§2.1)
/// We use varint-DoD rather than Gorilla-style bit-packing.  Gorilla needs a
/// bit-level writer and lookup tables for the bucket encoding; varint-DoD is
/// simpler and within ~15% in practice on Binance aggTrade data where deltas
/// are near-constant.
use crate::{varint, Error};

/// Encodes a slice of i64 nanosecond timestamps using delta-of-delta.
///
/// The output is appended to `buf`.  An empty input appends nothing.
pub fn encode(timestamps: &[i64], buf: &mut Vec<u8>) {
    if timestamps.is_empty() {
        return;
    }

    // Store the first timestamp verbatim in little-endian so a reader can seek
    // to any block and immediately recover absolute time.
    buf.extend_from_slice(&timestamps[0].to_le_bytes());

    if timestamps.len() == 1 {
        return;
    }

    // First delta: ts[1] - ts[0].  Needed to prime the dod recurrence.
    //
    // Uses wrapping arithmetic: real timestamps never overflow, but the
    // encoder/decoder must be panic-free on arbitrary i64 input (including
    // proptest's i64::MIN/MAX extremes), and wrapping_add/wrapping_sub form
    // a group under mod 2^64 — so `a.wrapping_add(b.wrapping_sub(a)) == b`
    // holds EXACTLY for every i64, making the roundtrip exact even when
    // intermediate deltas "overflow" by wrapping.
    let first_delta = timestamps[1].wrapping_sub(timestamps[0]);
    varint::encode_i64(first_delta, buf);

    // For i ≥ 2: dod[i] = (ts[i] - ts[i-1]) - (ts[i-1] - ts[i-2])
    // Maintaining prev_delta avoids recomputing the previous delta each step.
    let mut prev_delta = first_delta;
    let mut prev = timestamps[1];
    for &cur in &timestamps[2..] {
        let delta = cur.wrapping_sub(prev);
        let dod = delta.wrapping_sub(prev_delta);
        varint::encode_i64(dod, buf);
        prev_delta = delta;
        prev = cur;
    }
}

/// Decodes exactly `n_rows` timestamps from `src`.
///
/// Returns `Error::Truncated` or `Error::VarintOverflow` on malformed input.
/// Never panics; never allocates more than `n_rows` entries.
pub fn decode(src: &[u8], n_rows: usize) -> Result<Vec<i64>, Error> {
    if n_rows == 0 {
        return Ok(Vec::new());
    }

    // Allocation guard: at minimum we need 8 bytes for the first timestamp.
    if src.len() < 8 {
        return Err(Error::Truncated {
            expected: 8 - src.len(),
        });
    }

    // Additional guard: n_rows entries require at least n_rows bytes of varint
    // data after the first 8 bytes (each varint ≥ 1 byte).
    // For n_rows > 1 we also need the first_delta varint (≥ 1 byte).
    let min_needed = 8 + n_rows.saturating_sub(1);
    if src.len() < min_needed {
        return Err(Error::AllocationGuard {
            declared: n_rows,
            remaining: src.len(),
        });
    }

    let mut out = Vec::with_capacity(n_rows);

    // Read first timestamp (raw LE i64).
    let first = i64::from_le_bytes([
        src[0], src[1], src[2], src[3], src[4], src[5], src[6], src[7],
    ]);
    out.push(first);

    if n_rows == 1 {
        return Ok(out);
    }

    let mut pos = 8usize;

    // Read first delta to prime the recurrence.
    let (first_delta, consumed) = varint::decode_i64(&src[pos..])?;
    pos += consumed;
    let second = first.wrapping_add(first_delta);
    out.push(second);

    // Reconstruct using: delta[i] = dod[i] + delta[i-1]
    //                     ts[i]    = ts[i-1] + delta[i]
    //
    // wrapping_add is the exact inverse of encode's wrapping_sub (see the
    // comment in `encode`) — this roundtrips correctly even for the
    // pathological i64::MIN/MAX deltas that proptest generates.
    let mut prev_delta = first_delta;
    let mut prev_ts = second;

    for _ in 2..n_rows {
        let (dod, consumed) = varint::decode_i64(&src[pos..])?;
        pos += consumed;
        let delta = dod.wrapping_add(prev_delta);
        let ts = prev_ts.wrapping_add(delta);
        out.push(ts);
        prev_delta = delta;
        prev_ts = ts;
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
        assert_eq!(decode(&[], 0).unwrap(), Vec::<i64>::new());
    }

    #[test]
    fn single_timestamp() {
        let ts = [1_700_000_000_000_000_000i64];
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        assert_eq!(buf.len(), 8);
        assert_eq!(decode(&buf, 1).unwrap(), ts);
    }

    #[test]
    fn two_timestamps() {
        let ts = [1_000_000i64, 1_100_000];
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        assert_eq!(decode(&buf, 2).unwrap(), ts);
    }

    #[test]
    fn regular_ticks_are_compact() {
        // Simulate 1024 ticks arriving every 100µs (100_000 ns).
        // dod ≈ 0 for all i >= 2, so each dod encodes to 1 byte.
        let ts: Vec<i64> = (0..1024)
            .map(|i| 1_700_000_000_000_000_000i64 + i * 100_000)
            .collect();
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        // First ts = 8 bytes.
        // first_delta = 100_000; zigzag(100_000) = 200_000, which needs 3
        // LEB128 bytes (200_000 < 2^21 = 2_097_152, requires ceil(18/7) = 3 bytes).
        // 1022 dods of 0 = 1 byte each.
        // Total: 8 + 3 + 1022 = 1033 bytes, vs 1024*8 = 8192 raw (7.9x).
        assert!(buf.len() <= 1033, "buf.len() = {}", buf.len());
        assert_eq!(decode(&buf, 1024).unwrap(), ts);
    }

    #[test]
    fn irregular_ticks_roundtrip() {
        // Timestamps with varying deltas (burst + gap pattern).
        let ts = vec![0i64, 50, 100, 150, 10_000, 10_050, 10_100, 1_000_000];
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        assert_eq!(decode(&buf, ts.len()).unwrap(), ts);
    }

    #[test]
    fn negative_dod_roundtrip() {
        // Slightly out-of-order timestamps can produce negative DoDs.
        let ts = vec![100i64, 200, 190, 300, 250, 400];
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        assert_eq!(decode(&buf, ts.len()).unwrap(), ts);
    }

    #[test]
    fn decode_truncated_returns_err() {
        let ts = vec![1i64, 2, 3, 4];
        let mut buf = Vec::new();
        encode(&ts, &mut buf);
        // Chop the last byte off — must not panic.
        let truncated = &buf[..buf.len() - 1];
        assert!(decode(truncated, 4).is_err());
    }

    #[test]
    fn decode_arbitrary_bytes_no_panic() {
        // Fuzzer-style check in the unit tests: random junk must not panic.
        let junk = b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80";
        let _ = decode(junk, 5);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_roundtrip(
            ts_raw in proptest::collection::vec(any::<i64>(), 0..512)
        ) {
            // Sort to satisfy the ordering contract (the encoder doesn't require
            // sorted input, but realistic data is ts-sorted).
            let mut ts = ts_raw;
            ts.sort_unstable();

            let mut buf = Vec::new();
            encode(&ts, &mut buf);
            let decoded = decode(&buf, ts.len()).unwrap();
            prop_assert_eq!(decoded, ts);
        }

        #[test]
        fn prop_decode_no_panic(data: Vec<u8>, n in 0usize..1024) {
            // Decoder must never panic on arbitrary bytes for any declared row count.
            let _ = decode(&data, n);
        }
    }
}
