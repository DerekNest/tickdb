/// Plain LEB128 varint encoding for the `size` column (trade quantities).
///
/// # Rationale for not using delta encoding here
///
/// Trade sizes are erratic — a sequence like [100, 10000, 1, 500000, 25] has
/// deltas larger than the values themselves, so delta encoding gives no
/// benefit and may make things worse (zigzag on large-magnitude deltas = more
/// bytes than the raw value).
///
/// Plain varint works well because real sizes are skewed: the vast majority of
/// trades are small (1–1000 units) even on exchanges where the occasional
/// whale-order is 10^6+.  Small values LEB128-encode to 1–3 bytes vs. 8 bytes
/// raw.
///
/// The `trade_id` column is also encoded here using the same plain-varint
/// strategy.
use crate::{varint, Error};

/// Encodes a slice of u64 sizes as successive LEB128 varints.
pub fn encode(sizes: &[u64], buf: &mut Vec<u8>) {
    varint::encode_u64_slice(sizes, buf);
}

/// Decodes exactly `n_rows` u64 sizes from `src`.
///
/// Applies the same allocation guard as `varint::decode_u64_slice`.
pub fn decode(src: &[u8], n_rows: usize) -> Result<Vec<u64>, Error> {
    varint::decode_u64_slice(src, n_rows)
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
        assert_eq!(decode(&[], 0).unwrap(), Vec::<u64>::new());
    }

    #[test]
    fn small_sizes() {
        let sizes = vec![1u64, 5, 10, 100, 1000];
        let mut buf = Vec::new();
        encode(&sizes, &mut buf);
        // Each value < 128 → 1 byte each → 5 bytes total.
        // 1000 needs 2 bytes (LEB128: 0xE8 0x07).
        assert!(buf.len() <= 6);
        assert_eq!(decode(&buf, 5).unwrap(), sizes);
    }

    #[test]
    fn large_sizes() {
        let sizes = vec![u64::MAX, 0, 1_000_000_000_000u64];
        let mut buf = Vec::new();
        encode(&sizes, &mut buf);
        assert_eq!(decode(&buf, 3).unwrap(), sizes);
    }

    #[test]
    fn decode_arbitrary_no_panic() {
        let _ = decode(b"\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80", 3);
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_roundtrip(sizes in proptest::collection::vec(any::<u64>(), 0..512)) {
            let mut buf = Vec::new();
            encode(&sizes, &mut buf);
            let decoded = decode(&buf, sizes.len()).unwrap();
            prop_assert_eq!(decoded, sizes);
        }

        #[test]
        fn prop_decode_no_panic(data: Vec<u8>, n in 0usize..1024) {
            let _ = decode(&data, n);
        }
    }
}
