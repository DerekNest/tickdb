/// Symbol dictionary encoding.
///
/// # Design
///
/// Within a (segment, symbol) block the symbol column is **constant** —
/// every row belongs to the same symbol.  Therefore the symbol id is NOT
/// stored in the block body at all; it lives once in the block index entry
/// in the footer.
///
/// The footer holds a compact `u32 → utf8` dictionary so readers can resolve
/// symbol names without any external lookup:
///
/// ```text
/// [count: u32]
/// for each entry (sorted by id ascending):
///     [id:       u32]
///     [name_len: u16]
///     [name:     name_len utf-8 bytes]
/// ```
///
/// This module provides encode/decode for that dictionary wire format.
/// It is used by `segment.rs` when building and reading the footer.
///
/// # Why not varint for the id?
///
/// Symbol ids are densely allocated starting at 0, so most are small, but
/// the fixed u32 keeps the format simple and self-describing — a stranger
/// reading FORMAT.md can decode the footer without knowing varint rules.
use std::collections::BTreeMap;

use crate::types::SymbolId;
use crate::Error;

/// Encodes a `SymbolId → name` dictionary into `buf`.
///
/// Entries are written in ascending `id` order (BTreeMap iteration order).
/// Panics if any name is longer than 65,535 bytes — that limit is enforced
/// at symbol-intern time in the engine.
pub fn encode(dict: &BTreeMap<SymbolId, String>, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    for (id, name) in dict {
        let name_bytes = name.as_bytes();
        assert!(
            name_bytes.len() <= u16::MAX as usize,
            "symbol name too long: {} bytes",
            name_bytes.len()
        );
        buf.extend_from_slice(&id.to_le_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
    }
}

/// Decodes a symbol dictionary from `src`, returning `(dict, bytes_consumed)`.
///
/// Returns `Error::Truncated` if `src` is too short.
/// Returns `Error::Truncated` (reused as "invalid utf8") if a name is not
/// valid UTF-8 — corrupt files must never cause panics.
pub fn decode(src: &[u8]) -> Result<(BTreeMap<SymbolId, String>, usize), Error> {
    if src.len() < 4 {
        return Err(Error::Truncated {
            expected: 4 - src.len(),
        });
    }
    let count = u32::from_le_bytes(
        src[..4]
            .try_into()
            .map_err(|_| Error::Truncated { expected: 4 })?,
    ) as usize;
    let mut pos = 4usize;

    // Allocation guard: each entry requires at least 4 (id) + 2 (len) + 0 (name) = 6 bytes.
    if count > src.len() / 6 + 1 {
        return Err(Error::AllocationGuard {
            declared: count,
            remaining: src.len(),
        });
    }

    let mut dict = BTreeMap::new();

    for _ in 0..count {
        if pos + 6 > src.len() {
            return Err(Error::Truncated {
                expected: pos + 6 - src.len(),
            });
        }
        let id = u32::from_le_bytes(
            src[pos..pos + 4]
                .try_into()
                .map_err(|_| Error::Truncated { expected: 4 })?,
        );
        let name_len = u16::from_le_bytes(
            src[pos + 4..pos + 6]
                .try_into()
                .map_err(|_| Error::Truncated { expected: 2 })?,
        ) as usize;
        pos += 6;

        if pos + name_len > src.len() {
            return Err(Error::Truncated {
                expected: pos + name_len - src.len(),
            });
        }
        let name = std::str::from_utf8(&src[pos..pos + name_len])
            .map_err(|_| Error::Truncated { expected: 0 })?
            .to_owned();
        pos += name_len;

        dict.insert(id, name);
    }

    Ok((dict, pos))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dict(pairs: &[(u32, &str)]) -> BTreeMap<SymbolId, String> {
        pairs
            .iter()
            .map(|&(id, name)| (id, name.to_owned()))
            .collect()
    }

    #[test]
    fn empty_dict() {
        let dict = BTreeMap::new();
        let mut buf = Vec::new();
        encode(&dict, &mut buf);
        // 4 bytes for count=0
        assert_eq!(buf.len(), 4);
        let (decoded, consumed) = decode(&buf).unwrap();
        assert!(decoded.is_empty());
        assert_eq!(consumed, 4);
    }

    #[test]
    fn single_entry() {
        let dict = make_dict(&[(1, "BTCUSDT")]);
        let mut buf = Vec::new();
        encode(&dict, &mut buf);
        // 4 (count) + 4 (id) + 2 (name_len) + 7 (BTCUSDT) = 17
        assert_eq!(buf.len(), 17);
        let (decoded, _) = decode(&buf).unwrap();
        assert_eq!(decoded.get(&1).unwrap(), "BTCUSDT");
    }

    #[test]
    fn multi_entry_roundtrip() {
        let dict = make_dict(&[(1, "BTCUSDT"), (2, "ETHUSDT"), (100, "BNBUSDT")]);
        let mut buf = Vec::new();
        encode(&dict, &mut buf);
        let (decoded, consumed) = decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.get(&1).unwrap(), "BTCUSDT");
        assert_eq!(decoded.get(&100).unwrap(), "BNBUSDT");
    }

    #[test]
    fn decode_with_trailing_bytes() {
        // decode should stop at the end of the dict, not consume trailing bytes
        let dict = make_dict(&[(5, "SYM")]);
        let mut buf = Vec::new();
        encode(&dict, &mut buf);
        buf.extend_from_slice(b"junk");
        let (decoded, consumed) = decode(&buf).unwrap();
        assert_eq!(decoded.get(&5).unwrap(), "SYM");
        assert_eq!(consumed, buf.len() - 4); // junk not consumed
    }

    #[test]
    fn decode_truncated_returns_err() {
        let dict = make_dict(&[(1, "BTCUSDT")]);
        let mut buf = Vec::new();
        encode(&dict, &mut buf);
        // Remove last byte
        buf.pop();
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn decode_arbitrary_no_panic() {
        let _ = decode(b"\xFF\xFF\xFF\xFF\x00\x01\x02\x03\x04\x05");
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_roundtrip(
            pairs in proptest::collection::vec(
                (0u32..10000, "[A-Z]{3,8}"),
                0..64
            )
        ) {
            let dict: BTreeMap<SymbolId, String> = pairs.into_iter().collect();
            let mut buf = Vec::new();
            encode(&dict, &mut buf);
            let (decoded, consumed) = decode(&buf).unwrap();
            prop_assert_eq!(consumed, buf.len());
            prop_assert_eq!(decoded, dict);
        }

        #[test]
        fn prop_decode_no_panic(data: Vec<u8>) {
            let _ = decode(&data);
        }
    }
}
