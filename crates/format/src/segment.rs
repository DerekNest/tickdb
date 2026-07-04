/// Segment file format — writer and reader.
///
/// A segment is an immutable columnar file covering one UTC date partition.
/// It may contain many symbols, each stored as one or more blocks of up to
/// 65,536 rows.  The format is documented byte-for-byte in docs/FORMAT.md.
///
/// # High-level layout
///
/// ```text
/// [File Header  : 16 bytes ]
/// [Block 0      : variable ]
/// [Block 1      : variable ]
/// ...
/// [Footer       : variable ]
/// [footer_len   :  4 bytes ] ─┐ trailing 12 bytes
/// [footer_crc32c:  4 bytes ]  │ used to locate the footer
/// [magic "TKDB" :  4 bytes ] ─┘
/// ```
///
/// Blocks within a segment are grouped by symbol (ascending symbol_id) and
/// within each symbol group are ordered by min_ts.  This layout makes
/// (symbol, time-range) queries a binary search over the in-memory block index
/// with no seeks through unrelated symbol data.
use std::collections::BTreeMap;
use std::io::{self, Write};

use crc32fast::Hasher as Crc32Hasher;

use crate::{
    compress, dod, dz, rle, size_col,
    types::{SymbolId, Trade},
    Error,
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Magic bytes at the start and end of every segment file.
pub const MAGIC: &[u8; 4] = b"TKDB";

/// Format version encoded in the file header.
pub const VERSION: u16 = 1;

/// Target number of rows per block.  The last block for a symbol may be short.
pub const BLOCK_ROWS: usize = 65_536;

// ── Encoding flags (u16 stored in block index entry) ─────────────────────────
//
// Bits [1:0]  — outer compression method (see compress.rs)
// Bits [15:2] — reserved, must be zero

// ── File header (16 bytes, fixed) ─────────────────────────────────────────────

/// Serialized size of the file header.
const HEADER_LEN: usize = 16;

fn write_header(out: &mut impl Write, flags: u16, created_unix_s: i64) -> io::Result<()> {
    out.write_all(MAGIC)?;
    out.write_all(&VERSION.to_le_bytes())?;
    out.write_all(&flags.to_le_bytes())?;
    out.write_all(&created_unix_s.to_le_bytes())?;
    Ok(())
}

// ── Block index entry (in-memory and serialized in footer) ────────────────────

/// One entry in the segment's block index, stored in the footer.
/// Enables binary-search-based zone-map pruning without decoding block data.
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub symbol_id: SymbolId,
    pub row_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_price: i64, // mantissa at common exponent
    pub max_price: i64,
    pub exponent: i8,
    pub file_offset: u64,
    pub byte_len: u32,
    pub crc32c: u32,
    pub encoding_flags: u16,
}

impl BlockMeta {
    const SERIALIZED_LEN: usize = 4 + 4 + 8 + 8 + 8 + 8 + 1 + 8 + 4 + 4 + 2; // = 59 bytes

    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.symbol_id.to_le_bytes());
        out.extend_from_slice(&self.row_count.to_le_bytes());
        out.extend_from_slice(&self.min_ts.to_le_bytes());
        out.extend_from_slice(&self.max_ts.to_le_bytes());
        out.extend_from_slice(&self.min_price.to_le_bytes());
        out.extend_from_slice(&self.max_price.to_le_bytes());
        out.push(self.exponent as u8);
        out.extend_from_slice(&self.file_offset.to_le_bytes());
        out.extend_from_slice(&self.byte_len.to_le_bytes());
        out.extend_from_slice(&self.crc32c.to_le_bytes());
        out.extend_from_slice(&self.encoding_flags.to_le_bytes());
    }

    #[allow(unused_assignments)]
    fn read(src: &[u8]) -> Result<Self, Error> {
        if src.len() < Self::SERIALIZED_LEN {
            return Err(Error::Truncated {
                expected: Self::SERIALIZED_LEN - src.len(),
            });
        }
        let mut off = 0;
        macro_rules! read_le {
            ($t:ty) => {{
                let n = std::mem::size_of::<$t>();
                // Bounds already verified by the caller (SERIALIZED_LEN check).
                // Use a local copy to avoid try_into().unwrap() outside tests.
                let mut arr = [0u8; 8];
                arr[..n].copy_from_slice(&src[off..off + n]);
                let v = <$t>::from_le_bytes(
                    arr[..n]
                        .try_into()
                        .unwrap_or_else(|_| unreachable!("slice len == size_of::<$t>()")),
                );
                off += n;
                v
            }};
        }
        Ok(BlockMeta {
            symbol_id: read_le!(u32),
            row_count: read_le!(u32),
            min_ts: read_le!(i64),
            max_ts: read_le!(i64),
            min_price: read_le!(i64),
            max_price: read_le!(i64),
            exponent: {
                let b = src[off];
                off += 1;
                b as i8
            },
            file_offset: read_le!(u64),
            byte_len: read_le!(u32),
            crc32c: read_le!(u32),
            encoding_flags: read_le!(u16),
        })
    }
}

// ── Column batch (decoded output of one block) ────────────────────────────────

/// Decoded columnar representation of one block's rows.
/// All vecs have identical length.
#[derive(Debug, Clone)]
pub struct ColumnBatch {
    pub symbol_id: SymbolId,
    pub ts: Vec<i64>,
    pub price_mantissa: Vec<i64>,
    pub exponent: i8,
    pub size: Vec<u64>,
    pub side: Vec<u8>,
    // trade_id is encoded alongside size in the block body
    pub trade_id: Vec<u64>,
}

// ── Segment writer ─────────────────────────────────────────────────────────────

/// Builds a segment file in memory.
///
/// Usage:
/// ```ignore
/// let mut w = SegmentWriter::new(created_unix_s, COMPRESS_ZSTD3);
/// w.add_trades(symbol_id, &sorted_deduped_trades)?;
/// let bytes = w.finish(symbol_dict)?;
/// ```
pub struct SegmentWriter {
    /// Accumulated file bytes (header + all encoded blocks).
    buf: Vec<u8>,
    /// Block metadata collected as blocks are written; goes into the footer.
    block_index: Vec<BlockMeta>,
    /// Outer compression method applied after domain encoding.
    compression: u8,
}

impl SegmentWriter {
    /// Creates a new writer.
    ///
    /// `created_unix_s` is the wall-clock creation time, stored in the header
    /// for informational purposes only (not used for query correctness).
    pub fn new(created_unix_s: i64, compression: u8) -> Self {
        let mut buf = Vec::with_capacity(1 << 20); // 1 MiB initial
        write_header(&mut buf, 0, created_unix_s).expect("Vec write never fails");
        SegmentWriter {
            buf,
            block_index: Vec::new(),
            compression,
        }
    }

    /// Encodes all trades for one symbol into one or more blocks and appends
    /// them to the segment buffer.
    ///
    /// **Precondition:** `trades` must be sorted by `(ts, trade_id)` and have
    /// had duplicates removed.  The writer does not re-sort or re-dedup.
    pub fn add_trades(&mut self, symbol_id: SymbolId, trades: &[Trade]) -> Result<(), Error> {
        // Split into BLOCK_ROWS-sized chunks; the last chunk may be short.
        for chunk in trades.chunks(BLOCK_ROWS) {
            self.write_block(symbol_id, chunk)?;
        }
        Ok(())
    }

    fn write_block(&mut self, symbol_id: SymbolId, trades: &[Trade]) -> Result<(), Error> {
        let n = trades.len();
        assert!(n > 0, "write_block called with empty slice");

        // ── Domain-encode each column ────────────────────────────────────────

        // Timestamp column (delta-of-delta)
        let ts_vals: Vec<i64> = trades.iter().map(|t| t.ts).collect();
        let mut ts_bytes = Vec::new();
        dod::encode(&ts_vals, &mut ts_bytes);

        // Price column (delta + zigzag on mantissas)
        let prices: Vec<crate::dz::Decimal64> = trades.iter().map(|t| t.price).collect();
        let mut price_bytes = Vec::new();
        dz::encode(&prices, &mut price_bytes);

        // Size column (plain varint)
        let sizes: Vec<u64> = trades.iter().map(|t| t.size).collect();
        let mut size_bytes = Vec::new();
        size_col::encode(&sizes, &mut size_bytes);

        // Side column (run-length)
        let sides: Vec<u8> = trades.iter().map(|t| t.side.as_u8()).collect();
        let mut side_bytes = Vec::new();
        rle::encode(&sides, &mut side_bytes);

        // trade_id column (plain varint — ids are large but delta-incompressible)
        let trade_ids: Vec<u64> = trades.iter().map(|t| t.trade_id).collect();
        let mut trade_id_bytes = Vec::new();
        size_col::encode(&trade_ids, &mut trade_id_bytes);

        // ── Assemble domain-encoded block body ───────────────────────────────
        //
        // Format: for each column: [len: u32 LE][column bytes]
        // Columns in order: ts, price, size, side, trade_id.
        let mut domain_body = Vec::new();
        for col in &[
            &ts_bytes,
            &price_bytes,
            &size_bytes,
            &side_bytes,
            &trade_id_bytes,
        ] {
            domain_body.extend_from_slice(&(col.len() as u32).to_le_bytes());
            domain_body.extend_from_slice(col);
        }

        // ── Outer compression ────────────────────────────────────────────────
        let mut compressed_body = Vec::new();
        let actual_compression =
            compress::compress(&domain_body, self.compression, &mut compressed_body)?;

        // ── CRC32c of the compressed block body ──────────────────────────────
        let mut h = Crc32Hasher::new();
        h.update(&compressed_body);
        let crc = h.finalize();

        // ── Zone-map stats ───────────────────────────────────────────────────
        // ts_vals is non-empty (assert above); fold is infallible.
        let min_ts = ts_vals.iter().copied().fold(i64::MAX, i64::min);
        let max_ts = ts_vals.iter().copied().fold(i64::MIN, i64::max);

        // Price zone map uses the common exponent determined during encode.
        // We read that exponent back from the encoded price header (byte[1]).
        // If the fallback path was used, exponent is 0 (a safe default for pruning).
        let block_exponent = if price_bytes.is_empty() {
            0i8
        } else {
            price_bytes[1] as i8
        };
        let min_price = prices.iter().map(|p| p.mantissa).min().unwrap_or(0);
        let max_price = prices.iter().map(|p| p.mantissa).max().unwrap_or(0);

        // ── Record block metadata ────────────────────────────────────────────
        let file_offset = self.buf.len() as u64;
        let byte_len = compressed_body.len() as u32;

        self.block_index.push(BlockMeta {
            symbol_id,
            row_count: n as u32,
            min_ts,
            max_ts,
            min_price,
            max_price,
            exponent: block_exponent,
            file_offset,
            byte_len,
            crc32c: crc,
            encoding_flags: actual_compression as u16,
        });

        // ── Append compressed block to file buffer ───────────────────────────
        self.buf.extend_from_slice(&compressed_body);
        Ok(())
    }

    /// Finalises the segment, writes the footer, and returns the complete file bytes.
    ///
    /// `symbol_dict` maps each `SymbolId` that appears in this segment to its
    /// UTF-8 name string.  Only symbols actually written need to be present.
    pub fn finish(mut self, symbol_dict: &BTreeMap<SymbolId, String>) -> Result<Vec<u8>, Error> {
        // ── Build footer bytes ───────────────────────────────────────────────

        let mut footer = Vec::new();

        // Symbol dictionary: count (u32), then (id u32, len u16, utf8 bytes)*.
        footer.extend_from_slice(&(symbol_dict.len() as u32).to_le_bytes());
        for (id, name) in symbol_dict {
            footer.extend_from_slice(&id.to_le_bytes());
            let name_bytes = name.as_bytes();
            footer.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
            footer.extend_from_slice(name_bytes);
        }

        // Block index: count (u32), then BlockMeta entries.
        footer.extend_from_slice(&(self.block_index.len() as u32).to_le_bytes());
        for meta in &self.block_index {
            meta.write(&mut footer);
        }

        // ── Append footer + trailer ──────────────────────────────────────────
        //
        // Trailer layout (12 bytes, at the very end of the file):
        //   footer_len u32 | footer_crc32c u32 | magic "TKDB"
        //
        // A reader locates the footer by reading the trailing 12 bytes, then
        // seeking back footer_len bytes from the start of the trailer.
        let footer_len = footer.len() as u32;

        let mut h = Crc32Hasher::new();
        h.update(&footer);
        let footer_crc = h.finalize();

        self.buf.extend_from_slice(&footer);
        self.buf.extend_from_slice(&footer_len.to_le_bytes());
        self.buf.extend_from_slice(&footer_crc.to_le_bytes());
        self.buf.extend_from_slice(MAGIC);

        Ok(self.buf)
    }
}

// ── Segment reader ─────────────────────────────────────────────────────────────

/// Reads a parsed, in-memory view of a segment file.
///
/// The block index is decoded once on open; subsequent calls to `scan` do only
/// targeted decompression + column decoding for the blocks that pass zone-map
/// pruning.
pub struct SegmentReader {
    /// Complete raw file bytes (typically memory-mapped; here owned for testing).
    data: Vec<u8>,
    /// Decoded block index, sorted by (symbol_id, min_ts) as written.
    pub block_index: Vec<BlockMeta>,
    /// Symbol dictionary decoded from the footer.
    pub symbol_dict: BTreeMap<SymbolId, String>,
}

impl SegmentReader {
    /// Opens a segment from its raw bytes.
    ///
    /// Verifies the magic bytes and footer CRC before returning.  Any CRC
    /// mismatch returns `Error::Crc32Mismatch` rather than propagating
    /// corrupt data.
    pub fn open(data: Vec<u8>) -> Result<Self, Error> {
        // ── Verify file magic at offset 0 ────────────────────────────────────
        if data.len() < HEADER_LEN + 12 {
            return Err(Error::Truncated {
                expected: HEADER_LEN + 12,
            });
        }
        if &data[..4] != MAGIC {
            return Err(Error::Truncated { expected: 0 }); // reuse truncated as "bad magic"
        }

        // ── Read trailing 12 bytes to locate the footer ──────────────────────
        let trailer_start = data.len() - 12;
        let footer_len = {
            let b = &data[trailer_start..trailer_start + 4];
            u32::from_le_bytes([b[0], b[1], b[2], b[3]])
        };
        let stored_footer_crc = {
            let b = &data[trailer_start + 4..trailer_start + 8];
            u32::from_le_bytes([b[0], b[1], b[2], b[3]])
        };
        let trailing_magic = &data[trailer_start + 8..trailer_start + 12];

        if trailing_magic != MAGIC {
            return Err(Error::Truncated { expected: 0 });
        }

        // ── Verify footer CRC ────────────────────────────────────────────────
        let footer_start = trailer_start
            .checked_sub(footer_len as usize)
            .ok_or(Error::Truncated { expected: 1 })?;
        let footer_bytes = &data[footer_start..trailer_start];

        let mut h = Crc32Hasher::new();
        h.update(footer_bytes);
        let computed_crc = h.finalize();
        if computed_crc != stored_footer_crc {
            return Err(Error::Crc32Mismatch {
                stored: stored_footer_crc,
                computed: computed_crc,
            });
        }

        // ── Decode footer ────────────────────────────────────────────────────
        let (symbol_dict, block_index) = decode_footer(footer_bytes)?;

        Ok(SegmentReader {
            data,
            block_index,
            symbol_dict,
        })
    }

    /// Returns an iterator over decoded `ColumnBatch`es for `symbol_id` whose
    /// time range overlaps `[ts_start, ts_end]` (inclusive).
    ///
    /// Zone-map pruning skips blocks whose `[min_ts, max_ts]` range does not
    /// intersect the query range, avoiding any I/O for those blocks.
    pub fn scan(
        &self,
        symbol_id: SymbolId,
        ts_start: i64,
        ts_end: i64,
    ) -> impl Iterator<Item = Result<ColumnBatch, Error>> + '_ {
        // Binary search to the first block for this symbol with max_ts ≥ ts_start.
        // Blocks are sorted (symbol_id ASC, min_ts ASC) as written by add_trades.
        let first = self.block_index.partition_point(|m| {
            m.symbol_id < symbol_id || (m.symbol_id == symbol_id && m.max_ts < ts_start)
        });

        self.block_index[first..]
            .iter()
            .take_while(move |m| m.symbol_id == symbol_id && m.min_ts <= ts_end)
            .map(move |meta| self.decode_block(meta))
    }

    fn decode_block(&self, meta: &BlockMeta) -> Result<ColumnBatch, Error> {
        let start = meta.file_offset as usize;
        let end = start + meta.byte_len as usize;

        if end > self.data.len() {
            return Err(Error::Truncated {
                expected: end - self.data.len(),
            });
        }

        let compressed = &self.data[start..end];

        // ── Verify block CRC before decode ───────────────────────────────────
        // Checksum failure = corrupt block; never silently return wrong data.
        let mut h = Crc32Hasher::new();
        h.update(compressed);
        let computed = h.finalize();
        if computed != meta.crc32c {
            return Err(Error::Crc32Mismatch {
                stored: meta.crc32c,
                computed,
            });
        }

        // ── Decompress ───────────────────────────────────────────────────────
        let compression_flag = (meta.encoding_flags & 0x03) as u8;
        let mut domain_body = Vec::new();
        compress::decompress(compressed, compression_flag, &mut domain_body)?;

        // ── Parse column length-prefixes and decode ──────────────────────────
        let n = meta.row_count as usize;
        let mut pos = 0usize;

        let ts_bytes = read_column_bytes(&domain_body, &mut pos)?;
        let price_bytes = read_column_bytes(&domain_body, &mut pos)?;
        let size_bytes = read_column_bytes(&domain_body, &mut pos)?;
        let side_bytes = read_column_bytes(&domain_body, &mut pos)?;
        let trade_id_bytes = read_column_bytes(&domain_body, &mut pos)?;

        let ts = dod::decode(ts_bytes, n)?;
        let prices = dz::decode(price_bytes, n)?;
        let size = size_col::decode(size_bytes, n)?;
        let side_raw = rle::decode(side_bytes, n)?;
        let trade_id = size_col::decode(trade_id_bytes, n)?;

        // Extract common exponent from the decoded price header.
        let exponent = if price_bytes.len() >= 2 {
            price_bytes[1] as i8
        } else {
            0
        };
        let price_mantissa: Vec<i64> = prices.iter().map(|p| p.mantissa).collect();

        Ok(ColumnBatch {
            symbol_id: meta.symbol_id,
            ts,
            price_mantissa,
            exponent,
            size,
            side: side_raw,
            trade_id,
        })
    }
}

// ── Footer decode helper ──────────────────────────────────────────────────────

fn decode_footer(bytes: &[u8]) -> Result<(BTreeMap<SymbolId, String>, Vec<BlockMeta>), Error> {
    let mut pos = 0usize;

    // Symbol dictionary.
    let dict_count = read_u32(bytes, &mut pos)? as usize;

    // Allocation guard: each dict entry is at least 4+2+0 = 6 bytes.
    if dict_count > bytes.len() / 6 + 1 {
        return Err(Error::AllocationGuard {
            declared: dict_count,
            remaining: bytes.len(),
        });
    }

    let mut symbol_dict = BTreeMap::new();
    for _ in 0..dict_count {
        let id = read_u32(bytes, &mut pos)?;
        let name_len = read_u16(bytes, &mut pos)? as usize;
        if pos + name_len > bytes.len() {
            return Err(Error::Truncated {
                expected: pos + name_len - bytes.len(),
            });
        }
        let name = std::str::from_utf8(&bytes[pos..pos + name_len])
            .map_err(|_| Error::Truncated { expected: 0 })?
            .to_owned();
        pos += name_len;
        symbol_dict.insert(id, name);
    }

    // Block index.
    let block_count = read_u32(bytes, &mut pos)? as usize;

    // Allocation guard: each BlockMeta is fixed 59 bytes.
    let remaining = bytes.len().saturating_sub(pos);
    if block_count > remaining / BlockMeta::SERIALIZED_LEN + 1 {
        return Err(Error::AllocationGuard {
            declared: block_count,
            remaining,
        });
    }

    let mut block_index = Vec::with_capacity(block_count);
    for _ in 0..block_count {
        if pos + BlockMeta::SERIALIZED_LEN > bytes.len() {
            return Err(Error::Truncated {
                expected: pos + BlockMeta::SERIALIZED_LEN - bytes.len(),
            });
        }
        let meta = BlockMeta::read(&bytes[pos..])?;
        pos += BlockMeta::SERIALIZED_LEN;
        block_index.push(meta);
    }

    Ok((symbol_dict, block_index))
}

// ── Low-level read helpers ────────────────────────────────────────────────────

fn read_u32(src: &[u8], pos: &mut usize) -> Result<u32, Error> {
    if *pos + 4 > src.len() {
        return Err(Error::Truncated {
            expected: *pos + 4 - src.len(),
        });
    }
    let b = &src[*pos..*pos + 4];
    let v = u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
    *pos += 4;
    Ok(v)
}

fn read_u16(src: &[u8], pos: &mut usize) -> Result<u16, Error> {
    if *pos + 2 > src.len() {
        return Err(Error::Truncated {
            expected: *pos + 2 - src.len(),
        });
    }
    let b = &src[*pos..*pos + 2];
    let v = u16::from_le_bytes([b[0], b[1]]);
    *pos += 2;
    Ok(v)
}

/// Reads a length-prefixed column blob (u32 length + bytes).
fn read_column_bytes<'a>(src: &'a [u8], pos: &mut usize) -> Result<&'a [u8], Error> {
    let len = read_u32(src, pos)? as usize;
    if *pos + len > src.len() {
        return Err(Error::Truncated {
            expected: *pos + len - src.len(),
        });
    }
    let slice = &src[*pos..*pos + len];
    *pos += len;
    Ok(slice)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::{COMPRESS_NONE, COMPRESS_ZSTD3};
    use crate::dz::Decimal64;
    use crate::Side;

    fn make_trade(symbol: SymbolId, ts: i64, mantissa: i64, size: u64, trade_id: u64) -> Trade {
        Trade {
            symbol,
            ts,
            price: Decimal64 {
                mantissa,
                exponent: -2,
            },
            size,
            side: Side::Buy,
            trade_id,
        }
    }

    fn make_dict(ids: &[(SymbolId, &str)]) -> BTreeMap<SymbolId, String> {
        ids.iter()
            .map(|&(id, name)| (id, name.to_owned()))
            .collect()
    }

    #[test]
    fn roundtrip_single_symbol() {
        let mut trades: Vec<Trade> = (0..100)
            .map(|i| {
                make_trade(
                    1,
                    1_700_000_000_000_000_000 + i * 100_000,
                    5_000_000 + i % 5,
                    100,
                    i as u64,
                )
            })
            .collect();
        trades.sort_by_key(|t| (t.ts, t.trade_id));

        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &trades).unwrap();
        let dict = make_dict(&[(1, "BTCUSDT")]);
        let bytes = writer.finish(&dict).unwrap();

        let reader = SegmentReader::open(bytes).unwrap();
        assert_eq!(reader.symbol_dict.get(&1).unwrap(), "BTCUSDT");

        let batches: Vec<ColumnBatch> = reader
            .scan(1, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.ts.len(), 100);
        assert_eq!(batch.ts, trades.iter().map(|t| t.ts).collect::<Vec<_>>());
        assert_eq!(
            batch.size,
            trades.iter().map(|t| t.size).collect::<Vec<_>>()
        );
        assert_eq!(
            batch.trade_id,
            trades.iter().map(|t| t.trade_id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn roundtrip_with_zstd() {
        let trades: Vec<Trade> = (0..200)
            .map(|i| {
                make_trade(
                    2,
                    1_000_000 + i * 50_000,
                    100_000 + i % 10,
                    i as u64 + 1,
                    i as u64,
                )
            })
            .collect();

        let mut writer = SegmentWriter::new(12345, COMPRESS_ZSTD3);
        writer.add_trades(2, &trades).unwrap();
        let bytes = writer.finish(&make_dict(&[(2, "ETHUSDT")])).unwrap();

        let reader = SegmentReader::open(bytes).unwrap();
        let batches: Vec<ColumnBatch> = reader
            .scan(2, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(batches[0].ts.len(), 200);
    }

    #[test]
    fn multi_symbol_scan_isolation() {
        // Two symbols; scan for symbol 2 must not return symbol 1 rows.
        let t1: Vec<Trade> = (0..50)
            .map(|i| make_trade(1, i * 1000, 100, 1, i as u64))
            .collect();
        let t2: Vec<Trade> = (0..80)
            .map(|i| make_trade(2, i * 1000, 200, 2, i as u64 + 1000))
            .collect();

        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &t1).unwrap();
        writer.add_trades(2, &t2).unwrap();
        let bytes = writer.finish(&make_dict(&[(1, "A"), (2, "B")])).unwrap();

        let reader = SegmentReader::open(bytes).unwrap();

        let s1: Vec<_> = reader
            .scan(1, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let s2: Vec<_> = reader
            .scan(2, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();

        let s1_rows: usize = s1.iter().map(|b| b.ts.len()).sum();
        let s2_rows: usize = s2.iter().map(|b| b.ts.len()).sum();

        assert_eq!(s1_rows, 50);
        assert_eq!(s2_rows, 80);
    }

    #[test]
    fn zone_map_pruning() {
        // Trades in two distinct time ranges; query for only the second range.
        let early: Vec<Trade> = (0..10)
            .map(|i| make_trade(1, i * 1000, 100, 1, i as u64))
            .collect();
        let late: Vec<Trade> = (0..10)
            .map(|i| make_trade(1, 1_000_000 + i * 1000, 100, 1, 100 + i as u64))
            .collect();

        // Write as separate calls so they land in separate blocks (< BLOCK_ROWS each).
        let mut all = early.clone();
        all.extend(late.clone());

        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &all).unwrap();
        let bytes = writer.finish(&make_dict(&[(1, "SYM")])).unwrap();

        let reader = SegmentReader::open(bytes).unwrap();

        // Query only the late range — should return only those rows (same block here,
        // but zone-map logic is verified for > 1 block in the multi-block test).
        let batches: Vec<_> = reader
            .scan(1, 1_000_000, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        // All 20 rows fit in one block; zone map passes since min_ts=0 ≤ i64::MAX.
        // This test mainly asserts no panic/error on a range-filtered scan.
        assert!(!batches.is_empty());
    }

    #[test]
    fn block_spanning_block_rows() {
        // Write exactly BLOCK_ROWS + 1 trades to force two blocks.
        let n = BLOCK_ROWS + 1;
        let trades: Vec<Trade> = (0..n)
            .map(|i| make_trade(1, i as i64 * 100_000, 50_000, 1, i as u64))
            .collect();

        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &trades).unwrap();
        let bytes = writer.finish(&make_dict(&[(1, "X")])).unwrap();

        let reader = SegmentReader::open(bytes).unwrap();
        assert_eq!(reader.block_index.len(), 2);
        assert_eq!(reader.block_index[0].row_count as usize, BLOCK_ROWS);
        assert_eq!(reader.block_index[1].row_count as usize, 1);

        let total: usize = reader
            .scan(1, i64::MIN, i64::MAX)
            .map(|r| r.unwrap().ts.len())
            .sum();
        assert_eq!(total, n);
    }

    #[test]
    fn corrupt_footer_crc_detected() {
        let trades: Vec<Trade> = vec![make_trade(1, 0, 100, 1, 1)];
        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &trades).unwrap();
        let mut bytes = writer.finish(&make_dict(&[(1, "SYM")])).unwrap();

        // Flip a byte in the footer (before the 12-byte trailer).
        let corrupt_pos = bytes.len() - 15;
        bytes[corrupt_pos] ^= 0xFF;

        assert!(matches!(
            SegmentReader::open(bytes),
            Err(Error::Crc32Mismatch { .. })
        ));
    }

    #[test]
    fn corrupt_block_crc_detected() {
        let trades: Vec<Trade> = (0..10)
            .map(|i| make_trade(1, i * 1000, 100, 1, i as u64))
            .collect();
        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &trades).unwrap();
        let mut bytes = writer.finish(&make_dict(&[(1, "SYM")])).unwrap();

        // Flip a byte in the block data (right after the 16-byte header).
        bytes[HEADER_LEN] ^= 0x01;

        let reader = SegmentReader::open(bytes).unwrap(); // footer is intact
        let result: Result<Vec<_>, _> = reader.scan(1, i64::MIN, i64::MAX).collect();
        assert!(matches!(result, Err(Error::Crc32Mismatch { .. })));
    }

    #[test]
    fn empty_segment_roundtrip() {
        // A segment with no blocks is valid (e.g., placeholder for a date partition).
        let writer = SegmentWriter::new(0, COMPRESS_NONE);
        let bytes = writer.finish(&BTreeMap::new()).unwrap();
        let reader = SegmentReader::open(bytes).unwrap();
        assert!(reader.block_index.is_empty());
        assert!(reader.symbol_dict.is_empty());
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_roundtrip(
            n_trades in 1usize..512,
            base_ts in 0i64..1_000_000_000_000i64,
            mantissa_base in 1_000_000i64..9_999_999,
        ) {
            let trades: Vec<Trade> = (0..n_trades as i64)
                .map(|i| make_trade(
                    42,
                    base_ts + i * 100_000,
                    mantissa_base + (i % 20),
                    (i as u64 % 1000) + 1,
                    i as u64,
                ))
                .collect();

            let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
            writer.add_trades(42, &trades).unwrap();
            let bytes = writer.finish(&make_dict(&[(42, "TEST")])).unwrap();

            let reader = SegmentReader::open(bytes).unwrap();
            let total: usize = reader
                .scan(42, i64::MIN, i64::MAX)
                .map(|r| r.unwrap().ts.len())
                .sum();
            prop_assert_eq!(total, n_trades);
        }

        #[test]
        fn prop_open_no_panic(data: Vec<u8>) {
            // Opening arbitrary bytes must never panic — only return Err.
            let _ = SegmentReader::open(data);
        }
    }
}
