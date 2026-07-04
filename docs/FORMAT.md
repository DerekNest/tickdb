# tickdb — Byte-Level File Format Reference

All multi-byte integers are **little-endian**. All checksums are **crc32c** (Castagnoli polynomial) computed via `crc32fast`. Strings are UTF-8 with no null terminator.

---

## 1. Segment file (`.tkdb`)

A segment is an **immutable** columnar file covering one UTC date partition and zero or more symbols.

### 1.1 Overall layout

```
Offset 0          : File Header (16 bytes, fixed)
Offset 16         : Block 0 (variable)
                    Block 1
                    ...
                    Block N-1
Offset F          : Footer (variable)
Offset F+footer_len : footer_len  u32  (4 bytes)
                      footer_crc32c u32 (4 bytes)
                      magic "TKDB"   4 bytes
                   = trailing 12 bytes used to locate the footer
```

A reader locates the footer by:
1. Seeking to `EOF - 12`.
2. Reading `footer_len` (u32 LE) and `footer_crc32c` (u32 LE).
3. Verifying the trailing 4 bytes equal `"TKDB"`.
4. Seeking back `footer_len` bytes from the start of the trailer.
5. Reading `footer_len` bytes and verifying `crc32c(footer_bytes) == footer_crc32c`.
6. Decoding the footer.

### 1.2 File header (16 bytes)

```
Bytes  0– 3  magic        u8[4]   "TKDB"
Bytes  4– 5  version      u16     1
Bytes  6– 7  flags        u16     reserved, must be 0
Bytes  8–15  created_unix_s i64   wall-clock creation time (informational only)
```

### 1.3 Block body

Each block holds up to 65,536 rows of **a single symbol**. Its compressed bytes are stored contiguously in the data region; the block index in the footer records the file offset.

**Domain-encoded body** (before outer compression):

```
[ts_len:       u32 LE] [ts_bytes:       ts_len  bytes]
[price_len:    u32 LE] [price_bytes:    price_len bytes]
[size_len:     u32 LE] [size_bytes:     size_len  bytes]
[side_len:     u32 LE] [side_bytes:     side_len  bytes]
[trade_id_len: u32 LE] [trade_id_bytes: trade_id_len bytes]
```

Column encodings:

| Column     | Encoding                                  |
|------------|-------------------------------------------|
| `ts`       | delta-of-delta + zigzag + LEB128 varint   |
| `price`    | delta + zigzag + LEB128 varint (mantissa) |
| `size`     | plain LEB128 varint                       |
| `side`     | run-length: `(value u8, run LEB128)*`     |
| `trade_id` | plain LEB128 varint                       |

`symbol` is not stored in the block body — it is the block's identity (recorded once in the block index entry).

**Outer compression** wraps the entire domain-encoded body:

| `encoding_flags` bits [1:0] | Method           |
|-----------------------------|------------------|
| `0b00`                      | None (raw)       |
| `0b01`                      | LZ4 (reserved)   |
| `0b10`                      | zstd level 3     |

The CRC32c in the block index entry covers the **compressed** bytes exactly as stored on disk.

### 1.4 Footer

```
[dict_count: u32]
  for each symbol in the dictionary:
    [id:       u32]
    [name_len: u16]
    [name:     name_len utf-8 bytes]

[block_count: u32]
  for each block (59 bytes each):
    [symbol_id:      u32]
    [row_count:      u32]
    [min_ts:         i64]   zone map lower bound (inclusive)
    [max_ts:         i64]   zone map upper bound (inclusive)
    [min_price:      i64]   mantissa at block exponent
    [max_price:      i64]   mantissa at block exponent
    [exponent:       i8]    shared price exponent for this block
    [file_offset:    u64]   byte offset of the compressed block body
    [byte_len:       u32]   byte length of the compressed block body
    [crc32c:         u32]   crc32c of the compressed block body
    [encoding_flags: u16]   bits [1:0] = compression method; bits [15:2] reserved
```

**Block ordering within the footer index:** sorted by `(symbol_id ASC, min_ts ASC)`. This allows `(symbol_id, ts_range)` queries to binary-search directly into the block index.

---

## 2. WAL record format

The WAL (`WAL` file in the data directory) is a sequence of length-prefixed, CRC-framed records:

```
[len:    u32 LE]   byte length of payload
[crc32c: u32 LE]   crc32c of payload (not of the header itself)
[payload: bytes]   hand-rolled serialization of Vec<Trade> (see below)
```

### 2.1 Trade wire format (38 bytes, fixed-width, all LE)

```
Offset  0– 3  symbol    u32    dictionary id
Offset  4–11  ts        i64    nanoseconds since Unix epoch, UTC
Offset 12–19  mantissa  i64    price mantissa
Offset 20     exponent  i8     price exponent
Offset 21–28  size      u64    quantity in minor units
Offset 29     side      u8     0=buy 1=sell 2=unknown
Offset 30–37  trade_id  u64    source-assigned dedup key
              --------
              38 bytes total
```

### 2.2 Recovery contract

Recovery reads records from offset 0 until:
- **Clean EOF** (0 bytes available for the next header) — normal shutdown.
- **Truncated header** (< 8 bytes available) — crash mid-header write; drop and stop.
- **Truncated payload** (fewer bytes than `len` available) — crash mid-payload write; drop and stop.
- **CRC mismatch** — returns `Error::WalCrc`; the file is considered corrupt at this point.

Under `FsyncPolicy::EveryWrite`, all returned `ingest()` calls are durable: the WAL record was fsynced before the call returned, so no acked write can be lost.

---

## 3. Manifest (`MANIFEST`)

JSON array of segment metadata objects. Written via write-temp + atomic rename (`MANIFEST.tmp → MANIFEST`).

```json
[
  {
    "path":      "segments/2024-01-15/0.tkdb",
    "date":      "2024-01-15",
    "min_ts":    1705276800000000000,
    "max_ts":    1705363199999999999,
    "row_count": 48291042,
    "generation": 0
  }
]
```

`generation` is incremented by compaction. When multiple segments share the same `date`, the highest-generation segment is the compacted result; lower generations are candidates for deletion once no readers hold a reference to them.

---

## 4. Symbol table (`symbols.json`)

JSON object mapping UTF-8 symbol names to u32 ids. Written via write-temp + atomic rename (`symbols.json.tmp → symbols.json`). Node-global; shared across all date partitions.

```json
{
  "BTCUSDT": 1,
  "ETHUSDT": 2,
  "BNBUSDT": 3
}
```

---

## 5. Encoder details

### 5.1 LEB128 varint (unsigned)

Emit 7 bits per byte, LSB first. Set bit 7 (continuation bit) on all bytes except the last. A u64 fits in at most 10 bytes. Decoder returns `Error::VarintOverflow` after reading 10 bytes without a terminating byte.

### 5.2 Zigzag mapping (signed → unsigned)

```
encode(n) = (n << 1) ^ (n >> 63)
decode(u) = (u >> 1) ^ -(u & 1)
```

Maps small-magnitude signed values to small unsigned values for effective LEB128 compression of negative numbers.

### 5.3 Delta-of-delta (timestamps)

```
[first_ts:    i64 LE, 8 bytes]
[first_delta: zigzag+varint]     ts[1] - ts[0]
[dod[2]:      zigzag+varint]     (ts[2]-ts[1]) - (ts[1]-ts[0])
...
[dod[n-1]:    zigzag+varint]
```

For regularly-arriving ticks (constant inter-arrival time), `dod ≈ 0` for all rows, encoding each dod in 1 byte. Empirically within ~15% of Gorilla bit-packing on Binance aggTrade data while being significantly simpler.

### 5.4 Price encoding (delta + zigzag + varint)

All prices in a block are normalised to a common exponent (the minimum exponent present in the block). Mantissa deltas are then zigzag+varint encoded.

Block-level price header (2 bytes before the mantissa stream):
```
[flags:    u8]   bit 0: 0 = delta path, 1 = f64-XOR fallback
[exponent: i8]   common exponent (delta path only)
[first:    i64 LE, 8 bytes]
[deltas:   zigzag+varint...]
```

If rescaling any mantissa to the common exponent would overflow i64, the block falls back to the f64-XOR path: each price is converted to f64, the first stored raw (8 bytes LE), and successive values stored as `zigzag(bits(f64[i]) XOR bits(f64[i-1]))` varint.

### 5.5 Run-length encoding (side column)

```
[value: u8][run: LEB128 u64][value: u8][run: LEB128 u64]...
```

Decoder clamps each run to the number of remaining rows needed, preventing OOM on corrupt data.

---

## 6. Independent reader checklist

To write an independent reader:

1. Seek to `EOF - 12`; read `footer_len` (u32 LE), `footer_crc` (u32 LE), verify magic `"TKDB"`.
2. Seek to `EOF - 12 - footer_len`; read `footer_len` bytes; verify `crc32c == footer_crc`.
3. Decode the footer: symbol dict, then block index (59 bytes per entry, count-prefixed).
4. For each desired block: seek to `file_offset`, read `byte_len` bytes, verify `crc32c`.
5. Decompress according to `encoding_flags` bits [1:0].
6. Parse length-prefixed column sections; decode each column with its encoder.
7. Reconstruct trades: `symbol` from the block index entry, other fields from column batches.
