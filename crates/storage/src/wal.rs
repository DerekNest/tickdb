/// Write-ahead log (WAL).
///
/// # Record format (matches FORMAT.md §WAL)
///
/// ```text
/// [ len:    u32 LE ]   byte length of payload
/// [ crc32c: u32 LE ]   crc32c of payload only (not of len itself)
/// [ payload: bytes ]   hand-rolled serialization of Vec<Trade>
/// ```
///
/// Each Trade is serialized as fixed-width little-endian fields:
///   symbol   u32   4 bytes
///   ts       i64   8 bytes
///   mantissa i64   8 bytes
///   exponent i8    1 byte
///   size     u64   8 bytes
///   side     u8    1 byte
///   trade_id u64   8 bytes
///                 -------
///                 38 bytes per trade
///
/// Recovery reads records until EOF or the first truncated/corrupt record.
/// A truncated final record (partial write at crash time) is silently dropped —
/// only CRC-verified records are replayed.  This matches the `every_write`
/// durability contract: acked writes were fsynced before the ack.
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crc32fast::Hasher as Crc32Hasher;
use format::types::{Decimal64, Side, SymbolId, Trade};

use crate::error::Error;

/// Bytes per serialized Trade (fixed-width, no padding).
pub const TRADE_WIRE_SIZE: usize = 4 + 8 + 8 + 1 + 8 + 1 + 8; // = 38

/// Controls how often the WAL is fsynced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// fsync after every appended record.  Strongest durability; slowest.
    EveryWrite,
    /// fsync at most once every N milliseconds.  Batches durability cost.
    /// Default value from §2.3: 50 ms.
    EveryNMs(u64),
    /// Never fsync.  Fastest; data may be lost on OS crash.  Test use only.
    Never,
}

pub struct Wal {
    writer: BufWriter<File>,
    policy: FsyncPolicy,
    /// Total bytes written since last truncation — used for the 1 GiB flush trigger.
    bytes_written: u64,
    /// Wall-clock time of the last fsync (for EveryNMs).
    last_fsync: Instant,
}

impl Wal {
    /// Maximum WAL size before the engine triggers a flush (§2.3).
    pub const MAX_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB

    /// Opens (or creates) the WAL file at `path`.
    ///
    /// Append-only: the file cursor is positioned at the end so new records
    /// extend the log without overwriting existing data.
    pub fn open(path: impl AsRef<Path>, policy: FsyncPolicy) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.as_ref())?;

        let mut f = file;
        // Seek to end so appends don't overwrite recovered data.
        let existing_len = f.seek(SeekFrom::End(0))?;

        Ok(Wal {
            writer: BufWriter::new(f),
            policy,
            bytes_written: existing_len,
            last_fsync: Instant::now(),
        })
    }

    /// Total bytes written to this WAL since last truncation.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Returns true if the WAL has grown past the 1 GiB flush threshold.
    pub fn needs_flush(&self) -> bool {
        self.bytes_written >= Self::MAX_BYTES
    }

    /// Appends a batch of trades as a single WAL record.
    ///
    /// Under `FsyncPolicy::EveryWrite`, this call does not return until the
    /// data is on stable storage — the Raft persistence rule adapted to the
    /// single-node WAL: we must not ack the write before fsync.
    ///
    /// Under `FsyncPolicy::EveryNMs(n)`, an fsync is issued if at least `n`
    /// milliseconds have elapsed since the last one.
    pub fn append(&mut self, trades: &[Trade]) -> Result<(), Error> {
        let payload = serialize_trades(trades);
        let len = payload.len() as u32;

        let mut h = Crc32Hasher::new();
        h.update(&payload);
        let crc = h.finalize();

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&payload)?;
        self.writer.flush()?;

        let record_bytes = 8u64 + payload.len() as u64;
        self.bytes_written = self.bytes_written.saturating_add(record_bytes);

        match self.policy {
            FsyncPolicy::EveryWrite => {
                self.writer.get_ref().sync_data()?;
                self.last_fsync = Instant::now();
            }
            FsyncPolicy::EveryNMs(ms) => {
                if self.last_fsync.elapsed() >= Duration::from_millis(ms) {
                    self.writer.get_ref().sync_data()?;
                    self.last_fsync = Instant::now();
                }
            }
            FsyncPolicy::Never => {}
        }

        Ok(())
    }

    /// Truncates the WAL file to zero bytes and resets the cursor.
    ///
    /// Called after a successful segment flush.  The manifest is updated
    /// before this call; if we crash between manifest update and WAL
    /// truncation, recovery replays a WAL whose trades are already in the
    /// segment — the dedup-by-trade_id in the memtable handles the overlap.
    pub fn truncate(&mut self) -> Result<(), Error> {
        self.writer.flush()?;
        let file = self.writer.get_mut();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        self.bytes_written = 0;
        Ok(())
    }

    /// Replays all valid records in `path` into the returned trade batches.
    ///
    /// Stops at the first truncated record (safe: it was a partial write at
    /// crash time) and returns all records that passed CRC verification.
    pub fn recover(path: impl AsRef<Path>) -> Result<Vec<Vec<Trade>>, Error> {
        let mut file = match File::open(path.as_ref()) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut batches = Vec::new();
        let mut offset: u64 = 0;

        loop {
            // Read the 8-byte header (len + crc32c).
            let mut header = [0u8; 8];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            // Safe: header is exactly 8 bytes; slices are statically sized.
            let payload_len =
                u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
            let stored_crc = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);

            // Sanity cap: a single record cannot exceed 512 MiB.
            // (512 MiB / 38 bytes ≈ 14M trades — well above any realistic batch.)
            if payload_len > 512 * 1024 * 1024 {
                tracing::warn!(
                    offset,
                    payload_len,
                    "WAL: implausible record length, stopping recovery"
                );
                break;
            }

            let mut payload = vec![0u8; payload_len];
            match file.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // Partial payload — crash mid-write.  Drop this record.
                    tracing::warn!(offset, "WAL: truncated record at end of log, dropping");
                    break;
                }
                Err(e) => return Err(e.into()),
            }

            // Verify CRC before accepting data.
            let mut h = Crc32Hasher::new();
            h.update(&payload);
            let computed_crc = h.finalize();

            if computed_crc != stored_crc {
                return Err(Error::WalCrc {
                    stored: stored_crc,
                    computed: computed_crc,
                    offset,
                });
            }

            let trades = deserialize_trades(&payload)?;
            batches.push(trades);

            offset += 8 + payload_len as u64;
        }

        Ok(batches)
    }
}

// ── Serialization ─────────────────────────────────────────────────────────────

/// Serializes a slice of trades into a compact fixed-width byte payload.
///
/// Using fixed-width fields (no varints) here because:
/// 1. WAL records are held in memory during recovery — size matters less than
///    throughput of the serialization path.
/// 2. Fixed-width makes the format self-describing and trivial to verify.
pub fn serialize_trades(trades: &[Trade]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(trades.len() * TRADE_WIRE_SIZE);
    for t in trades {
        buf.extend_from_slice(&t.symbol.to_le_bytes());
        buf.extend_from_slice(&t.ts.to_le_bytes());
        buf.extend_from_slice(&t.price.mantissa.to_le_bytes());
        buf.push(t.price.exponent as u8);
        buf.extend_from_slice(&t.size.to_le_bytes());
        buf.push(t.side.as_u8());
        buf.extend_from_slice(&t.trade_id.to_le_bytes());
    }
    buf
}

/// Deserializes a WAL payload back into trades.
///
/// Returns `Error::WalTruncated` if the payload length is not a multiple of
/// `TRADE_WIRE_SIZE`.
pub fn deserialize_trades(payload: &[u8]) -> Result<Vec<Trade>, Error> {
    if !payload.len().is_multiple_of(TRADE_WIRE_SIZE) {
        return Err(Error::WalTruncated {
            offset: 0,
            need: TRADE_WIRE_SIZE,
            have: payload.len() % TRADE_WIRE_SIZE,
        });
    }

    let n = payload.len() / TRADE_WIRE_SIZE;
    let mut trades = Vec::with_capacity(n);
    let mut pos = 0;

    for _ in 0..n {
        // Each read is bounds-checked by the % TRADE_WIRE_SIZE guard above —
        // all slices are within the payload allocation.
        let symbol = u32::from_le_bytes([
            payload[pos],
            payload[pos + 1],
            payload[pos + 2],
            payload[pos + 3],
        ]);
        let ts = i64::from_le_bytes([
            payload[pos + 4],
            payload[pos + 5],
            payload[pos + 6],
            payload[pos + 7],
            payload[pos + 8],
            payload[pos + 9],
            payload[pos + 10],
            payload[pos + 11],
        ]);
        let mantissa = i64::from_le_bytes([
            payload[pos + 12],
            payload[pos + 13],
            payload[pos + 14],
            payload[pos + 15],
            payload[pos + 16],
            payload[pos + 17],
            payload[pos + 18],
            payload[pos + 19],
        ]);
        let exponent = payload[pos + 20] as i8;
        let size = u64::from_le_bytes([
            payload[pos + 21],
            payload[pos + 22],
            payload[pos + 23],
            payload[pos + 24],
            payload[pos + 25],
            payload[pos + 26],
            payload[pos + 27],
            payload[pos + 28],
        ]);
        let side = Side::from_u8(payload[pos + 29]);
        let trade_id = u64::from_le_bytes([
            payload[pos + 30],
            payload[pos + 31],
            payload[pos + 32],
            payload[pos + 33],
            payload[pos + 34],
            payload[pos + 35],
            payload[pos + 36],
            payload[pos + 37],
        ]);

        trades.push(Trade {
            symbol: symbol as SymbolId,
            ts,
            price: Decimal64 { mantissa, exponent },
            size,
            side,
            trade_id,
        });

        pos += TRADE_WIRE_SIZE;
    }

    Ok(trades)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use format::dz::Decimal64;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CTR: AtomicU64 = AtomicU64::new(0);

    struct NamedTempFile(std::path::PathBuf);
    impl NamedTempFile {
        fn new() -> Self {
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            NamedTempFile(std::env::temp_dir().join(format!("tickdb_wal_test_{n}.wal")))
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for NamedTempFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn sample_trade(id: u64) -> Trade {
        Trade {
            symbol: 1,
            ts: 1_700_000_000_000_000_000 + id as i64 * 100_000,
            price: Decimal64 {
                mantissa: 5_000_000 + id as i64 % 10,
                exponent: -2,
            },
            size: id % 1000 + 1,
            side: Side::Buy,
            trade_id: id,
        }
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let trades: Vec<Trade> = (0..100).map(sample_trade).collect();
        let payload = serialize_trades(&trades);
        assert_eq!(payload.len(), 100 * TRADE_WIRE_SIZE);
        let decoded = deserialize_trades(&payload).unwrap();
        assert_eq!(decoded.len(), trades.len());
        for (a, b) in trades.iter().zip(decoded.iter()) {
            assert_eq!(a.symbol, b.symbol);
            assert_eq!(a.ts, b.ts);
            assert_eq!(a.price, b.price);
            assert_eq!(a.size, b.size);
            assert_eq!(a.trade_id, b.trade_id);
        }
    }

    #[test]
    fn wal_append_and_recover() {
        let f = NamedTempFile::new();
        let trades: Vec<Trade> = (0..50).map(sample_trade).collect();
        {
            let mut wal = Wal::open(f.path(), FsyncPolicy::Never).unwrap();
            wal.append(&trades[..25]).unwrap();
            wal.append(&trades[25..]).unwrap();
            assert!(wal.bytes_written() > 0);
        }
        let recovered = Wal::recover(f.path()).unwrap();
        assert_eq!(recovered.len(), 2);
        assert_eq!(recovered[0].len(), 25);
        assert_eq!(recovered[1].len(), 25);
        assert_eq!(recovered[0][0].trade_id, 0);
        assert_eq!(recovered[1][24].trade_id, 49);
    }

    #[test]
    fn bytes_written_tracks_correctly() {
        let f = NamedTempFile::new();
        let mut wal = Wal::open(f.path(), FsyncPolicy::Never).unwrap();
        assert_eq!(wal.bytes_written(), 0);
        let trades: Vec<Trade> = (0..10).map(sample_trade).collect();
        wal.append(&trades).unwrap();
        // 8 byte header + 10 * 38 bytes payload
        assert_eq!(wal.bytes_written(), 8 + 10 * 38);
        wal.truncate().unwrap();
        assert_eq!(wal.bytes_written(), 0);
    }

    #[test]
    fn recover_from_missing_file() {
        let result =
            Wal::recover(std::env::temp_dir().join("tickdb_no_such_wal_xyzzy_99.wal")).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn recover_tolerates_truncated_final_record() {
        let f = NamedTempFile::new();
        let trades: Vec<Trade> = (0..10).map(sample_trade).collect();
        {
            let mut wal = Wal::open(f.path(), FsyncPolicy::Never).unwrap();
            wal.append(&trades).unwrap();
        }
        // Append a partial header to simulate crash mid-write.
        {
            let mut file = OpenOptions::new().append(true).open(f.path()).unwrap();
            file.write_all(&[0x00, 0x01, 0x02]).unwrap();
        }
        let recovered = Wal::recover(f.path()).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].len(), 10);
    }

    #[test]
    fn corrupt_crc_returns_error() {
        let f = NamedTempFile::new();
        let trades: Vec<Trade> = (0..5).map(sample_trade).collect();
        {
            let mut wal = Wal::open(f.path(), FsyncPolicy::Never).unwrap();
            wal.append(&trades).unwrap();
        }
        // Flip a payload byte.
        let mut data = std::fs::read(f.path()).unwrap();
        data[9] ^= 0xFF;
        std::fs::write(f.path(), data).unwrap();
        assert!(matches!(Wal::recover(f.path()), Err(Error::WalCrc { .. })));
    }

    #[test]
    fn wal_truncate_and_reuse() {
        let f = NamedTempFile::new();
        let trades: Vec<Trade> = (0..5).map(sample_trade).collect();
        let mut wal = Wal::open(f.path(), FsyncPolicy::Never).unwrap();
        wal.append(&trades).unwrap();
        wal.truncate().unwrap();
        wal.append(&trades).unwrap();
        let recovered = Wal::recover(f.path()).unwrap();
        assert_eq!(recovered.len(), 1);
    }

    #[test]
    fn every_n_ms_policy_compiles_and_runs() {
        let f = NamedTempFile::new();
        let trades: Vec<Trade> = (0..5).map(sample_trade).collect();
        let mut wal = Wal::open(f.path(), FsyncPolicy::EveryNMs(50)).unwrap();
        wal.append(&trades).unwrap();
        let recovered = Wal::recover(f.path()).unwrap();
        assert_eq!(recovered[0].len(), 5);
    }
}
