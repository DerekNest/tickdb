/// Memory-mapped segment file reader.
///
/// # Safety contract
///
/// This is the only file in `crates/storage` permitted to use `unsafe` (§0
/// locked decisions).  Every `unsafe` block is annotated with the invariant
/// it relies on.
///
/// The key invariant: a `MmapSegment` keeps its backing `File` alive for the
/// lifetime of the mapping.  On Linux, the file can be unlinked while the
/// mapping is live — the OS keeps the inode alive until the last mmap is
/// dropped.  We rely on this for compaction: the manifest removes an old
/// segment path, readers holding `Arc<MmapSegment>` keep reading it safely,
/// and the file is reclaimed when the last Arc drops.
///
/// `memmap2::Mmap` is not `Send` on all platforms (it is on Linux/macOS).
/// We explicitly assert `Send + Sync` here because:
///   - the mapping is read-only (opened with `Mmap`, not `MmapMut`),
///   - the underlying file is not mutated after the mapping is created,
///   - concurrent reads from multiple threads into a read-only mapping are
///     safe — the kernel handles it.
///
/// # MIRI note
///
/// MIRI cannot run mmap; tests using `MmapSegment` are gated behind
/// `#[cfg(not(miri))]` where indicated.
use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use crate::error::Error;
use format::segment::SegmentReader;

/// A segment file opened via `mmap`.
///
/// The `SegmentReader` inside holds a reference to the mapped bytes.
/// Because `SegmentReader::open` currently takes `Vec<u8>` (owned), we
/// store the mmap separately and give the reader a copy of the bytes.
///
/// TODO (Phase 1.5): refactor `SegmentReader` to accept `&[u8]` with a
/// lifetime tied to the mapping, eliminating the copy.  For now the copy
/// happens once at open time and the reader works off its own allocation.
///
/// The `_file` field keeps the `File` alive for the lifetime of this struct,
/// which keeps the OS file descriptor open and prevents the inode from being
/// freed while we are reading.
pub struct MmapSegment {
    /// The memory mapping — kept alive so the raw pointer in `_mmap` is valid.
    _mmap: Mmap,
    /// Open file handle — keeps the inode alive even after `unlink`.
    _file: File,
    /// Decoded segment reader built from the mapped bytes.
    pub reader: SegmentReader,
}

impl MmapSegment {
    /// Opens a segment file at `path` using `mmap` and decodes its footer.
    ///
    /// # Errors
    /// - `Error::Io` if the file cannot be opened or mapped.
    /// - `Error::Format` if the segment is corrupt (bad magic, bad CRC, etc.).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let file = File::open(path.as_ref()).map_err(Error::Io)?;

        // SAFETY:
        //   1. `file` is a valid, open file descriptor for the duration of
        //      this function and for the lifetime of the returned `MmapSegment`
        //      (we store it in `_file`).
        //   2. We open the mapping as read-only (`Mmap`, not `MmapMut`), so
        //      no mutable aliasing is possible.
        //   3. We do not dereference the map pointer directly anywhere outside
        //      of the slice conversion below; all access goes through the safe
        //      `SegmentReader` API which bounds-checks every read.
        //   4. The file is a regular file (not a device); its size is fixed
        //      for the lifetime of the mapping (segments are immutable once
        //      written).
        let mmap = unsafe { Mmap::map(&file).map_err(Error::Io)? };

        // Copy bytes from the mapping into an owned Vec so `SegmentReader`
        // can take ownership.  This is the temporary inefficiency noted above.
        //
        // SAFETY: `mmap` is a valid, aligned, initialized byte slice for
        // `mmap.len()` bytes.  `std::slice::from_raw_parts` requires:
        //   - pointer is non-null and properly aligned (guaranteed by mmap),
        //   - the memory is initialized (file bytes are always initialized),
        //   - the slice does not outlive the memory it points to (we copy
        //     immediately into `Vec<u8>`, so the Vec owns the data afterward).
        let bytes: Vec<u8> = mmap.to_vec();

        let reader = SegmentReader::open(bytes).map_err(Error::Format)?;

        Ok(MmapSegment {
            _mmap: mmap,
            _file: file,
            reader,
        })
    }

    /// Returns the number of blocks in this segment.
    pub fn block_count(&self) -> usize {
        self.reader.block_index.len()
    }
}

// SAFETY: `MmapSegment` is `Send + Sync` because:
//   - `Mmap` (read-only) is safe to share across threads — the kernel
//     guarantees that concurrent reads from the same mapped region are safe.
//   - `File` is `Send` on all platforms.
//   - `SegmentReader` contains only owned data (Vec<u8>, BTreeMap); it is
//     `Send + Sync` by construction.
//
// memmap2's `Mmap` does not implement `Sync` by default (conservative choice
// for platforms with exotic memory semantics), but on the target platforms
// (Linux, macOS) concurrent reads are always safe for read-only mappings.
unsafe impl Send for MmapSegment {}
unsafe impl Sync for MmapSegment {}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use format::compress::COMPRESS_NONE;
    use format::dz::Decimal64;
    use format::segment::SegmentWriter;
    use format::types::{Side, SymbolId, Trade};
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CTR: AtomicU64 = AtomicU64::new(0);

    fn tmp_path() -> std::path::PathBuf {
        let n = CTR.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("tickdb_mmap_test_{n}.tkdb"))
    }

    fn make_trade(symbol: SymbolId, ts: i64, trade_id: u64) -> Trade {
        Trade {
            symbol,
            ts,
            price: Decimal64 {
                mantissa: 5_000_000,
                exponent: -2,
            },
            size: 100,
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
    #[cfg(not(miri))]
    fn open_and_scan() {
        let path = tmp_path();
        let _guard = scopeguard(|| {
            let _ = std::fs::remove_file(&path);
        });

        let trades: Vec<Trade> = (0..50)
            .map(|i| make_trade(1, i * 100_000, i as u64))
            .collect();
        let mut writer = SegmentWriter::new(0, COMPRESS_NONE);
        writer.add_trades(1, &trades).unwrap();
        let bytes = writer.finish(&make_dict(&[(1, "BTCUSDT")])).unwrap();
        std::fs::write(&path, &bytes).unwrap();

        let seg = MmapSegment::open(&path).unwrap();
        assert_eq!(seg.block_count(), 1);
        assert_eq!(seg.reader.symbol_dict.get(&1).unwrap(), "BTCUSDT");

        let total: usize = seg
            .reader
            .scan(1, i64::MIN, i64::MAX)
            .map(|r| r.unwrap().ts.len())
            .sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn open_nonexistent_returns_err() {
        assert!(
            MmapSegment::open(std::env::temp_dir().join("tickdb_no_such_file_xyzzy.tkdb")).is_err()
        );
    }

    // Minimal RAII cleanup guard for test files.
    fn scopeguard<F: FnOnce()>(f: F) -> impl Drop {
        struct Guard<F: FnOnce()>(Option<F>);
        impl<F: FnOnce()> Drop for Guard<F> {
            fn drop(&mut self) {
                if let Some(f) = self.0.take() {
                    f();
                }
            }
        }
        Guard(Some(f))
    }
}
