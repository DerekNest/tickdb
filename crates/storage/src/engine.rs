/// Storage engine — the public face of the storage crate.
///
/// # Write path
///
/// ```text
/// ingest(trades) ──► WAL::append (crc-framed, optionally fsynced)
///               └──► Memtable::insert
///
/// if memtable.byte_size() ≥ FLUSH_THRESHOLD or wal too large:
///     flush() → sort + dedup per symbol → SegmentWriter → temp file
///                → fsync → atomic rename → Manifest::add_segment
///                → WAL::truncate
/// ```
///
/// # Read path
///
/// Reads merge live segments (mmap'd) ∪ memtable (sorted on the fly).
/// The memtable is always "newer" than any flushed segment.
///
/// # Thread safety
///
/// `Engine` is currently single-threaded — the caller is responsible for
/// serialising concurrent access.  The Raft integration layer in `crates/server`
/// will hold a `Mutex<Engine>`.
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use format::compress::COMPRESS_ZSTD3;
use format::segment::{ColumnBatch, SegmentWriter};
use format::types::{SymbolId, Trade};

use crate::error::Error;
use crate::manifest::{Manifest, SegmentMeta};
use crate::memtable::Memtable;
use crate::mmap::MmapSegment;
use crate::wal::{FsyncPolicy, Wal};

/// Flush the memtable when it exceeds 256 MiB (§2.3).
const FLUSH_THRESHOLD_BYTES: usize = 256 * 1024 * 1024;

/// Subdirectory within data_dir that holds segment files.
const SEGMENTS_SUBDIR: &str = "segments";

/// WAL filename within data_dir.
const WAL_FILENAME: &str = "WAL";

pub struct Engine {
    data_dir: PathBuf,
    wal: Wal,
    memtable: Memtable,
    manifest: Manifest,
    /// Memory-mapped open segment files.
    /// Wrapped in Arc so scan iterators can hold a reference without borrowing
    /// the engine (enabling concurrent reads during a flush in future).
    open_segments: Vec<Arc<MmapSegment>>,
    /// Symbol name → id table (node-global).
    symbol_table: BTreeMap<String, SymbolId>,
    /// Inverse: id → name (for segment footer population).
    symbol_names: BTreeMap<SymbolId, String>,
    /// Next free symbol id.
    next_symbol_id: u32,
    /// Stored for future EveryNMs background-flush timer (not yet implemented).
    #[allow(dead_code)]
    fsync_policy: FsyncPolicy,
}

impl Engine {
    /// Opens an existing engine at `data_dir`, or creates a new one.
    ///
    /// # Recovery sequence
    ///   1. Load MANIFEST → open all listed segment files.
    ///   2. Replay WAL → insert all batches into memtable.
    ///   3. Load symbol table from symbols.json.
    pub fn open(data_dir: impl AsRef<Path>, fsync_policy: FsyncPolicy) -> Result<Self, Error> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(data_dir.join(SEGMENTS_SUBDIR))?;

        let manifest = Manifest::load(&data_dir)?;
        let symbol_table = Manifest::load_symbol_table(&data_dir)?;

        // Build the inverse map and find the next free id.
        let mut symbol_names: BTreeMap<SymbolId, String> = BTreeMap::new();
        let mut max_id: u32 = 0;
        for (name, &id) in &symbol_table {
            symbol_names.insert(id, name.clone());
            if id >= max_id {
                max_id = id + 1;
            }
        }
        let next_symbol_id = max_id;

        // Open all segments listed in the manifest via mmap.
        let mut open_segments = Vec::new();
        for seg_meta in &manifest.segments {
            let seg_path = data_dir.join(&seg_meta.path);
            let seg = MmapSegment::open(&seg_path)?;
            open_segments.push(Arc::new(seg));
        }

        // Replay WAL into fresh memtable.
        let wal_path = data_dir.join(WAL_FILENAME);
        let wal_batches = Wal::recover(&wal_path)?;
        let mut memtable = Memtable::new();
        for batch in wal_batches {
            memtable.insert(&batch);
        }

        // Re-open WAL for appending (positioned at EOF).
        let wal = Wal::open(&wal_path, fsync_policy)?;

        tracing::info!(
            data_dir = %data_dir.display(),
            segments = open_segments.len(),
            memtable_bytes = memtable.byte_size(),
            "engine opened"
        );

        Ok(Engine {
            data_dir,
            wal,
            memtable,
            manifest,
            open_segments,
            symbol_table,
            symbol_names,
            next_symbol_id,
            fsync_policy,
        })
    }

    /// Resolves a symbol name to its id, creating a new entry if unknown.
    pub fn intern_symbol(&mut self, name: &str) -> Result<SymbolId, Error> {
        if let Some(&id) = self.symbol_table.get(name) {
            return Ok(id);
        }
        let id = self.next_symbol_id;
        self.next_symbol_id += 1;
        self.symbol_table.insert(name.to_owned(), id);
        self.symbol_names.insert(id, name.to_owned());
        Manifest::save_symbol_table(&self.data_dir, &self.symbol_table)?;
        Ok(id)
    }

    /// Looks up a symbol id by name without creating a new entry.
    ///
    /// Returns `None` if the symbol is not in the table — unlike `intern_symbol`
    /// this never modifies state, making it safe to call on a read-only engine
    /// handle (e.g., after recovery in tests).
    pub fn intern_symbol_ro(&self, name: &str) -> Option<SymbolId> {
        self.symbol_table.get(name).copied()
    }

    /// Provides read access to the data directory root (for snapshot
    /// bundling, milestone 9: the raft layer needs to locate the manifest
    /// and segment files on disk to package a leader-installed snapshot).
    pub fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    /// Provides read access to the manifest (for stats / admin commands).
    pub fn manifest(&self) -> &crate::manifest::Manifest {
        &self.manifest
    }

    /// Returns the name for a symbol id, if known.
    pub fn symbol_name(&self, id: SymbolId) -> Option<&str> {
        self.symbol_names.get(&id).map(|s| s.as_str())
    }

    /// Ingests a batch of trades.
    ///
    /// 1. Appends to WAL (with configured fsync policy).
    /// 2. Inserts into memtable.
    /// 3. If memtable exceeds the flush threshold, flushes synchronously.
    ///
    /// Under `FsyncPolicy::EveryWrite`, the function does not return until the
    /// WAL record is on stable storage.
    pub fn ingest(&mut self, trades: &[Trade]) -> Result<(), Error> {
        self.wal.append(trades)?;
        self.memtable.insert(trades);

        if self.memtable.byte_size() >= FLUSH_THRESHOLD_BYTES || self.wal.needs_flush() {
            self.flush()?;
        }

        Ok(())
    }

    /// Explicitly flushes the memtable to a segment file, even if below threshold.
    ///
    /// Flush procedure:
    ///   1. For each symbol in the memtable: drain_sorted → write block(s).
    ///   2. Write segment to a temp file, fsync, atomic rename into place.
    ///   3. Register in manifest (atomic rename inside Manifest::add_segment).
    ///   4. Truncate WAL.
    ///   5. Open the new segment for reads.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Determine the date partition from the minimum ts in the memtable.
        // We use UTC date of the earliest trade in this flush batch.
        let date = self.memtable_date();

        // Drain all symbols from the memtable.
        let per_symbol = self.memtable.drain_all_sorted();
        let total_rows: u64 = per_symbol.iter().map(|(_, t)| t.len() as u64).sum();

        if total_rows == 0 {
            return Ok(());
        }

        // Compute overall min/max ts across all symbols for the manifest entry.
        let min_ts = per_symbol
            .iter()
            .flat_map(|(_, t)| t.iter().map(|r| r.ts))
            .min()
            .unwrap_or(0);
        let max_ts = per_symbol
            .iter()
            .flat_map(|(_, t)| t.iter().map(|r| r.ts))
            .max()
            .unwrap_or(0);

        let generation = self.manifest.next_generation(&date);

        // ── Build segment file in memory ─────────────────────────────────────
        let created_unix_s = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut writer = SegmentWriter::new(created_unix_s, COMPRESS_ZSTD3);

        for (symbol_id, trades) in &per_symbol {
            writer.add_trades(*symbol_id, trades)?;
        }

        // Build symbol dict for segment footer (only symbols present in this flush).
        let symbol_dict: BTreeMap<SymbolId, String> = per_symbol
            .iter()
            .filter_map(|(id, _)| self.symbol_names.get(id).map(|name| (*id, name.clone())))
            .collect();

        let segment_bytes = writer.finish(&symbol_dict)?;

        // ── Write to temp file, fsync, atomic rename ─────────────────────────
        let date_dir = self.data_dir.join(SEGMENTS_SUBDIR).join(&date);
        fs::create_dir_all(&date_dir)?;

        let seg_filename = format!("{generation}.tkdb");
        let seg_rel_path = format!("{SEGMENTS_SUBDIR}/{date}/{seg_filename}");
        let seg_abs_path = self.data_dir.join(&seg_rel_path);
        let seg_tmp_path = seg_abs_path.with_extension("tkdb.tmp");

        fs::write(&seg_tmp_path, &segment_bytes)?;

        // fsync the temp file before rename — durability contract.
        {
            let f = fs::File::open(&seg_tmp_path)?;
            f.sync_data()?;
        }

        fs::rename(&seg_tmp_path, &seg_abs_path)?;

        // ── Register in manifest (atomic) ────────────────────────────────────
        let meta = SegmentMeta {
            path: seg_rel_path.clone(),
            date: date.clone(),
            min_ts,
            max_ts,
            row_count: total_rows,
            generation,
        };
        self.manifest.add_segment(meta)?;

        // ── Open new segment for reads (via mmap on the renamed file) ──────────
        let new_seg = MmapSegment::open(&seg_abs_path)?;
        self.open_segments.push(Arc::new(new_seg));

        // ── Truncate WAL (memtable is now empty; WAL is redundant) ───────────
        self.wal.truncate()?;

        tracing::info!(
            date,
            generation,
            rows = total_rows,
            path = seg_rel_path,
            "flushed segment"
        );

        Ok(())
    }

    /// Scans all data for `symbol_id` in `[ts_start, ts_end]`.
    ///
    /// Returns batches from flushed segments first, then from the memtable.
    /// Callers are responsible for merging and deduplicating across batches if
    /// an exact-ordered result is needed (the query layer does this).
    pub fn scan(
        &self,
        symbol_id: SymbolId,
        ts_start: i64,
        ts_end: i64,
    ) -> impl Iterator<Item = Result<ColumnBatch, Error>> + '_ {
        // Segment batches from all open segment files.
        let seg_iter = self.open_segments.iter().flat_map(move |seg| {
            seg.reader
                .scan(symbol_id, ts_start, ts_end)
                .map(|r| r.map_err(Error::Format))
                .collect::<Vec<_>>()
                .into_iter()
        });

        // Memtable batch — materialised from the in-memory buffer.
        let mem_batch = self.memtable_scan(symbol_id, ts_start, ts_end);

        seg_iter.chain(mem_batch.into_iter().map(Ok))
    }

    /// Scans all trades for a symbol from the memtable (snapshot — reads current state).
    fn memtable_scan(&self, symbol_id: SymbolId, ts_start: i64, ts_end: i64) -> Vec<ColumnBatch> {
        // The memtable is unsorted in-memory; we need a sorted view.
        // Clone and sort the trades for the symbol — this is the read path cost
        // accepted in §2.4: "memtable scanned last, sorted on the fly".
        // Use the public accessor for read-only access to the memtable bucket.
        let mut trades: Vec<&Trade> = self
            .memtable
            .iter_symbol(symbol_id)
            .filter(|t| t.ts >= ts_start && t.ts <= ts_end)
            .collect();

        if trades.is_empty() {
            return Vec::new();
        }

        trades.sort_unstable_by_key(|t| (t.ts, t.trade_id));

        // Convert to ColumnBatch chunks of ≤ 4096 rows (the query layer batch size).
        const BATCH_SIZE: usize = 4096;
        trades
            .chunks(BATCH_SIZE)
            .map(|chunk| ColumnBatch {
                symbol_id,
                ts: chunk.iter().map(|t| t.ts).collect(),
                price_mantissa: chunk.iter().map(|t| t.price.mantissa).collect(),
                exponent: chunk.first().map(|t| t.price.exponent).unwrap_or(0),
                size: chunk.iter().map(|t| t.size).collect(),
                side: chunk.iter().map(|t| t.side.as_u8()).collect(),
                trade_id: chunk.iter().map(|t| t.trade_id).collect(),
            })
            .collect()
    }

    /// Returns UTC date string "YYYY-MM-DD" derived from the earliest ts in the memtable.
    fn memtable_date(&self) -> String {
        // Find the min ts across all buffered symbols.
        let min_ts = self
            .memtable
            .symbol_ids()
            .flat_map(|sid| {
                // Iterate in insert order — we only need a min, not sort.
                self.memtable.iter_symbol(sid).map(|t| t.ts)
            })
            .min()
            .unwrap_or(0);

        ts_ns_to_date(min_ts)
    }

    /// Triggers compaction for `date` if the segment count exceeds the threshold.
    ///
    /// Called automatically after each flush.  Can also be called manually
    /// (e.g., from the CLI admin command).
    pub fn maybe_compact(&mut self, date: &str) -> Result<bool, Error> {
        let ran = crate::compaction::maybe_compact(
            &self.data_dir,
            date,
            &mut self.manifest,
            &mut self.open_segments,
        )?;

        if ran {
            // Reload all open segments from the manifest after compaction,
            // since maybe_compact clears open_segments and re-adds the new one.
            // Any readers that held Arc<MmapSegment> before this call continue
            // reading safely via their existing Arc references.
            self.reload_open_segments()?;
        }

        Ok(ran)
    }

    /// Reloads `open_segments` from the current manifest state.
    ///
    /// Called after compaction to ensure the engine's in-memory segment list
    /// reflects the on-disk manifest.
    fn reload_open_segments(&mut self) -> Result<(), Error> {
        self.open_segments.clear();
        for seg_meta in &self.manifest.segments {
            let seg_path = self.data_dir.join(&seg_meta.path);
            let new_seg = MmapSegment::open(&seg_path)?;
            self.open_segments.push(Arc::new(new_seg));
        }
        Ok(())
    }
}

/// Converts a nanosecond Unix timestamp to a UTC date string "YYYY-MM-DD".
///
/// Uses integer arithmetic to avoid a chrono/time dependency — the spec's
/// allowed deps list does not include those crates.
fn ts_ns_to_date(ts_ns: i64) -> String {
    let ts_s = (ts_ns / 1_000_000_000).max(0) as u64;
    let days_since_epoch = ts_s / 86400;

    // Gregorian calendar computation (Fliegel-Van Flandern algorithm).
    let jd = days_since_epoch as i64 + 2440588; // Unix epoch = JD 2440588
    let l = jd + 68569;
    let n = (4 * l) / 146097;
    let l = l - (146097 * n + 3) / 4;
    let i = (4000 * (l + 1)) / 1461001;
    let l = l - (1461 * i) / 4 + 31;
    let j = (80 * l) / 2447;
    let day = l - (2447 * j) / 80;
    let l = j / 11;
    let month = j + 2 - 12 * l;
    let year = 100 * (n - 49) + i + l;

    format!("{year:04}-{month:02}-{day:02}")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use format::dz::Decimal64;
    use format::types::Side;

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static CTR: AtomicU64 = AtomicU64::new(0);
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("tickdb_eng_{n}"));
            std::fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn make_trade(symbol: SymbolId, ts: i64, trade_id: u64) -> Trade {
        Trade {
            symbol,
            ts,
            price: Decimal64 {
                mantissa: 5_000_000 + trade_id as i64 % 100,
                exponent: -2,
            },
            size: trade_id % 500 + 1,
            side: Side::Buy,
            trade_id,
        }
    }

    fn open_engine(dir: &TempDir) -> Engine {
        Engine::open(dir.path(), FsyncPolicy::Never).unwrap()
    }

    #[test]
    fn ts_ns_to_date_epoch() {
        assert_eq!(ts_ns_to_date(0), "1970-01-01");
    }

    #[test]
    fn ts_ns_to_date_known() {
        // 2024-01-15 00:00:00 UTC = 1705276800 seconds = 1705276800_000_000_000 ns
        let ts = 1_705_276_800_000_000_000i64;
        assert_eq!(ts_ns_to_date(ts), "2024-01-15");
    }

    #[test]
    fn ingest_and_scan_memtable_only() {
        let dir = TempDir::new();
        let mut e = open_engine(&dir);
        let sym = e.intern_symbol("BTCUSDT").unwrap();

        let trades: Vec<Trade> = (0..100)
            .map(|i| make_trade(sym, i * 1_000_000, i as u64))
            .collect();
        e.ingest(&trades).unwrap();

        let batches: Vec<ColumnBatch> = e
            .scan(sym, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.ts.len()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn flush_and_rescan() {
        let dir = TempDir::new();
        let mut e = open_engine(&dir);
        let sym = e.intern_symbol("ETHUSDT").unwrap();

        let trades: Vec<Trade> = (0..500)
            .map(|i| make_trade(sym, 1_700_000_000_000_000_000 + i * 100_000, i as u64))
            .collect();
        e.ingest(&trades).unwrap();
        e.flush().unwrap();

        // After flush, memtable is empty; scan reads from the segment.
        let batches: Vec<ColumnBatch> = e
            .scan(sym, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.ts.len()).sum();
        assert_eq!(total, 500);
    }

    #[test]
    fn recovery_after_flush() {
        let dir = TempDir::new();
        let sym_id;

        {
            let mut e = open_engine(&dir);
            sym_id = e.intern_symbol("XYZUSDT").unwrap();
            let trades: Vec<Trade> = (0..200)
                .map(|i| make_trade(sym_id, 1_700_000_000_000_000_000 + i * 100_000, i as u64))
                .collect();
            e.ingest(&trades).unwrap();
            e.flush().unwrap();
            // Engine drops here — simulates restart.
        }

        // Reopen from disk.
        let e2 = Engine::open(dir.path(), FsyncPolicy::Never).unwrap();
        let batches: Vec<ColumnBatch> = e2
            .scan(sym_id, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.ts.len()).sum();
        assert_eq!(total, 200);
    }

    #[test]
    fn recovery_replays_wal() {
        // Ingest but do NOT flush; reopen and verify WAL replay.
        let dir = TempDir::new();
        let sym_id;

        {
            let mut e = open_engine(&dir);
            sym_id = e.intern_symbol("SOLANA").unwrap();
            let trades: Vec<Trade> = (0..50)
                .map(|i| make_trade(sym_id, 1_700_000_000_000_000_000 + i * 100_000, i as u64))
                .collect();
            e.ingest(&trades).unwrap();
            // Drop without flush — WAL remains.
        }

        let e2 = Engine::open(dir.path(), FsyncPolicy::Never).unwrap();
        let batches: Vec<ColumnBatch> = e2
            .scan(sym_id, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.ts.len()).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn dedup_across_reingest() {
        let dir = TempDir::new();
        let mut e = open_engine(&dir);
        let sym = e.intern_symbol("BNBUSDT").unwrap();

        let trades: Vec<Trade> = (0..10)
            .map(|i| make_trade(sym, i * 1000, i as u64))
            .collect();
        // Ingest twice (simulates re-ingest after a reconnect).
        e.ingest(&trades).unwrap();
        e.ingest(&trades).unwrap();
        e.flush().unwrap();

        let batches: Vec<ColumnBatch> = e
            .scan(sym, i64::MIN, i64::MAX)
            .collect::<Result<_, _>>()
            .unwrap();
        let total: usize = batches.iter().map(|b| b.ts.len()).sum();
        // Dedup by trade_id should collapse the 20 ingested to 10.
        assert_eq!(total, 10);
    }

    #[test]
    fn conservation_invariant() {
        // Property: ingest(random trades) → flush → scan returns exactly the
        // sorted, deduplicated input.  This is the Phase 1 correctness story.
        let dir = TempDir::new();
        let mut e = open_engine(&dir);
        let sym = e.intern_symbol("PROP").unwrap();

        let mut trades: Vec<Trade> = (0..300)
            .map(|i| {
                make_trade(
                    sym,
                    1_700_000_000_000_000_000 + (i * 17 % 300) * 100_000,
                    (i % 150) as u64,
                )
            })
            .collect();

        e.ingest(&trades).unwrap();
        e.flush().unwrap();

        // Expected: sort + dedup by trade_id.
        trades.sort_unstable_by_key(|t| (t.ts, t.trade_id));
        trades.dedup_by_key(|t| t.trade_id);

        let scanned: Vec<i64> = e
            .scan(sym, i64::MIN, i64::MAX)
            .flat_map(|r| r.unwrap().ts)
            .collect();

        let expected: Vec<i64> = trades.iter().map(|t| t.ts).collect();
        assert_eq!(scanned, expected);
    }
}
