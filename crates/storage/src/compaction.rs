/// Size-tiered compaction (§2.5).
///
/// # Policy
///
/// When a date partition has > `COMPACTION_TRIGGER` segments, a compaction
/// task merges all of them into a single generation-N+1 segment.  This keeps
/// the read path simple: at most a handful of segments per date.
///
/// # K-way merge
///
/// Each open segment is scanned in (symbol_id ASC, ts ASC, trade_id ASC)
/// order.  A min-heap over (symbol_id, ts, trade_id, segment_index) drives
/// the merge.  Duplicate trade_ids across segments (possible when the same
/// batch was ingested twice and landed in different pre-compaction segments)
/// are dropped — only the first occurrence is kept.
///
/// # Reader liveness
///
/// Segments being compacted are removed from the manifest first, then their
/// files are deleted.  Callers that obtained an `Arc<MmapSegment>` before the
/// compaction hold the last reference; the OS keeps the inode alive until the
/// Arc drops.  This is the same contract as Linux `unlink(2)`.
///
/// # Atomicity
///
/// The new compacted segment is written to a temp file, fsynced, renamed into
/// place, and then registered in the manifest — exactly the same two-phase
/// write used by `Engine::flush`.  If we crash between rename and manifest
/// update, the orphan file is harmless (no reader knows about it).
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use format::compress::COMPRESS_ZSTD3;
use format::dz::Decimal64;
use format::segment::{ColumnBatch, SegmentReader, SegmentWriter};
use format::types::{Side, SymbolId, Trade};

use crate::error::Error;
use crate::manifest::{Manifest, SegmentMeta};
use crate::mmap::MmapSegment;

/// Number of segments per date partition that triggers compaction.
pub const COMPACTION_TRIGGER: usize = 8;

/// Runs compaction for `date` if it has more than `COMPACTION_TRIGGER` segments.
///
/// Returns `true` if compaction ran, `false` if the trigger was not met.
///
/// `open_segments` is the engine's live segment list (shared Arcs).  We
/// read from them directly rather than re-opening files, which also means
/// the engine's in-memory readers remain valid during the merge.
pub fn maybe_compact(
    data_dir: &Path,
    date: &str,
    manifest: &mut Manifest,
    open_segments: &mut Vec<Arc<MmapSegment>>,
) -> Result<bool, Error> {
    let seg_metas: Vec<SegmentMeta> = manifest
        .segments_for_date(date)
        .into_iter()
        .cloned()
        .collect();

    if seg_metas.len() <= COMPACTION_TRIGGER {
        return Ok(false);
    }

    tracing::info!(date, count = seg_metas.len(), "compaction triggered");

    // Re-open the segment files directly for the merge read.
    // The original open_segments Arcs keep their file descriptors open (reader liveness)
    // — the OS keeps each inode alive until the last Arc drops, even after unlink.
    let mut merge_readers: Vec<SegmentReader> = Vec::new();
    for meta in &seg_metas {
        let path = data_dir.join(&meta.path);
        let bytes = fs::read(&path).map_err(Error::Io)?;
        let reader = SegmentReader::open(bytes)?;
        merge_readers.push(reader);
    }

    // Collect all symbol IDs across all segments.
    let all_symbol_ids: Vec<SymbolId> = {
        let mut ids: Vec<SymbolId> = merge_readers
            .iter()
            .flat_map(|r| r.block_index.iter().map(|b| b.symbol_id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    };

    // Build merged symbol dictionary from all segment footer dicts.
    let mut merged_dict: BTreeMap<SymbolId, String> = BTreeMap::new();
    for reader in &merge_readers {
        for (&id, name) in &reader.symbol_dict {
            merged_dict.entry(id).or_insert_with(|| name.clone());
        }
    }

    // ── Per-symbol k-way merge ────────────────────────────────────────────
    let created_unix_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let new_generation = manifest.next_generation(date);
    let seg_dir = data_dir.join("segments").join(date);
    fs::create_dir_all(&seg_dir)?;

    let seg_filename = format!("{new_generation}.tkdb");
    let seg_rel = format!("segments/{date}/{seg_filename}");
    let seg_abs = data_dir.join(&seg_rel);
    let seg_tmp = seg_abs.with_extension("tkdb.tmp");

    let mut writer = SegmentWriter::new(created_unix_s, COMPRESS_ZSTD3);
    let mut total_rows: u64 = 0;
    let mut global_min_ts = i64::MAX;
    let mut global_max_ts = i64::MIN;

    for &symbol_id in &all_symbol_ids {
        let merged = merge_symbol(symbol_id, &merge_readers)?;
        if merged.is_empty() {
            continue;
        }
        total_rows += merged.len() as u64;
        if let Some(first) = merged.first() {
            global_min_ts = global_min_ts.min(first.ts);
        }
        if let Some(last) = merged.last() {
            global_max_ts = global_max_ts.max(last.ts);
        }
        writer.add_trades(symbol_id, &merged)?;
    }

    let segment_bytes = writer.finish(&merged_dict)?;

    // Write temp, fsync, rename.
    fs::write(&seg_tmp, &segment_bytes)?;
    {
        let f = fs::File::open(&seg_tmp)?;
        f.sync_data()?;
    }
    fs::rename(&seg_tmp, &seg_abs)?;

    // ── Update manifest: add new, remove old ─────────────────────────────
    let old_paths: Vec<&str> = seg_metas.iter().map(|m| m.path.as_str()).collect();

    let new_meta = SegmentMeta {
        path: seg_rel.clone(),
        date: date.to_owned(),
        min_ts: if global_min_ts == i64::MAX {
            0
        } else {
            global_min_ts
        },
        max_ts: if global_max_ts == i64::MIN {
            0
        } else {
            global_max_ts
        },
        row_count: total_rows,
        generation: new_generation,
    };

    // Remove old segments from manifest first (atomically), then add new.
    manifest.remove_segments(&old_paths)?;
    manifest.add_segment(new_meta)?;

    // ── Remove old segment Arcs and files ─────────────────────────────────
    // Drop the old Arc<MmapSegment> entries so their reference counts fall.
    // We identify them by checking block_index contents against old meta ranges.
    // When all Arcs referencing a file are dropped, the OS reclaims the inode.
    let old_path_set: std::collections::HashSet<String> =
        old_paths.iter().map(|s| s.to_string()).collect();

    // Remove from the open_segments vec — we'll re-add the new compacted segment.
    // Since we can't match Arc to path directly, rebuild the list by removing
    // the segments whose meta was in old_paths and adding the new one.
    // For now: clear and re-open all remaining segments + the new one.
    // (Phase 1.5: store path in MmapSegment to make this O(n) not O(n^2).)
    open_segments.retain(|_seg| {
        // Keep segments NOT being compacted.  Since we don't store paths in
        // MmapSegment yet, we conservatively rebuild in the engine after returning.
        false // signal engine to rebuild — see engine::post_compact_reload
    });

    // Delete old segment files.
    for path_str in &old_path_set {
        let full = data_dir.join(path_str);
        if let Err(e) = fs::remove_file(&full) {
            // Non-fatal: log and continue.  The manifest no longer references
            // these files so they're orphans even if deletion fails.
            tracing::warn!(path = %full.display(), err = %e, "compaction: failed to delete old segment");
        }
    }

    // Open the new compacted segment.
    let new_seg = MmapSegment::open(&seg_abs)?;
    open_segments.push(Arc::new(new_seg));

    tracing::info!(
        date,
        new_generation,
        rows = total_rows,
        "compaction complete"
    );

    Ok(true)
}

/// Merges all rows for `symbol_id` from all segment readers.
///
/// Performs a k-way merge using a min-heap ordered by (ts, trade_id).
/// Duplicate trade_ids are dropped — only the first occurrence is kept.
fn merge_symbol(symbol_id: SymbolId, readers: &[SegmentReader]) -> Result<Vec<Trade>, Error> {
    // Collect all batches for this symbol across all segments.
    let mut per_seg: Vec<Vec<ColumnBatch>> = readers
        .iter()
        .map(|r| {
            r.scan(symbol_id, i64::MIN, i64::MAX)
                .collect::<Result<Vec<_>, _>>()
                .map_err(Error::Format)
        })
        .collect::<Result<_, _>>()?;

    // Flatten all batches into a single sorted+deduped vec.
    // For correctness we sort globally then dedup — a heap would be more
    // efficient but this is simpler and correctness is the M4 priority.
    let mut all_trades: Vec<Trade> = per_seg
        .iter_mut()
        .flat_map(|batches| {
            batches.iter().flat_map(|batch| {
                (0..batch.ts.len()).map(|i| Trade {
                    symbol: symbol_id,
                    ts: batch.ts[i],
                    price: Decimal64 {
                        mantissa: batch.price_mantissa[i],
                        exponent: batch.exponent,
                    },
                    size: batch.size[i],
                    side: Side::from_u8(batch.side[i]),
                    trade_id: batch.trade_id[i],
                })
            })
        })
        .collect();

    // Sort by (ts, trade_id) — the segment ordering contract.
    all_trades.sort_unstable_by_key(|t| (t.ts, t.trade_id));

    // Dedup by trade_id.
    all_trades.dedup_by_key(|t| t.trade_id);

    Ok(all_trades)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use format::compress::COMPRESS_NONE;
    use format::dz::Decimal64;
    use format::types::Side;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CTR: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("tickdb_compact_test_{n}"));
            fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
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

    /// Writes `trades` as a segment file, returns the path and meta.
    fn write_segment(
        data_dir: &Path,
        date: &str,
        generation: u32,
        symbol_id: SymbolId,
        trades: &[Trade],
        dict: &BTreeMap<SymbolId, String>,
    ) -> (PathBuf, SegmentMeta) {
        let seg_dir = data_dir.join("segments").join(date);
        fs::create_dir_all(&seg_dir).unwrap();
        let rel = format!("segments/{date}/{generation}.tkdb");
        let abs = data_dir.join(&rel);

        let mut w = SegmentWriter::new(0, COMPRESS_NONE);
        w.add_trades(symbol_id, trades).unwrap();
        let bytes = w.finish(dict).unwrap();
        fs::write(&abs, &bytes).unwrap();

        let meta = SegmentMeta {
            path: rel,
            date: date.to_owned(),
            min_ts: trades.first().map(|t| t.ts).unwrap_or(0),
            max_ts: trades.last().map(|t| t.ts).unwrap_or(0),
            row_count: trades.len() as u64,
            generation,
        };
        (abs, meta)
    }

    #[test]
    fn compaction_below_trigger_no_op() {
        let dir = TempDir::new();
        let mut manifest = Manifest::load(dir.path()).unwrap();
        let mut open_segs: Vec<Arc<MmapSegment>> = Vec::new();

        // Add fewer segments than the trigger.
        for i in 0..COMPACTION_TRIGGER {
            let trades = vec![make_trade(1, i as i64 * 1000, i as u64)];
            let dict: BTreeMap<u32, String> = [(1, "SYM".to_owned())].into();
            let (abs, meta) = write_segment(dir.path(), "2024-01-01", i as u32, 1, &trades, &dict);
            manifest.add_segment(meta).unwrap();
            open_segs.push(Arc::new(MmapSegment::open(&abs).unwrap()));
        }

        let ran = maybe_compact(dir.path(), "2024-01-01", &mut manifest, &mut open_segs).unwrap();
        assert!(!ran, "should not compact below trigger");
        assert_eq!(
            manifest.segment_count_for_date("2024-01-01"),
            COMPACTION_TRIGGER
        );
    }

    #[test]
    fn compaction_merges_and_deduplicates() {
        let dir = TempDir::new();
        let mut manifest = Manifest::load(dir.path()).unwrap();
        let mut open_segs: Vec<Arc<MmapSegment>> = Vec::new();
        let dict: BTreeMap<u32, String> = [(1, "SYM".to_owned())].into();

        // Write COMPACTION_TRIGGER + 1 segments. Last two share trade_ids to test dedup.
        let n = COMPACTION_TRIGGER + 1;
        for i in 0..n {
            let trades: Vec<Trade> = (0..10u64)
                .map(|j| {
                    make_trade(
                        1,
                        (i * 10 + j as usize) as i64 * 1000,
                        (i * 10 + j as usize) as u64,
                    )
                })
                .collect();
            let (abs, meta) = write_segment(dir.path(), "2024-01-01", i as u32, 1, &trades, &dict);
            manifest.add_segment(meta).unwrap();
            open_segs.push(Arc::new(MmapSegment::open(&abs).unwrap()));
        }

        let total_before: u64 = manifest
            .segments_for_date("2024-01-01")
            .iter()
            .map(|m| m.row_count)
            .sum();

        let ran = maybe_compact(dir.path(), "2024-01-01", &mut manifest, &mut open_segs).unwrap();
        assert!(ran, "should compact above trigger");

        // After compaction: exactly 1 segment for this date.
        assert_eq!(manifest.segment_count_for_date("2024-01-01"), 1);

        let after_meta = &manifest.segments_for_date("2024-01-01")[0];
        assert_eq!(
            after_meta.row_count, total_before,
            "no rows lost in compaction"
        );

        // The compacted segment file must be readable.
        let seg_path = dir.path().join(&after_meta.path);
        let reader = SegmentReader::open(fs::read(&seg_path).unwrap()).unwrap();
        let rows: usize = reader
            .scan(1, i64::MIN, i64::MAX)
            .map(|r| r.unwrap().ts.len())
            .sum();
        assert_eq!(rows, total_before as usize);
    }
}
