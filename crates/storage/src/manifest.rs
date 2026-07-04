/// Segment manifest — the authoritative list of live segment files.
///
/// # Durability model
///
/// The manifest is updated by write-temp + atomic rename so a crash during the
/// update never leaves a half-written manifest.  The invariant is:
///   "if a segment appears in MANIFEST, its segment file is complete and
///    fsync'd; if it doesn't appear, the file (if it exists) is ignored."
///
/// # Format
///
/// JSON array of `SegmentMeta` objects.  JSON is used here (not a binary
/// format) because the manifest is small (one entry per date×generation) and
/// human-readability aids debugging.  The segment files themselves use the
/// custom binary format.
///
/// # Recovery
///
/// On startup: load MANIFEST → for each entry, open the segment file with
/// memmap2, decode its footer into a block index.  Then replay the WAL into
/// the memtable.
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Metadata for one live segment file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Relative path from the data directory root.
    pub path: String,
    /// UTC date this segment covers, as "YYYY-MM-DD".
    pub date: String,
    /// Minimum timestamp (ns) across all rows in this segment.
    pub min_ts: i64,
    /// Maximum timestamp (ns) across all rows in this segment.
    pub max_ts: i64,
    /// Total row count across all symbols.
    pub row_count: u64,
    /// Generation counter — incremented by compaction.  When multiple segments
    /// share the same date, the highest generation is the compacted result and
    /// older ones are candidates for deletion once all readers release them.
    pub generation: u32,
}

pub struct Manifest {
    /// Path to the MANIFEST file.
    manifest_path: PathBuf,
    /// Temporary file path used during atomic rename.
    tmp_path: PathBuf,
    /// Live segment list, kept in memory.
    pub segments: Vec<SegmentMeta>,
}

impl Manifest {
    const MANIFEST_FILENAME: &'static str = "MANIFEST";
    const MANIFEST_TMP: &'static str = "MANIFEST.tmp";

    /// Loads or creates a manifest in `data_dir`.
    ///
    /// If the manifest file does not exist (first run), returns an empty
    /// manifest.  If it exists but is corrupt, returns `Error::ManifestCorrupt`.
    pub fn load(data_dir: impl AsRef<Path>) -> Result<Self, Error> {
        let manifest_path = data_dir.as_ref().join(Self::MANIFEST_FILENAME);
        let tmp_path = data_dir.as_ref().join(Self::MANIFEST_TMP);

        let segments = if manifest_path.exists() {
            let json = fs::read_to_string(&manifest_path).map_err(Error::Io)?;
            serde_json::from_str::<Vec<SegmentMeta>>(&json)
                .map_err(|e| Error::ManifestCorrupt(e.to_string()))?
        } else {
            Vec::new()
        };

        Ok(Manifest {
            manifest_path,
            tmp_path,
            segments,
        })
    }

    /// Adds a segment entry and atomically persists the updated manifest.
    ///
    /// The write order is:
    ///   1. Write JSON to MANIFEST.tmp
    ///   2. fsync MANIFEST.tmp
    ///   3. rename MANIFEST.tmp → MANIFEST  (atomic on POSIX)
    ///
    /// A crash at step 1 or 2 leaves MANIFEST unchanged (still valid).
    /// A crash at step 3 may leave .tmp behind, which is harmless — it's
    /// overwritten on the next update.
    pub fn add_segment(&mut self, meta: SegmentMeta) -> Result<(), Error> {
        self.segments.push(meta);
        self.persist()
    }

    /// Removes segments by path and persists.
    ///
    /// Used by compaction to remove superseded segments from the manifest
    /// before deleting their files.
    pub fn remove_segments(&mut self, paths: &[&str]) -> Result<(), Error> {
        self.segments.retain(|s| !paths.contains(&s.path.as_str()));
        self.persist()
    }

    /// Returns all segments covering a given date string ("YYYY-MM-DD"),
    /// sorted by generation ascending.
    pub fn segments_for_date(&self, date: &str) -> Vec<&SegmentMeta> {
        let mut segs: Vec<&SegmentMeta> = self.segments.iter().filter(|s| s.date == date).collect();
        segs.sort_by_key(|s| s.generation);
        segs
    }

    /// Returns the next generation number to use for `date`.
    pub fn next_generation(&self, date: &str) -> u32 {
        self.segments_for_date(date)
            .last()
            .map(|s| s.generation + 1)
            .unwrap_or(0)
    }

    /// Returns all unique dates covered by live segments.
    pub fn all_dates(&self) -> Vec<String> {
        let mut dates: Vec<String> = self
            .segments
            .iter()
            .map(|s| s.date.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        dates.sort();
        dates
    }

    /// Counts the number of live segments for a given date.
    pub fn segment_count_for_date(&self, date: &str) -> usize {
        self.segments.iter().filter(|s| s.date == date).count()
    }

    /// Builds a symbol-name → symbol-id mapping by scanning the data directory
    /// for a "symbols.json" side-file.  Returns an empty map if not found.
    ///
    /// The symbol table is a separate file (not in the manifest) so it can
    /// grow incrementally without rewriting the manifest.
    pub fn load_symbol_table(data_dir: impl AsRef<Path>) -> Result<BTreeMap<String, u32>, Error> {
        let path = data_dir.as_ref().join("symbols.json");
        if !path.exists() {
            return Ok(BTreeMap::new());
        }
        let json = fs::read_to_string(path)?;
        let map: BTreeMap<String, u32> =
            serde_json::from_str(&json).map_err(|e| Error::ManifestCorrupt(e.to_string()))?;
        Ok(map)
    }

    /// Persists the symbol table atomically.
    pub fn save_symbol_table(
        data_dir: impl AsRef<Path>,
        table: &BTreeMap<String, u32>,
    ) -> Result<(), Error> {
        let tmp = data_dir.as_ref().join("symbols.json.tmp");
        let final_path = data_dir.as_ref().join("symbols.json");
        let json = serde_json::to_string_pretty(table)
            .map_err(|e| Error::ManifestCorrupt(e.to_string()))?;
        fs::write(&tmp, json.as_bytes())?;
        // fsync the tmp file before rename.
        {
            let f = fs::File::open(&tmp)?;
            f.sync_data()?;
        }
        fs::rename(&tmp, &final_path)?;
        Ok(())
    }

    fn persist(&self) -> Result<(), Error> {
        let json = serde_json::to_string_pretty(&self.segments)
            .map_err(|e| Error::ManifestCorrupt(e.to_string()))?;

        fs::write(&self.tmp_path, json.as_bytes())?;

        // fsync the tmp file to ensure data reaches disk before the rename.
        {
            let f = fs::File::open(&self.tmp_path)?;
            f.sync_data()?;
        }

        // Atomic rename — on POSIX this is guaranteed to be atomic.
        fs::rename(&self.tmp_path, &self.manifest_path)?;

        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static CTR: AtomicU64 = AtomicU64::new(0);
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("tickdb_mfst_test_{n}"));
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

    fn sample_meta(date: &str, gen: u32) -> SegmentMeta {
        SegmentMeta {
            path: format!("segments/{date}/{gen}.tkdb"),
            date: date.to_owned(),
            min_ts: 0,
            max_ts: 1000,
            row_count: 100,
            generation: gen,
        }
    }

    #[test]
    fn load_empty_dir() {
        let dir = TempDir::new();
        let m = Manifest::load(dir.path()).unwrap();
        assert!(m.segments.is_empty());
    }

    #[test]
    fn add_and_reload() {
        let dir = TempDir::new();
        {
            let mut m = Manifest::load(dir.path()).unwrap();
            m.add_segment(sample_meta("2024-01-01", 0)).unwrap();
            m.add_segment(sample_meta("2024-01-02", 0)).unwrap();
        }
        let m2 = Manifest::load(dir.path()).unwrap();
        assert_eq!(m2.segments.len(), 2);
        assert_eq!(m2.segments[0].date, "2024-01-01");
    }

    #[test]
    fn remove_segment() {
        let dir = TempDir::new();
        let mut m = Manifest::load(dir.path()).unwrap();
        m.add_segment(sample_meta("2024-01-01", 0)).unwrap();
        m.add_segment(sample_meta("2024-01-01", 1)).unwrap();
        m.remove_segments(&["segments/2024-01-01/0.tkdb"]).unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.segments[0].generation, 1);

        // Reload confirms the removal is persisted.
        let m2 = Manifest::load(dir.path()).unwrap();
        assert_eq!(m2.segments.len(), 1);
    }

    #[test]
    fn next_generation() {
        let dir = TempDir::new();
        let mut m = Manifest::load(dir.path()).unwrap();
        assert_eq!(m.next_generation("2024-01-01"), 0);
        m.add_segment(sample_meta("2024-01-01", 0)).unwrap();
        assert_eq!(m.next_generation("2024-01-01"), 1);
        m.add_segment(sample_meta("2024-01-01", 1)).unwrap();
        assert_eq!(m.next_generation("2024-01-01"), 2);
    }

    #[test]
    fn segment_count_for_date() {
        let dir = TempDir::new();
        let mut m = Manifest::load(dir.path()).unwrap();
        m.add_segment(sample_meta("2024-01-01", 0)).unwrap();
        m.add_segment(sample_meta("2024-01-01", 1)).unwrap();
        m.add_segment(sample_meta("2024-01-02", 0)).unwrap();
        assert_eq!(m.segment_count_for_date("2024-01-01"), 2);
        assert_eq!(m.segment_count_for_date("2024-01-02"), 1);
        assert_eq!(m.segment_count_for_date("2024-01-03"), 0);
    }

    #[test]
    fn symbol_table_roundtrip() {
        let dir = TempDir::new();
        let mut table = BTreeMap::new();
        table.insert("BTCUSDT".to_owned(), 1u32);
        table.insert("ETHUSDT".to_owned(), 2u32);
        Manifest::save_symbol_table(dir.path(), &table).unwrap();

        let loaded = Manifest::load_symbol_table(dir.path()).unwrap();
        assert_eq!(loaded, table);
    }

    #[test]
    fn corrupt_manifest_returns_error() {
        let dir = TempDir::new();
        fs::write(dir.path().join("MANIFEST"), b"not valid json").unwrap();
        assert!(matches!(
            Manifest::load(dir.path()),
            Err(Error::ManifestCorrupt(_))
        ));
    }
}
