/// Snapshot bundling (§4.4): packages the storage engine's durable state —
/// `MANIFEST`, `symbols.json`, and every segment file it references —
/// into one self-contained byte blob suitable for `Message::InstallSnapshot`.
///
/// # Format
///
/// A trivial custom container (no external archive crate needed):
///
/// ```text
/// [u32 file_count]
/// repeated file_count times:
///   [u32 path_len][path bytes, UTF-8, relative to data_dir]
///   [u64 content_len][content bytes]
/// ```
///
/// This is deliberately simple rather than clever — the bundle's purpose is
/// bulk state transfer to a follower that's fallen far behind, not a
/// general-purpose archive format. `zip`/`tar` would add a dependency and a
/// compression-negotiation surface for no benefit here (the segment files
/// inside are already zstd-compressed by the format layer).
use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

/// Packs `data_dir`'s manifest, symbol table, and every segment file listed
/// in the manifest into one blob.
///
/// Only files the manifest actually references are included — this is what
/// makes the snapshot a true point-in-time bundle rather than "whatever
/// happens to be on disk," and keeps it from growing unboundedly if old
/// segment files are ever orphaned before cleanup.
pub fn pack(data_dir: &Path) -> io::Result<Vec<u8>> {
    let mut relative_paths: BTreeSet<PathBuf> = BTreeSet::new();
    relative_paths.insert(PathBuf::from("MANIFEST"));
    relative_paths.insert(PathBuf::from("symbols.json"));

    // Discover segment files by reading MANIFEST's own "path" fields rather
    // than depending on `storage`'s internal types (keeps this module
    // dependency-free of `storage`, which in turn depends on `format` —
    // avoids a needless coupling for what's fundamentally a generic
    // "bundle these files" operation).
    let manifest_path = data_dir.join("MANIFEST");
    if let Ok(bytes) = std::fs::read(&manifest_path) {
        // MANIFEST is a raw JSON array of segment metadata objects (see
        // `storage::manifest::Manifest::persist`), not an object wrapping
        // a "segments" field.
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            if let Some(segments) = json.as_array() {
                for seg in segments {
                    if let Some(path) = seg.get("path").and_then(|p| p.as_str()) {
                        relative_paths.insert(PathBuf::from(path));
                    }
                }
            }
        }
    }

    let mut out = Vec::new();
    let mut included = Vec::new();
    for rel in &relative_paths {
        let full = data_dir.join(rel);
        if let Ok(content) = std::fs::read(&full) {
            included.push((rel.clone(), content));
        }
        // Files that don't exist (e.g. symbols.json before the first
        // intern_symbol call) are silently skipped — `unpack` only
        // restores what was actually captured.
    }

    out.write_all(&(included.len() as u32).to_be_bytes())?;
    for (rel, content) in &included {
        let path_str = rel.to_string_lossy();
        let path_bytes = path_str.as_bytes();
        out.write_all(&(path_bytes.len() as u32).to_be_bytes())?;
        out.write_all(path_bytes)?;
        out.write_all(&(content.len() as u64).to_be_bytes())?;
        out.write_all(content)?;
    }

    Ok(out)
}

/// Unpacks a bundle produced by `pack` onto disk at `data_dir`, overwriting
/// whatever's there. Writes are atomic per-file (temp + rename) so a
/// crash mid-unpack can't leave a half-written segment file that looks
/// valid but isn't.
pub fn unpack(data: &[u8], data_dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(data_dir)?;

    let mut cursor = io::Cursor::new(data);
    let mut count_buf = [0u8; 4];
    cursor.read_exact(&mut count_buf)?;
    let count = u32::from_be_bytes(count_buf);

    for _ in 0..count {
        let mut len_buf = [0u8; 4];
        cursor.read_exact(&mut len_buf)?;
        let path_len = u32::from_be_bytes(len_buf) as usize;

        let mut path_bytes = vec![0u8; path_len];
        cursor.read_exact(&mut path_bytes)?;
        let rel_path = String::from_utf8(path_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut content_len_buf = [0u8; 8];
        cursor.read_exact(&mut content_len_buf)?;
        let content_len = u64::from_be_bytes(content_len_buf) as usize;

        let mut content = vec![0u8; content_len];
        cursor.read_exact(&mut content)?;

        let full_path = data_dir.join(&rel_path);
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let tmp_path = full_path.with_extension("tmp-unpack");
        std::fs::write(&tmp_path, &content)?;
        std::fs::rename(&tmp_path, &full_path)?;
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static CTR: AtomicU64 = AtomicU64::new(0);

    struct TempDir(PathBuf);
    impl TempDir {
        fn new() -> Self {
            let n = CTR.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("tickdb_snapshot_bundle_test_{n}"));
            std::fs::create_dir_all(&p).unwrap();
            TempDir(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn pack_unpack_roundtrip_preserves_file_contents() {
        let src = TempDir::new();
        let dst = TempDir::new();

        std::fs::write(
            src.path().join("MANIFEST"),
            br#"[{"path":"segments/2024-01-01/0.tkdb"}]"#,
        )
        .unwrap();
        std::fs::write(src.path().join("symbols.json"), b"[[1,\"BTCUSDT\"]]").unwrap();
        std::fs::create_dir_all(src.path().join("segments/2024-01-01")).unwrap();
        std::fs::write(
            src.path().join("segments/2024-01-01/0.tkdb"),
            vec![0xAAu8; 4096],
        )
        .unwrap();

        let bundle = pack(src.path()).unwrap();
        unpack(&bundle, dst.path()).unwrap();

        let manifest = std::fs::read(dst.path().join("MANIFEST")).unwrap();
        assert_eq!(
            manifest,
            std::fs::read(src.path().join("MANIFEST")).unwrap()
        );

        let segment = std::fs::read(dst.path().join("segments/2024-01-01/0.tkdb")).unwrap();
        assert_eq!(segment.len(), 4096);
        assert!(segment.iter().all(|&b| b == 0xAA));
    }

    #[test]
    fn pack_skips_missing_symbols_file() {
        let src = TempDir::new();
        std::fs::write(src.path().join("MANIFEST"), br#"[]"#).unwrap();
        // symbols.json deliberately absent.
        let bundle = pack(src.path()).unwrap();

        let dst = TempDir::new();
        unpack(&bundle, dst.path()).unwrap();
        assert!(dst.path().join("MANIFEST").exists());
        assert!(!dst.path().join("symbols.json").exists());
    }

    #[test]
    fn pack_only_includes_manifest_referenced_segments() {
        let src = TempDir::new();
        std::fs::write(
            src.path().join("MANIFEST"),
            br#"[{"path":"segments/keep.tkdb"}]"#,
        )
        .unwrap();
        std::fs::create_dir_all(src.path().join("segments")).unwrap();
        std::fs::write(src.path().join("segments/keep.tkdb"), b"keep-me").unwrap();
        std::fs::write(src.path().join("segments/orphan.tkdb"), b"orphaned").unwrap();

        let bundle = pack(src.path()).unwrap();
        let dst = TempDir::new();
        unpack(&bundle, dst.path()).unwrap();

        assert!(dst.path().join("segments/keep.tkdb").exists());
        assert!(
            !dst.path().join("segments/orphan.tkdb").exists(),
            "orphaned files not referenced by the manifest must not be bundled"
        );
    }

    #[test]
    fn empty_data_dir_packs_to_empty_bundle() {
        let src = TempDir::new(); // no MANIFEST at all
        let bundle = pack(src.path()).unwrap();
        let dst = TempDir::new();
        unpack(&bundle, dst.path()).unwrap();
        assert!(!dst.path().join("MANIFEST").exists());
    }
}
