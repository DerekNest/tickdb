use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("format error: {0}")]
    Format(#[from] format::Error),

    #[error("WAL record corrupt: crc32c mismatch (stored {stored:#010x}, computed {computed:#010x}) at offset {offset}")]
    WalCrc {
        stored: u32,
        computed: u32,
        offset: u64,
    },

    #[error("WAL record truncated at offset {offset}: need {need} bytes, have {have}")]
    WalTruncated {
        offset: u64,
        need: usize,
        have: usize,
    },

    #[error("manifest corrupt: {0}")]
    ManifestCorrupt(String),

    #[error("segment not found: {0}")]
    SegmentNotFound(String),

    #[error("symbol name too long: {len} bytes (max 65535)")]
    SymbolNameTooLong { len: usize },
}
