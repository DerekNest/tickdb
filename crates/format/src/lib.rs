pub mod compress;
pub mod dict;
pub mod dod;
pub mod dz;
pub mod rle;
pub mod segment;
pub mod size_col;
pub mod types;
pub mod varint;

pub use dz::Decimal64;
pub use error::Error;
pub use types::{Side, SymbolId, Trade};

mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum Error {
        #[error("input truncated: expected {expected} more bytes")]
        Truncated { expected: usize },
        #[error("varint overflowed 64-bit range")]
        VarintOverflow,
        #[error(
            "block allocation guard: declared {declared} rows but only {remaining} bytes remain"
        )]
        AllocationGuard { declared: usize, remaining: usize },
        #[error(
            "corrupt block: crc32c mismatch (stored {stored:#010x}, computed {computed:#010x})"
        )]
        Crc32Mismatch { stored: u32, computed: u32 },
        #[error("unknown compression flag: {0:#04x}")]
        UnknownCompression(u8),
        #[error("lz4 decompression failed: {0}")]
        Lz4(String),
        #[error("zstd decompression failed: {0}")]
        Zstd(String),
        #[error("price exponent rescale would overflow i64")]
        PriceOverflow,
    }
}
