use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("invalid interval: {0} ns (must be > 0)")]
    InvalidInterval(i64),

    #[error("invalid time range: start {start} > end {end}")]
    InvalidRange { start: i64, end: i64 },

    #[error("symbol not found: {0}")]
    SymbolNotFound(String),

    #[error("as-of join: tolerance must be ≥ 0, got {0}")]
    InvalidTolerance(i64),
}
