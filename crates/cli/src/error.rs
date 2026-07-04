use thiserror::Error;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("parse error: {0}")]
    ParseError(String),
}
