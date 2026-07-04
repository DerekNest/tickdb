use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("query error: {0}")]
    Query(#[from] query::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Reserved for future request-validation errors (e.g. malformed query params
    /// beyond what serde already rejects).
    #[allow(dead_code)]
    #[error("bad request: {0}")]
    BadRequest(String),
}

impl Error {
    fn status(&self) -> StatusCode {
        match self {
            Error::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Arrow(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Query(query::Error::SymbolNotFound(_)) => StatusCode::NOT_FOUND,
            Error::Query(_) => StatusCode::BAD_REQUEST,
            Error::BadRequest(_) => StatusCode::BAD_REQUEST,
        }
    }
}

/// Converts errors into JSON HTTP responses: `{"error": "..."}`.
///
/// Internal details (storage paths, etc.) are not leaked — the `Display`
/// impl of each error variant is already a safe, human-readable message.
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}
