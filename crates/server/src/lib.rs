pub mod arrow_convert;
pub mod error;
pub mod raft_runtime;
pub mod raft_transport;
pub mod routes;
pub mod snapshot_bundle;
pub mod state;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;

use state::AppState;

/// Builds the axum `Router` with all routes wired to `state`.
///
/// Shared between `main.rs` (real server) and integration tests (in-process
/// server via `axum::serve` on an ephemeral port).
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ingest", post(routes::ingest))
        .route("/query/bars", get(routes::bars))
        .route("/query/trades", get(routes::trades))
        .route("/query/vwap", get(routes::vwap))
        .route("/query/asof", get(routes::asof))
        .route("/admin/flush", post(routes::flush))
        .route("/admin/compact", post(routes::compact))
        .route("/stats", get(routes::stats))
        .route("/raft/status", get(routes::raft_status))
        .with_state(state)
}
