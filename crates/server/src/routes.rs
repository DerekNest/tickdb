/// HTTP route handlers.
///
/// # Endpoints
///
/// - `POST /ingest`            — ingest a batch of trades (JSON body)
/// - `GET  /query/bars`        — OHLCV bars, Arrow IPC stream
/// - `GET  /query/trades`      — raw trades in a range, Arrow IPC stream
/// - `GET  /query/vwap`        — VWAP over a range, JSON
/// - `GET  /query/asof`        — as-of join, Arrow IPC stream
/// - `POST /admin/flush`       — force a memtable flush
/// - `POST /admin/compact`     — run compaction for a date partition
/// - `GET  /stats`             — manifest summary, JSON
use std::sync::Arc;

use axum::extract::{Query as AxumQuery, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use format::types::{Decimal64, Side, SymbolId, Trade};
use query::builder::{Db, Interval};

use crate::arrow_convert::{
    asof_to_record_batch, bars_to_record_batch, to_ipc_bytes, trades_to_record_batch,
};
use crate::error::Error;
use crate::state::AppState;

const ARROW_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

// ── Shared response helper ───────────────────────────────────────────────────

/// Wraps Arrow IPC bytes in an HTTP response with the correct content type.
fn arrow_response(bytes: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, ARROW_CONTENT_TYPE)], bytes).into_response()
}

// ── POST /ingest ──────────────────────────────────────────────────────────────

/// JSON representation of a trade for the ingest endpoint.
///
/// Mirrors `format::types::Trade` field-for-field but uses plain JSON types
/// (no custom serde for `Decimal64`/`Side`) so clients can construct it from
/// any language without depending on tickdb's Rust types.
#[derive(Debug, Deserialize)]
pub struct TradeDto {
    pub ts: i64,
    /// Price mantissa (integer).
    pub price_mantissa: i64,
    /// Price exponent (actual price = mantissa * 10^exponent).
    pub price_exponent: i8,
    pub size: u64,
    /// 0 = buy, 1 = sell, 2 = unknown.
    pub side: u8,
    pub trade_id: u64,
}

#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub symbol: String,
    pub trades: Vec<TradeDto>,
}

#[derive(Debug, Serialize)]
pub struct IngestResponse {
    pub ingested: usize,
    /// The raft log index this write committed at, if the cluster is
    /// running in multi-node mode. `None` in single-node mode (there's no
    /// log — the write already landed directly).
    pub committed_index: Option<u64>,
}

/// Ingests a batch of trades for `symbol`.
///
/// # Single-node mode (`state.raft` is `None`)
/// Writes straight to the engine, same as milestones 1–8.
///
/// # Multi-node mode (`state.raft` is `Some`)
/// Proposes the batch through Raft consensus instead of writing directly.
/// If this node isn't the leader, responds `409 Conflict` with a
/// `Location` header pointing at the leader's address (when known) — the
/// caller is expected to retry there. The engine write itself happens
/// asynchronously inside the Raft runtime's driver loop once the entry
/// commits (`Effect::ApplyCommitted`); by the time this handler returns
/// `200 OK`, a majority of the cluster has durably persisted the write,
/// but this node's own engine may apply it a moment later.
pub async fn ingest(
    State(state): State<Arc<AppState>>,
    Json(req): Json<IngestRequest>,
) -> Result<Response, Error> {
    if req.trades.is_empty() {
        return Ok(Json(IngestResponse {
            ingested: 0,
            committed_index: None,
        })
        .into_response());
    }

    let n = req.trades.len();
    let trades_dto = req.trades;

    match &state.raft {
        None => {
            let mut engine = state.engine.lock().await;
            let symbol_id: SymbolId = engine.intern_symbol(&req.symbol)?;
            let trades = dtos_to_trades(trades_dto, symbol_id);
            engine.ingest(&trades)?;
            Ok(Json(IngestResponse {
                ingested: n,
                committed_index: None,
            })
            .into_response())
        }
        Some(raft) => {
            // The symbol is interned lazily by whichever node applies the
            // committed entry (inside `raft_runtime::apply_committed`), so
            // the proposal carries a placeholder symbol id (0) — every
            // node re-derives the real one from `write.symbol` itself,
            // which keeps symbol tables consistent without needing symbol
            // interning to itself go through consensus.
            let trades = dtos_to_trades(trades_dto, 0);
            let write = crate::raft_runtime::ReplicatedWrite {
                symbol: req.symbol,
                trades,
            };

            match raft.propose(write).await {
                Ok(index) => Ok(Json(IngestResponse {
                    ingested: n,
                    committed_index: Some(index),
                })
                .into_response()),
                Err(leader_hint) => {
                    let body = Json(serde_json::json!({
                        "error": "not the leader",
                        "leader_hint": leader_hint,
                    }));
                    Ok((axum::http::StatusCode::CONFLICT, body).into_response())
                }
            }
        }
    }
}

fn dtos_to_trades(dtos: Vec<TradeDto>, symbol_id: SymbolId) -> Vec<Trade> {
    dtos.into_iter()
        .map(|dto| Trade {
            symbol: symbol_id,
            ts: dto.ts,
            price: Decimal64 {
                mantissa: dto.price_mantissa,
                exponent: dto.price_exponent,
            },
            size: dto.size,
            side: Side::from_u8(dto.side),
            trade_id: dto.trade_id,
        })
        .collect()
}

// ── GET /raft/status ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct RaftStatusResponse {
    pub enabled: bool,
    pub node_id: Option<u32>,
    pub role: Option<String>,
    pub current_leader: Option<u32>,
}

/// Reports this node's Raft role and known leader — useful for a load
/// balancer or client to find the leader without guessing, and for the
/// chaos-test script to poll re-election status.
pub async fn raft_status(State(state): State<Arc<AppState>>) -> Json<RaftStatusResponse> {
    match &state.raft {
        None => Json(RaftStatusResponse {
            enabled: false,
            node_id: None,
            role: None,
            current_leader: None,
        }),
        Some(raft) => Json(RaftStatusResponse {
            enabled: true,
            node_id: Some(raft.node_id()),
            role: Some(format!("{:?}", raft.role())),
            current_leader: raft.current_leader(),
        }),
    }
}

// ── Common query params ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RangeParams {
    pub symbol: String,
    /// Range start, nanoseconds since epoch.  Defaults to `i64::MIN`.
    #[serde(default = "default_ts_min")]
    pub start: i64,
    /// Range end, nanoseconds since epoch.  Defaults to `i64::MAX`.
    #[serde(default = "default_ts_max")]
    pub end: i64,
}

fn default_ts_min() -> i64 {
    i64::MIN
}
fn default_ts_max() -> i64 {
    i64::MAX
}

// ── GET /query/bars ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BarsParams {
    pub symbol: String,
    #[serde(default = "default_ts_min")]
    pub start: i64,
    #[serde(default = "default_ts_max")]
    pub end: i64,
    /// Bar interval in nanoseconds.  Required.
    pub interval_ns: i64,
}

/// Returns OHLCV bars as an Arrow IPC stream.
///
/// Example: `GET /query/bars?symbol=BTCUSDT&start=0&end=1000000000000&interval_ns=60000000000`
pub async fn bars(
    State(state): State<Arc<AppState>>,
    AxumQuery(params): AxumQuery<BarsParams>,
) -> Result<Response, Error> {
    let engine = state.engine.lock().await;
    let db = Db::new(&engine);

    let bars = db
        .scan(&params.symbol)
        .range(params.start, params.end)
        .bars(Interval::Custom(params.interval_ns))
        .collect()?;

    let batch = bars_to_record_batch(&bars)?;
    Ok(arrow_response(to_ipc_bytes(&batch)?))
}

// ── GET /query/trades ─────────────────────────────────────────────────────────

/// Returns raw trades in `[start, end]` as an Arrow IPC stream.
///
/// Example: `GET /query/trades?symbol=BTCUSDT&start=0&end=1000000000000`
pub async fn trades(
    State(state): State<Arc<AppState>>,
    AxumQuery(params): AxumQuery<RangeParams>,
) -> Result<Response, Error> {
    let engine = state.engine.lock().await;
    let db = Db::new(&engine);

    let trades = db
        .scan(&params.symbol)
        .range(params.start, params.end)
        .collect_trades()?;

    let batch = trades_to_record_batch(&trades)?;
    Ok(arrow_response(to_ipc_bytes(&batch)?))
}

// ── GET /query/vwap ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct VwapResponse {
    pub vwap: Option<f64>,
}

/// Returns VWAP over `[start, end]` as JSON.
///
/// Example: `GET /query/vwap?symbol=BTCUSDT&start=0&end=1000000000000`
pub async fn vwap(
    State(state): State<Arc<AppState>>,
    AxumQuery(params): AxumQuery<RangeParams>,
) -> Result<Json<VwapResponse>, Error> {
    let engine = state.engine.lock().await;
    let db = Db::new(&engine);

    let v = db
        .scan(&params.symbol)
        .range(params.start, params.end)
        .vwap()?;

    Ok(Json(VwapResponse { vwap: v }))
}

// ── GET /query/asof ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AsOfParams {
    pub left: String,
    pub right: String,
    #[serde(default = "default_ts_min")]
    pub start: i64,
    #[serde(default = "default_ts_max")]
    pub end: i64,
    /// Tolerance in nanoseconds.  Defaults to `i64::MAX` (always match).
    #[serde(default = "default_ts_max")]
    pub tolerance_ns: i64,
}

/// Returns an as-of join of `left` onto `right` as an Arrow IPC stream.
///
/// Example: `GET /query/asof?left=ETHUSDT&right=BTCUSDT&start=0&end=1000000000000&tolerance_ns=500000000`
pub async fn asof(
    State(state): State<Arc<AppState>>,
    AxumQuery(params): AxumQuery<AsOfParams>,
) -> Result<Response, Error> {
    let engine = state.engine.lock().await;
    let db = Db::new(&engine);

    let rows = db
        .asof(&params.left, &params.right)
        .range(params.start, params.end)
        .tolerance_ns(params.tolerance_ns)
        .collect()?;

    let batch = asof_to_record_batch(&rows)?;
    Ok(arrow_response(to_ipc_bytes(&batch)?))
}

// ── POST /admin/flush ─────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct OkResponse {
    pub ok: bool,
}

pub async fn flush(State(state): State<Arc<AppState>>) -> Result<Json<OkResponse>, Error> {
    let mut engine = state.engine.lock().await;
    engine.flush()?;
    Ok(Json(OkResponse { ok: true }))
}

// ── POST /admin/compact ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CompactRequest {
    pub date: String,
}

#[derive(Debug, Serialize)]
pub struct CompactResponse {
    pub ran: bool,
}

pub async fn compact(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CompactRequest>,
) -> Result<Json<CompactResponse>, Error> {
    let mut engine = state.engine.lock().await;
    let ran = engine.maybe_compact(&req.date)?;
    Ok(Json(CompactResponse { ran }))
}

// ── GET /stats ─────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub segment_count: usize,
    pub total_rows: u64,
    pub dates: Vec<String>,
}

pub async fn stats(State(state): State<Arc<AppState>>) -> Result<Json<StatsResponse>, Error> {
    let engine = state.engine.lock().await;
    let manifest = engine.manifest();

    let total_rows: u64 = manifest.segments.iter().map(|s| s.row_count).sum();

    Ok(Json(StatsResponse {
        segment_count: manifest.segments.len(),
        total_rows,
        dates: manifest.all_dates(),
    }))
}
