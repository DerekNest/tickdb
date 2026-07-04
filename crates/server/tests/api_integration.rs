/// End-to-end HTTP API test.
///
/// Boots the axum router in-process (no real socket — `tower::ServiceExt::oneshot`),
/// ingests trades via `POST /ingest`, then verifies every query endpoint returns
/// correct results by decoding the Arrow IPC / JSON responses.
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;

use server::build_router;
use server::state::AppState;

static DIR_CTR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let n = DIR_CTR.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("tickdb_api_test_{n}"));
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

fn make_state(dir: &TempDir) -> Arc<AppState> {
    Arc::new(AppState::open(dir.path().to_path_buf(), storage::FsyncPolicy::Never).unwrap())
}

/// Sends a JSON POST request and returns (status, body bytes).
async fn post_json(
    app: axum::Router,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body.to_vec())
}

/// Sends a GET request and returns (status, content-type, body bytes).
async fn get(app: axum::Router, path: &str) -> (StatusCode, Option<String>, Vec<u8>) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_owned());
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, content_type, body.to_vec())
}

/// Decodes Arrow IPC bytes into RecordBatches.
fn decode_arrow(bytes: &[u8]) -> Vec<arrow::record_batch::RecordBatch> {
    let cursor = std::io::Cursor::new(bytes.to_vec());
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None).unwrap();
    reader.collect::<Result<_, _>>().unwrap()
}

fn ingest_body(symbol: &str, trades: Vec<serde_json::Value>) -> serde_json::Value {
    json!({ "symbol": symbol, "trades": trades })
}

fn trade_json(
    ts: i64,
    mantissa: i64,
    exponent: i8,
    size: u64,
    side: u8,
    trade_id: u64,
) -> serde_json::Value {
    json!({
        "ts": ts,
        "price_mantissa": mantissa,
        "price_exponent": exponent,
        "size": size,
        "side": side,
        "trade_id": trade_id,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn ingest_then_trades() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let trades: Vec<serde_json::Value> = (0..10)
        .map(|i| {
            trade_json(
                1_700_000_000_000_000_000 + i * 1_000_000,
                5_000_000 + i,
                -2,
                100,
                0,
                i as u64,
            )
        })
        .collect();

    let (status, body) = post_json(app.clone(), "/ingest", ingest_body("BTCUSDT", trades)).await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resp["ingested"], 10);

    let (status, content_type, body) = get(
        app.clone(),
        &format!("/query/trades?symbol=BTCUSDT&start=0&end={}", i64::MAX),
    )
    .await;
    // start=0..i64::MAX-ish; use a wide range
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.unwrap(), "application/vnd.apache.arrow.stream");

    let batches = decode_arrow(&body);
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 10);
}

#[tokio::test]
async fn ingest_then_bars() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let base_ts = 1_700_000_000_000_000_000i64;
    let trades: Vec<serde_json::Value> = (0..100)
        .map(|i| {
            trade_json(
                base_ts + i * 100_000_000,
                5_000_000 + (i % 10),
                -2,
                10,
                0,
                i as u64,
            )
        })
        .collect();

    let (status, _) = post_json(app.clone(), "/ingest", ingest_body("ETHUSDT", trades)).await;
    assert_eq!(status, StatusCode::OK);

    let range_end = base_ts + 100 * 100_000_000;
    let url = format!(
        "/query/bars?symbol=ETHUSDT&start={base_ts}&end={range_end}&interval_ns=1000000000"
    );
    let (status, content_type, body) = get(app.clone(), &url).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.unwrap(), "application/vnd.apache.arrow.stream");

    let batches = decode_arrow(&body);
    assert_eq!(batches.len(), 1);
    assert!(batches[0].num_rows() > 0, "expected at least one bar");

    // Check schema has the expected columns.
    let schema = batches[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "ts",
            "open",
            "high",
            "low",
            "close",
            "vwap",
            "volume",
            "trade_count"
        ]
    );
}

#[tokio::test]
async fn ingest_then_vwap() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let base_ts = 1_700_000_000_000_000_000i64;
    // Two trades: price 100.00 size 1, price 200.00 size 1 → vwap = 150.00
    let trades = vec![
        trade_json(base_ts, 10000, -2, 1, 0, 0),
        trade_json(base_ts + 1, 20000, -2, 1, 0, 1),
    ];
    post_json(app.clone(), "/ingest", ingest_body("VWAPSYM", trades)).await;

    let url = format!(
        "/query/vwap?symbol=VWAPSYM&start={base_ts}&end={}",
        base_ts + 1
    );
    let (status, _, body) = get(app.clone(), &url).await;
    assert_eq!(status, StatusCode::OK);

    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let vwap = resp["vwap"].as_f64().unwrap();
    assert!((vwap - 150.0).abs() < 1e-9, "vwap = {vwap}");
}

#[tokio::test]
async fn asof_join_via_http() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let base_ts = 1_700_000_000_000_000_000i64;

    // Left symbol: 1 trade at base_ts + 100ms
    let left = vec![trade_json(base_ts + 100_000_000, 50000, -2, 1, 0, 0)];
    // Right symbol: 1 trade at base_ts
    let right = vec![trade_json(base_ts, 49000, -2, 1, 0, 100)];

    post_json(app.clone(), "/ingest", ingest_body("LEFT", left)).await;
    post_json(app.clone(), "/ingest", ingest_body("RIGHT", right)).await;

    let url = format!(
        "/query/asof?left=LEFT&right=RIGHT&start={base_ts}&end={}&tolerance_ns=1000000000",
        base_ts + 200_000_000
    );
    let (status, content_type, body) = get(app.clone(), &url).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type.unwrap(), "application/vnd.apache.arrow.stream");

    let batches = decode_arrow(&body);
    assert_eq!(batches[0].num_rows(), 1);

    let right_ts_col = batches[0].column_by_name("right_ts").unwrap();
    assert!(
        !right_ts_col.is_null(0),
        "expected a right match within tolerance"
    );
}

#[tokio::test]
async fn flush_and_stats() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let trades: Vec<serde_json::Value> = (0..50)
        .map(|i| {
            trade_json(
                1_700_000_000_000_000_000 + i * 1_000_000,
                5_000_000,
                -2,
                1,
                0,
                i as u64,
            )
        })
        .collect();
    post_json(app.clone(), "/ingest", ingest_body("FLUSHSYM", trades)).await;

    let (status, body) = post_json(app.clone(), "/admin/flush", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resp["ok"], true);

    let (status, _, body) = get(app.clone(), "/stats").await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resp["segment_count"], 1);
    assert_eq!(resp["total_rows"], 50);
}

#[tokio::test]
async fn unknown_symbol_returns_404() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let (status, _, _) = get(app.clone(), "/query/vwap?symbol=NOPE&start=0&end=1000").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn empty_ingest_is_ok() {
    let dir = TempDir::new();
    let state = make_state(&dir);
    let app = build_router(state.clone());

    let (status, body) = post_json(app.clone(), "/ingest", ingest_body("EMPTY", vec![])).await;
    assert_eq!(status, StatusCode::OK);
    let resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(resp["ingested"], 0);
}
