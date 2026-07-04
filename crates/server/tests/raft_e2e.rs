/// End-to-end 3-node Raft cluster test, over real TCP sockets on
/// 127.0.0.1 — not the deterministic simulator (`crates/sim`), an actual
/// `tokio::net::TcpListener`/`TcpStream` pair per connection, three
/// independent storage engines on three independent temp directories, and
/// wall-clock timing.
///
/// This is the test that answers "does the real transport + runtime
/// actually work," as opposed to "is the Raft algorithm correct" (which
/// `crates/sim`'s 1,000-seed campaign already covers exhaustively under a
/// virtual clock). Wall-clock timing means these tests are slower and have
/// a little real scheduling jitter, but they're exercising genuinely
/// different code (`raft_transport`, `raft_runtime`, `snapshot_bundle`)
/// that the simulator never touches.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;

use format::types::{Decimal64, Side, Trade};
use raft::Role;
use server::raft_runtime::{self, ClusterConfig, RaftHandle, ReplicatedWrite};
use storage::{Engine, FsyncPolicy};

static DIR_CTR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let n = DIR_CTR.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("tickdb_raft_e2e_{n}"));
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

/// Grabs an OS-assigned free port by binding then immediately dropping the
/// listener — the same trick used elsewhere in this codebase's tests to
/// avoid hardcoded ports colliding across parallel test runs.
async fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

struct TestNode {
    handle: RaftHandle,
    engine: Arc<AsyncMutex<Engine>>,
    _data_dir: TempDir,
    _raft_dir: TempDir,
}

/// Spins up a real `n`-node cluster: each node gets its own temp data dir,
/// its own TCP listener on a free port, and its own `raft_runtime::spawn`
/// task talking to the others over real sockets.
async fn spawn_cluster(n: usize) -> Vec<TestNode> {
    let mut addrs = Vec::with_capacity(n);
    for _ in 0..n {
        addrs.push(free_addr().await);
    }

    let mut nodes = Vec::with_capacity(n);
    for i in 0..n {
        let node_id = i as u32;
        let mut peer_addrs = HashMap::new();
        for (j, &addr) in addrs.iter().enumerate() {
            if j != i {
                peer_addrs.insert(j as u32, addr);
            }
        }

        let data_dir = TempDir::new();
        let raft_dir = TempDir::new();
        let engine = Arc::new(AsyncMutex::new(
            Engine::open(data_dir.path(), FsyncPolicy::Never).unwrap(),
        ));

        let config = ClusterConfig {
            node_id,
            bind_addr: addrs[i],
            peer_addrs,
            raft_dir: raft_dir.path().to_path_buf(),
        };

        let handle = raft_runtime::spawn(config, engine.clone()).await.unwrap();
        nodes.push(TestNode {
            handle,
            engine,
            _data_dir: data_dir,
            _raft_dir: raft_dir,
        });
    }

    // Give the listeners a moment to bind before anyone tries to connect —
    // real TCP, real OS scheduling, so this isn't instantaneous.
    tokio::time::sleep(Duration::from_millis(100)).await;
    nodes
}

/// Polls every node's `RaftHandle` until exactly one reports `Role::Leader`,
/// or panics after `timeout`.
async fn wait_for_leader(nodes: &[TestNode], timeout: Duration) -> usize {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let leaders: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, n)| n.handle.role() == Role::Leader)
            .map(|(i, _)| i)
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("no single leader emerged within {timeout:?} (saw: {leaders:?})");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn sample_trades(n: usize) -> Vec<Trade> {
    (0..n)
        .map(|i| Trade {
            symbol: 0, // overwritten by apply_committed via intern_symbol
            ts: 1_700_000_000_000_000_000 + i as i64 * 1_000_000,
            price: Decimal64 {
                mantissa: 5_000_000 + i as i64,
                exponent: -2,
            },
            size: 100,
            side: Side::Buy,
            trade_id: i as u64,
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn three_real_nodes_elect_a_leader_over_tcp() {
    let nodes = spawn_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5)).await;

    // Exactly one leader; the other two must be followers.
    let leader_count = nodes
        .iter()
        .filter(|n| n.handle.role() == Role::Leader)
        .count();
    assert_eq!(leader_count, 1);
    let _ = leader_idx;
}

#[tokio::test]
async fn proposed_write_replicates_and_lands_in_every_engine() {
    let nodes = spawn_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5)).await;

    let write = ReplicatedWrite {
        symbol: "BTCUSDT".to_string(),
        trades: sample_trades(10),
    };

    let commit_index = nodes[leader_idx]
        .handle
        .propose(write)
        .await
        .expect("leader should accept the proposal");
    assert!(commit_index >= 1);

    // Give replication + apply a moment to reach every node.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for (i, node) in nodes.iter().enumerate() {
        let engine = node.engine.lock().await;
        let symbol_id = engine
            .intern_symbol_ro("BTCUSDT")
            .unwrap_or_else(|| panic!("node {i} never applied the replicated write"));
        let count: usize = engine
            .scan(symbol_id, i64::MIN, i64::MAX)
            .map(|b| b.unwrap().ts.len())
            .sum();
        assert_eq!(
            count, 10,
            "node {i} should have all 10 trades after replication"
        );
    }
}

#[tokio::test]
async fn write_to_a_follower_gets_rejected_with_leader_hint() {
    let nodes = spawn_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5)).await;
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();

    let write = ReplicatedWrite {
        symbol: "ETHUSDT".to_string(),
        trades: sample_trades(1),
    };
    let result = nodes[follower_idx].handle.propose(write).await;

    match result {
        Err(Some(hint)) => assert_eq!(
            hint, leader_idx as u32,
            "follower should hint at the real leader"
        ),
        Err(None) => panic!("expected a leader hint, got none"),
        Ok(_) => panic!("a follower must not accept a proposal directly"),
    }
}

#[tokio::test]
async fn multiple_writes_all_land_correctly() {
    let nodes = spawn_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5)).await;

    for batch in 0..5 {
        let write = ReplicatedWrite {
            symbol: "BTCUSDT".to_string(),
            trades: (0..4)
                .map(|i| Trade {
                    symbol: 0,
                    ts: 1_700_000_000_000_000_000 + (batch * 4 + i) as i64 * 1_000_000,
                    price: Decimal64 {
                        mantissa: 5_000_000,
                        exponent: -2,
                    },
                    size: 50,
                    side: Side::Sell,
                    trade_id: (batch * 4 + i) as u64,
                })
                .collect(),
        };
        nodes[leader_idx]
            .handle
            .propose(write)
            .await
            .expect("proposal should succeed");
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    for (i, node) in nodes.iter().enumerate() {
        let engine = node.engine.lock().await;
        let symbol_id = engine.intern_symbol_ro("BTCUSDT").unwrap();
        let count: usize = engine
            .scan(symbol_id, i64::MIN, i64::MAX)
            .map(|b| b.unwrap().ts.len())
            .sum();
        assert_eq!(
            count, 20,
            "node {i} should have all 20 trades (5 batches x 4)"
        );
    }
}

#[tokio::test]
async fn snapshot_bundle_pack_unpack_matches_real_engine_state() {
    // Not a full InstallSnapshot-over-the-wire test (that needs a node to
    // fall behind the leader's compacted log, which — for the real raft
    // runtime — requires either a long-running crash or explicit
    // compaction wiring in the HTTP admin layer, neither of which exists
    // yet). What *is* fully exercised here: a real `Engine` writes real
    // segment files via `flush()`, `snapshot_bundle::pack` bundles exactly
    // what the manifest references, and `unpack` reproduces byte-identical
    // files that a freshly `Engine::open`'d instance can read back
    // correctly — the actual mechanism `install_snapshot` relies on.
    let src_dir = TempDir::new();
    let mut engine = Engine::open(src_dir.path(), FsyncPolicy::Never).unwrap();
    let symbol_id = engine.intern_symbol("BTCUSDT").unwrap();
    let trades: Vec<Trade> = sample_trades(50)
        .into_iter()
        .map(|mut t| {
            t.symbol = symbol_id;
            t
        })
        .collect();
    engine.ingest(&trades).unwrap();
    engine.flush().unwrap();
    drop(engine);

    let bundle = server::snapshot_bundle::pack(src_dir.path()).unwrap();
    assert!(!bundle.is_empty());

    let dst_dir = TempDir::new();
    server::snapshot_bundle::unpack(&bundle, dst_dir.path()).unwrap();

    let reopened = Engine::open(dst_dir.path(), FsyncPolicy::Never).unwrap();
    let reopened_symbol = reopened.intern_symbol_ro("BTCUSDT").unwrap();
    let count: usize = reopened
        .scan(reopened_symbol, i64::MIN, i64::MAX)
        .map(|b| b.unwrap().ts.len())
        .sum();
    assert_eq!(
        count, 50,
        "unpacked snapshot should reproduce every flushed trade"
    );
}

// ── HTTP-layer test: POST /ingest through the real axum router ──────────────
//
// Everything above talks to `RaftHandle` directly. This test goes through
// `server::build_router` + `server::state::AppState` exactly as a real
// deployment would — the only difference from production is using
// `tower::ServiceExt::oneshot` instead of a bound TCP listener for the HTTP
// side (the Raft side is still real TCP, same as every other test in this
// file). This is what actually proves `routes::ingest`'s raft-routing
// logic — not just `RaftHandle::propose` — works end to end.

#[tokio::test]
async fn http_ingest_routes_through_raft_consensus() {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::json;
    use tower::ServiceExt;

    use server::build_router;
    use server::state::AppState;

    let nodes = spawn_cluster(3).await;
    let leader_idx = wait_for_leader(&nodes, Duration::from_secs(5)).await;
    let follower_idx = (0..3).find(|&i| i != leader_idx).unwrap();

    // Wrap each node's already-running RaftHandle + engine in a real
    // AppState + router, exactly as `main.rs` does in multi-node mode.
    let leader_state = Arc::new(AppState::with_raft(
        nodes[leader_idx].engine.clone(),
        nodes[leader_idx].handle.clone(),
    ));
    let leader_app = build_router(leader_state);

    let follower_state = Arc::new(AppState::with_raft(
        nodes[follower_idx].engine.clone(),
        nodes[follower_idx].handle.clone(),
    ));
    let follower_app = build_router(follower_state);

    let body = json!({
        "symbol": "BTCUSDT",
        "trades": [{
            "ts": 1_700_000_000_000_000_000i64,
            "price_mantissa": 5_000_000,
            "price_exponent": -2,
            "size": 10,
            "side": 0,
            "trade_id": 1,
        }]
    });

    // POST to the follower first: must be rejected with 409 + a leader hint.
    let req = Request::builder()
        .method("POST")
        .uri("/ingest")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = follower_app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    assert_eq!(parsed["leader_hint"], leader_idx as u32);

    // POST to the leader: must succeed and report a committed_index.
    let req = Request::builder()
        .method("POST")
        .uri("/ingest")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = leader_app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp_body = resp.into_body().collect().await.unwrap().to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&resp_body).unwrap();
    assert_eq!(parsed["ingested"], 1);
    assert!(parsed["committed_index"].is_number());

    // Give replication a moment, then confirm every node's engine has it —
    // proving the write really went through consensus, not a local shortcut.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for (i, node) in nodes.iter().enumerate() {
        let engine = node.engine.lock().await;
        let symbol_id = engine
            .intern_symbol_ro("BTCUSDT")
            .unwrap_or_else(|| panic!("node {i} never received the HTTP-proposed write"));
        let count: usize = engine
            .scan(symbol_id, i64::MIN, i64::MAX)
            .map(|b| b.unwrap().ts.len())
            .sum();
        assert_eq!(count, 1, "node {i} should have exactly the one trade");
    }
}
