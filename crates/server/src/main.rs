/// tickdb HTTP server.
///
/// # Single-node mode (default)
///
/// ```text
/// tickdb-server --data-dir ./tickdb-data --port 8080
/// ```
///
/// # Multi-node (Raft) mode
///
/// Every node needs `--node-id`, a `--raft-bind` address for consensus
/// traffic (separate from the HTTP `--port`), and `--peers` listing every
/// *other* node as `id=host:port`. Example, node 0 of a 3-node cluster:
///
/// ```text
/// tickdb-server --data-dir ./data-0 --port 8080 \
///   --node-id 0 --raft-bind 127.0.0.1:9000 \
///   --peers 1=127.0.0.1:9001,2=127.0.0.1:9002
/// ```
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use server::build_router;
use server::raft_runtime::{self, ClusterConfig};
use server::state::AppState;
use storage::Engine;

#[derive(Parser)]
#[command(name = "tickdb-server", about = "tickdb HTTP query server")]
struct Args {
    /// Data directory (must already exist or will be created).
    #[arg(long, default_value = "./tickdb-data")]
    data_dir: PathBuf,

    /// HTTP port to listen on.
    #[arg(long, default_value = "8080")]
    port: u16,

    /// WAL fsync policy: every-write | every-n-ms | never
    #[arg(long, default_value = "every-n-ms")]
    fsync: String,

    /// Interval in ms for every-n-ms fsync policy.
    #[arg(long, default_value = "50")]
    fsync_ms: u64,

    /// This node's Raft id. Omit to run in single-node mode (no consensus).
    #[arg(long)]
    node_id: Option<u32>,

    /// Address to listen on for Raft consensus traffic (distinct from
    /// `--port`, which serves HTTP). Required if `--node-id` is set.
    #[arg(long)]
    raft_bind: Option<SocketAddr>,

    /// Comma-separated peer list: `id=host:port,id=host:port,...`.
    /// Required if `--node-id` is set.
    #[arg(long, default_value = "")]
    peers: String,

    /// Directory for Raft's hard state and snapshot files. Defaults to
    /// `--data-dir` if omitted (they can safely share a root).
    #[arg(long)]
    raft_dir: Option<PathBuf>,
}

fn parse_peers(spec: &str) -> Result<HashMap<u32, String>, Box<dyn std::error::Error>> {
    let mut map = HashMap::new();
    for entry in spec.split(',').filter(|s| !s.is_empty()) {
        let (id_str, addr_str) = entry
            .split_once('=')
            .ok_or_else(|| format!("invalid --peers entry (expected id=host:port): {entry}"))?;
        let id: u32 = id_str.parse()?;
        let addr: String = addr_str.to_string();
        map.insert(id, addr);
    }
    Ok(map)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .with_target(false)
        .init();

    let args = Args::parse();

    let policy = match args.fsync.as_str() {
        "every-write" => storage::FsyncPolicy::EveryWrite,
        "never" => storage::FsyncPolicy::Never,
        _ => storage::FsyncPolicy::EveryNMs(args.fsync_ms),
    };

    let state = match args.node_id {
        None => {
            // Single-node mode: original Phase 1/2 behavior.
            Arc::new(AppState::open(args.data_dir.clone(), policy)?)
        }
        Some(node_id) => {
            let raft_bind = args
                .raft_bind
                .ok_or("--raft-bind is required when --node-id is set")?;
            let peer_addrs = parse_peers(&args.peers)?;
            let raft_dir = args
                .raft_dir
                .clone()
                .unwrap_or_else(|| args.data_dir.clone());

            let engine = Arc::new(Mutex::new(Engine::open(args.data_dir.clone(), policy)?));

            let config = ClusterConfig {
                node_id,
                bind_addr: raft_bind,
                peer_addrs,
                raft_dir,
            };

            let handle = raft_runtime::spawn(config, engine.clone()).await?;
            tracing::info!(node_id, %raft_bind, "raft runtime started");

            Arc::new(AppState::with_raft(engine, handle))
        }
    };

    let app = build_router(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    tracing::info!(%addr, data_dir = %args.data_dir.display(), "tickdb-server listening");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
