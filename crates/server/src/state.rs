/// Shared application state.
///
/// `engine` is wrapped in a `tokio::sync::Mutex` (not `std::sync::Mutex`) so
/// it can be shared identically between the HTTP handlers and the Raft
/// runtime's driver loop (`raft_runtime::spawn` takes the same
/// `Arc<tokio::sync::Mutex<Engine>>`) — in multi-node mode, both sides
/// operate on the exact same storage engine instance: the HTTP layer reads
/// from it directly, and writes go through `raft.propose()`, landing in the
/// engine only once `Effect::ApplyCommitted` fires inside the runtime.
///
/// `raft` is `None` in single-node mode (the original Phase 1/2 behavior:
/// `/ingest` writes straight to the engine, no consensus). When `Some`,
/// `/ingest` proposes through Raft instead — see `routes::ingest`.
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use storage::{Engine, FsyncPolicy};

use crate::raft_runtime::RaftHandle;

pub struct AppState {
    pub engine: Arc<Mutex<Engine>>,
    pub raft: Option<RaftHandle>,
}

impl AppState {
    /// Opens in single-node mode: no Raft, `/ingest` writes directly.
    pub fn open(data_dir: PathBuf, fsync_policy: FsyncPolicy) -> Result<Self, storage::Error> {
        let engine = Engine::open(data_dir, fsync_policy)?;
        Ok(AppState {
            engine: Arc::new(Mutex::new(engine)),
            raft: None,
        })
    }

    /// Wraps an already-open engine (used by multi-node mode, where the
    /// engine is opened first so its `Arc` can be handed to
    /// `raft_runtime::spawn` before this `AppState` exists) together with a
    /// live `RaftHandle`.
    pub fn with_raft(engine: Arc<Mutex<Engine>>, raft: RaftHandle) -> Self {
        AppState {
            engine,
            raft: Some(raft),
        }
    }
}
