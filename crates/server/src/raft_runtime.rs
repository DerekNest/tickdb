/// The Raft runtime driver (milestone 9).
///
/// Owns the single `RaftNode` for this server process and runs the event
/// loop that feeds it `Tick` and `Message` events, then executes whatever
/// `Effect`s come back:
///
///   - `Send`      → `PeerConnections` (real TCP, chunked for snapshots)
///   - `Persist`    → an on-disk `hardstate.json` (atomic write)
///   - `ApplyCommitted` → deserialize each entry's payload as a
///     `ReplicatedWrite` and call `Engine::ingest`
///   - `ApplySnapshot`  → unpack the bundled manifest + segment files
///     (§4.4) onto disk and reopen the storage engine from them
///   - `ResetElectionTimer` / `ProposeRejected` → handled inline (the tick
///     loop already ticks unconditionally, and a rejection notifies
///     whichever HTTP request is waiting on this proposal, if any)
///
/// This is the only place in the server that touches `RaftNode` directly —
/// everything else (the HTTP layer) talks to it through `RaftHandle`.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use format::types::Trade;
use raft::{Effect, Entry, Event, HardState, LogIndex, Message, NodeId, RaftNode, Role};
use storage::{Engine, FsyncPolicy};

use crate::raft_transport::{self, PeerConnections};
use crate::snapshot_bundle;

/// One tick = 10ms, matching `sim::cluster::TICK_DURATION` — chosen so
/// Raft's millisecond-based timers (150–300ms election timeout, 50ms
/// heartbeat) land on clean multiples without needing sub-tick precision.
const TICK_DURATION: Duration = Duration::from_millis(10);

/// The payload carried by every non-empty raft log entry: a batch of
/// trades destined for one symbol. Serialized as JSON (matching the
/// transport's wire format — simplicity over raw throughput, since this is
/// consensus control-plane traffic, not the query hot path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedWrite {
    pub symbol: String,
    pub trades: Vec<Trade>,
}

/// Lightweight, lock-free snapshot of Raft status for read-routing
/// decisions in HTTP handlers (e.g. "redirect writes to the leader").
/// Updated by the driver loop after every `step()` call — cheap enough to
/// do unconditionally, and avoids handlers needing to reach into the
/// single-threaded driver for a simple role check.
struct StatusCell {
    /// Encodes `Role` as 0=Follower, 1=Candidate, 2=Leader.
    role: AtomicU8,
    current_leader: AtomicU64, // NodeId, or u64::MAX for "none"
}

impl StatusCell {
    fn new() -> Self {
        StatusCell {
            role: AtomicU8::new(0),
            current_leader: AtomicU64::new(u64::MAX),
        }
    }

    fn update(&self, role: Role, current_leader: Option<NodeId>) {
        let encoded = match role {
            Role::Follower => 0,
            Role::Candidate => 1,
            Role::Leader => 2,
        };
        self.role.store(encoded, Ordering::Relaxed);
        self.current_leader.store(
            current_leader.map(|id| id as u64).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn role(&self) -> Role {
        match self.role.load(Ordering::Relaxed) {
            2 => Role::Leader,
            1 => Role::Candidate,
            _ => Role::Follower,
        }
    }

    fn current_leader(&self) -> Option<NodeId> {
        let v = self.current_leader.load(Ordering::Relaxed);
        if v == u64::MAX {
            None
        } else {
            Some(v as NodeId)
        }
    }
}

/// Result of a client proposal, delivered once the entry has either
/// committed or been rejected.
#[derive(Debug)]
pub enum ProposeOutcome {
    /// The entry committed at this log index — safe to consider durable.
    Committed { index: LogIndex },
    /// This node isn't the leader; `leader_hint` names the last-known
    /// leader, if any, for the HTTP layer to redirect to.
    NotLeader { leader_hint: Option<NodeId> },
}

/// A pending proposal awaiting commit confirmation, keyed by the log index
/// it was assigned when accepted.
struct PendingProposal {
    index: LogIndex,
    respond: oneshot::Sender<ProposeOutcome>,
}

/// Handle for interacting with the Raft runtime from HTTP handlers.
/// Cheap to clone (just an `Arc` + channel senders).
#[derive(Clone)]
pub struct RaftHandle {
    propose_tx: mpsc::Sender<(Vec<u8>, oneshot::Sender<ProposeOutcome>)>,
    status: Arc<StatusCell>,
    node_id: NodeId,
}

impl RaftHandle {
    /// Proposes `write` to the replicated log. Resolves once the entry
    /// commits (or is immediately rejected, if this node isn't leader).
    ///
    /// A commit confirmation here means a majority of the cluster has
    /// durably persisted the entry — it does **not** mean every node has
    /// applied it to its storage engine yet (that follows asynchronously
    /// via `ApplyCommitted`, same as any Raft-backed system).
    pub async fn propose(&self, write: ReplicatedWrite) -> Result<LogIndex, Option<NodeId>> {
        let payload = serde_json::to_vec(&write).expect("ReplicatedWrite always serializes");
        let (tx, rx) = oneshot::channel();
        if self.propose_tx.send((payload, tx)).await.is_err() {
            return Err(None); // runtime shut down
        }
        match rx.await {
            Ok(ProposeOutcome::Committed { index }) => Ok(index),
            Ok(ProposeOutcome::NotLeader { leader_hint }) => Err(leader_hint),
            Err(_) => Err(None), // runtime dropped the sender without responding
        }
    }

    pub fn role(&self) -> Role {
        self.status.role()
    }
    pub fn current_leader(&self) -> Option<NodeId> {
        self.status.current_leader()
    }
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }
}

/// Static cluster configuration: this node's id, every peer's TCP address,
/// and the local bind address to listen on.
pub struct ClusterConfig {
    pub node_id: NodeId,
    pub bind_addr: SocketAddr,
    /// peer id -> peer's raft-transport address (does NOT include this node).
    pub peer_addrs: HashMap<NodeId, String>,
    /// Directory for `hardstate.json` and snapshot bundles — separate from
    /// the storage engine's own `data_dir`, though in practice a server
    /// deployment points both at the same root.
    pub raft_dir: PathBuf,
}

/// Spawns the Raft runtime: the TCP listener, outbound peer connections,
/// and the driver loop itself. Returns a `RaftHandle` for the HTTP layer.
///
/// `engine` is the same `Arc<tokio::sync::Mutex<Engine>>` the HTTP query
/// handlers use — `ApplyCommitted` writes go through the identical ingest
/// path a direct (non-replicated) `/ingest` call would use.
pub async fn spawn(
    config: ClusterConfig,
    engine: Arc<AsyncMutex<Engine>>,
) -> std::io::Result<RaftHandle> {
    let hardstate_path = config.raft_dir.join("hardstate.json");
    let snapshot_path = config.raft_dir.join("snapshot.bin");
    tokio::fs::create_dir_all(&config.raft_dir).await?;

    let (hs, snapshot_data) = load_persisted_state(&hardstate_path, &snapshot_path).await;

    let peers: Vec<NodeId> = config.peer_addrs.keys().copied().collect();
    let seed = (config.node_id as u64).wrapping_mul(0x9E3779B97F4A7C15) ^ 0xC0FFEE; // deterministic per-node, distinct across nodes
    let node = RaftNode::restore(
        config.node_id,
        peers,
        seed,
        Instant::now(),
        hs,
        snapshot_data,
    );

    let (inbound_tx, inbound_rx) = mpsc::channel(1024);
    let (propose_tx, propose_rx) = mpsc::channel(256);

    raft_transport::spawn_listener(config.bind_addr, inbound_tx);
    let peer_connections = PeerConnections::spawn(config.peer_addrs);

    let status = Arc::new(StatusCell::new());
    let handle = RaftHandle {
        propose_tx,
        status: status.clone(),
        node_id: config.node_id,
    };

    let driver = Driver {
        node,
        peer_connections,
        inbound_rx,
        propose_rx,
        hardstate_path,
        snapshot_path,
        engine,
        status,
        pending: Vec::new(),
    };
    tokio::spawn(driver.run());

    Ok(handle)
}

async fn load_persisted_state(hardstate_path: &Path, snapshot_path: &Path) -> (HardState, Vec<u8>) {
    let hs = match tokio::fs::read(hardstate_path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(_) => HardState::default(),
    };
    let snapshot_data = tokio::fs::read(snapshot_path).await.unwrap_or_default();
    (hs, snapshot_data)
}

/// Atomically writes `bytes` to `path` (write-temp, fsync, rename — the
/// same durability pattern used throughout `crates/storage`).
async fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, bytes).await?;
    {
        let f = tokio::fs::File::open(&tmp).await?;
        f.sync_all().await?;
    }
    tokio::fs::rename(&tmp, path).await
}

struct Driver {
    node: RaftNode,
    peer_connections: PeerConnections,
    inbound_rx: mpsc::Receiver<(NodeId, Message)>,
    propose_rx: mpsc::Receiver<(Vec<u8>, oneshot::Sender<ProposeOutcome>)>,
    hardstate_path: PathBuf,
    snapshot_path: PathBuf,
    engine: Arc<AsyncMutex<Engine>>,
    status: Arc<StatusCell>,
    /// Proposals accepted by this node but not yet known to have committed.
    /// Resolved (in order) as `commit_index` advances past their index.
    pending: Vec<PendingProposal>,
}

impl Driver {
    async fn run(mut self) {
        let mut ticker = tokio::time::interval(TICK_DURATION);

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let effects = self.node.step(Event::Tick, Instant::now());
                    self.handle_effects(effects).await;
                }
                Some((from, msg)) = self.inbound_rx.recv() => {
                    let effects = self.node.step(Event::Message { from, msg }, Instant::now());
                    self.handle_effects(effects).await;
                }
                Some((payload, respond)) = self.propose_rx.recv() => {
                    let effects = self.node.step(Event::Propose(payload), Instant::now());

                    // `on_propose` either rejects outright (not leader) or
                    // accepts and appends exactly one entry — never both.
                    // Inspect the actual effect rather than inferring from
                    // post-step role, so a rejection always gets a response
                    // instead of silently dropping `respond`.
                    let rejection = effects.iter().find_map(|e| match e {
                        Effect::ProposeRejected { leader_hint } => Some(*leader_hint),
                        _ => None,
                    });

                    match rejection {
                        Some(leader_hint) => {
                            let _ = respond.send(ProposeOutcome::NotLeader { leader_hint });
                        }
                        None => {
                            // Accepted: the new entry is always the last one
                            // in the log (on_propose pushes exactly one).
                            // NOTE: if this node loses leadership before the
                            // entry commits (e.g. a higher-term leader
                            // truncates it), `respond` is simply never
                            // resolved and the caller's request eventually
                            // times out at the HTTP layer — same "in doubt"
                            // outcome any Raft-backed system has for writes
                            // that were accepted but never confirmed.
                            if let Some(last) = self.node.log().last() {
                                self.pending.push(PendingProposal { index: last.index, respond });
                            }
                        }
                    }

                    self.handle_effects(effects).await;
                }
            }

            self.status
                .update(self.node.role(), self.node.current_leader());
        }
    }

    async fn handle_effects(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send(to, msg) => {
                    self.peer_connections.send_maybe_chunked(to, msg).await;
                }
                Effect::Persist(hs) => {
                    if let Ok(bytes) = serde_json::to_vec(&hs) {
                        if let Err(e) = write_atomic(&self.hardstate_path, &bytes).await {
                            tracing::error!(err = %e, "failed to persist raft hard state");
                        }
                    }
                }
                Effect::ApplyCommitted(entries) => {
                    self.apply_committed(entries).await;
                }
                Effect::ApplySnapshot {
                    last_included_index,
                    data,
                } => {
                    if let Err(e) = write_atomic(&self.snapshot_path, &data).await {
                        tracing::error!(err = %e, "failed to persist raft snapshot bytes");
                    }
                    self.install_snapshot(last_included_index, &data).await;
                }
                Effect::ProposeRejected { leader_hint: _ } => {
                    // The rejected proposal is whichever one is at the
                    // FRONT of `pending` that we haven't resolved yet —
                    // but rejection happens synchronously with the
                    // Propose event itself (on_propose either accepts or
                    // rejects immediately, never both), so `pending`
                    // wasn't appended to for this call. Nothing to resolve
                    // here; the caller in `run()`'s propose_rx arm already
                    // holds `respond` and would need it — handled there
                    // instead. This arm exists so ProposeRejected doesn't
                    // silently fall through the match.
                }
                Effect::ResetElectionTimer => {} // the unconditional tick loop covers this
            }
        }

        self.resolve_committed_proposals();
    }

    /// Resolves any pending proposals whose index is now `<= commit_index`.
    fn resolve_committed_proposals(&mut self) {
        let commit_index = self.node.commit_index();
        let mut still_pending = Vec::with_capacity(self.pending.len());
        for p in self.pending.drain(..) {
            if p.index <= commit_index {
                let _ = p.respond.send(ProposeOutcome::Committed { index: p.index });
            } else {
                still_pending.push(p);
            }
        }
        self.pending = still_pending;
    }

    async fn apply_committed(&mut self, entries: Vec<Entry>) {
        let mut engine = self.engine.lock().await;
        for entry in &entries {
            if entry.payload.is_empty() {
                continue; // no-op entry from a leadership change
            }
            let write: ReplicatedWrite = match serde_json::from_slice(&entry.payload) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!(err = %e, index = entry.index, "failed to decode replicated write");
                    continue;
                }
            };
            let symbol_id = match engine.intern_symbol(&write.symbol) {
                Ok(id) => id,
                Err(e) => {
                    tracing::error!(err = %e, symbol = %write.symbol, "failed to intern symbol during apply");
                    continue;
                }
            };
            let trades: Vec<Trade> = write
                .trades
                .into_iter()
                .map(|mut t| {
                    t.symbol = symbol_id;
                    t
                })
                .collect();
            if let Err(e) = engine.ingest(&trades) {
                tracing::error!(err = %e, index = entry.index, "failed to apply committed entry to storage engine");
            }
        }
    }

    /// Installs a full snapshot: unpacks the bundled manifest + segment
    /// files onto disk and reopens the storage engine from them, replacing
    /// the shared `Arc<AsyncMutex<Engine>>`'s contents in place.
    async fn install_snapshot(&mut self, _last_included_index: LogIndex, data: &[u8]) {
        let engine = self.engine.lock().await;
        let data_dir = engine.data_dir().to_path_buf();
        drop(engine); // release the lock while doing file I/O

        if let Err(e) = snapshot_bundle::unpack(data, &data_dir) {
            tracing::error!(err = %e, "failed to unpack installed snapshot");
            return;
        }

        match Engine::open(&data_dir, FsyncPolicy::EveryNMs(50)) {
            Ok(new_engine) => {
                let mut engine = self.engine.lock().await;
                *engine = new_engine;
            }
            Err(e) => tracing::error!(err = %e, "failed to reopen engine after snapshot install"),
        }
    }
}
