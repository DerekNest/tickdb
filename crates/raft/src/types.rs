/// Core Raft protocol types.
///
/// This module defines the wire-level vocabulary of the protocol — every
/// type a `RaftNode` can send, receive, or be told to persist.  Nothing
/// here performs I/O; serialization to bytes for the real transport
/// (`crates/server`, milestone 9) is a separate concern.
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Identifies a node in the cluster.  Stable for the cluster's lifetime
/// (no membership changes in v1 — §4.1).
pub type NodeId = u32;

/// Raft term number.  Monotonically increasing; 0 means "no term yet".
pub type Term = u64;

/// 1-based log index.  0 means "no entries" (the position before the first
/// real entry — mirrors the Raft paper's convention for `prevLogIndex`).
pub type LogIndex = u64;

/// Election timeout range (§4.2): 150–300ms with seeded jitter.
pub const ELECTION_TIMEOUT_BASE: Duration = Duration::from_millis(150);
pub const ELECTION_TIMEOUT_JITTER_MAX: Duration = Duration::from_millis(150);

/// Heartbeat interval (§4.2): 50ms.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

// ── Log entry ────────────────────────────────────────────────────────────────

/// One entry in the replicated log.
///
/// `payload` is opaque to the Raft layer: in the real system it's the WAL
/// batch bytes from `storage::wal::serialize_trades` (§4.2).  An empty
/// payload denotes a no-op entry, appended by a new leader at the start of
/// its term so that prior-term entries become committable (see
/// `RaftNode::become_leader`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: Vec<u8>,
}

// ── Persisted (hard) state ──────────────────────────────────────────────────

/// The subset of state that MUST survive a crash.
///
/// # Persistence rule (§4.2)
///
/// Whenever `step` returns `Effect::Persist(hs)` followed by `Effect::Send(...)`,
/// the runtime **must** durably write `hs` (fsync) before transmitting the
/// send.  Violating this ordering allows a node to vote twice in the same
/// term after a crash-restart, breaking election safety.
///
/// For simplicity, `Persist` carries the *entire* hard state (full log
/// included) on every change.  A real runtime should diff against the
/// last-persisted log length and append-only the new entries to its WAL;
/// the simulation harness (milestone 8) can simply overwrite its in-memory
/// "disk" with the snapshot, since both represent the same durable state.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    /// Entries with index > `snapshot_index`. Entries at or before
    /// `snapshot_index` have been compacted away (§4.4, milestone 9) — their
    /// effect is captured by whatever state the snapshot represents instead.
    pub log: Vec<Entry>,
    /// Highest log index reflected in the last-installed/taken snapshot.
    /// `0` means no snapshot has ever been taken — the log then holds every
    /// entry from index 1, exactly as in milestones 1–8.
    pub snapshot_index: LogIndex,
    /// Term of the entry at `snapshot_index`, needed so a restarted node can
    /// answer "is index `snapshot_index` at term `snapshot_term`?" without
    /// that entry being present in `log` anymore.
    pub snapshot_term: Term,
}

// ── Messages (network wire types) ───────────────────────────────────────────

/// Messages exchanged between Raft peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message {
    /// Sent by a candidate to request a vote.
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    /// Response to `RequestVote`.
    RequestVoteResponse {
        term: Term,
        vote_granted: bool,
        voter_id: NodeId,
    },
    /// Sent by the leader: log replication + heartbeat (entries may be empty).
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
    },
    /// Response to `AppendEntries`.
    AppendEntriesResponse {
        term: Term,
        success: bool,
        follower_id: NodeId,
        /// On success: index of the last entry now present in the follower's log.
        /// On failure: ignored by the leader (it decrements `next_index` instead).
        match_index: LogIndex,
    },
    /// Sent by the leader when a follower's `next_index` falls at or before
    /// the leader's own `snapshot_index` — i.e. the follower needs entries
    /// the leader has already compacted away. Carries the *entire* snapshot
    /// as one logical unit; splitting it into wire-level chunks (§4.4's
    /// "1MiB chunks") is the transport layer's job (milestone 9,
    /// `crates/server`), not this state machine's — by the time `data`
    /// reaches `step`, the transport has already reassembled it.
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        /// Opaque to Raft: in the real system this is a serialized bundle
        /// of the storage engine's manifest + segment files (§4.4). Raft
        /// only needs to know *where* the snapshot ends (`last_included_*`)
        /// to reason about log truncation; it never inspects `data` itself.
        data: Vec<u8>,
    },
    /// Response to `InstallSnapshot`.
    InstallSnapshotResponse { term: Term, follower_id: NodeId },
}

impl Message {
    /// Returns the term carried by this message — used by `step` to detect
    /// when a peer is ahead of us and we must update our term / step down.
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. } => *term,
            Message::RequestVoteResponse { term, .. } => *term,
            Message::AppendEntries { term, .. } => *term,
            Message::AppendEntriesResponse { term, .. } => *term,
            Message::InstallSnapshot { term, .. } => *term,
            Message::InstallSnapshotResponse { term, .. } => *term,
        }
    }

    /// Returns the id of the node that sent this message.
    ///
    /// Every variant already carries its sender's id in a field (
    /// `candidate_id`, `voter_id`, `leader_id`, `follower_id`) for protocol
    /// reasons — this just extracts it uniformly. Used by the TCP transport
    /// (milestone 9, `crates/server`) to attribute inbound messages to a
    /// peer without needing a separate connection-handshake protocol.
    pub fn sender_id(&self) -> NodeId {
        match self {
            Message::RequestVote { candidate_id, .. } => *candidate_id,
            Message::RequestVoteResponse { voter_id, .. } => *voter_id,
            Message::AppendEntries { leader_id, .. } => *leader_id,
            Message::AppendEntriesResponse { follower_id, .. } => *follower_id,
            Message::InstallSnapshot { leader_id, .. } => *leader_id,
            Message::InstallSnapshotResponse { follower_id, .. } => *follower_id,
        }
    }
}

// ── Step input ───────────────────────────────────────────────────────────────

/// Input to `RaftNode::step`.
///
/// The spec's contract is written as `step(&mut self, msg: Message, now: Instant)`;
/// `Event` generalizes `msg` to also cover the two other inputs a deterministic
/// state machine needs: time advancing (`Tick`, drives election/heartbeat
/// timeouts) and a local client proposal (`Propose`, only accepted by the
/// leader). All three are handled by the single `step` entry point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A `Message` arrived from peer `from`.
    Message { from: NodeId, msg: Message },
    /// Time has advanced to `now` (passed separately to `step`). No payload —
    /// `step` compares `now` against internal deadlines.
    Tick,
    /// A client wants to append `payload` to the replicated log.
    /// Only the leader can accept this; followers respond with
    /// `Effect::ProposeRejected`.
    Propose(Vec<u8>),
}

// ── Effects ──────────────────────────────────────────────────────────────────

/// Side effects requested by `step`.  The runtime (real or simulated) is
/// responsible for executing these; `step` itself performs no I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Send `msg` to peer `to`.  If preceded by `Persist`, the runtime must
    /// apply the persist (fsync) first — see `HardState` doc comment.
    Send(NodeId, Message),
    /// Durably write the given hard state before any subsequent `Send`
    /// that depends on it (see the persistence rule above).
    Persist(HardState),
    /// These entries have been committed (replicated to a majority) and are
    /// ready to be applied to the state machine (the storage engine's
    /// ingest path, in the real system).  Entries are in index order.
    ApplyCommitted(Vec<Entry>),
    /// A snapshot covering everything up to and including
    /// `last_included_index` has just been installed (this node either
    /// took it locally via `RaftNode::compact_log`, or received it from a
    /// leader via `Message::InstallSnapshot`). The runtime must load `data`
    /// into the state machine as a full replacement for everything up to
    /// that index — it will never see `ApplyCommitted` for those indices
    /// individually, since the log entries covering them no longer exist.
    ApplySnapshot {
        last_included_index: LogIndex,
        data: Vec<u8>,
    },
    /// The election timer should be reset: pick a new randomized deadline
    /// `now + jitter(150..300ms)` and call `step` with `Event::Tick` no
    /// later than that deadline.  `RaftNode` computes the jitter itself
    /// (via its internal seeded RNG) and tracks the deadline — this effect
    /// is informational for runtimes that want to schedule a timer.
    ResetElectionTimer,
    /// A `Propose` was rejected because this node is not the leader.
    /// `leader_hint` is the last known leader, if any — the server layer
    /// (milestone 9) uses this to redirect the client.
    ProposeRejected { leader_hint: Option<NodeId> },
}
