//! Raft consensus core (§4.1, §4.2).
//!
//! This crate contains only the I/O-free state machine (`RaftNode::step`).
//! Cluster wiring (TOML config, TCP transport) and the real persistence
//! backend live in `crates/server` (milestone 9). The deterministic
//! simulation harness that drives `RaftNode` under fault injection lives in
//! `crates/sim` (milestone 8).

pub mod node;
pub mod types;

pub use node::{RaftNode, Role};
pub use types::{
    Effect, Entry, Event, HardState, LogIndex, Message, NodeId, Term, ELECTION_TIMEOUT_BASE,
    ELECTION_TIMEOUT_JITTER_MAX, HEARTBEAT_INTERVAL,
};
