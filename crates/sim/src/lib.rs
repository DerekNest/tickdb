//! deterministic simulation harness (§4.3).
//!
//! drives real `raft::RaftNode` instances through a virtual network and
//! virtual clock under seeded fault injection (message drop/delay/dup,
//! partitions, node crash+restart), checking the four safety/liveness
//! invariants every tick. a single `u64` seed determines an entire run —
//! any violation is reported with its seed for exact replay.

pub mod cluster;
pub mod invariants;
pub mod network;

pub use cluster::{ChaosSchedule, Cluster, TICK_DURATION};
pub use invariants::Violation;
pub use network::FaultConfig;
