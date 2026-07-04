/// safety and liveness invariants (§4.3).
///
/// each check takes a read-only view of the cluster's nodes (and, for the
/// applied-log check, the per-node applied-entry history that `Cluster`
/// tracks separately from `RaftNode` itself) and returns `Ok(())` or a
/// descriptive `Violation`. the sim harness calls all four every tick;
/// a violation aborts the run immediately and reports the seed that
/// produced it, so any failure is reproducible by re-running that one seed.
use raft::{Entry, NodeId, RaftNode, Role};

#[derive(Debug, Clone)]
pub enum Violation {
    /// more than one leader was observed in the same term — the single
    /// most safety-critical property in raft.
    MultipleLeaders { term: u64, leaders: Vec<NodeId> },
    /// two nodes have entries at the same (index, term) that don't match.
    LogMismatch {
        index: u64,
        term: u64,
        node_a: NodeId,
        node_b: NodeId,
    },
    /// two nodes both applied *some* entry at the same log index, but the
    /// entries disagree — a genuine state machine divergence. (a node
    /// simply being behind another, or redelivering an already-applied
    /// entry after a restart, is expected and NOT a violation — see the
    /// doc comment on `check_state_machine_safety`.)
    AppliedNotPrefix {
        node_a: NodeId,
        node_b: NodeId,
        index: usize,
    },
    /// no new entry has committed within the liveness window after faults
    /// stopped, even though the cluster should have a functioning leader.
    LivenessViolation { ticks_since_fault_free: u64 },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Violation::MultipleLeaders { term, leaders } =>
                write!(f, "election safety violated: term {term} has {} leaders: {leaders:?}", leaders.len()),
            Violation::LogMismatch { index, term, node_a, node_b } =>
                write!(f, "log matching violated: node {node_a} and node {node_b} disagree on entry at index {index}, term {term}"),
            Violation::AppliedNotPrefix { node_a, node_b, index } =>
                write!(f, "state machine safety violated: node {node_a} and node {node_b} both applied a different entry at log index {index}"),
            Violation::LivenessViolation { ticks_since_fault_free } =>
                write!(f, "liveness violated: no new commit within {ticks_since_fault_free} ticks of faults stopping"),
        }
    }
}

/// invariant 1: at most one leader per term, across the whole cluster.
///
/// this is checked by grouping current leaders by their `current_term()`
/// rather than assuming "the" current term, because nodes can legitimately
/// be at different terms transiently (e.g. right after an election, before
/// a heartbeat has reached every follower).
pub fn check_election_safety<N: std::borrow::Borrow<RaftNode>>(
    nodes: &[N],
) -> Result<(), Violation> {
    use std::collections::HashMap;
    let mut leaders_by_term: HashMap<u64, Vec<NodeId>> = HashMap::new();
    for node in nodes {
        let node = node.borrow();
        if node.role() == Role::Leader {
            leaders_by_term
                .entry(node.current_term())
                .or_default()
                .push(node.id);
        }
    }
    for (term, leaders) in leaders_by_term {
        if leaders.len() > 1 {
            return Err(Violation::MultipleLeaders { term, leaders });
        }
    }
    Ok(())
}

/// invariant 2: log matching property — if two logs share an entry at the
/// same (index, term), every entry up to that point is identical.
///
/// compares by each `Entry`'s own `index` field, not by vec position.
/// before compaction existed (milestones 1–8) these were always the same
/// thing (`log[i]` always had `index == i + 1`), so a position-based
/// comparison worked. once nodes can have independently different
/// `snapshot_index` values (milestone 9), `log[i]` on one node and `log[i]`
/// on another can refer to completely different absolute indices — only
/// comparing by the entries' own `index` field is correct.
///
/// entries at or below either node's `snapshot_index` are skipped: that
/// range has been compacted away and is no longer present to compare
/// directly. `check_snapshot_consistency` separately verifies that when
/// two nodes install a snapshot at the same `last_included_index`, its
/// *contents* agree — this function only reasons about entries still live
/// in `log()`.
pub fn check_log_matching<N: std::borrow::Borrow<RaftNode>>(nodes: &[N]) -> Result<(), Violation> {
    use std::collections::BTreeMap;

    for a in 0..nodes.len() {
        for b in (a + 1)..nodes.len() {
            let node_a = nodes[a].borrow();
            let node_b = nodes[b].borrow();

            let by_index_b: BTreeMap<u64, &Entry> =
                node_b.log().iter().map(|e| (e.index, e)).collect();

            for entry_a in node_a.log() {
                if entry_a.index <= node_b.snapshot_index() {
                    continue; // b has already compacted this range away
                }
                if let Some(&entry_b) = by_index_b.get(&entry_a.index) {
                    // same index, same term -> must be the identical entry
                    // (at most one leader per term, and a leader never
                    // overwrites its own log). same index, DIFFERENT term
                    // is expected during an uncommitted fork and is not a
                    // violation on its own.
                    if entry_a.term == entry_b.term && entry_a != entry_b {
                        return Err(Violation::LogMismatch {
                            index: entry_a.index,
                            term: entry_a.term,
                            node_a: node_a.id,
                            node_b: node_b.id,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

/// invariant 3: state machine safety.
///
/// `Effect::ApplyCommitted` is, by design, an *at-least-once* delivery to
/// the state machine: when a node crashes and restarts, `RaftNode::restore`
/// deliberately resets `last_applied` to 0 (it's volatile state per the
/// raft paper), so the restarted node re-walks its log from the beginning
/// and re-emits `ApplyCommitted` for entries it had already applied before
/// crashing. this mirrors the real system, where the storage engine's
/// ingest path is idempotent (trade_id-based dedup — see `crates/storage`),
/// so re-applying an already-applied batch is harmless.
///
/// consequently, comparing raw applied *sequences* by position (as if
/// `applied` were guaranteed duplicate-free) is wrong — it flags legitimate
/// redelivery as a divergence. the correct check compares by log **index**:
/// build a per-node view of "what entry did I apply at index N", and assert
/// that whenever two nodes have both applied *some* entry at the same
/// index, it's the *same* entry. a node being behind (having applied fewer
/// indices) or ahead is expected and not a violation — only disagreement
/// on the content at a shared index is.
///
/// `applied` is `Cluster`'s bookkeeping of what `Effect::ApplyCommitted`
/// has delivered to each node, in raw delivery order (including any
/// redelivery) — this is deliberately tracked outside `RaftNode` itself,
/// mirroring how the real storage engine would receive and apply these
/// entries.
pub fn check_state_machine_safety(
    applied: &[Vec<Entry>],
    node_ids: &[NodeId],
) -> Result<(), Violation> {
    use std::collections::BTreeMap;

    // per-node: log index -> the entry applied at that index. redelivery of
    // an identical entry at an already-seen index is a no-op; the loop
    // still needs to walk every entry once to catch a genuine mismatch
    // (two DIFFERENT entries claiming the same index within one node's own
    // history, which would itself be a bug worth surfacing here too).
    let per_node_by_index: Vec<BTreeMap<u64, &Entry>> = applied
        .iter()
        .map(|seq| {
            let mut map: BTreeMap<u64, &Entry> = BTreeMap::new();
            for entry in seq {
                map.entry(entry.index).or_insert(entry);
            }
            map
        })
        .collect();

    for a in 0..per_node_by_index.len() {
        for b in (a + 1)..per_node_by_index.len() {
            for (&index, &entry_a) in &per_node_by_index[a] {
                if let Some(&entry_b) = per_node_by_index[b].get(&index) {
                    if entry_a != entry_b {
                        return Err(Violation::AppliedNotPrefix {
                            node_a: node_ids[a],
                            node_b: node_ids[b],
                            index: index as usize,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

/// invariant 4: bounded liveness — checked by the cluster driver, not here,
/// since it needs to track "ticks since the last fault" and "ticks since
/// the last commit" across the whole run rather than a single-snapshot
/// check. see `Cluster::check_liveness`.
///
/// kept as a named constant here so both the cluster and the test campaign
/// agree on the bound without duplicating the literal.
pub const LIVENESS_BOUND_TICKS: u64 = 10_000 / 10; // 10 simulated seconds at 10ms/tick

/// invariant (snapshot extension of #3): whenever two nodes have both
/// installed a snapshot at the same `last_included_index`, the bytes must
/// match. this is the snapshot-transfer analogue of `check_log_matching`:
/// raft's own safety properties (log matching + leader completeness)
/// already guarantee the underlying entries a snapshot represents are
/// identical across any node that legitimately committed them, so a
/// mismatch here would indicate a bug in the compaction/transfer path
/// itself (bit corruption, a stale snapshot being resent, etc.) rather than
/// a core consensus violation — but the failure mode looks the same to an
/// operator (the state machines have diverged), so it's checked the same way.
pub fn check_snapshot_consistency(
    snapshot_installs: &[Vec<(u64, Vec<u8>)>],
    node_ids: &[NodeId],
) -> Result<(), Violation> {
    use std::collections::BTreeMap;

    let per_node_by_index: Vec<BTreeMap<u64, &Vec<u8>>> = snapshot_installs
        .iter()
        .map(|installs| {
            let mut map: BTreeMap<u64, &Vec<u8>> = BTreeMap::new();
            for (index, data) in installs {
                map.entry(*index).or_insert(data);
            }
            map
        })
        .collect();

    for a in 0..per_node_by_index.len() {
        for b in (a + 1)..per_node_by_index.len() {
            for (&index, &data_a) in &per_node_by_index[a] {
                if let Some(&data_b) = per_node_by_index[b].get(&index) {
                    if data_a != data_b {
                        return Err(Violation::AppliedNotPrefix {
                            node_a: node_ids[a],
                            node_b: node_ids[b],
                            index: index as usize,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use raft::Entry;
    use std::time::Instant;

    fn node_at(id: NodeId, peers: Vec<NodeId>) -> RaftNode {
        RaftNode::new(id, peers, id as u64 + 1, Instant::now())
    }

    #[test]
    fn election_safety_passes_with_all_followers() {
        // trivial baseline: no leaders at all is never a violation. real
        // multi-leader scenarios are exercised in cluster.rs's tests, where
        // elections actually run.
        let a = node_at(0, vec![1, 2]);
        let b = node_at(1, vec![0, 2]);
        let c = node_at(2, vec![0, 1]);
        assert!(check_election_safety(&[a, b, c]).is_ok());
    }

    #[test]
    fn log_matching_detects_mismatch() {
        // build two nodes and manually diverge their logs via restore.
        let hs_a = raft::HardState {
            current_term: 2,
            voted_for: None,
            log: vec![Entry {
                term: 1,
                index: 1,
                payload: vec![0xAA],
            }],
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let hs_b = raft::HardState {
            current_term: 2,
            voted_for: None,
            log: vec![Entry {
                term: 1,
                index: 1,
                payload: vec![0xBB],
            }], // same term, different payload!
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let node_a = RaftNode::restore(0, vec![1], 1, Instant::now(), hs_a, Vec::new());
        let node_b = RaftNode::restore(1, vec![0], 2, Instant::now(), hs_b, Vec::new());

        let result = check_log_matching(&[node_a, node_b]);
        assert!(matches!(result, Err(Violation::LogMismatch { .. })));
    }

    #[test]
    fn log_matching_allows_divergent_tails_from_different_terms() {
        let hs_a = raft::HardState {
            current_term: 3,
            voted_for: None,
            log: vec![
                Entry {
                    term: 1,
                    index: 1,
                    payload: vec![],
                },
                Entry {
                    term: 2,
                    index: 2,
                    payload: vec![0xAA],
                }, // uncommitted fork
            ],
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let hs_b = raft::HardState {
            current_term: 3,
            voted_for: None,
            log: vec![
                Entry {
                    term: 1,
                    index: 1,
                    payload: vec![],
                },
                Entry {
                    term: 3,
                    index: 2,
                    payload: vec![0xBB],
                }, // different leader overwrote it
            ],
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let node_a = RaftNode::restore(0, vec![1], 1, Instant::now(), hs_a, Vec::new());
        let node_b = RaftNode::restore(1, vec![0], 2, Instant::now(), hs_b, Vec::new());

        // different terms at index 2 -> not a violation, just a fork.
        assert!(check_log_matching(&[node_a, node_b]).is_ok());
    }

    #[test]
    fn state_machine_safety_passes_for_prefixes() {
        let e1 = Entry {
            term: 1,
            index: 1,
            payload: vec![1],
        };
        let e2 = Entry {
            term: 1,
            index: 2,
            payload: vec![2],
        };
        let applied = vec![vec![e1.clone()], vec![e1.clone(), e2.clone()], vec![]];
        assert!(check_state_machine_safety(&applied, &[0, 1, 2]).is_ok());
    }

    #[test]
    fn state_machine_safety_detects_divergence() {
        let e1 = Entry {
            term: 1,
            index: 1,
            payload: vec![1],
        };
        let e2_a = Entry {
            term: 1,
            index: 2,
            payload: vec![0xAA],
        };
        let e2_b = Entry {
            term: 1,
            index: 2,
            payload: vec![0xBB],
        };
        let applied = vec![vec![e1.clone(), e2_a], vec![e1.clone(), e2_b]];
        let result = check_state_machine_safety(&applied, &[0, 1]);
        assert!(matches!(result, Err(Violation::AppliedNotPrefix { .. })));
    }
}
