/// Multi-node Raft integration tests.
///
/// Drives a small 3-node cluster through a hand-rolled message-routing
/// harness (no sockets, no async — `crates/sim`'s deterministic harness in
/// milestone 8 generalizes this with fault injection). Each test advances a
/// shared virtual clock and delivers `Effect::Send` messages between nodes
/// until a stopping condition (leader elected, entry committed, etc.) or a
/// bounded iteration count is reached.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use raft::{Effect, Entry, Event, HardState, Message, NodeId, RaftNode, Role};

/// A 3-node cluster with a virtual clock and an in-memory message queue.
struct Cluster {
    nodes: Vec<RaftNode>,
    /// (to, from, msg) — FIFO per cluster, delivered in order.
    queue: VecDeque<(NodeId, NodeId, Message)>,
    /// Entries each node has applied, in order — for cross-node consistency checks.
    applied: Vec<Vec<Entry>>,
    /// Last persisted hard state per node — for crash/restart tests.
    persisted: Vec<HardState>,
    now: Instant,
}

impl Cluster {
    fn new(n: usize) -> Self {
        let now = Instant::now();
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let nodes = ids
            .iter()
            .map(|&id| {
                let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
                RaftNode::new(id, peers, id as u64 + 1, now)
            })
            .collect();

        Cluster {
            nodes,
            queue: VecDeque::new(),
            applied: vec![Vec::new(); n],
            persisted: vec![HardState::default(); n],
            now,
        }
    }

    /// Applies `effects` produced by `node_id`, routing `Send`s into the
    /// queue and recording `Persist`/`ApplyCommitted`.
    fn handle_effects(&mut self, node_id: NodeId, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send(to, msg) => self.queue.push_back((to, node_id, msg)),
                Effect::Persist(hs) => self.persisted[node_id as usize] = hs,
                Effect::ApplyCommitted(entries) => {
                    self.applied[node_id as usize].extend(entries);
                }
                Effect::ApplySnapshot { .. } => {
                    // not exercised by these tests — snapshot installation is
                    // covered separately (see raft's own snapshot unit tests
                    // and sim's compaction integration tests).
                }
                Effect::ResetElectionTimer | Effect::ProposeRejected { .. } => {}
            }
        }
    }

    /// Sends `Tick` to every node at the current virtual time.
    fn tick_all(&mut self) {
        for id in 0..self.nodes.len() as NodeId {
            let effects = self.nodes[id as usize].step(Event::Tick, self.now);
            self.handle_effects(id, effects);
        }
    }

    /// Delivers every message currently in the queue (one round — delivering
    /// a message may enqueue new messages, which are NOT delivered in this
    /// call; call repeatedly via `run` to drain transitively).
    fn deliver_round(&mut self) -> usize {
        let pending: Vec<(NodeId, NodeId, Message)> = self.queue.drain(..).collect();
        let n = pending.len();
        for (to, from, msg) in pending {
            let effects = self.nodes[to as usize].step(Event::Message { from, msg }, self.now);
            self.handle_effects(to, effects);
        }
        n
    }

    /// Advances the virtual clock by `dur`, then ticks all nodes and drains
    /// the message queue transitively (bounded to avoid infinite loops on a
    /// bug).
    fn advance(&mut self, dur: Duration) {
        self.now += dur;
        self.tick_all();
        self.drain();
    }

    /// Drains the message queue transitively (delivering a message can
    /// produce more messages — e.g., RequestVote -> RequestVoteResponse ->
    /// become_leader -> AppendEntries).
    fn drain(&mut self) {
        for _ in 0..100 {
            if self.deliver_round() == 0 {
                return;
            }
        }
        panic!("message queue did not drain within 100 rounds — possible infinite loop");
    }

    /// Returns the id of the current leader, if any (there should be ≤ 1 —
    /// callers can assert this directly via `leaders()`).
    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|n| n.role() == Role::Leader)
            .map(|n| n.id)
            .collect()
    }

    /// Repeatedly advances time by the given step until a leader exists or
    /// `max_iters` is reached.
    ///
    /// # Why small steps
    ///
    /// If every node's election deadline falls within the SAME large time
    /// step, all nodes time out simultaneously, vote for themselves, and
    /// reject each other's votes — a perpetual split vote (the deadlines
    /// then re-randomize to another overlapping window and repeat).
    ///
    /// Stepping in small increments (`small_step`) means whichever node's
    /// deadline elapses *first* gets to complete a full
    /// RequestVote → RequestVoteResponse → become_leader round (all within
    /// the same `advance` call, via `drain`'s transitive delivery) *before*
    /// any other node's deadline is reached — exactly as in real time.
    fn run_until_leader(&mut self, small_step: Duration, max_iters: usize) -> Option<NodeId> {
        for _ in 0..max_iters {
            self.advance(small_step);
            let leaders = self.leaders();
            if !leaders.is_empty() {
                assert_eq!(
                    leaders.len(),
                    1,
                    "election safety: at most one leader per term"
                );
                return Some(leaders[0]);
            }
        }
        None
    }

    /// A small time increment (10ms) used to drive elections one step at a
    /// time — see `run_until_leader`'s doc comment.
    fn election_step() -> Duration {
        Duration::from_millis(10)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn three_node_cluster_elects_a_leader() {
    let mut cluster = Cluster::new(3);
    let leader = cluster.run_until_leader(Cluster::election_step(), 50);
    assert!(leader.is_some(), "expected a leader to be elected");

    // Election safety (§4.3 invariant 1): exactly one leader.
    assert_eq!(cluster.leaders().len(), 1);

    // The elected leader's no-op entry should be committed (3-node majority = 2;
    // the leader's own log counts as one ack, so it needs only one follower).
    let leader_id = leader.unwrap();
    // It may take one more round for a follower's ack to arrive.
    cluster.drain();
    assert!(cluster.nodes[leader_id as usize].commit_index() >= 1);
}

#[test]
fn all_nodes_converge_on_same_term_after_election() {
    let mut cluster = Cluster::new(3);
    cluster.run_until_leader(Cluster::election_step(), 50);

    let terms: Vec<u64> = cluster.nodes.iter().map(|n| n.current_term()).collect();
    let first = terms[0];
    assert!(
        terms.iter().all(|&t| t == first),
        "all nodes should converge on the winning term: {terms:?}"
    );
}

#[test]
fn proposed_entry_replicates_and_commits_on_majority() {
    let mut cluster = Cluster::new(3);
    let leader_id = cluster
        .run_until_leader(Cluster::election_step(), 50)
        .unwrap();
    cluster.drain();

    // Propose a client entry on the leader.
    let effects = cluster.nodes[leader_id as usize]
        .step(Event::Propose(b"trade-batch-1".to_vec()), cluster.now);
    cluster.handle_effects(leader_id, effects);
    cluster.drain();

    // Leader should have committed the new entry (index 2: no-op + this one).
    let leader = &cluster.nodes[leader_id as usize];
    assert_eq!(leader.log().len(), 2);
    assert_eq!(leader.commit_index(), 2);

    // The committed entry's payload must match what was proposed.
    let committed = &cluster.applied[leader_id as usize];
    assert!(committed.iter().any(|e| e.payload == b"trade-batch-1"));
}

#[test]
fn followers_eventually_match_leader_log() {
    let mut cluster = Cluster::new(3);
    let leader_id = cluster
        .run_until_leader(Cluster::election_step(), 50)
        .unwrap();
    cluster.drain();

    let effects =
        cluster.nodes[leader_id as usize].step(Event::Propose(b"entry-A".to_vec()), cluster.now);
    cluster.handle_effects(leader_id, effects);
    cluster.drain();

    // One more heartbeat round so commit_index propagates to followers.
    cluster.advance(raft::HEARTBEAT_INTERVAL + Duration::from_millis(1));

    let leader_log = cluster.nodes[leader_id as usize].log().to_vec();
    for (i, node) in cluster.nodes.iter().enumerate() {
        if i as NodeId == leader_id {
            continue;
        }
        assert_eq!(
            node.log(),
            leader_log.as_slice(),
            "follower {i} log diverges from leader"
        );
    }
}

#[test]
fn log_matching_property_holds_after_replication() {
    // §4.3 invariant 2: if two logs share (index, term), they're identical
    // up to that index. After normal replication, this should trivially hold —
    // verify it explicitly by comparing full logs.
    let mut cluster = Cluster::new(3);
    let leader_id = cluster
        .run_until_leader(Cluster::election_step(), 50)
        .unwrap();
    cluster.drain();

    for i in 0..5 {
        let effects = cluster.nodes[leader_id as usize].step(
            Event::Propose(format!("entry-{i}").into_bytes()),
            cluster.now,
        );
        cluster.handle_effects(leader_id, effects);
        cluster.drain();
    }
    cluster.advance(raft::HEARTBEAT_INTERVAL + Duration::from_millis(1));

    // Pairwise: for every shared (index, term), entries must be identical.
    for a in 0..cluster.nodes.len() {
        for b in (a + 1)..cluster.nodes.len() {
            let log_a = cluster.nodes[a].log();
            let log_b = cluster.nodes[b].log();
            let min_len = log_a.len().min(log_b.len());
            for i in 0..min_len {
                if log_a[i].term == log_b[i].term {
                    assert_eq!(
                        log_a[i],
                        log_b[i],
                        "logs diverge at index {} despite matching term",
                        i + 1
                    );
                }
            }
        }
    }
}

#[test]
fn state_machine_safety_applied_sequences_are_prefixes() {
    // §4.3 invariant 3: applied sequences across nodes are prefixes of each other.
    let mut cluster = Cluster::new(3);
    let leader_id = cluster
        .run_until_leader(Cluster::election_step(), 50)
        .unwrap();
    cluster.drain();

    for i in 0..3 {
        let effects = cluster.nodes[leader_id as usize].step(
            Event::Propose(format!("entry-{i}").into_bytes()),
            cluster.now,
        );
        cluster.handle_effects(leader_id, effects);
        cluster.drain();
    }
    // Give followers a chance to catch up on commit_index via heartbeats.
    for _ in 0..3 {
        cluster.advance(raft::HEARTBEAT_INTERVAL + Duration::from_millis(1));
    }

    let max_len = cluster.applied.iter().map(|a| a.len()).max().unwrap();
    let longest = cluster
        .applied
        .iter()
        .find(|a| a.len() == max_len)
        .unwrap()
        .clone();

    for (i, applied) in cluster.applied.iter().enumerate() {
        for (j, entry) in applied.iter().enumerate() {
            assert_eq!(
                entry, &longest[j],
                "node {i}'s applied entry at position {j} is not a prefix of the longest applied sequence"
            );
        }
    }
}

#[test]
fn leader_does_not_commit_prior_term_entry_via_replication_count_alone() {
    // The classic Raft Figure 8 scenario, simplified: a leader cannot mark an
    // entry from a PREVIOUS term as committed purely because it's replicated
    // to a majority — it must wait until an entry from ITS OWN term is
    // replicated to a majority (which then commits everything before it too).
    //
    // Construction (via `restore`, which loads a HardState without driving
    // an election — avoiding the need to script multiple elections by hand):
    //   - 3-node cluster, this node = 0, peers = [1, 2].
    //   - Restore with current_term = 2, log = [ (term=1, idx=1) ] — i.e.,
    //     this node already has one entry from a PREVIOUS term (term 1) and
    //     is at term 2.
    //   - Drive an election at term 2 -> term 3 so this node becomes leader
    //     and appends its own no-op at (term=3, idx=2).
    //
    // Now: log = [ (term=1, idx=1), (term=3, idx=2) ], current_term = 3.
    // A peer acking match_index=1 must NOT commit index 1, even though
    // 1 is replicated to a majority — because log[0].term (1) != current_term (3).
    let now = Instant::now();

    let hs = HardState {
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
    let mut leader = RaftNode::restore(0, vec![1, 2], 1, now, hs, Vec::new());

    // Drive an election: term 2 -> 3, becomes leader, appends no-op @ (term=3, idx=2).
    let deadline = now
        + raft::ELECTION_TIMEOUT_BASE
        + raft::ELECTION_TIMEOUT_JITTER_MAX
        + Duration::from_millis(1);
    leader.step(Event::Tick, deadline);
    assert_eq!(leader.role(), Role::Candidate);
    assert_eq!(leader.current_term(), 3);

    leader.step(
        Event::Message {
            from: 1,
            msg: Message::RequestVoteResponse {
                term: 3,
                vote_granted: true,
                voter_id: 1,
            },
        },
        deadline,
    );
    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(
        leader.log().len(),
        2,
        "log = [term1@1 (inherited), term3@2 (no-op)]"
    );
    assert_eq!(leader.log()[0].term, 1);
    assert_eq!(leader.log()[1].term, 3);

    // become_leader's internal advance_commit_index: majority=2, indices =
    // [match[1]=0, match[2]=0, leader_log_len=2] -> sorted [2,0,0] ->
    // candidate = indices[1] = 0 -> no commit yet.
    assert_eq!(leader.commit_index(), 0);

    // Peer 1 acks match_index=1 (the OLD term-1 entry only — simulates a
    // follower that's behind and hasn't received the new no-op yet).
    let effects = leader.step(
        Event::Message {
            from: 1,
            msg: Message::AppendEntriesResponse {
                term: 3,
                success: true,
                follower_id: 1,
                match_index: 1,
            },
        },
        deadline,
    );

    // CRITICAL ASSERTION: indices = [match[1]=1, match[2]=0, leader_log_len=2]
    // -> sorted [2,1,0] -> candidate = indices[1] = 1. commit_index must NOT
    // advance to 1, because log[0].term (1) != current_term (3).
    assert_eq!(
        leader.commit_index(),
        0,
        "must not commit a prior-term entry via replication count alone"
    );
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::ApplyCommitted(_))));

    // Peer 1 now acks match_index=2 (the term-3 no-op). NOW commit_index may
    // advance to 2 — and index 1 commits too, as a side effect of committing
    // index 2 (which IS in the current term).
    let effects = leader.step(
        Event::Message {
            from: 1,
            msg: Message::AppendEntriesResponse {
                term: 3,
                success: true,
                follower_id: 1,
                match_index: 2,
            },
        },
        deadline,
    );

    assert_eq!(
        leader.commit_index(),
        2,
        "committing the current-term entry also commits everything before it"
    );
    let applied = effects
        .iter()
        .find_map(|e| match e {
            Effect::ApplyCommitted(entries) => Some(entries.clone()),
            _ => None,
        })
        .expect("expected ApplyCommitted");
    assert_eq!(
        applied.len(),
        2,
        "both entries (term1@1 and term3@2) become applied together"
    );
    assert_eq!(applied[0].term, 1);
    assert_eq!(applied[1].term, 3);
}
