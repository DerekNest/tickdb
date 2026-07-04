/// virtual cluster (§4.3).
///
/// owns a fixed set of `RaftNode`s, a `Network`, and a virtual clock
/// expressed in ticks. every tick:
///   1. delivers messages whose delay has elapsed
///   2. sends `Event::Tick` to every live node (crashed nodes are skipped)
///   3. occasionally injects a fault: crash a node, restart a crashed node,
///      open a partition, or heal one — all gated by the single seeded rng
///   4. checks the three snapshot invariants (election safety, log
///      matching, state machine safety)
///
/// a single `u64` seed determines the entire run: node election jitter
/// (each `RaftNode` gets a sub-seed derived from the cluster seed),
/// network fault timing, and fault-injection scheduling. re-running the
/// same seed reproduces the exact same sequence of events, which is the
/// whole point — any invariant violation is handed back with its seed so
/// it can be replayed and debugged deterministically.
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use raft::{Effect, Entry, Event, HardState, LogIndex, NodeId, RaftNode};

use crate::invariants::{self, Violation, LIVENESS_BOUND_TICKS};
use crate::network::{FaultConfig, Network, Tick};

/// one virtual tick = 10ms of raft time. chosen so that raft's real
/// millisecond-based constants (150-300ms election timeout, 50ms heartbeat)
/// land on clean tick-count boundaries (15-30 ticks, 5 ticks) without
/// needing sub-tick precision anywhere.
pub const TICK_DURATION: Duration = Duration::from_millis(10);

/// per-tick probabilities for the coarser faults (crash / restart /
/// partition / heal) — separate from `FaultConfig`, which governs
/// per-message drop/delay/dup. kept low so a run has room to make forward
/// progress between disruptions; the liveness check specifically verifies
/// this by requiring a commit within a bounded window once faults settle.
#[derive(Debug, Clone, Copy)]
pub struct ChaosSchedule {
    pub crash_prob_per_tick: f64,
    pub restart_prob_per_tick: f64,
    pub partition_prob_per_tick: f64,
    pub heal_prob_per_tick: f64,
}

impl ChaosSchedule {
    pub fn none() -> Self {
        ChaosSchedule {
            crash_prob_per_tick: 0.0,
            restart_prob_per_tick: 0.0,
            partition_prob_per_tick: 0.0,
            heal_prob_per_tick: 0.0,
        }
    }

    /// the §4.3 default: rare enough that the cluster spends most ticks
    /// undisturbed, frequent enough that a 10,000-tick run sees dozens of
    /// crash/partition events.
    pub fn default_chaos() -> Self {
        ChaosSchedule {
            crash_prob_per_tick: 0.0005,
            restart_prob_per_tick: 0.02, // conditioned on "currently crashed", so this can be higher
            partition_prob_per_tick: 0.0003,
            heal_prob_per_tick: 0.01,
        }
    }
}

/// per-node liveness state: whether it's up, and — if crashed — its last
/// persisted `HardState` plus snapshot bytes (kept separately, mirroring
/// how `RaftNode::restore` takes them as two arguments), which `restart`
/// reloads via `RaftNode::restore`.
enum NodeState {
    Up(Box<RaftNode>),
    Crashed(HardState, Vec<u8>),
}

pub struct Cluster {
    node_ids: Vec<NodeId>,
    states: Vec<NodeState>,
    network: Network,
    /// every entry each node has applied, in order — mirrors what the real
    /// storage engine's ingest path would receive from `Effect::ApplyCommitted`.
    applied: Vec<Vec<Entry>>,
    /// per-node history of snapshot installs: `(last_included_index, data)`
    /// pairs, in the order this node applied them. see `check_snapshot_consistency`.
    snapshot_installs: Vec<Vec<(LogIndex, Vec<u8>)>>,
    /// wall-clock `Instant` corresponding to tick 0. `RaftNode::step` wants
    /// real `Instant`s, so we convert ticks to instants via this anchor —
    /// this is the only place real time appears; everything else is ticks.
    epoch: Instant,
    now: Tick,
    chaos: ChaosSchedule,
    rng: SmallRng,
    /// last tick at which a fault (crash, partition open/close, drop, dup,
    /// or extra delay beyond 1 tick) was injected — used by the liveness check.
    last_fault_tick: Tick,
    /// highest commit_index observed on any node, checked each tick to
    /// detect commit progress without re-deriving it from scratch.
    max_commit_seen: u64,
    /// tick at which the oldest still-uncommitted accepted proposal was
    /// made, if any. `None` means every proposal the cluster has accepted
    /// has since committed — an idle cluster with nothing outstanding is
    /// never a liveness violation, only a cluster that accepted work and
    /// then failed to commit it within the bound is.
    pending_proposal_since: Option<Tick>,
}

impl Cluster {
    /// creates an `n`-node cluster. `seed` drives everything (see module
    /// doc comment); `faults` governs per-message network faults;
    /// `chaos` governs node crash/restart/partition scheduling.
    pub fn new(n: usize, seed: u64, faults: FaultConfig, chaos: ChaosSchedule) -> Self {
        let epoch = Instant::now();
        let mut rng = SmallRng::seed_from_u64(seed);

        let node_ids: Vec<NodeId> = (0..n as NodeId).collect();
        let states = node_ids
            .iter()
            .map(|&id| {
                let peers: Vec<NodeId> = node_ids.iter().copied().filter(|&p| p != id).collect();
                // derive a per-node sub-seed from the cluster rng so the whole
                // run is still fully determined by the single top-level seed.
                let node_seed: u64 = rng.gen();
                NodeState::Up(Box::new(RaftNode::new(id, peers, node_seed, epoch)))
            })
            .collect();

        Cluster {
            node_ids: node_ids.clone(),
            states,
            network: Network::new(faults),
            applied: vec![Vec::new(); n],
            snapshot_installs: vec![Vec::new(); n],
            epoch,
            now: 0,
            chaos,
            rng,
            last_fault_tick: 0,
            max_commit_seen: 0,
            pending_proposal_since: None,
        }
    }

    fn instant_at(&self, tick: Tick) -> Instant {
        self.epoch + TICK_DURATION * tick as u32
    }

    /// applies the effects produced by `node_id`'s `step` call: routes
    /// `Send`s through the network, records `Persist`ed hard state (used on
    /// crash+restart), appends `ApplyCommitted` entries to `applied`, and
    /// records `ApplySnapshot` installs to `snapshot_installs`.
    fn handle_effects(&mut self, node_id: NodeId, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send(to, msg) => {
                    self.network.send(node_id, to, msg, self.now, &mut self.rng);
                }
                Effect::Persist(_hs) => {
                    // the "hardware" here is `NodeState::Up`'s RaftNode
                    // itself, which already owns its persisted fields —
                    // `hard_state()` reads them directly at crash time, so
                    // there's nothing to do here except note this tick
                    // wasn't fault-free from a durability-timing standpoint.
                    // (kept as an explicit no-op arm, not folded into `_`,
                    // so future work — e.g. tracking fsync latency — has an
                    // obvious place to land.)
                }
                Effect::ApplyCommitted(entries) => {
                    if !entries.is_empty() {
                        self.applied[node_id as usize].extend(entries);
                    }
                }
                Effect::ApplySnapshot {
                    last_included_index,
                    data,
                } => {
                    // like `applied`, this is an append-only log — a node
                    // can legitimately install more than one snapshot over
                    // a run (compaction is periodic), and
                    // `check_snapshot_consistency` walks the whole history.
                    self.snapshot_installs[node_id as usize].push((last_included_index, data));
                }
                Effect::ResetElectionTimer | Effect::ProposeRejected { .. } => {}
            }
        }
    }

    /// advances the cluster by exactly one tick: deliver ready messages,
    /// tick every live node, then roll the chaos dice.
    pub fn step(&mut self) {
        self.now += 1;
        let deliver_at = self.now;

        let step_instant = self.instant_at(self.now);

        let ready = self.network.drain_ready(deliver_at);
        for (to, from, msg) in ready {
            if let NodeState::Up(node) = &mut self.states[to as usize] {
                let effects = node.step(Event::Message { from, msg }, step_instant);
                self.handle_effects(to, effects);
            }
            // messages to a crashed node are simply lost — this is
            // realistic (the real transport would get a connection
            // refused/reset) and requires no special handling here.
        }

        for id in self.node_ids.clone() {
            if let NodeState::Up(node) = &mut self.states[id as usize] {
                let effects = node.step(Event::Tick, step_instant);
                self.handle_effects(id, effects);
            }
        }

        self.inject_chaos();
        self.track_commit_progress();
    }

    /// sends a client proposal to the current leader, if one exists.
    /// returns `true` if a leader was found and accepted the proposal.
    pub fn propose(&mut self, payload: Vec<u8>) -> bool {
        let leader_id = self.node_ids.iter().copied().find(|&id| {
            matches!(&self.states[id as usize], NodeState::Up(n) if n.role() == raft::Role::Leader)
        });

        if let Some(id) = leader_id {
            let step_instant = self.instant_at(self.now);
            if let NodeState::Up(node) = &mut self.states[id as usize] {
                let effects = node.step(Event::Propose(payload), step_instant);
                self.handle_effects(id, effects);
                if self.pending_proposal_since.is_none() {
                    self.pending_proposal_since = Some(self.now);
                }
                return true;
            }
        }
        false
    }

    /// compacts the current leader's log up to `up_to_index`, storing
    /// `snapshot_data` as the new snapshot's bytes. returns `true` if a
    /// leader was found and compaction actually happened (see
    /// `RaftNode::compact_log`'s preconditions — `up_to_index` must be
    /// `<= commit_index`).
    ///
    /// exposed for tests to exercise the `InstallSnapshot` path
    /// deterministically: compact the leader's log, then let a lagging or
    /// newly-restarted follower's `next_index` fall behind
    /// `snapshot_index`, forcing `append_entries_for` to redirect.
    pub fn compact_leader_log(&mut self, up_to_index: LogIndex, snapshot_data: Vec<u8>) -> bool {
        let leader_id = self.node_ids.iter().copied().find(|&id| {
            matches!(&self.states[id as usize], NodeState::Up(n) if n.role() == raft::Role::Leader)
        });

        if let Some(id) = leader_id {
            if let NodeState::Up(node) = &mut self.states[id as usize] {
                return node.compact_log(up_to_index, snapshot_data);
            }
        }
        false
    }

    fn track_commit_progress(&mut self) {
        let max_commit = self
            .node_ids
            .iter()
            .filter_map(|&id| match &self.states[id as usize] {
                NodeState::Up(n) => Some(n.commit_index()),
                NodeState::Crashed(..) => None,
            })
            .max()
            .unwrap_or(0);
        if max_commit > self.max_commit_seen {
            self.max_commit_seen = max_commit;
            // a commit landed — any proposal accepted before this point has
            // now committed (raft commits log entries in order), so there's
            // nothing outstanding until the next successful `propose`.
            self.pending_proposal_since = None;
        }
    }

    fn inject_chaos(&mut self) {
        // crash a random live node.
        if self.rng.gen_bool(self.chaos.crash_prob_per_tick) {
            let live: Vec<NodeId> = self
                .node_ids
                .iter()
                .copied()
                .filter(|&id| matches!(&self.states[id as usize], NodeState::Up(_)))
                .collect();
            if !live.is_empty() {
                let victim = live[self.rng.gen_range(0..live.len())];
                self.crash(victim);
            }
        }

        // restart a random crashed node.
        if self.rng.gen_bool(self.chaos.restart_prob_per_tick) {
            let crashed: Vec<NodeId> = self
                .node_ids
                .iter()
                .copied()
                .filter(|&id| matches!(&self.states[id as usize], NodeState::Crashed(..)))
                .collect();
            if !crashed.is_empty() {
                let target = crashed[self.rng.gen_range(0..crashed.len())];
                self.restart(target);
            }
        }

        // open a partition, bisecting the cluster at random.
        if !self.network.is_partitioned() && self.rng.gen_bool(self.chaos.partition_prob_per_tick) {
            let (a, b) = self.random_bisection();
            if !a.is_empty() && !b.is_empty() {
                self.network.partition(a, b);
                self.last_fault_tick = self.now;
            }
        }

        // heal an existing partition.
        if self.network.is_partitioned() && self.rng.gen_bool(self.chaos.heal_prob_per_tick) {
            self.network.heal_partition();
            self.last_fault_tick = self.now;
        }
    }

    /// splits `node_ids` into two random non-empty groups.
    fn random_bisection(&mut self) -> (Vec<NodeId>, Vec<NodeId>) {
        let mut ids = self.node_ids.clone();
        // fisher-yates shuffle using the cluster's own rng, so the split is
        // deterministic given the seed.
        for i in (1..ids.len()).rev() {
            let j = self.rng.gen_range(0..=i);
            ids.swap(i, j);
        }
        let split = (ids.len() / 2).max(1);
        let a = ids[..split].to_vec();
        let b = ids[split..].to_vec();
        (a, b)
    }

    /// crashes node `id`: it stops processing ticks/messages and its
    /// last-persisted `HardState` (plus current snapshot bytes) is retained
    /// for a later `restart`. a no-op if the node is already crashed.
    ///
    /// exposed publicly (alongside `restart`) so tests and the campaign
    /// driver can inject faults deterministically, in addition to — or
    /// instead of — the probabilistic `ChaosSchedule`.
    pub fn crash(&mut self, id: NodeId) {
        if let NodeState::Up(node) = &self.states[id as usize] {
            let hs = node.hard_state();
            let snapshot_data = node.snapshot_data().to_vec();
            self.states[id as usize] = NodeState::Crashed(hs, snapshot_data);
            self.last_fault_tick = self.now;
        }
    }

    /// restarts a previously crashed node `id` from its last-persisted
    /// `HardState` via `RaftNode::restore`. a no-op if the node is not
    /// currently crashed.
    pub fn restart(&mut self, id: NodeId) {
        if let NodeState::Crashed(hs, snapshot_data) = &self.states[id as usize] {
            let peers: Vec<NodeId> = self.node_ids.iter().copied().filter(|&p| p != id).collect();
            let node_seed: u64 = self.rng.gen();
            let node = RaftNode::restore(
                id,
                peers,
                node_seed,
                self.instant_at(self.now),
                hs.clone(),
                snapshot_data.clone(),
            );
            self.states[id as usize] = NodeState::Up(Box::new(node));
            self.last_fault_tick = self.now;
        }
    }

    /// checks the three snapshot invariants against the cluster's current
    /// state. call this every tick from the driving loop (kept separate
    /// from `step` so a caller can choose to check less often for speed,
    /// though the 1000-seed campaign checks every tick).
    pub fn check_invariants(&self) -> Result<(), Violation> {
        let live_nodes: Vec<&RaftNode> = self
            .states
            .iter()
            .filter_map(|s| match s {
                NodeState::Up(n) => Some(n.as_ref()),
                NodeState::Crashed(..) => None,
            })
            .collect();

        // election safety and log matching only make sense over live nodes
        // — a crashed node's last-known state is frozen and shouldn't be
        // compared as if it were still participating.
        invariants::check_election_safety(&live_nodes)?;
        invariants::check_log_matching(&live_nodes)?;
        invariants::check_state_machine_safety(&self.applied, &self.node_ids)?;
        invariants::check_snapshot_consistency(&self.snapshot_installs, &self.node_ids)?;

        Ok(())
    }

    /// invariant 4: bounded liveness. call this periodically (e.g. once per
    /// tick, same as `check_invariants`) — it's a no-op unless there's an
    /// outstanding accepted proposal, so it's cheap and safe to call
    /// unconditionally even on an idle cluster.
    ///
    /// fires only when *all* of the following hold:
    ///   - a proposal was accepted by some leader (`pending_proposal_since`
    ///     is set) and has not yet committed anywhere,
    ///   - that proposal has been outstanding for at least
    ///     `LIVENESS_BOUND_TICKS`,
    ///   - the network has been fault-free for at least
    ///     `LIVENESS_BOUND_TICKS` (so a slow commit during active chaos is
    ///     expected and not a violation — the bound only applies once
    ///     things have settled).
    ///
    /// an idle cluster with nothing proposed is never a violation: there's
    /// no work outstanding for it to be failing to complete.
    pub fn check_liveness(&self) -> Result<(), Violation> {
        let Some(since) = self.pending_proposal_since else {
            return Ok(()); // nothing outstanding — nothing to check
        };

        let fault_free_for = self.now.saturating_sub(self.last_fault_tick);
        let proposal_age = self.now.saturating_sub(since);

        if fault_free_for >= LIVENESS_BOUND_TICKS && proposal_age >= LIVENESS_BOUND_TICKS {
            return Err(Violation::LivenessViolation {
                ticks_since_fault_free: fault_free_for,
            });
        }
        Ok(())
    }

    pub fn now(&self) -> Tick {
        self.now
    }
    pub fn node_ids(&self) -> &[NodeId] {
        &self.node_ids
    }

    /// number of currently-live (non-crashed) nodes.
    pub fn live_count(&self) -> usize {
        self.states
            .iter()
            .filter(|s| matches!(s, NodeState::Up(_)))
            .count()
    }
}
