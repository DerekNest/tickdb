use std::cmp::Ordering;
/// virtual network (§4.3).
///
/// models message delivery between raft nodes with injected faults, all
/// driven by a single seeded rng passed in from `Cluster`. no wall-clock
/// time and no real sockets — messages are queued with a *delivery tick*
/// and handed back to the cluster once the virtual clock reaches that tick.
///
/// # fault model
///
/// - **drop**: a message is discarded with probability `drop_prob` and
///   never delivered.
/// - **delay**: every surviving message gets an extra delay drawn from an
///   exponential distribution (mean `mean_delay_ticks`), so most messages
///   arrive almost immediately but a long tail arrives much later —
///   this mimics real network jitter far better than a uniform delay would.
/// - **duplicate**: with probability `dup_prob`, a second copy of the
///   message is enqueued at a further, independently-drawn delay.
/// - **partition**: while `partition` is `Some((group_a, group_b))`,
///   any message whose sender and receiver fall in different groups is
///   dropped outright, regardless of `drop_prob`. this models a network
///   split, not just packet loss.
use std::collections::BinaryHeap;

use rand::rngs::SmallRng;
use rand::Rng;

use raft::{Message, NodeId};

/// a single virtual tick — the sim's unit of logical time. one tick maps to
/// a fixed real duration (see `Cluster::TICK_DURATION`) so raft's
/// millisecond-based timers (election timeout, heartbeat interval) behave
/// correctly when driven by ticks instead of wall clock time.
pub type Tick = u64;

/// probabilities and delay parameters for fault injection. all fields are
/// plain floats/ticks so a test can dial faults up or down without touching
/// the network's internals.
#[derive(Debug, Clone, Copy)]
pub struct FaultConfig {
    pub drop_prob: f64,
    pub dup_prob: f64,
    /// mean of the exponential delay distribution, in ticks.
    pub mean_delay_ticks: f64,
}

impl FaultConfig {
    /// no faults at all — useful for isolating whether a test failure is a
    /// genuine raft bug versus a fault-injection interaction.
    pub fn none() -> Self {
        FaultConfig {
            drop_prob: 0.0,
            dup_prob: 0.0,
            mean_delay_ticks: 0.0,
        }
    }

    /// the §4.3 default fault profile: drop 10%, duplicate 1%, mean delay
    /// of 2 ticks (light jitter under normal conditions).
    pub fn default_chaos() -> Self {
        FaultConfig {
            drop_prob: 0.1,
            dup_prob: 0.01,
            mean_delay_ticks: 2.0,
        }
    }
}

/// a message queued for delivery at a specific tick.
///
/// wrapped in `Ord` (reversed, for use in a min-heap via `BinaryHeap`) so
/// the network can always pop the next-soonest delivery regardless of
/// insertion order — necessary because delayed/duplicated messages don't
/// arrive in send order.
struct QueuedMessage {
    deliver_at: Tick,
    to: NodeId,
    from: NodeId,
    msg: Message,
    /// monotonic sequence number, used only to break ties in `Ord` so the
    /// heap has a total order (two messages queued for the same tick still
    /// need a deterministic relative order for reproducibility).
    seq: u64,
}

impl PartialEq for QueuedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at && self.seq == other.seq
    }
}
impl Eq for QueuedMessage {}
impl Ord for QueuedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        // reversed: BinaryHeap is a max-heap, we want the smallest
        // (deliver_at, seq) at the top.
        other
            .deliver_at
            .cmp(&self.deliver_at)
            .then(other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for QueuedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Network {
    queue: BinaryHeap<QueuedMessage>,
    faults: FaultConfig,
    /// active partition, if any: (group containing node A, group containing node B).
    /// a message is dropped if its `from` and `to` are in different groups.
    partition: Option<(Vec<NodeId>, Vec<NodeId>)>,
    next_seq: u64,
}

impl Network {
    pub fn new(faults: FaultConfig) -> Self {
        Network {
            queue: BinaryHeap::new(),
            faults,
            partition: None,
            next_seq: 0,
        }
    }

    /// installs a partition: `group_a` and `group_b` should together cover
    /// every node id in the cluster. messages crossing the boundary are
    /// dropped until `heal_partition` is called.
    pub fn partition(&mut self, group_a: Vec<NodeId>, group_b: Vec<NodeId>) {
        self.partition = Some((group_a, group_b));
    }

    pub fn heal_partition(&mut self) {
        self.partition = None;
    }

    pub fn is_partitioned(&self) -> bool {
        self.partition.is_some()
    }

    /// returns true if `a` and `b` are separated by the active partition.
    fn crosses_partition(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            None => false,
            Some((group_a, group_b)) => {
                let a_in_a = group_a.contains(&a);
                let b_in_a = group_a.contains(&b);
                let a_in_b = group_b.contains(&a);
                let b_in_b = group_b.contains(&b);
                (a_in_a && b_in_b) || (a_in_b && b_in_a)
            }
        }
    }

    /// enqueues `msg` for delivery from `from` to `to`, applying drop /
    /// delay / duplicate / partition faults. `now` is the current virtual
    /// tick; all randomness is drawn from `rng` (owned by the caller, so a
    /// single seed drives the whole simulation deterministically).
    pub fn send(&mut self, from: NodeId, to: NodeId, msg: Message, now: Tick, rng: &mut SmallRng) {
        if self.crosses_partition(from, to) {
            return; // dropped: network split
        }
        if rng.gen_bool(self.faults.drop_prob) {
            return; // dropped: random loss
        }

        let delay = self.sample_delay(rng);
        self.enqueue(to, from, msg.clone(), now + delay);

        if rng.gen_bool(self.faults.dup_prob) {
            let dup_delay = self.sample_delay(rng);
            self.enqueue(to, from, msg, now + dup_delay);
        }
    }

    fn enqueue(&mut self, to: NodeId, from: NodeId, msg: Message, deliver_at: Tick) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(QueuedMessage {
            deliver_at,
            to,
            from,
            msg,
            seq,
        });
    }

    /// draws a delay from an exponential distribution with the configured
    /// mean, rounded to the nearest tick (minimum 1 tick — a message can
    /// never arrive in the same tick it was sent, which keeps causality
    /// straightforward: nothing "reacts" to a send within the same step).
    fn sample_delay(&self, rng: &mut SmallRng) -> Tick {
        if self.faults.mean_delay_ticks <= 0.0 {
            return 1;
        }
        // inverse-CDF sampling: -mean * ln(1 - u), u ~ Uniform(0, 1).
        let u: f64 = rng.gen_range(0.0..1.0_f64);
        let sample = -self.faults.mean_delay_ticks * (1.0 - u).ln();
        (sample.round() as Tick).max(1)
    }

    /// pops and returns every message whose `deliver_at <= now`, in
    /// delivery order. called once per tick by the cluster.
    pub fn drain_ready(&mut self, now: Tick) -> Vec<(NodeId, NodeId, Message)> {
        let mut ready = Vec::new();
        while let Some(top) = self.queue.peek() {
            if top.deliver_at > now {
                break;
            }
            let qm = self.queue.pop().unwrap();
            ready.push((qm.to, qm.from, qm.msg));
        }
        ready
    }

    /// number of messages still in flight — used by tests to confirm the
    /// network eventually quiesces.
    pub fn pending_count(&self) -> usize {
        self.queue.len()
    }
}

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use raft::Message;
    use rand::SeedableRng;

    fn dummy_msg() -> Message {
        Message::RequestVoteResponse {
            term: 1,
            vote_granted: true,
            voter_id: 0,
        }
    }

    #[test]
    fn no_faults_always_delivers() {
        let mut net = Network::new(FaultConfig::none());
        let mut rng = SmallRng::seed_from_u64(1);
        for _ in 0..100 {
            net.send(0, 1, dummy_msg(), 0, &mut rng);
        }
        // mean_delay_ticks = 0 -> delay is exactly 1 tick, deterministic.
        assert_eq!(net.drain_ready(0).len(), 0);
        assert_eq!(net.drain_ready(1).len(), 100);
    }

    #[test]
    fn full_drop_delivers_nothing() {
        let cfg = FaultConfig {
            drop_prob: 1.0,
            dup_prob: 0.0,
            mean_delay_ticks: 1.0,
        };
        let mut net = Network::new(cfg);
        let mut rng = SmallRng::seed_from_u64(1);
        for _ in 0..100 {
            net.send(0, 1, dummy_msg(), 0, &mut rng);
        }
        assert_eq!(net.pending_count(), 0);
    }

    #[test]
    fn partition_blocks_cross_group_messages() {
        let mut net = Network::new(FaultConfig::none());
        let mut rng = SmallRng::seed_from_u64(1);
        net.partition(vec![0, 1], vec![2]);

        net.send(0, 1, dummy_msg(), 0, &mut rng); // same group -> delivered
        net.send(0, 2, dummy_msg(), 0, &mut rng); // crosses partition -> dropped
        net.send(2, 1, dummy_msg(), 0, &mut rng); // crosses partition -> dropped

        assert_eq!(net.pending_count(), 1);
    }

    #[test]
    fn healed_partition_allows_delivery_again() {
        let mut net = Network::new(FaultConfig::none());
        let mut rng = SmallRng::seed_from_u64(1);
        net.partition(vec![0], vec![1]);
        net.send(0, 1, dummy_msg(), 0, &mut rng);
        assert_eq!(net.pending_count(), 0);

        net.heal_partition();
        net.send(0, 1, dummy_msg(), 0, &mut rng);
        assert_eq!(net.pending_count(), 1);
    }

    #[test]
    fn deterministic_given_same_seed() {
        // two networks fed the same seed and the same operations must
        // produce byte-identical delivery schedules.
        let run = || {
            let mut net = Network::new(FaultConfig::default_chaos());
            let mut rng = SmallRng::seed_from_u64(42);
            for i in 0..50u64 {
                net.send(0, 1, dummy_msg(), i, &mut rng);
            }
            let mut delivered = Vec::new();
            for t in 0..200 {
                delivered.push(net.drain_ready(t).len());
            }
            delivered
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn drain_ready_respects_tick_ordering() {
        let mut net = Network::new(FaultConfig::none());
        let mut rng = SmallRng::seed_from_u64(1);
        net.send(0, 1, dummy_msg(), 5, &mut rng); // delivers at tick 6
        assert_eq!(net.drain_ready(5).len(), 0);
        assert_eq!(net.drain_ready(6).len(), 1);
    }
}
