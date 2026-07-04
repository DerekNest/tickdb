/// `RaftNode` — the I/O-free Raft state machine (§4.1, §4.2).
///
/// All nondeterminism is injected:
/// - **Time**: every `step` call receives `now: Instant` from the caller.
/// - **Network**: messages arrive as `Event::Message`; nothing is read from
///   a socket here.
/// - **Storage**: `Effect::Persist` and `Effect::ApplyCommitted` describe what
///   must be written / applied; `step` never touches disk.
/// - **Randomness**: election timeout jitter uses a `SmallRng` seeded at
///   construction. Given the same seed and the same sequence of `(Event, now)`
///   inputs, `step` is fully deterministic — this is what makes the
///   simulation harness (milestone 8) possible.
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::types::{
    Effect, Entry, Event, HardState, LogIndex, Message, NodeId, Term, ELECTION_TIMEOUT_BASE,
    ELECTION_TIMEOUT_JITTER_MAX, HEARTBEAT_INTERVAL,
};

/// The three Raft roles (Raft paper §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    /// This node's id.
    pub id: NodeId,
    /// IDs of all other nodes in the cluster (static membership, §4.1).
    pub peers: Vec<NodeId>,

    // ── Persistent state (Raft paper Figure 2) ──────────────────────────────
    current_term: Term,
    voted_for: Option<NodeId>,
    /// entries with index > `snapshot_index`. see `log_pos` for how an
    /// absolute log index maps to a position in this vec once compaction
    /// has shifted that mapping away from the naive `index - 1`.
    log: Vec<Entry>,
    /// highest index reflected in the last snapshot (0 = no snapshot taken;
    /// the log then holds every entry from index 1, as in milestones 1–8).
    snapshot_index: LogIndex,
    /// term of the entry at `snapshot_index`.
    snapshot_term: Term,
    /// the snapshot bytes themselves — opaque to Raft, see
    /// `Message::InstallSnapshot`'s doc comment. Sent to a peer whose
    /// `next_index` has fallen at or before `snapshot_index`.
    snapshot_data: Vec<u8>,

    // ── Volatile state (all nodes) ──────────────────────────────────────────
    commit_index: LogIndex,
    /// Highest log entry applied to the state machine.  `step` advances this
    /// implicitly by emitting `ApplyCommitted` for the newly committed range;
    /// callers are expected to apply them in order. Tracked here so `step`
    /// never re-emits an already-applied entry.
    last_applied: LogIndex,
    role: Role,
    /// Last leader we heard from (for `ProposeRejected` hints).
    current_leader: Option<NodeId>,

    // ── Volatile state (leader only) ────────────────────────────────────────
    /// For each peer: index of the next log entry to send.
    next_index: HashMap<NodeId, LogIndex>,
    /// For each peer: highest log entry known to be replicated.
    match_index: HashMap<NodeId, LogIndex>,

    // ── Volatile state (candidate only) ─────────────────────────────────────
    votes_received: HashSet<NodeId>,

    // ── Timers ───────────────────────────────────────────────────────────────
    /// Followers/candidates: become candidate if `now >= election_deadline`.
    election_deadline: Instant,
    /// Leaders: send a heartbeat (AppendEntries) if `now >= next_heartbeat`.
    next_heartbeat: Instant,

    /// Source of election-timeout jitter. Seeded at construction — see the
    /// module doc comment on why this preserves determinism.
    rng: SmallRng,
}

impl RaftNode {
    /// Creates a new node in the `Follower` role with an empty log.
    ///
    /// `now` is used to compute the first election deadline.  `seed` drives
    /// all election-timeout jitter for this node's lifetime.
    pub fn new(id: NodeId, peers: Vec<NodeId>, seed: u64, now: Instant) -> Self {
        let mut node = RaftNode {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            snapshot_index: 0,
            snapshot_term: 0,
            snapshot_data: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            current_leader: None,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_received: HashSet::new(),
            election_deadline: now, // overwritten immediately below
            next_heartbeat: now,
            rng: SmallRng::seed_from_u64(seed),
        };
        node.election_deadline = node.new_election_deadline(now);
        node
    }

    /// Restores a node from previously persisted `HardState` after a crash.
    ///
    /// Volatile state (commit_index, role, leader-only maps) starts fresh —
    /// this is correct: a restarted follower re-learns commit_index from the
    /// next AppendEntries, and a restarted candidate/leader simply becomes a
    /// follower and re-runs the election protocol if needed.
    ///
    /// `last_applied` starts at `hs.snapshot_index` rather than `0`: if a
    /// snapshot was taken before the crash, the state machine's data up to
    /// that point is durable in the snapshot itself (in the real system,
    /// already-flushed storage-engine segments) — the node has no log
    /// entries below `snapshot_index` to re-walk even if it wanted to.
    /// entries *above* `snapshot_index` still get re-applied from scratch
    /// (see `apply_committed`'s doc comment on at-least-once delivery).
    ///
    /// `snapshot_data` is supplied separately rather than living inside
    /// `HardState` (which gets persisted wholesale on every `Effect::Persist`
    /// — cheap for a small log, expensive for a multi-hundred-MiB snapshot
    /// blob). the real caller keeps the snapshot as its own file on disk and
    /// passes the bytes back in here at restart time; pass `Vec::new()` if
    /// no snapshot has been taken (or the caller hasn't reloaded it yet —
    /// see the doc comment on `snapshot_data`'s field for the consequence).
    pub fn restore(
        id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        now: Instant,
        hs: HardState,
        snapshot_data: Vec<u8>,
    ) -> Self {
        let mut node = Self::new(id, peers, seed, now);
        node.current_term = hs.current_term;
        node.voted_for = hs.voted_for;
        node.log = hs.log;
        node.snapshot_index = hs.snapshot_index;
        node.snapshot_term = hs.snapshot_term;
        node.snapshot_data = snapshot_data;
        node.commit_index = hs.snapshot_index;
        node.last_applied = hs.snapshot_index;
        node
    }

    // ── Public accessors (read-only views for tests and the sim harness) ────

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    pub fn log(&self) -> &[Entry] {
        &self.log
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn current_leader(&self) -> Option<NodeId> {
        self.current_leader
    }
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }
    pub fn snapshot_data(&self) -> &[u8] {
        &self.snapshot_data
    }

    /// Snapshot of the current hard state (for test assertions / sim "disk").
    pub fn hard_state(&self) -> HardState {
        HardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        }
    }

    // ── Entry point ──────────────────────────────────────────────────────────

    /// The single state-transition function.  See module doc comment.
    pub fn step(&mut self, event: Event, now: Instant) -> Vec<Effect> {
        match event {
            Event::Tick => self.on_tick(now),
            Event::Message { from, msg } => self.on_message(from, msg, now),
            Event::Propose(payload) => self.on_propose(payload, now),
        }
    }

    // ── Tick: drive election / heartbeat timeouts ────────────────────────────

    fn on_tick(&mut self, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();

        match self.role {
            Role::Leader => {
                // Leaders never time out elections; they send heartbeats instead.
                if now >= self.next_heartbeat {
                    effects.extend(self.broadcast_append_entries());
                    self.next_heartbeat = now + HEARTBEAT_INTERVAL;
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    effects.extend(self.start_election(now));
                }
            }
        }

        effects
    }

    // ── Election ─────────────────────────────────────────────────────────────

    /// Computes a fresh randomized election deadline: `now + 150..300ms`.
    ///
    /// Jitter comes from `self.rng` — see the module doc comment on
    /// determinism.
    fn new_election_deadline(&mut self, now: Instant) -> Instant {
        let jitter_ms = self
            .rng
            .gen_range(0..=ELECTION_TIMEOUT_JITTER_MAX.as_millis() as u64);
        now + ELECTION_TIMEOUT_BASE + Duration::from_millis(jitter_ms)
    }

    /// Begins a new election: increment term, vote for self, request votes
    /// from all peers.
    ///
    /// # Single-node cluster
    /// If `peers` is empty, a "majority" is just this node's own vote, so we
    /// become leader immediately (see `tally_votes`).
    fn start_election(&mut self, now: Instant) -> Vec<Effect> {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.election_deadline = self.new_election_deadline(now);

        let mut effects = vec![
            // PERSISTENCE RULE: current_term and voted_for changed — this
            // must be fsync'd before the RequestVote messages below are sent,
            // otherwise a crash+restart could let this node vote again for a
            // different candidate in the same term (double vote = safety
            // violation).
            Effect::Persist(self.hard_state()),
            Effect::ResetElectionTimer,
        ];

        // Single-node cluster: a vote from ourselves is already a majority.
        if let Some(leader_effects) = self.tally_votes(now) {
            effects.extend(leader_effects);
            return effects;
        }

        let (last_log_index, last_log_term) = self.last_log_info();
        for &peer in &self.peers {
            effects.push(Effect::Send(
                peer,
                Message::RequestVote {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                },
            ));
        }

        effects
    }

    /// Returns `(index, term)` of the last log entry, or the snapshot's
    /// `(snapshot_index, snapshot_term)` if the log is empty — this
    /// subsumes the old "(0, 0) when nothing has ever happened" case, since
    /// `snapshot_index`/`snapshot_term` default to `(0, 0)` until a
    /// snapshot is taken.
    fn last_log_info(&self) -> (LogIndex, Term) {
        match self.log.last() {
            Some(e) => (e.index, e.term),
            None => (self.snapshot_index, self.snapshot_term),
        }
    }

    /// Maps an absolute log index to a position in `self.log`, accounting
    /// for compaction having shifted `log[0]` away from index 1.
    ///
    /// Returns `None` if `index` has been compacted away (`<= snapshot_index`)
    /// or hasn't arrived yet (`> snapshot_index + log.len()`).
    fn log_pos(&self, index: LogIndex) -> Option<usize> {
        if index <= self.snapshot_index {
            return None;
        }
        let pos = (index - self.snapshot_index - 1) as usize;
        if pos < self.log.len() {
            Some(pos)
        } else {
            None
        }
    }

    /// If `votes_received` now constitutes a majority, transitions to
    /// `Leader` and returns the effects of doing so. Returns `None` if not
    /// yet a majority (caller continues the election).
    fn tally_votes(&mut self, now: Instant) -> Option<Vec<Effect>> {
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        if self.votes_received.len() >= majority {
            Some(self.become_leader(now))
        } else {
            None
        }
    }

    /// Transitions to `Leader`: initializes per-follower replication state,
    /// appends a no-op entry for the new term, and sends initial
    /// AppendEntries (heartbeat + the no-op) to all peers.
    ///
    /// # Why the no-op entry
    /// Raft safety forbids a leader from advancing `commit_index` past an
    /// entry from a *previous* term based on replication count alone (§4.2,
    /// "leader completeness"). Appending a no-op entry in the *new* term
    /// gives the leader something it can commit via the normal majority
    /// rule; once that no-op commits, every entry before it commits too
    /// (entries are committed in log order). Without this, a leader that
    /// inherits a long uncommitted tail could stall indefinitely if no new
    /// client request arrives — which would violate the simulation's
    /// liveness check (§4.3 item 4).
    fn become_leader(&mut self, now: Instant) -> Vec<Effect> {
        self.role = Role::Leader;
        self.current_leader = Some(self.id);
        self.votes_received.clear();

        let next = self.snapshot_index + self.log.len() as LogIndex + 1;
        for &peer in &self.peers {
            // start optimistically at `next`; a peer that's actually behind
            // will reject the first AppendEntries and we'll back off via the
            // normal failure path (or redirect to InstallSnapshot if it's
            // fallen behind our snapshot_index — see `append_entries_for`).
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
        }

        // No-op entry for the new term (see doc comment above).
        self.log.push(Entry {
            term: self.current_term,
            index: next,
            payload: Vec::new(),
        });

        self.next_heartbeat = now + HEARTBEAT_INTERVAL;

        let mut effects = vec![
            // PERSISTENCE RULE: the log grew by one entry (the no-op) — must
            // be fsync'd before the AppendEntries below, which promises peers
            // this entry exists at `index = next`.
            Effect::Persist(self.hard_state()),
        ];

        // Single-node cluster: `advance_commit_index` with `peers.is_empty()`
        // has majority = 1, indices = [log.len()] = [next], which is >= 1
        // and log[next-1].term == current_term — so the no-op (and anything
        // before it) commits immediately. For multi-node clusters this is a
        // no-op (no peer has acked yet).
        effects.extend(self.advance_commit_index());

        effects.extend(self.broadcast_append_entries());
        effects
    }

    // ── Message handling ──────────────────────────────────────────────────────

    fn on_message(&mut self, from: NodeId, msg: Message, now: Instant) -> Vec<Effect> {
        let mut effects = Vec::new();

        // Universal rule (Raft paper §5.1): if the message's term is newer
        // than ours, update our term, clear our vote, and step down to
        // Follower *before* processing the message body.
        if msg.term() > self.current_term {
            self.current_term = msg.term();
            self.voted_for = None;
            if self.role != Role::Follower {
                self.role = Role::Follower;
                self.election_deadline = self.new_election_deadline(now);
                effects.push(Effect::ResetElectionTimer);
            }
            // PERSISTENCE RULE: current_term/voted_for changed — persist
            // before any response below that depends on the new term.
            effects.push(Effect::Persist(self.hard_state()));
        }

        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                effects.extend(self.on_request_vote(
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    now,
                ));
            }
            Message::RequestVoteResponse {
                term,
                vote_granted,
                voter_id,
            } => {
                effects.extend(self.on_request_vote_response(term, vote_granted, voter_id, now));
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                effects.extend(self.on_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    now,
                ));
            }
            Message::AppendEntriesResponse {
                term,
                success,
                follower_id,
                match_index,
            } => {
                effects.extend(self.on_append_entries_response(
                    term,
                    success,
                    follower_id,
                    match_index,
                ));
            }
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
            } => {
                effects.extend(self.on_install_snapshot(
                    term,
                    leader_id,
                    last_included_index,
                    last_included_term,
                    data,
                    now,
                ));
            }
            Message::InstallSnapshotResponse { term, follower_id } => {
                effects.extend(self.on_install_snapshot_response(term, follower_id));
            }
        }

        let _ = from; // `from` is implicit in each message's *_id field
        effects
    }

    /// Handles `RequestVote` (Raft paper §5.2, §5.4).
    ///
    /// Grants the vote iff:
    ///   1. `term >= current_term` (stale candidates are rejected outright), and
    ///   2. we haven't voted for someone else this term, and
    ///   3. the candidate's log is at least as up-to-date as ours.
    ///
    /// "At least as up-to-date" (§5.4.1): compare `last_log_term` first
    /// (higher term wins), then `last_log_index` (longer log wins on a tie).
    /// This is the core of Raft's leader-completeness guarantee: a candidate
    /// can only win if its log contains every entry that could have been
    /// committed by a previous leader.
    fn on_request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        now: Instant,
    ) -> Vec<Effect> {
        if term < self.current_term {
            return vec![Effect::Send(
                candidate_id,
                Message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                    voter_id: self.id,
                },
            )];
        }

        let (our_last_index, our_last_term) = self.last_log_info();
        let candidate_log_ok = (last_log_term, last_log_index) >= (our_last_term, our_last_index);

        let can_vote = match self.voted_for {
            None => true,
            Some(v) => v == candidate_id,
        };

        let grant = candidate_log_ok && can_vote;

        let mut effects = Vec::new();

        if grant {
            self.voted_for = Some(candidate_id);
            // Granting a vote means we've heard from a viable candidate —
            // reset our election timer so we don't immediately also start
            // a competing election (reduces split votes, Raft paper §5.2).
            self.election_deadline = self.new_election_deadline(now);
            effects.push(Effect::ResetElectionTimer);
            // PERSISTENCE RULE: voted_for changed — must be fsync'd before
            // the RequestVoteResponse below, otherwise a crash+restart could
            // let us grant a second, conflicting vote in the same term.
            effects.push(Effect::Persist(self.hard_state()));
        }

        effects.push(Effect::Send(
            candidate_id,
            Message::RequestVoteResponse {
                term: self.current_term,
                vote_granted: grant,
                voter_id: self.id,
            },
        ));

        effects
    }

    /// Handles `RequestVoteResponse`. Ignores stale responses (wrong term or
    /// we're no longer a candidate — e.g., we already won/lost this election).
    fn on_request_vote_response(
        &mut self,
        term: Term,
        vote_granted: bool,
        voter_id: NodeId,
        now: Instant,
    ) -> Vec<Effect> {
        if self.role != Role::Candidate || term != self.current_term || !vote_granted {
            return Vec::new();
        }

        self.votes_received.insert(voter_id);
        self.tally_votes(now).unwrap_or_default()
    }

    /// Handles `AppendEntries` (Raft paper §5.3).
    ///
    /// # Steps
    /// 1. Reject if `term < current_term` (stale leader).
    /// 2. Otherwise: this is a valid leader for our term — step down to
    ///    Follower if we were a Candidate, record `current_leader`, reset
    ///    the election timer (we just heard from a live leader).
    /// 3. **Log matching check**: reject if our log doesn't contain an entry
    ///    at `prev_log_index` with term `prev_log_term`. This is the crux of
    ///    the "log matching property" (§4.3 invariant 2): if this check
    ///    passes, our log and the leader's log are identical up to
    ///    `prev_log_index`.
    /// 4. **Conflict resolution**: for each new entry, if our log already has
    ///    a *different* entry at that index, truncate from there (the
    ///    leader's version wins) and append. If our entry matches, skip it
    ///    (idempotent — handles retransmitted AppendEntries).
    /// 5. Advance `commit_index` to `min(leader_commit, last_new_index)` and
    ///    emit `ApplyCommitted` for any newly committed entries.
    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        now: Instant,
    ) -> Vec<Effect> {
        if term < self.current_term {
            return vec![Effect::Send(
                leader_id,
                Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    follower_id: self.id,
                    match_index: 0,
                },
            )];
        }

        let mut effects = Vec::new();

        // Valid leader for our term: step down if we were a Candidate, and
        // (re)acknowledge this leader.
        if self.role == Role::Candidate {
            self.role = Role::Follower;
        }
        self.current_leader = Some(leader_id);
        // We heard from the current leader — postpone our own election.
        self.election_deadline = self.new_election_deadline(now);
        effects.push(Effect::ResetElectionTimer);

        // ── Log matching check ──────────────────────────────────────────────
        // three cases: (a) leader claims no prior entries — trivially ok;
        // (b) prev_log_index lands exactly on our snapshot boundary — check
        // against snapshot_term, since that entry no longer lives in `log`;
        // (c) prev_log_index is within our live log — check normally.
        // (a fourth case, prev_log_index < snapshot_index, would mean the
        // leader is offering entries we've already compacted well past —
        // shouldn't happen from a well-behaved leader, since
        // `append_entries_for` redirects to InstallSnapshot once a peer's
        // next_index falls at or below our snapshot_index; trust it
        // defensively rather than reject a legitimate leader.)
        let log_ok = if prev_log_index == 0 {
            true
        } else if prev_log_index == self.snapshot_index {
            prev_log_term == self.snapshot_term
        } else if prev_log_index < self.snapshot_index {
            true
        } else {
            match self.log_pos(prev_log_index) {
                Some(pos) => self.log[pos].term == prev_log_term,
                None => false, // our log is shorter than prev_log_index
            }
        };

        if !log_ok {
            effects.push(Effect::Send(
                leader_id,
                Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    follower_id: self.id,
                    match_index: 0,
                },
            ));
            return effects;
        }

        // ── Conflict resolution: append/overwrite entries ───────────────────
        let mut last_new_index = prev_log_index;
        let mut log_changed = false;
        for (i, entry) in entries.into_iter().enumerate() {
            let idx = prev_log_index + 1 + i as LogIndex;
            last_new_index = idx;

            if idx <= self.snapshot_index {
                // already covered by our snapshot — nothing to do.
                continue;
            }

            match self.log_pos(idx) {
                Some(pos) if self.log[pos].term == entry.term => {
                    // Same entry already present (retransmit) — no-op.
                    // INVARIANT (log matching): if term matches at the same
                    // index, the entries are identical because at most one
                    // leader exists per term and a leader never overwrites
                    // its own log.
                }
                Some(pos) => {
                    // Conflict: truncate from `idx` onward, then append the
                    // leader's version. Everything after this point in our
                    // old log is discarded — it was from a leader that never
                    // committed it (committed entries are never overwritten,
                    // guaranteed by the election restriction in
                    // `on_request_vote`).
                    self.log.truncate(pos);
                    self.log.push(entry);
                    log_changed = true;
                }
                None => {
                    // Beyond the end of our log — append.
                    self.log.push(entry);
                    log_changed = true;
                }
            }
        }

        if log_changed {
            // PERSISTENCE RULE: the log changed — must be fsync'd before the
            // success response below, which promises the leader these
            // entries are durable on this follower.
            effects.push(Effect::Persist(self.hard_state()));
        }

        // ── Advance commit index ─────────────────────────────────────────────
        let highest_index = self.snapshot_index + self.log.len() as LogIndex;
        let new_commit_index = leader_commit.min(last_new_index).min(highest_index);
        if new_commit_index > self.commit_index {
            self.commit_index = new_commit_index;
            effects.extend(self.apply_committed());
        }

        effects.push(Effect::Send(
            leader_id,
            Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                follower_id: self.id,
                match_index: last_new_index,
            },
        ));

        effects
    }

    /// Handles `AppendEntriesResponse`. Ignores stale responses (wrong term
    /// or we're no longer leader).
    ///
    /// On success: advances `match_index`/`next_index` for the follower and
    /// recomputes `commit_index` via the majority rule (§5.3, §5.4.2):
    /// `commit_index` may advance to `N` only if a majority of nodes (the
    /// leader included) have `match_index >= N` **and** `log[N].term ==
    /// current_term`. The term check is the safety-critical part — see
    /// `become_leader`'s doc comment for why.
    ///
    /// On failure: decrements `next_index` for the follower (the standard
    /// "back off by one and retry" strategy) and immediately retries with
    /// the updated `prev_log_index`, rather than waiting for the next
    /// heartbeat — this bounds convergence time after a follower falls
    /// behind or restarts with a shorter log.
    fn on_append_entries_response(
        &mut self,
        term: Term,
        success: bool,
        follower_id: NodeId,
        match_index: LogIndex,
    ) -> Vec<Effect> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }

        if success {
            let cur = self.match_index.get(&follower_id).copied().unwrap_or(0);
            if match_index > cur {
                self.match_index.insert(follower_id, match_index);
            }
            self.next_index.insert(follower_id, match_index + 1);

            return self.advance_commit_index();
        }

        // Failure: back off next_index and retry immediately.
        let cur_next = self.next_index.get(&follower_id).copied().unwrap_or(1);
        let new_next = cur_next.saturating_sub(1).max(1);
        self.next_index.insert(follower_id, new_next);

        vec![self.append_entries_for(follower_id)]
    }

    /// Handles `InstallSnapshot` (Raft paper §7).
    ///
    /// Unlike the real paper's chunked RPC, `data` arrives here already
    /// fully reassembled — the transport layer (milestone 9's TCP
    /// transport) is responsible for splitting/joining the wire-level
    /// chunks; this state machine only reasons about the *logical* install.
    ///
    /// Simplification versus the full paper: rather than the optimization
    /// of retaining a log suffix when it happens to extend past the
    /// snapshot with matching entries, this always discards the entire
    /// existing log and starts fresh from `last_included_index`. Simpler,
    /// always safe, costs at most one extra round of re-replication for the
    /// (rare) case where the follower actually had some of those entries
    /// already — acceptable for how infrequently this path is exercised
    /// compared to normal `AppendEntries` replication.
    fn on_install_snapshot(
        &mut self,
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        now: Instant,
    ) -> Vec<Effect> {
        if term < self.current_term {
            return vec![Effect::Send(
                leader_id,
                Message::InstallSnapshotResponse {
                    term: self.current_term,
                    follower_id: self.id,
                },
            )];
        }

        if self.role == Role::Candidate {
            self.role = Role::Follower;
        }
        self.current_leader = Some(leader_id);
        self.election_deadline = self.new_election_deadline(now);

        let mut effects = vec![Effect::ResetElectionTimer];

        if last_included_index <= self.snapshot_index {
            // We already have an equal-or-newer snapshot — nothing to do,
            // but still ack so the leader doesn't retry indefinitely.
            effects.push(Effect::Send(
                leader_id,
                Message::InstallSnapshotResponse {
                    term: self.current_term,
                    follower_id: self.id,
                },
            ));
            return effects;
        }

        self.log.clear();
        self.snapshot_index = last_included_index;
        self.snapshot_term = last_included_term;
        self.snapshot_data = data.clone();
        self.commit_index = self.commit_index.max(last_included_index);
        self.last_applied = last_included_index;

        effects.push(Effect::Persist(self.hard_state()));
        effects.push(Effect::ApplySnapshot {
            last_included_index,
            data,
        });
        effects.push(Effect::Send(
            leader_id,
            Message::InstallSnapshotResponse {
                term: self.current_term,
                follower_id: self.id,
            },
        ));

        effects
    }

    /// Handles `InstallSnapshotResponse`. Ignores stale responses (wrong
    /// term or we're no longer leader) — same rule as
    /// `on_append_entries_response`.
    fn on_install_snapshot_response(&mut self, term: Term, follower_id: NodeId) -> Vec<Effect> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }

        // The follower has now caught up to (at least) our snapshot
        // boundary — resume normal AppendEntries replication from there.
        // (We use our *current* snapshot_index as the assumed install
        // point; if a newer snapshot has been taken since we sent this
        // InstallSnapshot, the follower will simply receive another one on
        // the next round, since `append_entries_for` re-checks
        // `next <= snapshot_index` fresh every time.)
        self.match_index.insert(follower_id, self.snapshot_index);
        self.next_index.insert(follower_id, self.snapshot_index + 1);

        vec![self.append_entries_for(follower_id)]
    }

    // ── Log compaction ────────────────────────────────────────────────────────

    /// Compacts the log up to and including `up_to_index`, replacing those
    /// entries with a snapshot boundary at `(up_to_index, term_at(up_to_index))`.
    ///
    /// Called by the server layer (milestone 9) after it has durably
    /// flushed the storage engine's state through `up_to_index` — at that
    /// point the log entries below it are redundant (their effect is
    /// already captured in `snapshot_data`) and can be discarded to bound
    /// the log's size.
    ///
    /// # Safety requirement
    /// `up_to_index` must be `<= commit_index`: compacting an uncommitted
    /// entry would let it silently disappear rather than being correctly
    /// resolved (committed or overwritten) by the normal replication
    /// protocol. Returns `false` (no-op) if this doesn't hold, or if
    /// `up_to_index <= snapshot_index` (nothing new to compact).
    pub fn compact_log(&mut self, up_to_index: LogIndex, snapshot_data: Vec<u8>) -> bool {
        if up_to_index > self.commit_index || up_to_index <= self.snapshot_index {
            return false;
        }

        let Some(pos) = self.log_pos(up_to_index) else {
            return false;
        };
        let term_at = self.log[pos].term;

        // Drop entries [0, pos] inclusive — keep everything after.
        self.log.drain(0..=pos);
        self.snapshot_index = up_to_index;
        self.snapshot_term = term_at;
        self.snapshot_data = snapshot_data;

        true
    }

    /// Recomputes `commit_index` from `match_index` across all nodes and
    /// emits `ApplyCommitted` for any newly committed entries.
    ///
    /// `commit_index` is the largest `N` such that at least `majority` of
    /// `{leader's own log length} ∪ {match_index[p] for p in peers}` are
    /// `>= N`. Equivalently: sort match indices descending and take the
    /// `majority`-th value. The additional `log[N-1].term == current_term`
    /// check enforces the "only commit entries from the current term"
    /// safety rule.
    fn advance_commit_index(&mut self) -> Vec<Effect> {
        let total_nodes = self.peers.len() + 1;
        let majority = total_nodes / 2 + 1;

        let mut indices: Vec<LogIndex> = self
            .peers
            .iter()
            .map(|p| self.match_index.get(p).copied().unwrap_or(0))
            .collect();
        indices.push(self.snapshot_index + self.log.len() as LogIndex); // leader's own log is fully matched

        indices.sort_unstable_by(|a, b| b.cmp(a)); // descending
        let candidate = indices[majority - 1];

        // term of the entry at `candidate`: if it's exactly our snapshot
        // boundary, use snapshot_term (that entry no longer lives in
        // `log`); otherwise look it up normally. a candidate strictly below
        // snapshot_index can't arise from a correctly-operating leader
        // (compact_log only compacts already-committed entries), but is
        // treated as matching defensively rather than silently stalling.
        let candidate_term = if candidate == self.snapshot_index {
            Some(self.snapshot_term)
        } else if candidate < self.snapshot_index {
            Some(self.current_term)
        } else {
            self.log_pos(candidate).map(|pos| self.log[pos].term)
        };

        if candidate > self.commit_index
            && candidate >= 1
            && candidate_term == Some(self.current_term)
        {
            self.commit_index = candidate;
            return self.apply_committed();
        }

        Vec::new()
    }

    /// Emits `ApplyCommitted` for entries in `(last_applied, commit_index]`
    /// and advances `last_applied`.
    ///
    /// # At-least-once delivery
    /// `last_applied` is volatile state (Raft paper Figure 2) — it is
    /// deliberately **not** persisted, and `RaftNode::restore` resets it to
    /// `hs.snapshot_index` rather than remembering exactly where a crashed
    /// node left off. This means a restarted node re-walks its log from
    /// the snapshot boundary and re-emits `ApplyCommitted` for entries it
    /// may have already applied before crashing. This is intentional and
    /// correct: the state machine (the storage engine's ingest path, in the
    /// real system) is required to be idempotent under redelivery — tickdb's
    /// engine already guarantees this via trade_id-based dedup (§2.3). The
    /// simulation harness's state-machine-safety check is written to
    /// tolerate this too (see `sim::invariants::check_state_machine_safety`).
    fn apply_committed(&mut self) -> Vec<Effect> {
        if self.commit_index <= self.last_applied {
            return Vec::new();
        }
        // both bounds are absolute log indices; convert to vec positions
        // relative to the snapshot boundary. `last_applied` is clamped at
        // `snapshot_index` (never goes below it — see `restore` and
        // `install_snapshot`), so this subtraction never underflows.
        let start = (self.last_applied - self.snapshot_index) as usize;
        let end = (self.commit_index - self.snapshot_index) as usize;
        let newly_committed: Vec<Entry> = self.log[start..end].to_vec();
        self.last_applied = self.commit_index;
        vec![Effect::ApplyCommitted(newly_committed)]
    }

    // ── Client proposals ─────────────────────────────────────────────────────

    /// Handles a local client proposal (`Event::Propose`).
    ///
    /// Only the leader may accept new entries directly (§4.1: "Followers
    /// redirect writes to leader" — the redirect itself happens at the
    /// server layer, milestone 9; here we just signal rejection with a hint).
    fn on_propose(&mut self, payload: Vec<u8>, _now: Instant) -> Vec<Effect> {
        if self.role != Role::Leader {
            return vec![Effect::ProposeRejected {
                leader_hint: self.current_leader,
            }];
        }

        let index = self.snapshot_index + self.log.len() as LogIndex + 1;
        self.log.push(Entry {
            term: self.current_term,
            index,
            payload,
        });

        let mut effects = vec![
            // PERSISTENCE RULE: the log grew — fsync before the AppendEntries
            // below, which promises followers this entry exists.
            Effect::Persist(self.hard_state()),
        ];

        // Single-node cluster: commits immediately (see become_leader's
        // analogous call for the full explanation).
        effects.extend(self.advance_commit_index());

        effects.extend(self.broadcast_append_entries());
        effects
    }

    // ── Replication helpers ──────────────────────────────────────────────────

    /// Builds a replication message for `peer` based on `next_index[peer]`.
    ///
    /// If `peer` needs an entry we've already compacted away (`next <=
    /// snapshot_index`), sends `InstallSnapshot` instead of an
    /// `AppendEntries` we structurally can't satisfy (the entries it would
    /// need no longer exist in `self.log`). Otherwise, sends the normal
    /// `AppendEntries` with every entry from `next_index[peer]` onward.
    fn append_entries_for(&self, peer: NodeId) -> Effect {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(self.snapshot_index + self.log.len() as LogIndex + 1);

        if next <= self.snapshot_index {
            return Effect::Send(
                peer,
                Message::InstallSnapshot {
                    term: self.current_term,
                    leader_id: self.id,
                    last_included_index: self.snapshot_index,
                    last_included_term: self.snapshot_term,
                    data: self.snapshot_data.clone(),
                },
            );
        }

        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else if prev_log_index == self.snapshot_index {
            self.snapshot_term
        } else {
            self.log_pos(prev_log_index)
                .map(|pos| self.log[pos].term)
                .unwrap_or(0)
        };

        let entries: Vec<Entry> = match self.log_pos(next) {
            Some(pos) => self.log[pos..].to_vec(),
            None => Vec::new(), // next is exactly one past our last entry — heartbeat only
        };

        Effect::Send(
            peer,
            Message::AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        )
    }

    /// Sends an `AppendEntries` (heartbeat + any pending entries) to every peer.
    fn broadcast_append_entries(&self) -> Vec<Effect> {
        self.peers
            .iter()
            .map(|&p| self.append_entries_for(p))
            .collect()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::Role;
    use super::*;
    use crate::types::{Effect, Event, Message};

    fn t0() -> Instant {
        Instant::now()
    }

    /// Advances past the maximum possible election timeout (300ms) so a
    /// `Tick` is guaranteed to trigger an election regardless of jitter.
    fn past_election_timeout(t: Instant) -> Instant {
        t + ELECTION_TIMEOUT_BASE + ELECTION_TIMEOUT_JITTER_MAX + Duration::from_millis(1)
    }

    #[test]
    fn new_node_starts_as_follower_term_zero() {
        let node = RaftNode::new(0, vec![1, 2], 42, t0());
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 0);
        assert!(node.log().is_empty());
        assert_eq!(node.commit_index(), 0);
    }

    #[test]
    fn single_node_cluster_becomes_leader_and_commits_immediately() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![], 1, now);

        let now2 = past_election_timeout(now);
        let effects = node.step(Event::Tick, now2);

        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.current_term(), 1);
        assert_eq!(node.log().len(), 1, "no-op entry appended");
        assert_eq!(node.commit_index(), 1, "single node commits immediately");

        // ApplyCommitted must be present since commit_index advanced.
        let applied = effects
            .iter()
            .any(|e| matches!(e, Effect::ApplyCommitted(entries) if entries.len() == 1));
        assert!(applied, "expected ApplyCommitted for the no-op entry");

        // Persist must appear before any Send (there are none here, but check
        // the persist effects are present at all).
        let persist_count = effects
            .iter()
            .filter(|e| matches!(e, Effect::Persist(_)))
            .count();
        assert!(persist_count >= 1);
    }

    #[test]
    fn follower_does_not_start_election_before_timeout() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // Tick at now + 1ms — well before the 150ms minimum.
        let effects = node.step(Event::Tick, now + Duration::from_millis(1));
        assert!(effects.is_empty());
        assert_eq!(node.role(), Role::Follower);
    }

    #[test]
    fn propose_rejected_when_not_leader() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        let effects = node.step(Event::Propose(vec![1, 2, 3]), now);
        assert_eq!(effects, vec![Effect::ProposeRejected { leader_hint: None }]);
        assert!(node.log().is_empty());
    }

    #[test]
    fn higher_term_message_causes_step_down_and_persist() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // Force into candidate role with term 1.
        node.step(Event::Tick, past_election_timeout(now));
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.current_term(), 1);

        // A peer with a much higher term sends AppendEntries.
        let effects = node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntries {
                    term: 5,
                    leader_id: 1,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                },
            },
            now,
        );

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.current_term(), 5);
        assert_eq!(node.current_leader(), Some(1));

        // Must have persisted the new term.
        assert!(effects
            .iter()
            .any(|e| matches!(e, Effect::Persist(hs) if hs.current_term == 5)));
    }

    #[test]
    fn vote_rejected_for_stale_term() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);
        // Bump node's term to 5 via a higher-term AppendEntries first.
        node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntries {
                    term: 5,
                    leader_id: 1,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: 0,
                },
            },
            now,
        );
        assert_eq!(node.current_term(), 5);

        // A candidate with a stale term (3) requests a vote.
        let effects = node.step(
            Event::Message {
                from: 2,
                msg: Message::RequestVote {
                    term: 3,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            },
            now,
        );

        assert_eq!(
            effects,
            vec![Effect::Send(
                2,
                Message::RequestVoteResponse {
                    term: 5,
                    vote_granted: false,
                    voter_id: 0,
                }
            )]
        );
        // Term must not have decreased.
        assert_eq!(node.current_term(), 5);
    }

    #[test]
    fn vote_rejected_if_already_voted_for_different_candidate() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // Vote for candidate 1 in term 1.
        let e1 = node.step(
            Event::Message {
                from: 1,
                msg: Message::RequestVote {
                    term: 1,
                    candidate_id: 1,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            },
            now,
        );
        assert!(e1.iter().any(|e| matches!(
            e,
            Effect::Send(
                _,
                Message::RequestVoteResponse {
                    vote_granted: true,
                    ..
                }
            )
        )));
        assert_eq!(node.voted_for(), Some(1));

        // Candidate 2 requests a vote in the SAME term — must be rejected.
        let e2 = node.step(
            Event::Message {
                from: 2,
                msg: Message::RequestVote {
                    term: 1,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            },
            now,
        );
        assert_eq!(
            e2,
            vec![Effect::Send(
                2,
                Message::RequestVoteResponse {
                    term: 1,
                    vote_granted: false,
                    voter_id: 0,
                }
            )]
        );
    }

    #[test]
    fn vote_rejected_if_candidate_log_less_up_to_date() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // Bump term to 3 and give the node a log entry at (index=1, term=3)
        // via AppendEntries from a leader.
        node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntries {
                    term: 3,
                    leader_id: 1,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![Entry {
                        term: 3,
                        index: 1,
                        payload: vec![],
                    }],
                    leader_commit: 0,
                },
            },
            now,
        );
        assert_eq!(node.log().len(), 1);
        assert_eq!(node.current_term(), 3);

        // A candidate in the same term but with an EMPTY log (less
        // up-to-date) requests a vote. Must be rejected even though we
        // haven't voted yet this term.
        let effects = node.step(
            Event::Message {
                from: 2,
                msg: Message::RequestVote {
                    term: 3,
                    candidate_id: 2,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            },
            now,
        );

        assert_eq!(
            effects,
            vec![Effect::Send(
                2,
                Message::RequestVoteResponse {
                    term: 3,
                    vote_granted: false,
                    voter_id: 0,
                }
            )]
        );
        assert_eq!(
            node.voted_for(),
            None,
            "vote was not granted, so voted_for stays None"
        );
    }

    #[test]
    fn follower_rejects_append_entries_on_log_mismatch() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // Leader claims prev_log_index=5 (we have an empty log) — mismatch.
        let effects = node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntries {
                    term: 1,
                    leader_id: 1,
                    prev_log_index: 5,
                    prev_log_term: 1,
                    entries: vec![],
                    leader_commit: 0,
                },
            },
            now,
        );

        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Send(_, Message::AppendEntriesResponse { success: false, .. })
        )));
        assert!(node.log().is_empty());
    }

    #[test]
    fn follower_truncates_conflicting_entries() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);

        // First, install a log with entries at index 1 (term 1) and index 2 (term 1).
        node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntries {
                    term: 1,
                    leader_id: 1,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![
                        Entry {
                            term: 1,
                            index: 1,
                            payload: vec![0xAA],
                        },
                        Entry {
                            term: 1,
                            index: 2,
                            payload: vec![0xBB],
                        },
                    ],
                    leader_commit: 0,
                },
            },
            now,
        );
        assert_eq!(node.log().len(), 2);

        // A NEW leader (term 2) overwrites index 2 with a different entry.
        node.step(
            Event::Message {
                from: 2,
                msg: Message::AppendEntries {
                    term: 2,
                    leader_id: 2,
                    prev_log_index: 1,
                    prev_log_term: 1,
                    entries: vec![Entry {
                        term: 2,
                        index: 2,
                        payload: vec![0xCC],
                    }],
                    leader_commit: 0,
                },
            },
            now,
        );

        assert_eq!(node.log().len(), 2);
        assert_eq!(node.log()[1].term, 2);
        assert_eq!(node.log()[1].payload, vec![0xCC]);
        // The old index-1 entry survives (it matched prev_log_index/term).
        assert_eq!(node.log()[0].payload, vec![0xAA]);
    }

    #[test]
    fn append_entries_response_failure_decrements_next_index_and_retries() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);
        // Become leader (single-node-style election won't work with peers;
        // drive it manually by forcing candidate -> leader via votes).
        node.step(Event::Tick, past_election_timeout(now));
        assert_eq!(node.role(), Role::Candidate);
        assert_eq!(node.current_term(), 1);

        // Grant votes from both peers -> becomes leader.
        node.step(
            Event::Message {
                from: 1,
                msg: Message::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                    voter_id: 1,
                },
            },
            now,
        );
        assert_eq!(node.role(), Role::Leader);
        // Leader's log has 1 entry (the no-op).
        assert_eq!(node.log().len(), 1);

        // Follower 1 reports failure (it's missing entries).
        let effects = node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntriesResponse {
                    term: 1,
                    success: false,
                    follower_id: 1,
                    match_index: 0,
                },
            },
            now,
        );

        // Must retry with a new AppendEntries containing prev_log_index=0
        // (next_index decremented from 1 to... max(1-1,1)=1, so prev=0).
        assert_eq!(effects.len(), 1);
        match &effects[0] {
            Effect::Send(
                1,
                Message::AppendEntries {
                    prev_log_index,
                    entries,
                    ..
                },
            ) => {
                assert_eq!(*prev_log_index, 0);
                assert_eq!(entries.len(), 1); // resends the no-op
            }
            other => panic!("expected AppendEntries retry, got {other:?}"),
        }
    }

    #[test]
    fn stale_request_vote_response_ignored() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);
        node.step(Event::Tick, past_election_timeout(now));
        assert_eq!(node.current_term(), 1);

        // A response for a DIFFERENT (older) term must be ignored.
        let effects = node.step(
            Event::Message {
                from: 1,
                msg: Message::RequestVoteResponse {
                    term: 0,
                    vote_granted: true,
                    voter_id: 1,
                },
            },
            now,
        );
        assert!(effects.is_empty());
        assert_eq!(
            node.role(),
            Role::Candidate,
            "should not have become leader from a stale vote"
        );
    }

    #[test]
    fn heartbeat_sent_on_tick_when_leader() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);
        node.step(Event::Tick, past_election_timeout(now));
        node.step(
            Event::Message {
                from: 1,
                msg: Message::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                    voter_id: 1,
                },
            },
            now,
        );
        assert_eq!(node.role(), Role::Leader);

        // Advance time past the heartbeat interval (50ms).
        let later = now + HEARTBEAT_INTERVAL + Duration::from_millis(1);
        let effects = node.step(Event::Tick, later);

        let heartbeats = effects
            .iter()
            .filter(|e| matches!(e, Effect::Send(_, Message::AppendEntries { .. })))
            .count();
        assert_eq!(heartbeats, 2, "one heartbeat per peer");
    }

    // ── Snapshot / compaction tests ─────────────────────────────────────────

    /// drives a single-node "cluster" to leader and commits `n` proposed
    /// entries, returning the node. single-node clusters commit immediately
    /// (see `become_leader`'s doc comment), which keeps these tests focused
    /// purely on compaction mechanics rather than replication timing.
    fn leader_with_committed_entries(n: u64) -> RaftNode {
        let now = t0();
        let mut node = RaftNode::new(0, vec![], 1, now);
        node.step(Event::Tick, past_election_timeout(now));
        assert_eq!(node.role(), Role::Leader);
        assert_eq!(node.log().len(), 1); // the no-op, already committed

        for i in 0..n {
            node.step(Event::Propose(format!("entry-{i}").into_bytes()), now);
        }
        assert_eq!(node.commit_index(), n + 1); // no-op + n proposals
        node
    }

    #[test]
    fn compact_log_removes_entries_and_sets_snapshot_boundary() {
        let mut node = leader_with_committed_entries(5);
        assert_eq!(node.log().len(), 6); // no-op + 5 entries
        assert_eq!(node.snapshot_index(), 0);

        let ok = node.compact_log(3, b"snapshot-bytes".to_vec());
        assert!(ok);
        assert_eq!(node.snapshot_index(), 3);
        assert_eq!(node.log().len(), 3); // entries 4, 5, 6 remain
        assert_eq!(node.log()[0].index, 4);
        assert_eq!(node.snapshot_data(), b"snapshot-bytes");
    }

    #[test]
    fn compact_log_rejects_uncommitted_index() {
        let mut node = RaftNode::new(0, vec![1, 2], 1, t0());
        // no election has happened — nothing is committed.
        let ok = node.compact_log(1, b"x".to_vec());
        assert!(!ok);
        assert_eq!(node.snapshot_index(), 0);
    }

    #[test]
    fn compact_log_rejects_index_already_compacted() {
        let mut node = leader_with_committed_entries(5);
        assert!(node.compact_log(3, b"first".to_vec()));
        // trying to compact at or before the existing boundary is a no-op.
        assert!(!node.compact_log(2, b"stale".to_vec()));
        assert!(!node.compact_log(3, b"same".to_vec()));
        assert_eq!(node.snapshot_index(), 3);
        assert_eq!(node.snapshot_data(), b"first"); // unchanged
    }

    #[test]
    fn last_log_info_falls_back_to_snapshot_when_log_empty() {
        let mut node = leader_with_committed_entries(2);
        // compact away everything, including the last entry.
        let last_index = node.log().last().unwrap().index;
        assert!(node.compact_log(last_index, b"snap".to_vec()));
        assert!(node.log().is_empty());

        // a subsequent proposal must still index from snapshot_index + 1,
        // not from 1 (which would collide with already-compacted indices).
        let effects = node.step(Event::Propose(b"after-snapshot".to_vec()), t0());
        let sent_index = effects.iter().find_map(|e| match e {
            Effect::Persist(hs) => hs.log.last().map(|entry| entry.index),
            _ => None,
        });
        assert_eq!(sent_index, Some(last_index + 1));
    }

    #[test]
    fn append_entries_for_lagging_peer_sends_install_snapshot() {
        let now = t0();
        let mut node = RaftNode::new(0, vec![1, 2], 1, now);
        node.step(Event::Tick, past_election_timeout(now));
        node.step(
            Event::Message {
                from: 1,
                msg: Message::RequestVoteResponse {
                    term: 1,
                    vote_granted: true,
                    voter_id: 1,
                },
            },
            now,
        );
        assert_eq!(node.role(), Role::Leader);

        // Propose a few entries and manually advance commit_index by
        // simulating both peers acking, so compaction is legal.
        for i in 0..5 {
            node.step(Event::Propose(format!("e{i}").into_bytes()), now);
        }
        node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntriesResponse {
                    term: 1,
                    success: true,
                    follower_id: 1,
                    match_index: 6,
                },
            },
            now,
        );
        node.step(
            Event::Message {
                from: 2,
                msg: Message::AppendEntriesResponse {
                    term: 1,
                    success: true,
                    follower_id: 2,
                    match_index: 6,
                },
            },
            now,
        );
        assert_eq!(node.commit_index(), 6);

        // Compact everything up to index 6.
        assert!(node.compact_log(6, b"full-snapshot".to_vec()));
        assert_eq!(node.log().len(), 0);

        // Peer 1 is now WAY behind (its next_index, from the initial
        // become_leader() optimism, is 7 — actually past the snapshot, so
        // force it back down to simulate a peer that fell behind before the
        // snapshot was taken).
        let effects = node.step(
            Event::Message {
                from: 1,
                msg: Message::AppendEntriesResponse {
                    term: 1,
                    success: false,
                    follower_id: 1,
                    match_index: 0,
                },
            },
            now,
        );
        // repeatedly fail until next_index drops to/below snapshot_index (6).
        let mut last_effects = effects;
        for _ in 0..10 {
            if last_effects
                .iter()
                .any(|e| matches!(e, Effect::Send(_, Message::InstallSnapshot { .. })))
            {
                break;
            }
            last_effects = node.step(
                Event::Message {
                    from: 1,
                    msg: Message::AppendEntriesResponse {
                        term: 1,
                        success: false,
                        follower_id: 1,
                        match_index: 0,
                    },
                },
                now,
            );
        }

        assert!(
            last_effects.iter().any(|e| matches!(e, Effect::Send(1, Message::InstallSnapshot { last_included_index: 6, .. }))),
            "expected an InstallSnapshot once peer 1's next_index fell to/below snapshot_index=6, got {last_effects:?}"
        );
    }

    #[test]
    fn on_install_snapshot_replaces_log_and_emits_apply_snapshot() {
        let now = t0();
        let mut follower = RaftNode::new(1, vec![0, 2], 5, now);

        let effects = follower.step(
            Event::Message {
                from: 0,
                msg: Message::InstallSnapshot {
                    term: 1,
                    leader_id: 0,
                    last_included_index: 10,
                    last_included_term: 1,
                    data: b"the-snapshot".to_vec(),
                },
            },
            now,
        );

        assert_eq!(follower.snapshot_index(), 10);
        assert_eq!(follower.snapshot_term(), 1);
        assert_eq!(follower.commit_index(), 10);
        assert!(follower.log().is_empty());
        assert_eq!(follower.snapshot_data(), b"the-snapshot");

        let applied = effects.iter().find_map(|e| match e {
            Effect::ApplySnapshot {
                last_included_index,
                data,
            } => Some((*last_included_index, data.clone())),
            _ => None,
        });
        assert_eq!(applied, Some((10, b"the-snapshot".to_vec())));

        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::Send(0, Message::InstallSnapshotResponse { follower_id: 1, .. })
        )));
    }

    #[test]
    fn on_install_snapshot_ignores_older_snapshot() {
        let now = t0();
        let mut follower = RaftNode::new(1, vec![0], 5, now);

        follower.step(
            Event::Message {
                from: 0,
                msg: Message::InstallSnapshot {
                    term: 1,
                    leader_id: 0,
                    last_included_index: 10,
                    last_included_term: 1,
                    data: b"v2".to_vec(),
                },
            },
            now,
        );
        assert_eq!(follower.snapshot_index(), 10);

        // An older/duplicate snapshot must not regress state.
        follower.step(
            Event::Message {
                from: 0,
                msg: Message::InstallSnapshot {
                    term: 1,
                    leader_id: 0,
                    last_included_index: 5,
                    last_included_term: 1,
                    data: b"stale".to_vec(),
                },
            },
            now,
        );
        assert_eq!(
            follower.snapshot_index(),
            10,
            "must not regress to an older snapshot"
        );
        assert_eq!(follower.snapshot_data(), b"v2");
    }

    #[test]
    fn restore_resumes_from_persisted_snapshot_boundary() {
        let mut node = leader_with_committed_entries(4);
        assert!(node.compact_log(3, b"snap".to_vec()));
        let hs = node.hard_state();
        assert_eq!(hs.snapshot_index, 3);

        let restored = RaftNode::restore(0, vec![1, 2], 9, t0(), hs, b"snap".to_vec());
        assert_eq!(restored.snapshot_index(), 3);
        assert_eq!(
            restored.commit_index(),
            3,
            "commit_index resumes at the snapshot boundary"
        );
        assert_eq!(restored.snapshot_data(), b"snap");
        // log entries above the snapshot boundary are preserved verbatim.
        assert_eq!(restored.log().len(), node.log().len());
    }
}
