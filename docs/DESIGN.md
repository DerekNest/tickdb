# tickdb — Design Notes

Incrementally updated. Each milestone appends its section.

---

## Architecture overview

```
                  ┌─────────────────────────────────────────────┐
                  │               crates/server                  │
                  │  axum HTTP/1.1  │  Raft transport (tokio TCP)│
                  └────────┬────────┴──────────────┬────────────┘
                           │                       │
              ┌────────────▼──────────┐   ┌────────▼───────────┐
              │    crates/query       │   │    crates/raft      │
              │  ColumnBatch pipeline │   │  I/O-free state     │
              │  OHLCV / VWAP / asof  │   │  machine + sim      │
              └────────────┬──────────┘   └────────────────────┘
                           │
              ┌────────────▼──────────────────────────────────┐
              │              crates/storage                    │
              │  Engine  WAL  Memtable  Manifest  MmapSegment  │
              └────────────┬──────────────────────────────────┘
                           │
              ┌────────────▼──────────────────────────────────┐
              │              crates/format                     │
              │  varint · dod · dz · rle · dict · segment      │
              │  (pure, no I/O policy, no unsafe)              │
              └───────────────────────────────────────────────┘
```

**Dependency rule:** format ← storage ← query ← server. `raft` and `sim` have zero deps on storage/query — they only exchange `Vec<u8>` log entry payloads.

---

## Phase 1 — Storage engine

### Encoding choices

**Why delta-of-delta for timestamps?**

For regularly-arriving ticks (e.g., Binance aggTrades at ~1 kHz), timestamps form a near-arithmetic sequence. If ticks arrive every 1 ms:

```
delta[i]     = ts[i] - ts[i-1]      ≈ 1_000_000  (ns)
dod[i]       = delta[i] - delta[i-1] ≈ 0
```

Most DoD values cluster near zero, so zigzag + LEB128 encodes them in 1 byte vs. 8 bytes raw. Measured on BTCUSDT aggTrades: ~1.1 bytes/timestamp vs. 8 bytes raw.

We use varint-DoD rather than Gorilla-style bit-packing (Facebook's gorilla paper, VLDB 2015). Gorilla needs a bit-level writer and bucket-based lookup tables; varint-DoD is ~80 lines of code and within ~15% in practice. The simplicity is worth the throughput difference at this scale. **This tradeoff is deliberate and documented** — if Phase 4 benchmarks show timestamp encoding is a bottleneck, Gorilla is the upgrade path.

**Why delta + zigzag for prices?**

Trade prices within a symbol are highly autocorrelated — consecutive prices differ by 1–10 ticks. A tick on BTCUSDT at $50,000 with 2 decimal places has mantissa ≈ 5,000,000; a tick move is ±1 in mantissa. Zigzag of ±1 = 1–3 LEB128 byte vs. 8 bytes raw.

**Why plain varint for sizes?**

Trade sizes are erratic: [100, 10000, 1, 500000, 25]. Deltas of this sequence are larger-magnitude than the raw values, so delta encoding actively hurts. Plain LEB128 exploits the size skew (most trades are small) without making large trades worse.

**Why RLE for side?**

Aggressor side exhibits strong autocorrelation in real order flow — momentum bursts of 50+ consecutive buy-aggressive trades are common. RLE reduces a 50-buy run to 2 bytes.

### WAL durability model

Three fsync policies (§2.3):
- `EveryWrite`: fsync inside `ingest()` before returning. Acked writes survive SIGKILL + power loss. Throughput limited by disk latency (~100–500 µs per fsync on NVMe).
- `EveryNMs(50)`: default. Groups fsyncs into 50 ms windows. Up to 50 ms of unacked writes at risk; throughput ~20× higher.
- `Never`: test use only.

Flush triggers (§2.3): memtable ≥ 256 MiB **or** WAL ≥ 1 GiB — whichever comes first.

### Segment immutability and mmap

Segments are write-once: once atomically renamed into place, they are never modified. This makes `mmap` safe — the kernel can serve reads directly from page cache without copy. The unsafe invariant: `MmapSegment` holds both the `Mmap` and the `File` alive so the mapping remains valid even after compaction unlinks the path (`mmap.rs` contains the full safety argument).

### Crash-recovery correctness argument

On restart, the engine:
1. Loads MANIFEST — lists all segments that completed their atomic rename.
2. Replays the WAL — recovers all batches for which `ingest()` returned `Ok(())` under `EveryWrite`.
3. Inserts WAL batches into a fresh memtable — `dedup_by_key(trade_id)` handles the case where a batch appears in both the WAL and an already-flushed segment.

The only case where data loss is correct behaviour: batches where `ingest()` had not yet returned (in-flight writes at kill time). These were never acked to the caller.

### Compaction (Phase 1.5 — milestone 4)

Size-tiered: when a date partition exceeds 8 segments, a background task performs a k-way merge (by symbol, ts, trade_id) into a single generation-N+1 segment. Old segments are removed from the manifest, then physically deleted. Readers holding `Arc<MmapSegment>` continue reading safely — the OS keeps the inode alive until the last Arc drops.

---

## Phase 2 — Query engine (milestones 5–6)

Pull-based `ColumnBatch` pipeline (`crates/query`), operating directly on
the same batches `MmapSegment::scan` yields — no intermediate row
materialization between storage and query layers.

**OHLCV** (`agg.rs::ohlcv`): single pass over batches, accumulating into a
`BTreeMap<i64, Bar>` keyed by bucket start timestamp. This was originally a
densely pre-allocated `Vec<Option<Bar>>` indexed by bucket number — correct
and fast for a sane query (a day of 1-minute bars = 1,440 slots), but
capable of a catastrophic allocation for a broad, sparse range with a small
interval (`.range(i64::MIN, i64::MAX).bars(Interval::Sec1)` tried to
allocate ~22GB). Found via the benchmark suite (§5, `docs/BENCHMARKS.md`)
and fixed by switching to the sparse map: memory now scales with the
number of buckets the *data* touches, not the query's nominal span. A
related `i64` overflow in the bucket-timestamp arithmetic (`ts -
range_start` overflowing when `range_start = i64::MIN`) was caught by the
same test run and fixed with widened `i128` intermediates. Both are now
regression-guarded by dedicated differential tests.

**As-of join** (`asof.rs::asof_join`): two-pointer merge over two
ts-sorted streams, O(|left| + |right|). The pointer invariant ("`right[rp]`
is the most recent right row with `ts ≤ left[lp].ts`") is proven in the
function's doc comment and verified against an O(n×m) naive reference in
differential tests across 50 seeds.

**Top-N by volume** (`agg.rs::top_n_by_volume`): fixed-size min-heap of
size N, so memory is O(N) regardless of range size. `TopNEntry`'s `Ord`
impl is intentionally "backwards" (smaller volume = "greater") so a plain
`BinaryHeap<TopNEntry>` — not `BinaryHeap<Reverse<TopNEntry>>` — pops the
smallest-volume entry for eviction; the differential tests caught a
double-reversal bug here early (§ tool-use notes) where the initial
`Reverse`-wrapped version evicted the *largest* volume instead.

Every kernel above is checked against a naive, obviously-correct reference
implementation over 50 seeds via `crates/query/tests/differential.rs` —
this is the primary correctness argument for the whole query layer, not
just spot-checked examples.

---

## Phase 3 — Raft (milestones 7–9)

Core design principle: `RaftNode::step(&mut self, event, now) -> Vec<Effect>`
(`crates/raft`) is pure — no I/O, no real clock reads, no real randomness
internally except a seeded RNG for election jitter. All nondeterminism is
injected: time via `now: Instant`, network via `Event::Message`, storage
via `Effect::Persist`/`Effect::ApplyCommitted`. This is what makes
`crates/sim`'s deterministic simulation possible: a single `u64` seed
determines an entire run (node election jitter, network fault timing,
fault-injection scheduling), so any invariant violation is reproducible
exactly by re-running that seed.

**Milestone 7 — core state machine**: leader election (150–300ms
randomized timeout, 50ms heartbeat), log replication with the standard
conflict-truncation rule, and the no-op-entry-on-leadership trick (a new
leader appends an empty entry in its own term so prior-term entries become
committable without waiting for a client write — required for the
"only commit current-term entries via the majority rule" safety property).

**Milestone 8 — deterministic simulation** (`crates/sim`): a virtual
network (drop/delay/duplicate/partition) and virtual cluster driver, both
seeded from one top-level `u64`. Four invariants checked every tick:
election safety, log matching, state machine safety, and bounded liveness
(a leader must commit a new entry within 10 simulated seconds of faults
settling — but only when there's genuinely an outstanding proposal; an idle
cluster with nothing proposed is never a "liveness violation", a
distinction the harness got wrong on the first pass and was corrected
after a failing test surfaced it). **1,000 seeds, full fault injection,
zero violations** — the primary correctness evidence for the consensus
layer under adversarial conditions, not just the happy path.

State-machine-safety checking required a real fix along the way: `RaftNode`
correctly treats `last_applied` as volatile (never persisted), so a
restarted node legitimately *re-emits* `ApplyCommitted` for entries it
already applied before crashing — expected at-least-once delivery, since
the real storage engine's ingest path is idempotent via trade_id dedup.
The simulation's first-draft safety check compared applied sequences by
raw position and flagged this redelivery as a false divergence; it was
rewritten to compare by each entry's log *index* instead, which correctly
tolerates redelivery while still catching a genuine same-index
disagreement.

**Milestone 9 — log compaction, snapshots, and real transport**:
`RaftNode::compact_log` truncates the log up to a committed index,
recording a `(snapshot_index, snapshot_term)` boundary; a leader whose
peer's `next_index` falls at or below that boundary sends
`Message::InstallSnapshot` instead of an impossible `AppendEntries`. The
snapshot payload — a bundle of the storage engine's manifest and every
referenced segment file (`crates/server/src/snapshot_bundle.rs`) — is
opaque to Raft itself; the real TCP transport
(`crates/server/src/raft_transport.rs`) splits it into ≤1MiB chunks for the
wire and reassembles them before handing a complete `InstallSnapshot` back
up to the state machine, which never knows chunking happened.

The HTTP layer (`crates/server/src/raft_runtime.rs`) wires `POST /ingest`
to `RaftHandle::propose` when running in multi-node mode: a follower
returns `409 Conflict` with a `leader_hint`, a leader's accepted proposal
resolves once it commits. Verified against a genuine 3-process cluster
(not just the simulator) over real TCP sockets on localhost: election
completes, a `POST /ingest` on the leader replicates and lands in every
node's independent storage engine, and a follower correctly redirects.

---

## Phase 4 — Benchmarks (milestone 10)

See `docs/BENCHMARKS.md` for the full methodology and results. Summary:
tickdb is ~20% more compact than Zstd-compressed Parquet, and that is its
only win. DuckDB is faster on every benchmarked query shape: roughly
1.5–1.6x on OHLCV bar aggregation (the kernel tickdb is hand-tuned for),
18–22x on VWAP and 9–17x on Top-N (simple full-table aggregates, exactly
where a mature vectorized execution engine is expected to dominate a
hand-written scalar kernel). An earlier version of this summary said
tickdb was ~3x faster on OHLCV; that result could not be reproduced and
is retracted in `docs/BENCHMARKS.md`. Numbers are on 2M-row synthetic data — real
Binance data (`bench/download.sh`) is the remaining step for final numbers,
not reachable from the sandboxed environment that built this project.
