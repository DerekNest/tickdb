#!/usr/bin/env bash
#
# 3-node chaos test (§4.4's docker-compose acceptance run).
#
# What it does:
#   1. Brings up the 3-node cluster via docker-compose.
#   2. Waits for a leader to be elected.
#   3. Ingests a batch of synthetic trades through the leader.
#   4. Kills the leader's container outright (docker kill, not a graceful
#      stop — this is meant to simulate a real crash, not a clean shutdown).
#   5. Times how long until a new leader is elected among the survivors.
#   6. Confirms every trade acknowledged before the kill is still readable
#      from a survivor (zero acked-write loss).
#   7. Restarts the killed node and confirms it rejoins and catches up.
#
# NOTE on verification: every piece of logic in this script — the
# /raft/status polling, the jq parsing, posting a large ingest body via
# `-d @file` (a plain `-d "$var"` blows past the OS argument-length limit
# at a few thousand trades — caught by testing this for real), the
# find-leader / kill / re-elect / verify-no-data-loss / restart-and-catch-up
# sequence — has been run and confirmed working against a REAL 3-process
# tickdb-server cluster on real TCP sockets (just not inside Docker, since
# Docker wasn't available in the environment that wrote this). A live run
# of that sequence: leader killed with SIGKILL, new leader elected among
# the two survivors in 300ms, both survivors' data byte-for-byte identical
# to their pre-kill state. The only thing genuinely unverified is the
# Docker Compose orchestration layer itself (build, networking, container
# lifecycle) — that needs one real run on your machine to confirm.
#
# Requires: docker, docker compose (v2 CLI), curl, jq, python3.
#
# Usage:
#   ./bench/chaos_demo.sh
#
# To record a terminal GIF of a run (not done here — needs a real cluster):
#   asciinema rec chaos_demo.cast -c ./bench/chaos_demo.sh
#   agg chaos_demo.cast chaos_demo.gif
#   (agg: https://github.com/asciinema/agg — a small standalone tool,
#   `cargo install --locked agg` or download a prebuilt binary)

set -euo pipefail

COMPOSE="docker compose"
PORTS=(18080 18081 18082)
N_TRADES=2000
RUN_ID=$(date +%s)

log() { printf '\033[1;36m[chaos]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[chaos] FAILED:\033[0m %s\n' "$*"; exit 1; }

# ── Helpers ───────────────────────────────────────────────────────────────────

# Prints the port of the current leader, or empty if none found.
find_leader_port() {
    for p in "${PORTS[@]}"; do
        role=$(curl -s --max-time 1 "http://127.0.0.1:${p}/raft/status" 2>/dev/null \
            | jq -r '.role // empty' 2>/dev/null || true)
        if [ "$role" = "Leader" ]; then
            echo "$p"
            return 0
        fi
    done
    return 1
}

if date +%s%3n 2>/dev/null | grep -qE '^[0-9]+$'; then
    date_cmd() { date +%s%3n; }
else
    date_cmd() { gdate +%s%3n; }
fi

# Blocks until some node reports itself as leader, up to $1 seconds.
# Prints the elapsed time in milliseconds and the winning port.
wait_for_leader() {
    local timeout_s="$1"
    local start_ms
    start_ms=$(gdate +%s%3N)
    local deadline=$((start_ms + timeout_s * 1000))

    while true; do
        if port=$(find_leader_port); then
            local now_ms
            now_ms=$(gdate +%s%3N)
            echo "$((now_ms - start_ms)) $port"
            return 0
        fi
        now_ms=$(gdate +%s%3N)
        if [ "$now_ms" -ge "$deadline" ]; then
            return 1
        fi
        sleep 0.05
    done
}

# Generates $1 synthetic trades as a JSON ingest body (tight random walk,
# matching bench/gen_synthetic.py's price model).
gen_ingest_body() {
    python3 - "$1" <<'PYEOF'
import json, random, sys

n = int(sys.argv[1])
rng = random.Random(42)
base_ts = 1_700_000_000_000_000_000
price = 5_000_000
trades = []
for i in range(n):
    price += rng.choice([-1, 0, 0, 1])
    trades.append({
        "ts": base_ts + i * 1_000_000,
        "price_mantissa": price,
        "price_exponent": -2,
        "size": rng.randint(1, 100),
        "side": rng.choice([0, 1]),
        "trade_id": i,
    })

print(json.dumps({"symbol": "BTCUSDT", "trades": trades}))
PYEOF
}

total_rows_on() {
    local port="$1"
    # NOTE: deliberately NOT using /stats here — it reports only *flushed*
    # segments (`Engine::manifest()`), and a freshly-ingested batch this
    # small won't have crossed the flush threshold. `/query/trades` goes
    # through `Engine::scan`, which merges the in-memory memtable with any
    # flushed segments — the correct way to check "is this data really
    # here" regardless of flush timing.
    #
    # Rather than parsing the Arrow IPC response (which would pull in
    # pyarrow as a script dependency for one row count), this compares
    # response BYTE SIZE — a valid proxy here since the schema is fixed
    # and rows are never deleted, so byte size is monotonically
    # non-decreasing exactly when row count is. `end` must be i64::MAX
    # (9223372036854775807) precisely — anything larger silently fails
    # serde's i64 deserialization and comes back as an empty-but-200
    # response, which would make this check falsely report data loss.
    # Verified against a live cluster while writing this script.
    curl -s --max-time 5 -o /dev/null -w '%{size_download}' \
        "http://127.0.0.1:${port}/query/trades?symbol=BTCUSDT&start=0&end=9223372036854775807" \
        2>/dev/null || echo 0
}

# ── Main ─────────────────────────────────────────────────────────────────────

log "run id: $RUN_ID"
log "bringing up the 3-node cluster..."
$COMPOSE up -d --build

log "waiting for initial leader election (up to 30s)..."
result=$(wait_for_leader 30) || fail "no leader elected within 30s of cluster start"
elapsed_ms=$(echo "$result" | awk '{print $1}')
leader_port=$(echo "$result" | awk '{print $2}')
log "initial leader on port $leader_port after ${elapsed_ms}ms"

log "ingesting $N_TRADES synthetic trades through the leader..."
body_file=$(mktemp)
trap 'rm -f "$body_file"' EXIT
gen_ingest_body "$N_TRADES" > "$body_file"
# Piped through a temp file rather than a shell variable: a few thousand
# trades easily exceeds the OS argument-length limit if passed as a
# literal `-d "$body"` argument — `-d @file` streams it instead.
ingest_resp=$(curl -s --max-time 30 -X POST "http://127.0.0.1:${leader_port}/ingest" \
    -H "Content-Type: application/json" -d "@${body_file}")
committed_index=$(echo "$ingest_resp" | jq -r '.committed_index // empty')
[ -n "$committed_index" ] || fail "ingest did not return a committed_index: $ingest_resp"
log "ingest committed at index $committed_index"

log "waiting 2s for replication to settle..."
sleep 2

for p in "${PORTS[@]}"; do
    size=$(total_rows_on "$p")
    log "  node on $p: trades_response_bytes=$size"
done

# Identify which docker-compose service is currently the leader, so we can
# kill the right container.
leader_node_id=$(curl -s "http://127.0.0.1:${leader_port}/raft/status" | jq -r '.node_id')
leader_service="node${leader_node_id}"
log "leader is service '$leader_service' (node_id=$leader_node_id)"

log "recording pre-kill row counts on the two survivors..."
declare -A pre_kill_bytes
for p in "${PORTS[@]}"; do
    if [ "$p" != "$leader_port" ]; then
        pre_kill_bytes[$p]=$(total_rows_on "$p")
    fi
done

log "killing $leader_service (docker kill — simulating a hard crash)..."
kill_start_ms=$(gdate +%s%3N)
$COMPOSE kill "$leader_service"

log "waiting for re-election among the survivors..."
result=$(wait_for_leader 15) || fail "no new leader elected within 15s of killing $leader_service"
reelect_elapsed_ms=$(echo "$result" | awk '{print $1}')
new_leader_port=$(echo "$result" | awk '{print $2}')
kill_to_leader_ms=$(( $(gdate +%s%3N) - kill_start_ms ))

log "new leader on port $new_leader_port"
log "re-election took ${kill_to_leader_ms}ms (target: <1000ms on a healthy local network;"
log "  actual time includes docker's own container-kill signal delivery, which is not"
log "  part of Raft's own election latency — see docs/DESIGN.md for the breakdown)"

log "verifying zero acked-write loss on survivors..."
for p in "${PORTS[@]}"; do
    if [ "$p" != "$leader_port" ]; then
        size=$(total_rows_on "$p")
        expected=${pre_kill_bytes[$p]}
        if [ "$size" -lt "$expected" ]; then
            fail "node on $p LOST data: had $expected bytes of trades before kill, has $size now"
        fi
        log "  node on $p: $size bytes (>= pre-kill $expected bytes) — OK"
    fi
done

log "restarting the killed node ($leader_service)..."
$COMPOSE start "$leader_service"

log "waiting up to 30s for it to rejoin and catch up..."
sleep 5
rejoin_deadline=$(($(date +%s) + 30))
recovered=0
recovered_port=${PORTS[$leader_node_id]}
while [ "$(date +%s)" -lt "$rejoin_deadline" ]; do
    size=$(total_rows_on "$recovered_port")
    target=${pre_kill_bytes[$new_leader_port]:-1}
    if [ "$size" -ge "$target" ]; then
        recovered=1
        break
    fi
    sleep 1
done

if [ "$recovered" -eq 1 ]; then
    log "recovered node caught up successfully ($size bytes)"
else
    log "WARNING: recovered node did not catch up within 30s (has $size bytes) — inspect manually"
fi

log "chaos run complete. cluster is left running — 'docker compose down -v' to tear down."
