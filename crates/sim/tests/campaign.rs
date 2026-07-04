/// the §4.3 1,000-seed chaos campaign.
///
/// runs the cluster under full fault injection for a fixed number of ticks,
/// once per seed, checking all four invariants every tick. any violation
/// fails the test immediately and prints the exact seed and tick, which is
/// all that's needed to reproduce it: `run_one(seed)` replays that single
/// run deterministically.
///
/// kept in its own test file (rather than folded into `sanity.rs`) so it
/// can be run in isolation — `cargo test -p sim --test campaign` — without
/// paying its ~1000x cost every time the faster sanity tests run.
use sim::{ChaosSchedule, Cluster, FaultConfig};

const TICKS_PER_RUN: u64 = 5_000;
const NODES: usize = 5;
const SEED_COUNT: u64 = 1_000;
/// client proposes a new entry every this-many ticks — without a workload,
/// there's nothing for the liveness check to verify (see cluster.rs's doc
/// comment on `pending_proposal_since`).
const PROPOSE_INTERVAL: u64 = 25;

/// runs a single seed to completion, returning `Err` with a message
/// describing exactly what failed and when, or `Ok(())` if all
/// `TICKS_PER_RUN` ticks passed every invariant check.
fn run_one(seed: u64) -> Result<(), String> {
    let mut cluster = Cluster::new(
        NODES,
        seed,
        FaultConfig::default_chaos(),
        ChaosSchedule::default_chaos(),
    );

    for tick in 0..TICKS_PER_RUN {
        cluster.step();

        if let Err(v) = cluster.check_invariants() {
            return Err(format!("seed={seed} tick={tick}: {v}"));
        }
        if let Err(v) = cluster.check_liveness() {
            return Err(format!("seed={seed} tick={tick}: {v}"));
        }

        if tick % PROPOSE_INTERVAL == 0 {
            cluster.propose(format!("seed{seed}-tick{tick}").into_bytes());
        }
        if tick % 500 == 250 {
            let target = (tick / PROPOSE_INTERVAL).max(1);
            cluster.compact_leader_log(
                target,
                format!("seed{seed}-snapshot-tick{tick}").into_bytes(),
            );
        }
    }

    Ok(())
}

#[test]
fn chaos_campaign_1000_seeds() {
    let mut failures: Vec<String> = Vec::new();

    for seed in 0..SEED_COUNT {
        if let Err(msg) = run_one(seed) {
            failures.push(msg);
        }
    }

    if !failures.is_empty() {
        panic!(
            "{} of {} seeds failed:\n{}\n\nreproduce a single failure with:\n  cargo test -p sim --test campaign -- replay_one_seed --ignored\n(set REPLAY_SEED env var or edit the seed constant in that test)",
            failures.len(),
            SEED_COUNT,
            failures.join("\n"),
        );
    }
}

/// replays exactly one seed — the tool for turning a campaign failure
/// message into an isolated, debuggable repro. edit `SEED` (or wire up an
/// env var) to the failing seed printed by `chaos_campaign_1000_seeds`.
///
/// marked `#[ignore]` so it doesn't run as part of the normal suite (it's
/// only useful once a specific seed is known to be interesting); invoke
/// directly with `cargo test -p sim --test campaign replay_one_seed -- --ignored`.
#[test]
#[ignore]
fn replay_one_seed() {
    const SEED: u64 = 0;
    if let Err(msg) = run_one(SEED) {
        panic!("{msg}");
    } else {
        println!("seed {SEED} completed {TICKS_PER_RUN} ticks with no violations");
    }
}

/// smaller, faster variant for local iteration — same logic as the full
/// campaign but far fewer seeds, so it's cheap enough to run on every
/// `cargo test` without slowing down the normal workflow. the full
/// 1,000-seed campaign is the CI-grade check.
#[test]
fn chaos_campaign_smoke_20_seeds() {
    let mut failures: Vec<String> = Vec::new();
    for seed in 10_000..10_020 {
        if let Err(msg) = run_one(seed) {
            failures.push(msg);
        }
    }
    assert!(
        failures.is_empty(),
        "{} of 20 smoke seeds failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// runs a specific seed range — used to split the full 1000-seed campaign
/// into chunks that fit comfortably within a single CI job's time budget.
/// each of these is equivalent to a slice of `chaos_campaign_1000_seeds`;
/// running all four covers the same 0..1000 range.
fn run_seed_range(start: u64, end: u64) -> Vec<String> {
    let mut failures = Vec::new();
    for seed in start..end {
        if let Err(msg) = run_one(seed) {
            failures.push(msg);
        }
    }
    failures
}

#[test]
fn chaos_campaign_chunk_0_250() {
    let f = run_seed_range(0, 250);
    assert!(f.is_empty(), "{} failures:\n{}", f.len(), f.join("\n"));
}

#[test]
fn chaos_campaign_chunk_250_500() {
    let f = run_seed_range(250, 500);
    assert!(f.is_empty(), "{} failures:\n{}", f.len(), f.join("\n"));
}

#[test]
fn chaos_campaign_chunk_500_750() {
    let f = run_seed_range(500, 750);
    assert!(f.is_empty(), "{} failures:\n{}", f.len(), f.join("\n"));
}

#[test]
fn chaos_campaign_chunk_750_1000() {
    let f = run_seed_range(750, 1000);
    assert!(f.is_empty(), "{} failures:\n{}", f.len(), f.join("\n"));
}
