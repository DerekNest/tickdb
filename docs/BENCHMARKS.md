## results (2,000,000 rows, 2 runs on this machine, july 2026)

| metric | tickdb | duckdb / parquet | polars |
|---|---|---|---|
| ingest / csv load | 7.32m rows/sec (1 run only) | 20.05–20.67m rows/sec | — |
| on-disk bytes/trade | 6.50 | 8.13 (parquet+zstd) | — |
| ohlcv (1-min bars) | 0.126–0.132s (15.2–15.8m rows/sec) | **0.081–0.086s** (23.4–24.6m rows/sec) | — |
| vwap (full range) | 0.059–0.063s (31.8–33.8m rows/sec) | **0.0029–0.0032s** (621–697m rows/sec) | 0.0125–0.0469s (42.6–160.5m rows/sec, high variance) |
| top-10 by volume | 0.077–0.078s (25.7–26.0m rows/sec) | **0.0045–0.0082s** (245–441m rows/sec, high variance) | — |

## reading these numbers honestly

**ingest**: duckdb's csv load is roughly 2.7x faster. note this compares duckdb's raw csv parsing against tickdb's full ingest path (parse, wal write, memtable, flush) — not a fully apples-to-apples comparison, since tickdb's number includes durability guarantees duckdb's csv load doesn't provide.

**storage**: tickdb is genuinely 20% smaller than zstd-compressed parquet. this is the one number that held steady from the original benchmark.

**ohlcv**: duckdb wins by roughly 1.5–1.6x. an earlier internal run reported tickdb as ~3x faster here; that result could not be reproduced on two separate reruns of the same 2m-row synthetic dataset and is considered stale or incorrect. the current sparse `btreemap`-based bucket accumulation in `crates/query/src/agg.rs` trades a per-row tree insert for bounded memory on unbounded ranges — a deliberate correctness fix after an earlier dense-array version caused a catastrophic allocation on `range(i64::min, i64::max)` queries. that fix likely explains why the earlier number can't be reproduced: the code as it stands today is not duckdb's equal on raw ohlcv throughput.

**vwap**: duckdb wins by roughly 190–240x depending on the run. polars also beats tickdb, though polars' own number was unstable across runs (0.0125s vs 0.0469s), so treat that column as noisier than the others.

**top-10**: duckdb wins by roughly 55–115x depending on the run.

**overall honest picture**: duckdb's vectorized execution engine beats tickdb on all three benchmarked query shapes. tickdb's genuine advantages in this project are storage compactness and the from-scratch raft consensus layer with deterministic simulation testing — not out-executing a mature, SIMD-tuned execution engine on general aggregates it's specifically built for.