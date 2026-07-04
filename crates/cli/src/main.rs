use std::fs::File;
/// tickdb command-line interface.
///
/// # Subcommands
///
/// - `ingest --symbol SYM --data-dir DIR [--csv FILE | --stdin] [--fsync POLICY]`
///   Ingests a Binance aggTrades CSV.  Reports throughput (rows/sec) on exit.
///
/// - `flush --data-dir DIR`
///   Forces the memtable to flush to a segment file.
///
/// - `compact --data-dir DIR --date DATE`
///   Runs compaction for a specific date partition.
///
/// - `stats --data-dir DIR`
///   Prints manifest contents, segment count, and total row count.
use std::io::{self, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

use cli::csv::parse_binance_csv;
use cli::IngestError;
use storage::{Engine, FsyncPolicy};

// ── CLI argument types ─────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "tickdb", about = "Columnar tick-data store", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Ingest a Binance aggTrades CSV file (or stdin) into the data directory.
    Ingest {
        /// Data directory (created if it doesn't exist).
        #[arg(long, default_value = "./tickdb-data")]
        data_dir: PathBuf,

        /// Symbol name to associate with the ingested trades (e.g., BTCUSDT).
        #[arg(long)]
        symbol: String,

        /// Path to CSV file.  Reads from stdin if omitted.
        #[arg(long)]
        csv: Option<PathBuf>,

        /// WAL fsync policy.
        #[arg(long, default_value = "every-n-ms")]
        fsync: FsyncArg,

        /// Interval in ms for every-n-ms fsync policy.
        #[arg(long, default_value = "50")]
        fsync_ms: u64,
    },

    /// Force-flush the memtable to a segment file.
    Flush {
        #[arg(long, default_value = "./tickdb-data")]
        data_dir: PathBuf,
    },

    /// Run compaction for a date partition (format: YYYY-MM-DD).
    Compact {
        #[arg(long, default_value = "./tickdb-data")]
        data_dir: PathBuf,

        #[arg(long)]
        date: String,
    },

    /// Print manifest contents and summary statistics.
    Stats {
        #[arg(long, default_value = "./tickdb-data")]
        data_dir: PathBuf,
    },
}

#[derive(Clone, ValueEnum)]
enum FsyncArg {
    EveryWrite,
    EveryNMs,
    Never,
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    // Initialise tracing; RUST_LOG controls verbosity (default: info).
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .with_target(false)
        .init();

    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest {
            data_dir,
            symbol,
            csv,
            fsync,
            fsync_ms,
        } => {
            cmd_ingest(data_dir, symbol, csv, fsync, fsync_ms)?;
        }
        Command::Flush { data_dir } => {
            cmd_flush(data_dir)?;
        }
        Command::Compact { data_dir, date } => {
            cmd_compact(data_dir, date)?;
        }
        Command::Stats { data_dir } => {
            cmd_stats(data_dir)?;
        }
    }

    Ok(())
}

// ── Subcommand implementations ────────────────────────────────────────────────

fn cmd_ingest(
    data_dir: PathBuf,
    symbol: String,
    csv_path: Option<PathBuf>,
    fsync: FsyncArg,
    fsync_ms: u64,
) -> Result<(), IngestError> {
    let policy = match fsync {
        FsyncArg::EveryWrite => FsyncPolicy::EveryWrite,
        FsyncArg::EveryNMs => FsyncPolicy::EveryNMs(fsync_ms),
        FsyncArg::Never => FsyncPolicy::Never,
    };

    let mut engine = Engine::open(&data_dir, policy)?;
    let symbol_id = engine.intern_symbol(&symbol)?;

    let start = Instant::now();

    let total = match csv_path {
        Some(path) => {
            let file = File::open(&path)?;
            parse_binance_csv(BufReader::new(file), symbol_id, |batch| {
                engine.ingest(batch).map_err(IngestError::Storage)
            })?
        }
        None => parse_binance_csv(io::stdin().lock(), symbol_id, |batch| {
            engine.ingest(batch).map_err(IngestError::Storage)
        })?,
    };

    engine.flush().map_err(IngestError::Storage)?;

    let elapsed = start.elapsed();
    let rows_per_sec = total as f64 / elapsed.as_secs_f64();

    println!(
        "ingested {total} rows in {elapsed:.2?} ({rows_per_sec:.0} rows/sec)",
        elapsed = elapsed,
    );

    Ok(())
}

fn cmd_flush(data_dir: PathBuf) -> Result<(), storage::Error> {
    let mut engine = Engine::open(&data_dir, FsyncPolicy::EveryWrite)?;
    engine.flush()?;
    println!("flush complete");
    Ok(())
}

fn cmd_compact(data_dir: PathBuf, date: String) -> Result<(), storage::Error> {
    let mut engine = Engine::open(&data_dir, FsyncPolicy::EveryWrite)?;
    let ran = engine.maybe_compact(&date)?;
    if ran {
        println!("compaction complete for {date}");
    } else {
        println!("no compaction needed for {date} (below threshold)");
    }
    Ok(())
}

fn cmd_stats(data_dir: PathBuf) -> Result<(), storage::Error> {
    let engine = Engine::open(&data_dir, FsyncPolicy::Never)?;

    let manifest = engine.manifest();
    if manifest.segments.is_empty() {
        println!("no segments found in {}", data_dir.display());
        return Ok(());
    }

    let total_rows: u64 = manifest.segments.iter().map(|s| s.row_count).sum();
    let dates: Vec<String> = manifest.all_dates();

    println!("data dir:   {}", data_dir.display());
    println!("segments:   {}", manifest.segments.len());
    println!("dates:      {}", dates.join(", "));
    println!("total rows: {total_rows}");
    println!();

    for seg in &manifest.segments {
        println!(
            "  {} | gen={} | rows={} | ts=[{},{}]",
            seg.path, seg.generation, seg.row_count, seg.min_ts, seg.max_ts
        );
    }

    Ok(())
}
