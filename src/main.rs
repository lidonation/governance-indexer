use anyhow::Context;
use dotenvy::dotenv;
use pallas::network::miniprotocols::Point;
use sqlx::PgPool;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{error, info, warn};

mod config;
mod db;
mod parser;
mod pipeline;
mod reconciler;
mod utils;

use clap::{Parser, Subcommand};
use config::Config;
use sysinfo::System;

struct PidCleanup;

impl Drop for PidCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(PID_FILE);
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the governance indexer (historical sync → continuous monitor)
    Start,
    /// Run historical sync only, then exit
    SyncHistory,
    /// Run continuous sync from last checkpoint (skip historical sync)
    Monitor,
    /// Wipe all indexed data (drep timeline and checkpoints)
    Wipe {
        #[arg(short, long)]
        force: bool,
    },
    /// Wipe all data and start a fresh sync
    Reset {
        #[arg(short, long)]
        force: bool,
    },
    /// Show current status and database statistics
    Status,
    /// Stop the running indexer process
    Stop,
    /// Pause the running indexer process (SIGSTOP)
    Pause,
    /// Continue/Resume the paused indexer process (SIGCONT)
    #[command(name = "continue")]
    Continue,
    /// Fix known data discrepancies (constant-DRep drep_ids, previous_drep, orphans)
    Reconcile {
        /// Only run SQL fixes (skip Blockfrost orphan scan)
        #[arg(long)]
        sql_only: bool,
        /// Also scan for and delete orphaned rollback events via Blockfrost
        #[arg(long)]
        scan_orphans: bool,
        /// Print what would be changed without applying any writes
        #[arg(long)]
        dry_run: bool,
        /// Only check events at or after this slot number
        #[arg(long)]
        since_slot: Option<u64>,
        /// Blockfrost project ID (or set BLOCKFROST_PROJECT_ID env var)
        #[arg(long, env = "BLOCKFROST_PROJECT_ID")]
        blockfrost_key: Option<String>,
    },
    /// Alias for start
    Build,
}

const PID_FILE: &str = ".indexer.pid";

fn get_running_pid() -> Result<nix::unistd::Pid, anyhow::Error> {
    let pid_str = std::fs::read_to_string(PID_FILE)
        .with_context(|| "Indexer is not running (no PID file found)")?;
    let pid: i32 = pid_str
        .trim()
        .parse()
        .with_context(|| "Corrupted PID file")?;
    Ok(nix::unistd::Pid::from_raw(pid))
}

fn send_signal(sig: nix::sys::signal::Signal, action_name: &str) -> Result<(), anyhow::Error> {
    let mut system = System::new_all();
    system.refresh_all();

    let my_pid = std::process::id();
    let mut found_count = 0;

    for (pid, process) in system.processes() {
        let name = process.name();
        if (name == "indexer" || name == "rust-indexer") && pid.as_u32() != my_pid {
            let nix_pid = nix::unistd::Pid::from_raw(pid.as_u32() as i32);
            if let Err(e) = nix::sys::signal::kill(nix_pid, sig) {
                error!("Failed to signal process {} (PID {}): {:?}", name, pid, e);
            } else {
                info!("{} signal sent to {} (PID: {})", action_name, name, pid);
                found_count += 1;
            }
        }
    }

    if found_count == 0 {
        if let Ok(pid) = get_running_pid() {
            if pid.as_raw() != my_pid as i32 {
                nix::sys::signal::kill(pid, sig)?;
                info!("Sent {} signal to PID {} from PID file", action_name, pid);
                found_count += 1;
            }
        }
    }

    if found_count > 0 {
        println!(
            "Successfully sent {} signal to {} instance(s).",
            action_name, found_count
        );
    } else {
        warn!("No running indexer instances found to {}.", action_name.to_lowercase());
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    tracing_subscriber::fmt::init();

    if std::path::Path::new(".env").exists() {
        info!("Note: Found .env file on disk");
    } else {
        warn!("Warning: No .env file found in current directory");
    }

    match dotenv() {
        Ok(path) => info!("Loaded environment from: {}", path.display()),
        Err(_) => warn!("Note: Using system environment variables"),
    }

    let config = Config::from_env()?;

    match &cli.command {
        Some(Commands::Wipe { force }) => {
            if !force {
                println!("Warning: This will permanently delete all indexed data and checkpoints.");
                print!("Are you sure you want to proceed? (y/N): ");
                use std::io::{Write, stdin};
                std::io::stdout().flush()?;
                let mut input = String::new();
                stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Operation cancelled.");
                    return Ok(());
                }
            }
            let pool = db::connect(&config.database).await?;
            db::ensure_schema(&pool).await?;
            db::wipe_data(&pool).await?;
            println!("Database wiped successfully.");
        }
        Some(Commands::Reset { force }) => {
            if !force {
                println!("Warning: This will stop the indexer, wipe all data, and start a fresh sync.");
                print!("Are you sure you want to proceed? (y/N): ");
                use std::io::{Write, stdin};
                std::io::stdout().flush()?;
                let mut input = String::new();
                stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Operation cancelled.");
                    return Ok(());
                }
            }
            let pool = db::connect(&config.database).await?;
            db::ensure_schema(&pool).await?;
            db::wipe_data(&pool).await?;
            println!("Database wiped. Starting fresh sync...");
            run_sync(config, pool, false).await?;
        }
        Some(Commands::Status) => {
            let pool = db::connect(&config.database).await?;
            db::ensure_schema(&pool).await?;
            let stats = db::get_stats(&pool).await?;
            println!("Indexer Status:");
            println!("   Events:      {}", stats.event_count);
            println!("   Checkpoints: {}", stats.checkpoint_count);
            if let Some(last) = stats.last_event_time {
                println!("   Last event:  {}", last);
            }
        }
        Some(Commands::Stop) => {
            send_signal(nix::sys::signal::Signal::SIGTERM, "STOP")?;
        }
        Some(Commands::Pause) => {
            send_signal(nix::sys::signal::Signal::SIGSTOP, "PAUSE")?;
        }
        Some(Commands::Continue) => {
            send_signal(nix::sys::signal::Signal::SIGCONT, "CONTINUE")?;
        }
        Some(Commands::SyncHistory) => {
            let pool = db::connect(&config.database).await?;
            run_sync(config, pool, true).await?;
        }
        Some(Commands::Monitor) => {
            let pool = db::connect(&config.database).await?;
            run_monitor(config, pool).await?;
        }
        Some(Commands::Reconcile {
            sql_only,
            scan_orphans,
            dry_run,
            since_slot,
            blockfrost_key,
        }) => {
            let pool = db::connect(&config.database).await?;
            db::ensure_schema(&pool).await?;
            reconciler::run_reconciler(
                pool,
                reconciler::ReconcileConfig {
                    sql_only: *sql_only,
                    scan_orphans: *scan_orphans,
                    dry_run: *dry_run,
                    since_slot: *since_slot,
                    blockfrost_key: blockfrost_key.clone(),
                },
            )
            .await?;
        }
        Some(Commands::Start) | Some(Commands::Build) | None => {
            let pool = db::connect(&config.database).await?;
            run_sync(config, pool, false).await?;
        }
    }

    Ok(())
}

fn load_slot_hashes(path: &str) -> anyhow::Result<Vec<(u64, String)>> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open slot hashes file: {}", path))?;
    let reader = BufReader::new(file);
    let mut slot_hashes = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 2 {
            let slot: u64 = parts[0].trim().parse()?;
            let hash = parts[1].trim().to_string();
            if !hash.is_empty() {
                slot_hashes.push((slot, hash));
            }
        }
    }

    Ok(slot_hashes)
}

async fn run_sync(config: Config, pool: PgPool, history_only: bool) -> anyhow::Result<()> {
    let _pid_cleanup = if !history_only {
        let pid = std::process::id();
        std::fs::write(PID_FILE, pid.to_string())?;
        Some(PidCleanup)
    } else {
        None
    };

    config.validate()?;
    config.log_summary();

    db::ensure_schema(&pool).await?;

    let slot_hashes = load_slot_hashes(&config.processing.slot_hashes_file)?;
    info!("Loaded {} epoch boundary points from CSV", slot_hashes.len());

    if slot_hashes.len() < 2 {
        anyhow::bail!("Need at least 2 slot hashes to define a range");
    }

    let start_slot = slot_hashes.first().unwrap().0;
    let end_slot = slot_hashes.last().unwrap().0;
    info!("Sync plan: slots {} → {}", start_slot, end_slot);

    let ranges = pipeline::build_epoch_ranges(&slot_hashes)?;

    info!("Starting historical sync ({} ranges)...", ranges.len());

    let pool_h = pool.clone();
    let config_h = config.clone();
    tokio::task::spawn_blocking(move || {
        pipeline::run_historical_pipeline(&config_h, pool_h, ranges)
    })
    .await
    .context("historical pipeline thread panicked")?
    .context("historical pipeline failed")?;

    if history_only {
        info!("Historical sync complete. Exiting.");
        return Ok(());
    }

    run_monitor(config, pool).await
}

async fn run_monitor(config: Config, pool: PgPool) -> anyhow::Result<()> {
    let _pid_cleanup = {
        let pid = std::process::id();
        std::fs::write(PID_FILE, pid.to_string())?;
        PidCleanup
    };

    db::ensure_schema(&pool).await?;

    let slot_hashes = load_slot_hashes(&config.processing.slot_hashes_file)?;

    let start_point = get_continuous_start_point(&pool, &slot_hashes).await?;

    info!(
        "Starting continuous sync from slot {}...",
        match &start_point {
            Point::Specific(s, _) => *s,
            Point::Origin => 0,
        }
    );

    let pool_c = pool.clone();
    let config_c = config.clone();
    tokio::task::spawn_blocking(move || {
        pipeline::run_continuous_pipeline(&config_c, pool_c, start_point)
    })
    .await
    .context("continuous pipeline thread panicked")?
    .context("continuous pipeline failed")?;

    Ok(())
}

async fn get_continuous_start_point(
    pool: &PgPool,
    slot_hashes: &[(u64, String)],
) -> anyhow::Result<Point> {
    let worker_id = "continuous";
    let csv_last_slot = slot_hashes.last().unwrap().0;

    if let Ok(Some((last_slot, last_hash_hex))) = db::get_checkpoint(pool, worker_id).await {
        if last_slot < csv_last_slot {
            warn!(
                "Continuous checkpoint (slot {}) is behind historical plan (slot {}). Fast-forwarding.",
                last_slot, csv_last_slot
            );
            let last_hash = hex::decode(&slot_hashes.last().unwrap().1)?;
            return Ok(Point::Specific(csv_last_slot, last_hash));
        }
        let last_hash = hex::decode(&last_hash_hex)?;
        info!("Resuming continuous sync from checkpoint at slot {}", last_slot);
        return Ok(Point::Specific(last_slot, last_hash));
    }

    let last_hash = hex::decode(&slot_hashes.last().unwrap().1)?;
    Ok(Point::Specific(csv_last_slot, last_hash))
}
