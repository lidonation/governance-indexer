use anyhow::Context;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pallas::network::miniprotocols::Point;
use sqlx::{Pool, Postgres};
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{error, info, warn};

mod config;
mod db;
mod metrics;
mod monitor;
mod parser;
mod scanner;

use clap::{Parser, Subcommand};
use config::Config;
use metrics::Metrics;

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
    /// Start the governance indexer (full sync)
    Start,
    /// Wipe all indexed data (drep timeline and checkpoints)
    Wipe {
        /// Force wipe without confirmation
        #[arg(short, long)]
        force: bool,
    },
    /// Wipe all data and start a fresh sync
    Reset {
        /// Force reset without confirmation
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
    /// Alias for start (prototype use)
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
    let pid = get_running_pid()?;
    nix::sys::signal::kill(pid, sig)
        .with_context(|| format!("Failed to send {} signal to process {}", action_name, pid))?;
    println!(
        "{} {} signal to indexer (PID: {})",
        "Success:", action_name, pid
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    // 1. Initialize logging first so we can see diagnostics
    tracing_subscriber::fmt::init();

    // 2. Check if .env exists on disk
    if std::path::Path::new(".env").exists() {
        info!("{}", "Note: Found .env file on disk");
    } else {
        warn!("{}", "Warning: No .env file found in current directory");
    }

    // 3. Load .env file
    match dotenv() {
        Ok(path) => info!("{} {}", "Success: Loaded environment from:", path.display()),
        Err(_) => warn!("{}", "Note: Using system environment variables"),
    }

    // Load configuration
    let config = Config::from_env()?;

    match &cli.command {
        Some(Commands::Wipe { force }) => {
            if !force {
                println!(
                    "{}",
                    "Warning: This will permanently delete all indexed data and checkpoints."
                );
                print!("Are you sure you want to proceed? (y/N): ");
                use std::io::{Write, stdin};
                std::io::stdout().flush()?;
                let mut input = String::new();
                stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("{}", "Operation cancelled.");
                    return Ok(());
                }
            }
            let pool = db::connect(&config.database).await?;
            db::wipe_data(&pool).await?;
            println!("{}", "Success: Database wiped successfully.");
        }
        Some(Commands::Reset { force }) => {
            if !force {
                println!(
                    "{}",
                    "Warning: This will stop the indexer, wipe all data, and start a fresh sync."
                );
                print!("Are you sure you want to proceed? (y/N): ");
                use std::io::{Write, stdin};
                std::io::stdout().flush()?;
                let mut input = String::new();
                stdin().read_line(&mut input)?;
                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("{}", "Operation cancelled.");
                    return Ok(());
                }
            }
            let pool = db::connect(&config.database).await?;
            db::wipe_data(&pool).await?;
            println!("{}", "Success: Database wiped. Starting fresh sync...");
            run_sync(config, pool).await?;
        }
        Some(Commands::Status) => {
            let pool = db::connect(&config.database).await?;
            let stats = db::get_stats(&pool).await?;
            println!("{}", "Indexer Status:");
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
        Some(Commands::Start) | Some(Commands::Build) | None => {
            let pool = db::connect(&config.database).await?;
            run_sync(config, pool).await?;
        }
    }

    Ok(())
}

async fn run_sync(config: Config, pool: Pool<Postgres>) -> Result<(), anyhow::Error> {
    // Write PID file
    let pid = std::process::id();
    std::fs::write(PID_FILE, pid.to_string())?;

    // Ensure PID file is removed on exit
    let _pid_cleanup = PidCleanup;

    config.validate()?;
    config.log_summary();

    // Initialize metrics
    let metrics = Metrics::new();
    let _metrics_task = metrics.clone().start_reporting_task(60);

    info!("{}", "Initializing Cardano Governance Indexer...");
    info!(
        "Configuration: Parallel workers={}, Batch size={}",
        config.processing.max_concurrent_workers, config.processing.batch_size
    );
    info!("Connection: Relay={}", config.network.relay_address);

    // 2. Load slot hashes
    let file = File::open(&config.processing.slot_hashes_file)
        .with_context(|| format!("Failed to open {}", config.processing.slot_hashes_file))?;
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
    info!(
        "Status: Loaded {} epoch boundary points from CSV",
        slot_hashes.len()
    );

    if slot_hashes.len() < 2 {
        anyhow::bail!("Error: Need at least 2 slot hashes to define a range");
    }

    // 3. Plan sync
    let start_slot = slot_hashes.first().unwrap().0;
    let end_slot = slot_hashes.last().unwrap().0;
    info!("Sync Plan: Slots {} to {}", start_slot, end_slot);

    // 4. Get chain tip
    let mut peer = scanner::connect(&config.network.relay_address).await?;
    let _tip = peer.chainsync().intersect_tip().await?;
    info!("{}", "Success: Connected to Cardano node");
    peer.abort().await;

    // 5. Run Parallel Historical Sync
    info!("{}", "Starting historical synchronization...");

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(
        config.processing.max_concurrent_workers,
    ));

    let tasks: Vec<_> = (0..slot_hashes.len() - 1)
        .map(|i| {
            let (start_slot_ref, start_hash_hex) = &slot_hashes[i];
            let (end_slot_ref, end_hash_hex) = &slot_hashes[i + 1];

            let start_slot = *start_slot_ref;
            let end_slot = *end_slot_ref;

            let start_hash = hex::decode(start_hash_hex).expect("Invalid hex in CSV");
            let end_hash = hex::decode(end_hash_hex).expect("Invalid hex in CSV");

            let start_point = Point::Specific(start_slot, start_hash);
            let end_point = Point::Specific(end_slot, end_hash);

            let pool = pool.clone();
            let relay = config.network.relay_address.clone();
            let worker_idx = i;
            let sem = semaphore.clone();

            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let worker_id = format!("historical_{}", worker_idx);
                match scanner::worker((start_point, end_point), relay, pool, worker_id).await {
                    Ok((blocks, events)) => {
                        info!(
                            "Worker {}: Completed ({} blocks, {} events)",
                            worker_idx, blocks, events
                        );
                        Ok(events)
                    }
                    Err(e) => {
                        error!("Worker {}: Failed - {:?}", worker_idx, e);
                        Err(e)
                    }
                }
            })
        })
        .collect();

    let mut stream = stream::iter(tasks).buffer_unordered(config.processing.max_concurrent_workers);

    let mut total_events = 0;
    while let Some(result) = stream.next().await {
        match result {
            Ok(Ok(events)) => total_events += events,
            Ok(Err(_)) => error!("{}", "Error: Data synchronization worker failed"),
            Err(e) => error!("Error: Task join error - {:?}", e),
        }
    }

    info!(
        "{}",
        format!(
            "Success: Historical synchronization complete. Total events: {}",
            total_events
        )
    );

    // 6. Continuous Sync
    let last_slot = slot_hashes.last().unwrap().0;
    let last_hash = hex::decode(&slot_hashes.last().unwrap().1)?;
    let start_point = Point::Specific(last_slot, last_hash);

    info!("{}", "Transitioning to real-time monitoring...");
    monitor::continuous_sync(&config.network.relay_address, pool, start_point, &config).await?;

    Ok(())
}
