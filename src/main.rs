use anyhow::Context;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pallas::network::miniprotocols::Point;
use std::fs::File;
use std::io::{BufRead, BufReader};
use tracing::{error, info, warn};

mod config;
mod db;
mod metrics;
mod monitor;
mod parser;
mod scanner;

use config::Config;
use metrics::Metrics;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // 1. Initialize logging first so we can see diagnostics
    tracing_subscriber::fmt::init();

    // 2. Check if .env exists on disk (independent of dotenv-rs)
    if std::path::Path::new(".env").exists() {
        info!(
            "📂 Found .env file on disk at {:?}",
            std::env::current_dir()?.join(".env")
        );
    } else {
        warn!(
            "❓ No .env file found in current directory: {:?}",
            std::env::current_dir()?
        );
    }

    // 3. Load .env file
    match dotenv() {
        Ok(path) => info!("✅ dotenv loaded variables from: {:?}", path),
        Err(e) => warn!(
            "⚠️  dotenv did not load a file: {} (this is normal if using real environment variables)",
            e
        ),
    }

    // Load configuration
    let config = Config::from_env()?;
    config.validate()?;
    config.log_summary();

    // Initialize metrics
    let metrics = Metrics::new();
    let _metrics_task = metrics.clone().start_reporting_task(60); // Report every minute

    info!("🚀 Starting Cardano Governance Indexer (Rust)...");
    info!(
        "⚡ Parallel workers: {}",
        config.processing.max_concurrent_workers
    );
    info!("📊 Batch size: {}", config.processing.batch_size);
    info!("🔗 Relay: {}", config.network.relay_address);
    info!("📈 Metrics reporting enabled");

    // 1. Connect to DB
    let pool = db::connect(&config.database).await?;

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
            let hash = parts[1].trim().to_string(); // Hash is in column 2, not 1
            if !hash.is_empty() {
                slot_hashes.push((slot, hash));
            }
        }
    }
    info!(
        "📚 Loaded {} epoch boundary points from CSV",
        slot_hashes.len()
    );

    if slot_hashes.len() < 2 {
        anyhow::bail!("Need at least 2 slot hashes to define a range");
    }

    // 3. Plan sync
    let start_slot = slot_hashes.first().unwrap().0;
    let end_slot = slot_hashes.last().unwrap().0;
    info!("📊 Sync Plan:");
    info!("   Start slot: {}", start_slot);
    info!("   End slot:   {}", end_slot);
    info!("   Epoch boundaries: {}", slot_hashes.len());

    // 4. Get chain tip (just to verify connection)
    info!(
        "🔌 Connecting to Cardano node: {}",
        config.network.relay_address
    );
    let mut peer = scanner::connect(&config.network.relay_address).await?;

    // We don't strictly need chainsync here for historical, but good check
    let tip = peer.chainsync().intersect_tip().await?;
    info!("✅ Connected! Chain tip: {:?}", tip);
    peer.abort().await; // Close this connection, workers will open their own

    // 5. Run Parallel Historical Sync
    info!("🔄 Starting parallel historical sync...");

    // Use a semaphore to limit concurrent connections to the relay
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
                let _permit = sem.acquire().await.unwrap(); // Limit concurrent connections
                info!(
                    "🔨 Worker {} starting: slots {}-{}",
                    worker_idx, start_slot, end_slot
                );
                let worker_id = format!("historical_{}", worker_idx);
                match scanner::worker((start_point, end_point), relay, pool, worker_id).await {
                    Ok((blocks, events)) => {
                        info!(
                            "✅ Worker {} finished: {} blocks, {} events",
                            worker_idx, blocks, events
                        );
                        Ok(events)
                    }
                    Err(e) => {
                        error!("❌ Worker {} failed: {:?}", worker_idx, e);
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
            Ok(Err(_)) => error!("A worker failed"),
            Err(e) => error!("Task join error: {:?}", e),
        }
    }

    info!(
        "🎉 Historical sync complete! Total events: {}",
        total_events
    );

    // 6. Continuous Sync using chainsync protocol
    info!("🔄 Switching to continuous sync...");

    // Start from the last slot we processed in historical sync
    let last_slot = slot_hashes.last().unwrap().0;
    let last_hash = hex::decode(&slot_hashes.last().unwrap().1)?;
    let start_point = Point::Specific(last_slot, last_hash);

    // Run continuous sync - this will follow the chain indefinitely
    monitor::continuous_sync(&config.network.relay_address, pool, start_point, &config).await?;

    Ok(())
}
