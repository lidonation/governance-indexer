use crate::config::{NetworkConfig, cardano};
use crate::db;
use crate::parser;
use chrono::{DateTime, TimeZone, Utc};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::Point;
use sqlx::{Pool, Postgres};
use std::time::Instant;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

fn slot_to_timestamp(slot: u64) -> DateTime<Utc> {
    let slots_since_shelley = slot.saturating_sub(cardano::SHELLEY_START_SLOT);
    let seconds_offset = slots_since_shelley * cardano::SLOT_LENGTH_SECONDS;
    let timestamp = cardano::SHELLEY_START_TIME + seconds_offset as i64;
    Utc.timestamp_opt(timestamp, 0).unwrap()
}

fn slot_to_epoch(slot: u64) -> u64 {
    let slots_since_shelley = slot.saturating_sub(cardano::SHELLEY_START_SLOT);
    let epochs_since_shelley = slots_since_shelley / cardano::EPOCH_LENGTH_SLOTS;
    cardano::SHELLEY_START_EPOCH + epochs_since_shelley
}

pub async fn connect(addresses: Vec<String>) -> Result<PeerClient, anyhow::Error> {
    let default_config = NetworkConfig {
        relay_address: addresses.first().cloned().unwrap_or_default(),
        relays: addresses.clone(),
        connection_timeout_secs: 10,
        magic: 764824073,
        retry_attempts: 3,
        retry_delay_secs: 5,
    };
    connect_with_config(addresses, &default_config).await
}

pub async fn connect_with_config(
    addresses: Vec<String>,
    config: &NetworkConfig,
) -> Result<PeerClient, anyhow::Error> {
    let mut attempts = 0;
    let start_time = Instant::now();

    loop {
        attempts += 1;

        for address in &addresses {
            info!("Note: Connection attempt {} to {}", attempts, address);

            match tokio::time::timeout(
                Duration::from_secs(config.connection_timeout_secs),
                PeerClient::connect(address, config.magic.into()),
            )
            .await
            {
                Ok(Ok(peer)) => {
                    info!(
                        "{} Connected to {} in {:?} (attempt {})",
                        "Success:",
                        address,
                        start_time.elapsed(),
                        attempts
                    );
                    return Ok(peer);
                }
                Ok(Err(e)) => {
                    warn!("{} Connection failed to {}: {:?}", "Warning:", address, e);
                }
                Err(_) => {
                    warn!(
                        "{} Connection timeout to {} after {}s",
                        "Warning:", address, config.connection_timeout_secs
                    );
                }
            }
        }

        if attempts >= config.retry_attempts {
            return Err(anyhow::anyhow!(
                format!(
                    "Error: Failed to connect to any of {:?} after {} attempts in {:?}",
                    addresses,
                    attempts,
                    start_time.elapsed()
                )
                .to_string()
            ));
        }

        // Exponential backoff with jitter
        let delay = config.retry_delay_secs * (1 << (attempts - 1).min(5))
            + fastrand::u64(0..config.retry_delay_secs);
        warn!("Note: Retrying connection in {}s...", delay);
        sleep(Duration::from_secs(delay)).await;
    }
}

// Removed unused function fetch_single_block

pub async fn worker(
    range: (Point, Point),
    addresses: Vec<String>,
    pool: Pool<Postgres>,
    worker_id: String,
) -> Result<(u64, u64), anyhow::Error> {
    let (start, end) = range;

    // Check for checkpoint
    let effective_start =
        if let Ok(Some((last_slot, last_hash_hex))) = db::get_checkpoint(&pool, &worker_id).await {
            let last_hash = hex::decode(&last_hash_hex)?;
            let checkpoint_point = Point::Specific(last_slot, last_hash);
            info!(
                "Note: Worker {} resuming from checkpoint at slot {}",
                worker_id, last_slot
            );
            checkpoint_point
        } else {
            start
        };

    let mut peer = connect(addresses).await?;
    let client = peer.blockfetch();

    // Request the range - this initiates the streaming protocol
    match client
        .request_range((effective_start.clone(), end.clone()))
        .await
    {
        Ok(_) => {}
        Err(e) => {
            // Handle empty ranges gracefully - some ranges may legitimately have no blocks
            let err_msg = format!("{:?}", e);
            if err_msg.contains("doesn't contain any blocks") || err_msg.contains("no blocks") {
                return Ok((0, 0)); // Empty range is not an error
            }
            return Err(e.into());
        }
    };

    let mut block_count = 0;
    let mut event_count = 0;

    // Stream blocks one at a time instead of loading all into memory
    while let Some(body) = client.recv_while_streaming().await? {
        let block = MultiEraBlock::decode(&body)?;
        let slot = block.slot();
        let hash = hex::encode(block.hash());
        let timestamp = slot_to_timestamp(slot);
        let epoch = slot_to_epoch(slot);

        let events = parser::extract_governance(&body);
        if !events.is_empty() {
            event_count += events.len() as u64;
            db::persist_events(&pool, events, timestamp, epoch as i64, slot as i64).await?;
        }
        block_count += 1;

        // Update checkpoint every block (or every N blocks for performance)
        // For simplicity during initial sync, we do it every block or after a batch
        if block_count % 1000 == 0 {
            db::update_checkpoint(&pool, &worker_id, slot, &hash).await?;
        }

        if block_count % 1000 == 0 {
            info!(
                "Status: Worker {} processed {} blocks, {} events (Current slot: {})",
                worker_id, block_count, event_count, slot
            );
        }
    }

    // Final checkpoint update
    if let Some(body_opt) = client.recv_while_streaming().await.ok().flatten() {
        let block = MultiEraBlock::decode(&body_opt)?;
        db::update_checkpoint(&pool, &worker_id, block.slot(), &hex::encode(block.hash())).await?;
    }

    Ok((block_count, event_count))
}
