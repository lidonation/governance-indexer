use crate::config::{cardano, NetworkConfig};
use crate::db;
use crate::parser;
use chrono::{DateTime, TimeZone, Utc};
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::Point;
use sqlx::{Pool, Postgres};
use tracing::{info, warn};
use tokio::time::{sleep, Duration};
use std::time::Instant;

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

pub async fn connect(address: &str) -> Result<PeerClient, anyhow::Error> {
    let default_config = NetworkConfig {
        relay_address: address.to_string(),
        connection_timeout_secs: 10,
        magic: 764824073,
        retry_attempts: 3,
        retry_delay_secs: 5,
    };
    connect_with_config(address, &default_config).await
}

pub async fn connect_with_config(address: &str, config: &NetworkConfig) -> Result<PeerClient, anyhow::Error> {
    let mut attempts = 0;
    let start_time = Instant::now();
    
    loop {
        attempts += 1;
        info!("Connection attempt {} to {}", attempts, address);
        
        match tokio::time::timeout(
            Duration::from_secs(config.connection_timeout_secs),
            PeerClient::connect(address, config.magic.into()),
        ).await {
            Ok(Ok(peer)) => {
                info!("✅ Connected to {} in {:?} (attempt {})", 
                    address, start_time.elapsed(), attempts);
                return Ok(peer);
            },
            Ok(Err(e)) => {
                warn!("❌ Connection failed to {}: {:?}", address, e);
            },
            Err(_) => {
                warn!("⏰ Connection timeout to {} after {}s", address, config.connection_timeout_secs);
            }
        }
        
        if attempts >= config.retry_attempts {
            return Err(anyhow::anyhow!(
                "Failed to connect to {} after {} attempts in {:?}", 
                address, attempts, start_time.elapsed()
            ));
        }
        
        // Exponential backoff with jitter
        let delay = config.retry_delay_secs * (1 << (attempts - 1).min(5)) + 
                   fastrand::u64(0..config.retry_delay_secs);
        warn!("⏳ Retrying connection in {}s...", delay);
        sleep(Duration::from_secs(delay)).await;
    }
}

// Removed unused function fetch_single_block

pub async fn worker(
    range: (Point, Point),
    address: String,
    pool: Pool<Postgres>,
) -> Result<(u64, u64), anyhow::Error> {
    let (start, end) = range;
    let mut peer = connect(&address).await?;
    let client = peer.blockfetch();

    // Request the range - this initiates the streaming protocol
    match client.request_range((start.clone(), end.clone())).await {
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
        let timestamp = slot_to_timestamp(slot);
        let epoch = slot_to_epoch(slot);

        let events = parser::extract_governance(&body);
        if !events.is_empty() {
            event_count += events.len() as u64;
            db::persist_events(&pool, events, timestamp, epoch as i64, slot as i64).await?;
        }
        block_count += 1;
    }

    Ok((block_count, event_count))
}
