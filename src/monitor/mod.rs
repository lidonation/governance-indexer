use crate::config::{Config, cardano};
use crate::{db, parser, scanner};
use chrono::{DateTime, TimeZone, Utc};
use pallas::network::miniprotocols::{Point, chainsync};
use sqlx::{Pool, Postgres};
use tokio::time::{Duration, sleep};
use tracing::{error, info, warn};

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

pub async fn continuous_sync(
    relays: Vec<String>,
    pool: Pool<Postgres>,
    mut start_point: Point,
    config: &Config,
    worker_id: &str,
) -> Result<(), anyhow::Error> {
    info!(
        "Note: Starting continuous synchronization from {:?}",
        start_point
    );

    loop {
        match run_sync_iteration(relays.clone(), &pool, &start_point, worker_id).await {
            Ok(new_tip) => {
                info!(
                    "{} Sync iteration complete. New tip: {:?}",
                    "Success:", new_tip
                );
                // Update start_point for next iteration
                start_point = new_tip;
            }
            Err(e) => {
                error!("{} Sync iteration failed: {:?}", "Error:", e);

                // Try to recover latest checkpoint from DB to avoid re-processing blocks
                match db::get_checkpoint(&pool, worker_id).await {
                    Ok(Some((last_slot, last_hash_hex))) => {
                        if let Ok(last_hash) = hex::decode(&last_hash_hex) {
                            let new_point = Point::Specific(last_slot, last_hash);
                            info!("Note: Recovered checkpoint from DB: {:?}", new_point);
                            start_point = new_point;
                        }
                    }
                    Ok(None) => {
                        // No checkpoint found, keep using original start_point
                    }
                    Err(db_err) => {
                        error!("Failed to fetch checkpoint during recovery: {:?}", db_err);
                    }
                }
            }
        }

        // Wait before next iteration
        sleep(Duration::from_secs(
            config.processing.continuous_sync_interval_secs,
        ))
        .await;
    }
}

async fn run_sync_iteration(
    relays: Vec<String>,
    pool: &Pool<Postgres>,
    known_point: &Point,
    worker_id: &str,
) -> Result<Point, anyhow::Error> {
    // Create separate connections for chainsync and blockfetch to avoid borrow conflicts
    let mut chainsync_peer = scanner::connect(relays.clone()).await?;
    let mut blockfetch_peer = scanner::connect(relays.clone()).await?;

    let client = chainsync_peer.chainsync();

    // Find intersection with our known point
    let intersection = client.find_intersect(vec![known_point.clone()]).await?;

    info!("Status: Found intersection: {:?}", intersection);

    let mut processed_count = 0;
    let mut last_point = known_point.clone();

    // Processing blocks - chainsync state machine pattern
    info!("{}", "Status: Entering real-time monitoring loop");

    loop {
        // In Pallas 0.33, request_next() sends the message AND blocks for the response,
        // returning the NextResponse directly. Calling recv_while_must_reply() again
        // would cause a protocol agency error.
        info!("Note: Requesting next block...");
        let next = match tokio::time::timeout(Duration::from_secs(60), client.request_next()).await {
            Ok(res) => res?,
            Err(_) => return Err(anyhow::anyhow!("request_next timed out after 60s. Node connection stalled.")),
        };
        info!("{}", "Success: Response received");

        match next {
            chainsync::NextResponse::RollForward(header_content, tip) => {
                // Decode the header from CBOR bytes to get slot and hash
                let cbor_bytes = &header_content.cbor;
                let subtag = header_content.byron_prefix.map(|(tag, _)| tag);

                if let Ok(header) = pallas::ledger::traverse::MultiEraHeader::decode(
                    header_content.variant,
                    subtag,
                    cbor_bytes,
                ) {
                    let slot = header.slot();
                    let hash = header.hash();
                    let point = Point::Specific(slot, hash.to_vec());
                    let hash_hex = hex::encode(hash);

                    // Fetch the full block body using a separate blockfetch connection
                    match blockfetch_peer
                        .blockfetch()
                        .fetch_single(point.clone())
                        .await
                    {
                        Ok(body) => {
                            // Process the block
                            let events = parser::extract_governance(&body);

                            if !events.is_empty() {
                                let timestamp = slot_to_timestamp(slot);
                                let epoch = slot_to_epoch(slot);

                                info!(
                                    "📦 Block at slot {} has {} governance events",
                                    slot,
                                    events.len()
                                );

                                if let Err(e) = db::persist_events(
                                    pool,
                                    events,
                                    timestamp,
                                    epoch as i64,
                                    slot as i64,
                                )
                                .await
                                {
                                    error!("Failed to persist events: {:?}", e);
                                }
                            }

                            // Checkpoint every block in continuous sync
                            if let Err(e) =
                                db::update_checkpoint(pool, worker_id, slot, &hash_hex).await
                            {
                                error!("Failed to update checkpoint: {:?}", e);
                            }

                            processed_count += 1;
                            last_point = point;
                        }
                        Err(e) => {
                            warn!("Failed to fetch block body at slot {}: {:?}", slot, e);
                        }
                    }
                }

                // Check if we've reached the tip
                if let Point::Specific(tip_slot, _) = tip.0 {
                    if let Point::Specific(current_slot, _) = &last_point {
                        if current_slot >= &tip_slot {
                            info!(
                                "{} Caught up to tip. Processed {} blocks",
                                "Success:", processed_count
                            );
                            return Ok(last_point);
                        }
                    }
                }

                // Loop will request next block at the start of next iteration
            }
            chainsync::NextResponse::RollBackward(point, _tip) => {
                warn!("{} Chain rollback to {:?}", "Warning:", point);
                // In a production system, we'd need to handle rollbacks properly
                // For now, we'll just log and continue
                last_point = point;

                // Loop will request next block at the start of next iteration
            }
            chainsync::NextResponse::Await => {
                info!(
                    "Note: No new blocks available. Processed {} blocks",
                    processed_count
                );
                return Ok(last_point);
            }
        }
    }
}
