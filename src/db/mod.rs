use crate::config::DatabaseConfig;
use crate::parser::GovEvent;
use crate::pipeline::types::ParsedBlock;
use chrono::{DateTime, Utc};
use lru::LruCache;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Transaction};
use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tracing::info;

/// Global cache for delegation lookups (stake_address -> previous_drep)
static DELEGATION_CACHE: OnceLock<Mutex<LruCache<String, String>>> = OnceLock::new();

fn get_delegation_cache() -> &'static Mutex<LruCache<String, String>> {
    DELEGATION_CACHE.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(100000).unwrap())))
}

pub async fn connect(config: &DatabaseConfig) -> Result<Pool<Postgres>, anyhow::Error> {
    info!("{}", "Database: Initializing connection pool...");

    let mut attempt = 0;
    loop {
        attempt += 1;
        let pool_result = PgPoolOptions::new()
            .max_connections(config.max_connections + 5) // Add buffer for status/monitor
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET statement_timeout = '5s'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(&config.url)
            .await;

        match pool_result {
            Ok(pool) => {
                info!(
                    "{} Connected with {} connections",
                    "Success:", config.max_connections
                );
                return Ok(pool);
            }
            Err(e) => {
                if config.retry_attempts > 0 && attempt >= config.retry_attempts {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to database after {} attempts: {}",
                        attempt,
                        e
                    ));
                }

                tracing::warn!(
                    "Failed to connect to database (attempt {}): {}. Retrying in {}s...",
                    attempt,
                    e,
                    config.retry_delay_secs
                );
                tokio::time::sleep(Duration::from_secs(config.retry_delay_secs)).await;
            }
        }
    }
}

/// Unified function to insert all governance events into drep_timeline_event table
async fn insert_governance_event(
    tx: &mut Transaction<'_, Postgres>,
    event: &GovEvent,
    timestamp: DateTime<Utc>,
    epoch: i64,
    slot: i64,
) -> Result<(), anyhow::Error> {
    match event {
        GovEvent::Registration {
            tx_hash,
            tx_index,
            drep_id,
            deposit,
            anchor_url,
            anchor_hash,
        } => {
            let metadata = json!({
                "drep_id": drep_id,
                "deposit": deposit,
                "anchor_url": anchor_url,
                "anchor_hash": anchor_hash,
                "tx_hash": tx_hash,
                "epoch_no": epoch
            });

            sqlx::query(
                r#"
                INSERT INTO drep_timeline_event (
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata, stake_address, previous_drep
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (tx_hash, tx_index) DO NOTHING
                "#,
            )
            .bind("registration")
            .bind(timestamp)
            .bind(epoch as i32)
            .bind(slot)
            .bind(tx_hash)
            .bind(*tx_index as i32)
            .bind(drep_id)
            .bind(metadata)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .execute(tx.as_mut())
            .await?;
        }

        GovEvent::Retirement {
            tx_hash,
            tx_index,
            drep_id,
            deposit,
        } => {
            let metadata = json!({
                "drep_id": drep_id,
                "deposit": deposit,
                "tx_hash": tx_hash
            });

            sqlx::query(
                r#"
                INSERT INTO drep_timeline_event (
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata, stake_address, previous_drep
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (tx_hash, tx_index) DO NOTHING
                "#,
            )
            .bind("retirement")
            .bind(timestamp)
            .bind(epoch as i32)
            .bind(slot)
            .bind(tx_hash)
            .bind(*tx_index as i32)
            .bind(drep_id)
            .bind(metadata)
            .bind(Option::<String>::None)
            .bind(Option::<String>::None)
            .execute(tx.as_mut())
            .await?;
        }

        GovEvent::Delegation {
            tx_hash,
            tx_index,
            stake_credential,
            drep_id,
            drep_has_script,
        } => {
            // Check cache first
            let cached_drep = {
                let mut cache = get_delegation_cache().lock().unwrap();
                cache.get(stake_credential).cloned()
            };

            let previous_drep = if let Some(drep) = cached_drep {
                drep
            } else {
                let db_drep = sqlx::query_scalar::<_, Option<String>>(
                    r#"
                    SELECT previous_drep
                    FROM drep_timeline_event
                    WHERE stake_address = $1
                      AND event_type = 'delegation'
                    ORDER BY timestamp DESC
                    LIMIT 1
                    "#,
                )
                .bind(stake_credential)
                .fetch_optional(tx.as_mut())
                .await?
                .flatten()
                .unwrap_or("drep_always_abstain".to_string());

                // Update cache
                {
                    let mut cache = get_delegation_cache().lock().unwrap();
                    cache.put(stake_credential.clone(), db_drep.clone());
                }
                db_drep
            };

            let metadata = json!({
                "stake_address": stake_credential,
                "target_drep": drep_id,
                "current_drep": drep_id,
                "drep_has_script": drep_has_script,
                "previous_drep": previous_drep,
                "tx_hash": tx_hash,
                "timestamp": timestamp.to_rfc3339(),
                "delegation_epoch": epoch,
                "total_stake": "0",
                "added_power": true
            });

            sqlx::query(
                r#"
                INSERT INTO drep_timeline_event (
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata, stake_address, previous_drep
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (tx_hash, tx_index) DO NOTHING
                "#,
            )
            .bind("delegation")
            .bind(timestamp)
            .bind(epoch as i32)
            .bind(slot)
            .bind(tx_hash)
            .bind(*tx_index as i32)
            .bind(drep_id)
            .bind(metadata)
            .bind(stake_credential)
            .bind(previous_drep)
            .execute(tx.as_mut())
            .await?;
        }

        GovEvent::Proposal {
            tx_hash,
            tx_index,
            index,
            action_type,
            deposit,
            anchor_url,
            anchor_hash,
            reward_account,
            details,
        } => {
            let metadata = json!({
                "index": index,
                "action_type": action_type,
                "deposit": deposit,
                "anchor_url": anchor_url,
                "anchor_hash": anchor_hash,
                "reward_account": reward_account,
                "details": details,
                "action_id": format!("{}#{}", tx_hash, index)
            });

            sqlx::query(
                r#"
                INSERT INTO drep_timeline_event (
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata, stake_address, previous_drep
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (tx_hash, tx_index) DO NOTHING
                "#,
            )
            .bind("proposal")
            .bind(timestamp)
            .bind(epoch as i32)
            .bind(slot)
            .bind(tx_hash)
            .bind(*tx_index as i32)
            .bind(Option::<String>::None)
            .bind(metadata)
            .bind(Option::<String>::None) // stake_address
            .bind(Option::<String>::None) // previous_drep
            .execute(tx.as_mut())
            .await?;
        }

        GovEvent::Vote {
            tx_hash,
            tx_index,
            voter_id,
            proposal_tx_hash,
            proposal_index,
            vote,
            anchor_url,
            anchor_hash,
        } => {
            let action_id = format!("{}#{}", proposal_tx_hash, proposal_index);

            let metadata = json!({
                "voter_id": voter_id,
                "proposal_tx_hash": proposal_tx_hash,
                "proposal_index": proposal_index,
                "vote": vote,
                "anchor_url": anchor_url,
                "anchor_hash": anchor_hash,
                "action_id": action_id,
                "view": voter_id,
                "gov_action_proposal_id": proposal_tx_hash,
                "time_voted": timestamp.to_rfc3339(),
                "voting_epoch": epoch,
                "vote_rationale": anchor_url.clone().unwrap_or_default(),
                "url": anchor_url.clone().unwrap_or_default()
            });

            let drep_id_value =
                if voter_id.starts_with("drep") && !voter_id.starts_with("drep-always") {
                    Some(voter_id.clone())
                } else {
                    None
                };

            sqlx::query(
                r#"
                INSERT INTO drep_timeline_event (
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata, stake_address, previous_drep
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (tx_hash, tx_index) DO NOTHING
                "#,
            )
            .bind("vote")
            .bind(timestamp)
            .bind(epoch as i32)
            .bind(slot)
            .bind(tx_hash)
            .bind(*tx_index as i32)
            .bind(drep_id_value)
            .bind(metadata)
            .bind(Option::<String>::None) // stake_address
            .bind(Option::<String>::None) // previous_drep
            .execute(tx.as_mut())
            .await?;
        }
    }

    Ok(())
}

/// Load all historical range checkpoints in one query.
/// Returns a map of worker_id → last completed slot.
pub async fn get_historical_checkpoints(
    pool: &Pool<Postgres>,
) -> anyhow::Result<std::collections::HashMap<String, u64>> {
    let rows = sqlx::query_as::<_, (String, i64)>(
        "SELECT worker_id, last_slot FROM sync_checkpoint WHERE worker_id LIKE 'historical_%'",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, slot)| (id, slot as u64))
        .collect())
}

pub async fn get_checkpoint(
    pool: &Pool<Postgres>,
    worker_id: &str,
) -> Result<Option<(u64, String)>, anyhow::Error> {
    let row = sqlx::query_as::<_, (i64, String)>(
        "SELECT last_slot, last_hash FROM sync_checkpoint WHERE worker_id = $1",
    )
    .bind(worker_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|(slot, hash)| (slot as u64, hash)))
}

pub async fn update_checkpoint(
    pool: &Pool<Postgres>,
    worker_id: &str,
    slot: u64,
    hash: &str,
) -> Result<(), anyhow::Error> {
    sqlx::query(
        r#"
        INSERT INTO sync_checkpoint (worker_id, last_slot, last_hash, updated_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (worker_id) DO UPDATE SET
            last_slot = EXCLUDED.last_slot,
            last_hash = EXCLUDED.last_hash,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(worker_id)
    .bind(slot as i64)
    .bind(hash)
    .execute(pool)
    .await?;

    Ok(())
}
pub async fn wipe_data(pool: &Pool<Postgres>) -> Result<(), anyhow::Error> {
    sqlx::query("TRUNCATE TABLE drep_timeline_event, sync_checkpoint RESTART IDENTITY")
        .execute(pool)
        .await?;
    Ok(())
}

pub struct DbStats {
    pub event_count: i64,
    pub checkpoint_count: i64,
    pub last_event_time: Option<DateTime<Utc>>,
}

pub async fn get_stats(pool: &Pool<Postgres>) -> Result<DbStats, anyhow::Error> {
    let (event_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM drep_timeline_event")
        .fetch_one(pool)
        .await?;

    let (checkpoint_count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM sync_checkpoint")
        .fetch_one(pool)
        .await?;

    let last_event_time: Option<(DateTime<Utc>,)> =
        sqlx::query_as("SELECT MAX(timestamp) FROM drep_timeline_event")
            .fetch_optional(pool)
            .await?;

    Ok(DbStats {
        event_count,
        checkpoint_count,
        last_event_time: last_event_time.map(|t| t.0),
    })
}

/// Persist all governance events from a parsed block in a single transaction.
/// Idempotent — ON CONFLICT DO NOTHING on (tx_hash, tx_index).
pub async fn persist_parsed_block(
    pool: &Pool<Postgres>,
    block: &ParsedBlock,
) -> anyhow::Result<()> {
    if block.events.is_empty() {
        return Ok(());
    }

    let epoch = block.epoch as i64;
    let slot = block.slot as i64;
    let timestamp = block.timestamp;

    let mut tx = pool.begin().await?;

    // Skip WAL flush wait — safe because inserts are idempotent (ON CONFLICT DO NOTHING).
    // Postgres still commits in shared memory; a crash just means re-processing from checkpoint.
    sqlx::query("SET LOCAL synchronous_commit = off")
        .execute(tx.as_mut())
        .await?;

    for event in &block.events {
        insert_governance_event(&mut tx, event, timestamp, epoch, slot).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Delete all events with slot > `after_slot`. Returns the number of rows deleted.
pub async fn delete_after_slot(pool: &Pool<Postgres>, after_slot: u64) -> anyhow::Result<u64> {
    let result = sqlx::query("DELETE FROM drep_timeline_event WHERE slot > $1")
        .bind(after_slot as i64)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Update the last_slot for a checkpoint without changing the hash.
/// Used during rollbacks where we want to record the rollback point.
pub async fn update_checkpoint_slot(
    pool: &Pool<Postgres>,
    worker_id: &str,
    slot: u64,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
        UPDATE sync_checkpoint SET last_slot = $1, updated_at = now()
        WHERE worker_id = $2
        "#,
    )
    .bind(slot as i64)
    .bind(worker_id)
    .execute(pool)
    .await?;
    Ok(())
}
