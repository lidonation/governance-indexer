use crate::config::DatabaseConfig;
use crate::parser::GovEvent;
use chrono::{DateTime, Utc};
use lru::LruCache;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Transaction};
use std::num::NonZeroUsize;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tracing::info;

/// Global cache for delegation lookups (stake_address -> previous_drep)
static DELEGATION_CACHE: OnceLock<Mutex<LruCache<String, String>>> = OnceLock::new();

fn get_delegation_cache() -> &'static Mutex<LruCache<String, String>> {
    DELEGATION_CACHE.get_or_init(|| Mutex::new(LruCache::new(NonZeroUsize::new(10000).unwrap())))
}

pub async fn connect(config: &DatabaseConfig) -> Result<Pool<Postgres>, anyhow::Error> {
    info!("{}", "Database: Initializing connection pool...");

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .min_connections(config.min_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
        .max_lifetime(Duration::from_secs(config.max_lifetime_secs))
        .test_before_acquire(true) // Ping connection before using it
        .connect(&config.url)
        .await?;

    info!(
        "{} Connected with {} connections",
        "Success:",
        config.max_connections
    );
    Ok(pool)
}

pub async fn persist_events(
    pool: &Pool<Postgres>,
    events: Vec<GovEvent>,
    timestamp: DateTime<Utc>,
    epoch: i64,
    slot: i64,
) -> Result<(), anyhow::Error> {
    if events.is_empty() {
        return Ok(());
    }

    let retry_strategy = ExponentialBackoff::from_millis(100)
        .map(jitter) // add jitter to delays
        .take(5); // limit to 5 retries

    Retry::spawn(retry_strategy, || {
        let pool = pool.clone();
        let events = events.clone();
        let timestamp = timestamp;
        let epoch = epoch;
        let slot = slot;

        async move {
            let mut tx = pool.begin().await?;

            for event in &events {
                insert_governance_event(&mut tx, event, timestamp, epoch, slot).await?;
            }

            tx.commit().await?;
            Ok::<(), anyhow::Error>(())
        }
    })
    .await
    .map_err(|e| anyhow::anyhow!("Failed to persist events after retries: {}", e))
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
                "vote_rationale": "",
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
