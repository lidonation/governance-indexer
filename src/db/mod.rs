use crate::config::DatabaseConfig;
use crate::parser::GovEvent;
use crate::pipeline::types::ParsedBlock;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Transaction};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;



/// Create all required tables and indexes if they don't already exist.
/// Safe to call on every startup — all statements use IF NOT EXISTS.
pub async fn ensure_schema(pool: &Pool<Postgres>) -> anyhow::Result<()> {
    // Acquire a dedicated connection and disable statement_timeout for DDL.
    // CREATE INDEX on a large existing table can take minutes — the default 5s
    // session timeout would kill it. We restore the timeout before releasing.
    let mut conn = pool.acquire().await?;
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut *conn)
        .await?;

    let stmts = [
        // Events table — includes stake_address/previous_drep from the start
        r#"CREATE TABLE IF NOT EXISTS drep_timeline_event (
            id             BIGSERIAL    PRIMARY KEY,
            event_type     TEXT         NOT NULL,
            timestamp      TIMESTAMPTZ  NOT NULL,
            epoch          INTEGER      NOT NULL,
            slot           BIGINT       NOT NULL,
            tx_hash        TEXT         NOT NULL,
            tx_index       INTEGER      NOT NULL,
            block_hash     TEXT,
            drep_id        TEXT,
            metadata       JSONB        NOT NULL,
            stake_address  TEXT,
            previous_drep  TEXT,
            created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
        )"#,
        // Checkpoint table
        r#"CREATE TABLE IF NOT EXISTS sync_checkpoint (
            worker_id   TEXT         PRIMARY KEY,
            last_slot   BIGINT       NOT NULL,
            last_hash   TEXT         NOT NULL,
            updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
        )"#,
        // Idempotency constraint
        "CREATE UNIQUE INDEX IF NOT EXISTS uniq_drep_timeline_tx_event ON drep_timeline_event (tx_hash, tx_index)",
        // Query indexes
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_type      ON drep_timeline_event (event_type)",
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_timestamp  ON drep_timeline_event (timestamp DESC)",
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_epoch      ON drep_timeline_event (epoch)",
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_drep_id    ON drep_timeline_event (drep_id) WHERE drep_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_drep_time  ON drep_timeline_event (drep_id, timestamp DESC) WHERE drep_id IS NOT NULL",
        "CREATE INDEX IF NOT EXISTS idx_drep_timeline_stake      ON drep_timeline_event (stake_address, slot DESC) WHERE event_type = 'delegation'",
        // ADD COLUMN IF NOT EXISTS for tables created by old schema without these columns
        "ALTER TABLE drep_timeline_event ADD COLUMN IF NOT EXISTS stake_address TEXT",
        "ALTER TABLE drep_timeline_event ADD COLUMN IF NOT EXISTS previous_drep TEXT",
    ];

    for stmt in &stmts {
        sqlx::query(stmt).execute(&mut *conn).await?;
    }

    info!("schema verified");
    Ok(())
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
                    sqlx::query("SET statement_timeout = 0")
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

/// Unified function to insert all governance events into drep_timeline_event table.
/// `prev_drep_map` is pre-resolved before the transaction — no DB reads happen inside.
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
            let metadata = json!({
                "stake_address": stake_credential,
                "target_drep": drep_id,
                "current_drep": drep_id,
                "drep_has_script": drep_has_script,
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
            .bind(Option::<String>::None)
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
