use crate::config::DatabaseConfig;
use crate::parser::GovEvent;
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Transaction};
use std::time::Duration;
use tracing::info;

pub async fn connect(config: &DatabaseConfig) -> Result<Pool<Postgres>, anyhow::Error> {
    info!("Connecting to database with optimized pool settings...");
    info!("  Max connections: {}", config.max_connections);
    info!("  Min connections: {}", config.min_connections);
    info!("  Acquire timeout: {}s", config.acquire_timeout_secs);

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
        "✅ Connected to database with {} connections",
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

    // Use a transaction for better performance and consistency
    let mut tx = pool.begin().await?;

    // Process all events - simplified now that we have a unified table
    for event in &events {
        insert_governance_event(&mut tx, event, timestamp, epoch, slot).await?;
    }

    // Commit the transaction
    tx.commit().await?;
    Ok(())
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
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
            let previous_drep = sqlx::query_scalar::<_, Option<String>>(
                r#"
                SELECT metadata->>'target_drep' as drep
                FROM drep_timeline_event
                WHERE metadata->>'stake_address' = $1
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
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
                    event_type, timestamp, epoch, slot, tx_hash, tx_index, drep_id, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
            .execute(tx.as_mut())
            .await?;
        }
    }

    Ok(())
}
