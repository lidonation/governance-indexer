use std::time::Duration;

use anyhow::Context;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::info;

use gasket::metrics::Counter;
use gasket::runtime::{Policy, TetherState, spawn_stage};
use gasket::retries;

mod audit;
mod blockfrost;
mod fetch;
mod patch;

use audit::AuditStage;
use fetch::FetchStage;
use patch::PatchStage;

#[derive(Debug, Clone)]
pub struct ReconcileConfig {
    pub sql_only: bool,
    pub scan_orphans: bool,
    pub dry_run: bool,
    pub since_slot: Option<u64>,
    pub blockfrost_key: Option<String>,
}

pub async fn run_reconciler(pool: PgPool, config: ReconcileConfig) -> anyhow::Result<()> {
    info!("reconciler: starting");

    run_sql_fixes(&pool, config.dry_run).await?;

    if config.sql_only {
        info!("reconciler: sql-only mode complete");
        return Ok(());
    }

    if !config.scan_orphans {
        info!("reconciler: orphan scan not enabled (pass --scan-orphans to enable)");
        return Ok(());
    }

    let blockfrost_key = config
        .blockfrost_key
        .context("BLOCKFROST_PROJECT_ID required for --scan-orphans")?;

    let pool_clone = pool.clone();
    let since = config.since_slot;
    let dry_run = config.dry_run;

    tokio::task::spawn_blocking(move || {
        run_orphan_pipeline(pool_clone, since, blockfrost_key, dry_run)
    })
    .await
    .context("orphan pipeline thread panicked")?
    .context("orphan pipeline failed")?;

    Ok(())
}

async fn run_sql_fixes(pool: &PgPool, dry_run: bool) -> anyhow::Result<()> {
    // Fix 1: NULL out constant-DRep voter drep_ids on vote events.
    // These were incorrectly set because format_voter uses underscores but the
    // old filter used hyphens, so constant-DRep voters were never filtered out.
    let fix1_count: i64 = if dry_run {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM drep_timeline_event \
             WHERE event_type = 'vote' \
               AND drep_id IN ('drep_always_abstain', 'drep_always_no_confidence')",
        )
        .fetch_one(pool)
        .await?
    } else {
        let r = sqlx::query(
            "UPDATE drep_timeline_event SET drep_id = NULL \
             WHERE event_type = 'vote' \
               AND drep_id IN ('drep_always_abstain', 'drep_always_no_confidence')",
        )
        .execute(pool)
        .await?;
        r.rows_affected() as i64
    };

    if dry_run {
        info!("reconciler fix 1 (dry run): {} constant-DRep vote rows would be NULLed", fix1_count);
    } else {
        info!("reconciler fix 1: NULLed {} constant-DRep vote drep_ids", fix1_count);
    }

    // Fix 2: Recompute previous_drep using a slot-ordered LAG window.
    // Parallel historical sync caused cache races where workers on later epochs
    // could write to the shared LRU before workers on earlier epochs read it,
    // producing previous_drep values from the future.
    let fix2_count: i64 = if dry_run {
        sqlx::query_scalar(
            r#"
            WITH corrected AS (
                SELECT id,
                    LAG(drep_id) OVER (PARTITION BY stake_address ORDER BY slot) AS correct_prev
                FROM drep_timeline_event
                WHERE event_type = 'delegation'
            )
            SELECT COUNT(*) FROM corrected c
            JOIN drep_timeline_event dte ON dte.id = c.id
            WHERE dte.previous_drep IS DISTINCT FROM c.correct_prev
            "#,
        )
        .fetch_one(pool)
        .await?
    } else {
        let r = sqlx::query(
            r#"
            WITH corrected AS (
                SELECT id,
                    LAG(drep_id) OVER (PARTITION BY stake_address ORDER BY slot) AS correct_prev
                FROM drep_timeline_event
                WHERE event_type = 'delegation'
            )
            UPDATE drep_timeline_event dte
            SET previous_drep = c.correct_prev
            FROM corrected c
            WHERE dte.id = c.id
              AND dte.event_type = 'delegation'
              AND (dte.previous_drep IS DISTINCT FROM c.correct_prev)
            "#,
        )
        .execute(pool)
        .await?;
        r.rows_affected() as i64
    };

    if dry_run {
        info!("reconciler fix 2 (dry run): {} previous_drep values would be recomputed", fix2_count);
    } else {
        info!("reconciler fix 2: recomputed {} previous_drep values", fix2_count);
    }

    Ok(())
}

fn reconciler_policy() -> Policy {
    Policy {
        tick_timeout: Some(Duration::from_secs(60)),
        bootstrap_retry: retries::Policy::no_retry(),
        work_retry: retries::Policy {
            max_retries: 5,
            backoff_unit: Duration::from_secs(2),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(30),
            dismissible: false,
        },
        teardown_retry: retries::Policy::no_retry(),
    }
}

fn run_orphan_pipeline(
    pool: PgPool,
    since_slot: Option<u64>,
    blockfrost_key: String,
    dry_run: bool,
) -> anyhow::Result<()> {
    let (suspect_tx, suspect_rx) = mpsc::channel(64);
    let (verified_tx, verified_rx) = mpsc::channel(64);

    let audit = AuditStage {
        pool: pool.clone(),
        since_slot,
        output: suspect_tx,
        suspects_found: Counter::default(),
    };

    let fetch = FetchStage {
        api_key: blockfrost_key,
        input: suspect_rx,
        output: verified_tx,
        interval_ms: 200, // 5 req/s max
        checked: Counter::default(),
        orphans_found: Counter::default(),
    };

    let patch = PatchStage {
        pool,
        dry_run,
        input: verified_rx,
        orphans_deleted: Counter::default(),
    };

    let audit_tether = spawn_stage(audit, reconciler_policy());
    let fetch_tether = spawn_stage(fetch, reconciler_policy());
    let patch_tether = spawn_stage(patch, reconciler_policy());

    info!("reconciler: orphan pipeline running...");

    loop {
        let patch_done = matches!(
            patch_tether.check_state(),
            TetherState::Dropped | TetherState::Alive(gasket::runtime::StagePhase::Ended)
        );
        if patch_done {
            info!("reconciler: orphan pipeline complete");
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    for tether in [&audit_tether, &fetch_tether, &patch_tether] {
        tether.dismiss_stage().ok();
    }

    audit_tether.join_stage();
    fetch_tether.join_stage();
    patch_tether.join_stage();

    Ok(())
}
