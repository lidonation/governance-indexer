use sqlx::PgPool;
use tracing::info;

pub struct ReconcileConfig {
    pub dry_run: bool,
}

pub async fn run_reconciler(pool: PgPool, config: ReconcileConfig) -> anyhow::Result<()> {
    info!("reconciler: starting");
    run_sql_fixes(&pool, config.dry_run).await?;
    info!("reconciler: done");
    Ok(())
}

async fn run_sql_fixes(pool: &PgPool, dry_run: bool) -> anyhow::Result<()> {
    // Fix 1: NULL out constant-DRep drep_ids on vote rows.
    // The old parser set drep_id = "drep_always_abstain" / "drep_always_no_confidence"
    // on votes cast by those pseudo-DReps. Those are not real DRep IDs.
    let fix1 = if dry_run {
        sqlx::query_scalar::<_, i64>(
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
        info!("fix 1 (dry run): {} vote rows would have drep_id NULLed", fix1);
    } else {
        info!("fix 1: NULLed drep_id on {} vote rows", fix1);
    }

    // Fix 2: Recompute previous_drep via slot-ordered LAG window.
    // Parallel historical workers had a shared LRU cache race: a worker on a
    // later epoch could cache a drep_id before an earlier-epoch worker read it,
    // producing previous_drep values from the wrong point in time.
    let fix2 = if dry_run {
        sqlx::query_scalar::<_, i64>(
            r#"
            WITH corrected AS (
                SELECT id,
                    LAG(drep_id) OVER (PARTITION BY stake_address ORDER BY slot) AS correct_prev
                FROM drep_timeline_event
                WHERE event_type = 'delegation'
            )
            SELECT COUNT(*) FROM corrected c
            JOIN drep_timeline_event e ON e.id = c.id
            WHERE e.previous_drep IS DISTINCT FROM c.correct_prev
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
            UPDATE drep_timeline_event e
            SET previous_drep = c.correct_prev
            FROM corrected c
            WHERE e.id = c.id
              AND e.event_type = 'delegation'
              AND (e.previous_drep IS DISTINCT FROM c.correct_prev)
            "#,
        )
        .execute(pool)
        .await?;
        r.rows_affected() as i64
    };

    if dry_run {
        info!("fix 2 (dry run): {} delegation rows would have previous_drep recomputed", fix2);
    } else {
        info!("fix 2: recomputed previous_drep on {} delegation rows", fix2);
    }

    Ok(())
}
