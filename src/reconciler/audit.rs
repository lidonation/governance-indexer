use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::info;

use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

/// A distinct tx_hash that needs to be verified against Blockfrost.
#[derive(Debug, Clone)]
pub struct SuspectRecord {
    pub tx_hash: String,
}

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

pub struct AuditStage {
    pub pool: PgPool,
    pub since_slot: Option<u64>,
    pub output: mpsc::Sender<SuspectRecord>,
    pub suspects_found: Counter,
}

impl Stage for AuditStage {
    type Unit = ();
    type Worker = AuditWorker;

    fn name(&self) -> &str {
        "audit"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("suspects_found", &self.suspects_found);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub struct AuditWorker {
    scheduled: bool,
}

#[async_trait(?Send)]
impl Worker<AuditStage> for AuditWorker {
    async fn bootstrap(_stage: &AuditStage) -> Result<Self, WorkerError> {
        Ok(AuditWorker { scheduled: false })
    }

    async fn schedule(
        &mut self,
        _stage: &mut AuditStage,
    ) -> Result<WorkSchedule<()>, WorkerError> {
        if self.scheduled {
            Ok(WorkSchedule::Done)
        } else {
            self.scheduled = true;
            Ok(WorkSchedule::Unit(()))
        }
    }

    /// Queries distinct tx_hashes from the DB and sends them to the Fetch stage.
    async fn execute(&mut self, _unit: &(), stage: &mut AuditStage) -> Result<(), WorkerError> {
        let rows: Vec<(String,)> = if let Some(since) = stage.since_slot {
            sqlx::query_as(
                "SELECT DISTINCT tx_hash FROM drep_timeline_event WHERE slot >= $1 ORDER BY tx_hash",
            )
            .bind(since as i64)
            .fetch_all(&stage.pool)
            .await
            .map_err(|e| {
                tracing::error!("audit: DB query failed: {}", e);
                WorkerError::Retry
            })?
        } else {
            sqlx::query_as(
                "SELECT DISTINCT tx_hash FROM drep_timeline_event ORDER BY tx_hash",
            )
            .fetch_all(&stage.pool)
            .await
            .map_err(|e| {
                tracing::error!("audit: DB query failed: {}", e);
                WorkerError::Retry
            })?
        };

        info!("audit: found {} distinct tx_hashes to verify", rows.len());

        for (tx_hash,) in rows {
            let record = SuspectRecord { tx_hash };
            stage.suspects_found.inc(1);
            if stage.output.send(record).await.is_err() {
                // Downstream closed — pipeline shutting down.
                return Ok(());
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
