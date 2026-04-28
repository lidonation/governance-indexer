use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{info, warn};

use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

use crate::reconciler::fetch::VerifiedRecord;

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

pub struct PatchStage {
    pub pool: PgPool,
    pub dry_run: bool,
    pub input: mpsc::Receiver<VerifiedRecord>,
    pub orphans_deleted: Counter,
}

impl Stage for PatchStage {
    type Unit = VerifiedRecord;
    type Worker = PatchWorker;

    fn name(&self) -> &str {
        "patch"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("orphans_deleted", &self.orphans_deleted);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub struct PatchWorker;

#[async_trait(?Send)]
impl Worker<PatchStage> for PatchWorker {
    async fn bootstrap(_stage: &PatchStage) -> Result<Self, WorkerError> {
        Ok(PatchWorker)
    }

    async fn schedule(
        &mut self,
        stage: &mut PatchStage,
    ) -> Result<WorkSchedule<VerifiedRecord>, WorkerError> {
        match stage.input.recv().await {
            Some(record) => Ok(WorkSchedule::Unit(record)),
            None => Ok(WorkSchedule::Done),
        }
    }

    async fn execute(
        &mut self,
        unit: &VerifiedRecord,
        stage: &mut PatchStage,
    ) -> Result<(), WorkerError> {
        if !unit.is_orphan {
            return Ok(());
        }

        if stage.dry_run {
            info!("patch (dry run): would delete orphan tx {}", unit.tx_hash);
            return Ok(());
        }

        match sqlx::query("DELETE FROM drep_timeline_event WHERE tx_hash = $1")
            .bind(&unit.tx_hash)
            .execute(&stage.pool)
            .await
        {
            Ok(result) => {
                let deleted = result.rows_affected();
                info!("patch: deleted {} rows for orphan tx {}", deleted, unit.tx_hash);
                stage.orphans_deleted.inc(deleted);
            }
            Err(e) => {
                warn!("patch: delete failed for tx {}: {}", unit.tx_hash, e);
                return Err(WorkerError::Retry);
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
