use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;

use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

use crate::reconciler::audit::SuspectRecord;
use crate::reconciler::blockfrost::BlockfrostClient;

/// A verified record — either confirmed on-chain or identified as an orphan.
#[derive(Debug, Clone)]
pub struct VerifiedRecord {
    pub tx_hash: String,
    pub is_orphan: bool,
}

// ---------------------------------------------------------------------------
// Stage
// ---------------------------------------------------------------------------

pub struct FetchStage {
    pub api_key: String,
    pub input: mpsc::Receiver<SuspectRecord>,
    pub output: mpsc::Sender<VerifiedRecord>,
    /// Minimum ms between Blockfrost requests (rate limiting).
    pub interval_ms: u64,
    pub checked: Counter,
    pub orphans_found: Counter,
}

impl Stage for FetchStage {
    type Unit = SuspectRecord;
    type Worker = FetchWorker;

    fn name(&self) -> &str {
        "blockfrost-fetch"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("checked", &self.checked);
        r.track_counter("orphans_found", &self.orphans_found);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

pub struct FetchWorker {
    client: BlockfrostClient,
}

#[async_trait(?Send)]
impl Worker<FetchStage> for FetchWorker {
    async fn bootstrap(stage: &FetchStage) -> Result<Self, WorkerError> {
        Ok(FetchWorker {
            client: BlockfrostClient::new(stage.api_key.clone()),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut FetchStage,
    ) -> Result<WorkSchedule<SuspectRecord>, WorkerError> {
        match stage.input.recv().await {
            Some(record) => Ok(WorkSchedule::Unit(record)),
            None => Ok(WorkSchedule::Done),
        }
    }

    async fn execute(
        &mut self,
        unit: &SuspectRecord,
        stage: &mut FetchStage,
    ) -> Result<(), WorkerError> {
        // Rate-limit requests to stay within Blockfrost free tier.
        tokio::time::sleep(Duration::from_millis(stage.interval_ms)).await;

        let is_orphan = match self.client.tx_exists(&unit.tx_hash).await {
            Ok(exists) => !exists,
            Err(e) => {
                warn!("fetch: Blockfrost error for {}: {}", unit.tx_hash, e);
                return Err(WorkerError::Retry);
            }
        };

        stage.checked.inc(1);
        if is_orphan {
            stage.orphans_found.inc(1);
        }

        let verified = VerifiedRecord {
            tx_hash: unit.tx_hash.clone(),
            is_orphan,
        };

        stage
            .output
            .send(verified)
            .await
            .map_err(|_| WorkerError::Send)?;

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
