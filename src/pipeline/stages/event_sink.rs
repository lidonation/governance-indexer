use async_trait::async_trait;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::db;
use crate::pipeline::types::ChainEvent;
use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

// ---------------------------------------------------------------------------
// Stage definition
// ---------------------------------------------------------------------------

pub struct EventSinkStage {
    pub pool: PgPool,
    pub input: mpsc::Receiver<ChainEvent>,
    pub blocks_committed: Counter,
    pub events_persisted: Counter,
    pub rollbacks_handled: Counter,
}

impl Stage for EventSinkStage {
    type Unit = ChainEvent;
    type Worker = EventSinkWorker;

    fn name(&self) -> &str {
        "event-sink"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("blocks_committed", &self.blocks_committed);
        r.track_counter("events_persisted", &self.events_persisted);
        r.track_counter("rollbacks_handled", &self.rollbacks_handled);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker definition
// ---------------------------------------------------------------------------

pub struct EventSinkWorker;

#[async_trait(?Send)]
impl Worker<EventSinkStage> for EventSinkWorker {
    async fn bootstrap(_stage: &EventSinkStage) -> Result<Self, WorkerError> {
        Ok(EventSinkWorker)
    }

    async fn schedule(
        &mut self,
        stage: &mut EventSinkStage,
    ) -> Result<WorkSchedule<ChainEvent>, WorkerError> {
        match stage.input.recv().await {
            Some(event) => Ok(WorkSchedule::Unit(event)),
            None => Ok(WorkSchedule::Done),
        }
    }

    async fn execute(
        &mut self,
        unit: &ChainEvent,
        stage: &mut EventSinkStage,
    ) -> Result<(), WorkerError> {
        match unit {
            ChainEvent::Block(parsed) => {
                let event_count = parsed.events.len();

                match db::persist_parsed_block(&stage.pool, parsed).await {
                    Ok(_) => {
                        stage.blocks_committed.inc(1);
                        stage.events_persisted.inc(event_count as u64);
                    }
                    Err(e) => {
                        error!("persist_parsed_block failed at slot {}: {}", parsed.slot, e);
                        return Err(WorkerError::Retry);
                    }
                }
            }
            ChainEvent::Rollback { to_slot } => {
                warn!("chain rollback to slot {}", to_slot);
                match db::delete_after_slot(&stage.pool, *to_slot).await {
                    Ok(deleted) => {
                        info!("rollback: deleted {} events after slot {}", deleted, to_slot);
                        stage.rollbacks_handled.inc(1);
                    }
                    Err(e) => {
                        error!("rollback delete failed: {}", e);
                        return Err(WorkerError::Retry);
                    }
                }
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
