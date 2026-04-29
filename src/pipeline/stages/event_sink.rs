use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::pipeline::types::ChainEvent;
use crate::sinks::SinkSet;
use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

// ---------------------------------------------------------------------------
// Stage definition
// ---------------------------------------------------------------------------

pub struct EventSinkStage {
    pub sinks: SinkSet,
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
                let mut had_error = false;

                for sink in &stage.sinks {
                    if let Err(e) = sink.handle_block(parsed).await {
                        error!("sink failed at slot {}: {}", parsed.slot, e);
                        had_error = true;
                    }
                }

                if had_error {
                    return Err(WorkerError::Retry);
                }

                stage.blocks_committed.inc(1);
                stage.events_persisted.inc(event_count as u64);
            }
            ChainEvent::Rollback { to_slot } => {
                warn!("chain rollback to slot {}", to_slot);
                let mut had_error = false;

                for sink in &stage.sinks {
                    if let Err(e) = sink.handle_rollback(*to_slot).await {
                        error!("sink rollback failed: {}", e);
                        had_error = true;
                    }
                }

                if had_error {
                    return Err(WorkerError::Retry);
                }

                info!("rollback to slot {} handled by {} sink(s)", to_slot, stage.sinks.len());
                stage.rollbacks_handled.inc(1);
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
