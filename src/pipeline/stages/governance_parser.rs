use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::warn;

use crate::parser;
use crate::pipeline::types::{ChainEvent, ParsedBlock, RawBlock};
use gasket::framework::{Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

// ---------------------------------------------------------------------------
// Stage definition
// ---------------------------------------------------------------------------

pub struct GovernanceParserStage {
    pub input: mpsc::Receiver<RawBlock>,
    pub output: mpsc::Sender<ChainEvent>,
    pub blocks_parsed: Counter,
    pub events_found: Counter,
}

impl Stage for GovernanceParserStage {
    type Unit = RawBlock;
    type Worker = GovernanceParserWorker;

    fn name(&self) -> &str {
        "governance-parser"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("blocks_parsed", &self.blocks_parsed);
        r.track_counter("events_found", &self.events_found);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker definition
// ---------------------------------------------------------------------------

pub struct GovernanceParserWorker;

#[async_trait(?Send)]
impl Worker<GovernanceParserStage> for GovernanceParserWorker {
    async fn bootstrap(_stage: &GovernanceParserStage) -> Result<Self, WorkerError> {
        Ok(GovernanceParserWorker)
    }

    async fn schedule(
        &mut self,
        stage: &mut GovernanceParserStage,
    ) -> Result<WorkSchedule<RawBlock>, WorkerError> {
        match stage.input.recv().await {
            Some(raw_block) => Ok(WorkSchedule::Unit(raw_block)),
            None => Ok(WorkSchedule::Done),
        }
    }

    async fn execute(
        &mut self,
        unit: &RawBlock,
        stage: &mut GovernanceParserStage,
    ) -> Result<(), WorkerError> {
        if unit.body.is_empty() {
            warn!("empty block body at slot {}", unit.slot);
            stage.blocks_parsed.inc(1);
            return Ok(());
        }

        let events = parser::extract_governance(&unit.body);
        let event_count = events.len();

        stage.blocks_parsed.inc(1);

        // Skip blocks with no governance events — don't waste channel + sink capacity.
        if events.is_empty() {
            return Ok(());
        }

        stage.events_found.inc(event_count as u64);

        let parsed = ParsedBlock {
            slot: unit.slot,
            epoch: unit.epoch,
            timestamp: unit.timestamp,
            events,
        };

        stage
            .output
            .send(ChainEvent::Block(parsed))
            .await
            .map_err(|_| WorkerError::Send)?;

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}
