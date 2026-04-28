use std::time::Duration;

use async_trait::async_trait;
use pallas::ledger::traverse::MultiEraBlock;
use pallas::network::facades::PeerClient;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::pipeline::types::{EpochRange, RawBlock};
use crate::utils::{slot_to_epoch, slot_to_timestamp};
use gasket::framework::{AsWorkError, Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;

// ---------------------------------------------------------------------------
// Stage definition
// ---------------------------------------------------------------------------

pub struct HistoricalFetcherStage {
    /// Ranges partitioned into N groups (one per worker goroutine).
    pub partitioned_ranges: Vec<Vec<EpochRange>>,
    pub relay_addresses: Vec<String>,
    pub magic: u64,
    /// Sends RawBlocks downstream to GovernanceParser.
    pub output: mpsc::Sender<RawBlock>,
    pub blocks_fetched: Counter,
    pub ranges_completed: Counter,
}

impl Stage for HistoricalFetcherStage {
    type Unit = ();
    type Worker = HistoricalFetcherWorker;

    fn name(&self) -> &str {
        "historical-fetcher"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("blocks_fetched", &self.blocks_fetched);
        r.track_counter("ranges_completed", &self.ranges_completed);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker definition
// ---------------------------------------------------------------------------

pub struct HistoricalFetcherWorker {
    scheduled: bool,
}

#[async_trait(?Send)]
impl Worker<HistoricalFetcherStage> for HistoricalFetcherWorker {
    async fn bootstrap(_stage: &HistoricalFetcherStage) -> Result<Self, WorkerError> {
        Ok(HistoricalFetcherWorker { scheduled: false })
    }

    async fn schedule(
        &mut self,
        _stage: &mut HistoricalFetcherStage,
    ) -> Result<WorkSchedule<()>, WorkerError> {
        if self.scheduled {
            Ok(WorkSchedule::Done)
        } else {
            self.scheduled = true;
            Ok(WorkSchedule::Unit(()))
        }
    }

    /// Spawns N concurrent tasks, one per partition, each fetching its epoch ranges.
    /// Blocks until all tasks complete. The output Sender is dropped when this stage
    /// ends, closing the channel and signalling GovernanceParser to finish.
    async fn execute(
        &mut self,
        _unit: &(),
        stage: &mut HistoricalFetcherStage,
    ) -> Result<(), WorkerError> {
        let mut handles = Vec::new();

        for partition in stage.partitioned_ranges.iter() {
            if partition.is_empty() {
                continue;
            }

            let ranges = partition.clone();
            let relays = stage.relay_addresses.clone();
            let magic = stage.magic;
            let output = stage.output.clone();
            let blocks_counter = stage.blocks_fetched.clone();
            let ranges_counter = stage.ranges_completed.clone();

            let handle = tokio::spawn(async move {
                fetch_partition(ranges, relays, magic, output, blocks_counter, ranges_counter).await
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.or_panic()?.or_retry()?;
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal fetch logic
// ---------------------------------------------------------------------------

async fn connect_relay(relays: &[String], magic: u64) -> anyhow::Result<PeerClient> {
    for relay in relays {
        match tokio::time::timeout(
            Duration::from_secs(30),
            PeerClient::connect(relay, magic),
        )
        .await
        {
            Ok(Ok(peer)) => return Ok(peer),
            Ok(Err(e)) => warn!("connect to {} failed: {}", relay, e),
            Err(_) => warn!("connect to {} timed out", relay),
        }
    }
    anyhow::bail!("all relay addresses failed")
}

async fn fetch_partition(
    ranges: Vec<EpochRange>,
    relays: Vec<String>,
    magic: u64,
    output: mpsc::Sender<RawBlock>,
    blocks_counter: Counter,
    ranges_counter: Counter,
) -> anyhow::Result<()> {
    for range in &ranges {
        fetch_range(range, &relays, magic, &output, &blocks_counter).await?;
        ranges_counter.inc(1);
    }
    Ok(())
}

async fn fetch_range(
    range: &EpochRange,
    relays: &[String],
    magic: u64,
    output: &mpsc::Sender<RawBlock>,
    blocks_counter: &Counter,
) -> anyhow::Result<()> {
    let mut attempts = 0;

    loop {
        attempts += 1;

        let mut peer = match connect_relay(relays, magic).await {
            Ok(p) => p,
            Err(e) => {
                if attempts > 5 {
                    return Err(e);
                }
                warn!("worker {} relay connect failed, retrying in 5s", range.worker_id);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        let client = peer.blockfetch();

        match client
            .request_range((range.start.clone(), range.end.clone()))
            .await
        {
            Ok(_) => {}
            Err(e) => {
                let msg = format!("{:?}", e);
                if msg.contains("doesn't contain any blocks") || msg.contains("no blocks") {
                    info!("worker {}: empty range, skipping", range.worker_id);
                    return Ok(());
                }
                warn!("worker {}: request_range failed: {:?}", range.worker_id, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }

        let mut connection_ok = true;

        loop {
            match tokio::time::timeout(Duration::from_secs(60), client.recv_while_streaming()).await
            {
                Ok(Ok(Some(body))) => {
                    let block = MultiEraBlock::decode(&body).map_err(|e| {
                        warn!("block decode failed in {}: {}", range.worker_id, e);
                        e
                    })?;

                    let slot = block.slot();
                    let raw_block = RawBlock {
                        slot,
                        body,
                        epoch: slot_to_epoch(slot),
                        timestamp: slot_to_timestamp(slot),
                    };

                    if output.send(raw_block).await.is_err() {
                        // Downstream closed — pipeline is shutting down.
                        return Ok(());
                    }

                    blocks_counter.inc(1);
                }
                Ok(Ok(None)) => break, // end of range stream
                Ok(Err(e)) => {
                    warn!("worker {}: stream error: {:?}", range.worker_id, e);
                    connection_ok = false;
                    break;
                }
                Err(_) => {
                    warn!("worker {}: stream timed out", range.worker_id);
                    connection_ok = false;
                    break;
                }
            }
        }

        if connection_ok {
            return Ok(());
        }

        // Connection dropped mid-range — reconnect and restart range from beginning.
        // ON CONFLICT DO NOTHING makes this safe.
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
