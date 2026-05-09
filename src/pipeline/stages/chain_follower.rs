use std::time::Duration;

use async_trait::async_trait;
use hex;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader};
use pallas::network::facades::PeerClient;
use pallas::network::miniprotocols::{Point, chainsync};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::db;
use crate::parser;
use crate::pipeline::types::{ChainEvent, ParsedBlock};
use crate::utils::{slot_to_epoch, slot_to_timestamp};
use gasket::framework::{AsWorkError, Stage, WorkSchedule, Worker, WorkerError};
use gasket::metrics::Counter;
use sqlx::PgPool;

// How many block headers to collect from chainsync before issuing one request_range.
// Higher = more efficient during catch-up. Lower = shorter feedback loops at tip.
const BATCH_SIZE: usize = 128;

// ---------------------------------------------------------------------------
// Unit type: what schedule() hands to execute()
// ---------------------------------------------------------------------------

pub enum ChainAction {
    // (slot, hash) pairs collected from chainsync headers
    ForwardBatch(Vec<(u64, Vec<u8>)>),
    Rollback { to_slot: u64 },
}

// ---------------------------------------------------------------------------
// Stage definition
// ---------------------------------------------------------------------------

pub struct ChainFollowerStage {
    pub relay_addresses: Vec<String>,
    pub magic: u64,
    pub start_point: Point,
    pub interval_secs: u64,
    pub pool: PgPool,
    pub worker_id: String,
    pub output: mpsc::Sender<ChainEvent>,
    pub blocks_followed: Counter,
    pub rollbacks_seen: Counter,
}

impl Stage for ChainFollowerStage {
    type Unit = ChainAction;
    type Worker = ChainFollowerWorker;

    fn name(&self) -> &str {
        "chain-follower"
    }

    fn metrics(&self) -> gasket::metrics::Registry {
        let mut r = gasket::metrics::Registry::default();
        r.track_counter("blocks_followed", &self.blocks_followed);
        r.track_counter("rollbacks_seen", &self.rollbacks_seen);
        r
    }
}

// ---------------------------------------------------------------------------
// Worker definition
// ---------------------------------------------------------------------------

pub struct ChainFollowerWorker {
    chainsync_peer: Option<PeerClient>,
    blockfetch_peer: Option<PeerClient>,
    at_tip: bool,
    // A rollback received mid-batch is stored here and emitted on the next schedule() call,
    // after the in-flight batch has been flushed.
    pending_rollback: Option<u64>,
}

impl ChainFollowerWorker {
    async fn connect(stage: &ChainFollowerStage) -> anyhow::Result<(PeerClient, PeerClient)> {
        let mut cs_peer = None;
        let mut bf_peer = None;

        for relay in &stage.relay_addresses {
            if cs_peer.is_none() {
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    PeerClient::connect(relay, stage.magic),
                )
                .await
                {
                    Ok(Ok(p)) => cs_peer = Some(p),
                    Ok(Err(e)) => warn!("chainsync connect to {} failed: {}", relay, e),
                    Err(_) => warn!("chainsync connect to {} timed out", relay),
                }
            }
            if bf_peer.is_none() {
                match tokio::time::timeout(
                    Duration::from_secs(30),
                    PeerClient::connect(relay, stage.magic),
                )
                .await
                {
                    Ok(Ok(p)) => bf_peer = Some(p),
                    Ok(Err(e)) => warn!("blockfetch connect to {} failed: {}", relay, e),
                    Err(_) => warn!("blockfetch connect to {} timed out", relay),
                }
            }
            if cs_peer.is_some() && bf_peer.is_some() {
                break;
            }
        }

        match (cs_peer, bf_peer) {
            (Some(cs), Some(bf)) => Ok((cs, bf)),
            _ => anyhow::bail!("failed to connect both chainsync and blockfetch peers"),
        }
    }
}

#[async_trait(?Send)]
impl Worker<ChainFollowerStage> for ChainFollowerWorker {
    async fn bootstrap(stage: &ChainFollowerStage) -> Result<Self, WorkerError> {
        let (mut cs_peer, bf_peer) = Self::connect(stage).await.or_restart()?;

        // On reconnect, resume from the latest DB checkpoint rather than the original
        // start_point. Without this, every connection drop replays from the beginning
        // of continuous sync instead of the last persisted slot.
        let resume_point = match db::get_checkpoint(&stage.pool, &stage.worker_id).await {
            Ok(Some((slot, hash_hex))) => match hex::decode(&hash_hex) {
                Ok(hash) => {
                    info!("chain-follower: reconnecting from checkpoint at slot {}", slot);
                    Point::Specific(slot, hash)
                }
                Err(_) => stage.start_point.clone(),
            },
            _ => stage.start_point.clone(),
        };

        let intersection = cs_peer
            .chainsync()
            .find_intersect(vec![resume_point])
            .await
            .or_restart()?;

        info!("chain-follower: intersection at {:?}", intersection);

        Ok(ChainFollowerWorker {
            chainsync_peer: Some(cs_peer),
            blockfetch_peer: Some(bf_peer),
            at_tip: false,
            pending_rollback: None,
        })
    }

    /// Collect up to BATCH_SIZE block headers from chainsync, then return the batch.
    /// This lets execute() use request_range for the whole batch instead of fetch_single
    /// per block, which is critical for catch-up when the chain follower is many blocks behind.
    async fn schedule(
        &mut self,
        stage: &mut ChainFollowerStage,
    ) -> Result<WorkSchedule<ChainAction>, WorkerError> {
        // A rollback received while a batch was in progress — emit it now.
        if let Some(to_slot) = self.pending_rollback.take() {
            return Ok(WorkSchedule::Unit(ChainAction::Rollback { to_slot }));
        }

        let mut batch: Vec<(u64, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);

        loop {
            let next = match tokio::time::timeout(
                Duration::from_secs(60),
                self.chainsync_peer.as_mut().unwrap().chainsync().request_next(),
            )
            .await
            {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    warn!("chain-follower: request_next error: {}", e);
                    return Err(WorkerError::Restart);
                }
                Err(_) => {
                    warn!("chain-follower: request_next timed out");
                    return Err(WorkerError::Restart);
                }
            };

            match next {
                chainsync::NextResponse::RollForward(header_content, _tip) => {
                    let cbor_bytes = &header_content.cbor;
                    let subtag = header_content.byron_prefix.map(|(tag, _)| tag);

                    match MultiEraHeader::decode(header_content.variant, subtag, cbor_bytes) {
                        Ok(header) => {
                            batch.push((header.slot(), header.hash().to_vec()));
                            if batch.len() >= BATCH_SIZE {
                                return Ok(WorkSchedule::Unit(ChainAction::ForwardBatch(batch)));
                            }
                        }
                        Err(e) => {
                            warn!("chain-follower: header decode failed: {}", e);
                            // Skip — likely a Byron-era edge case. Continue collecting.
                        }
                    }
                }

                chainsync::NextResponse::RollBackward(point, _tip) => {
                    let to_slot = match &point {
                        Point::Specific(s, _) => *s,
                        Point::Origin => 0,
                    };
                    if batch.is_empty() {
                        return Ok(WorkSchedule::Unit(ChainAction::Rollback { to_slot }));
                    } else {
                        // Flush the batch first; store rollback for the next schedule() call.
                        self.pending_rollback = Some(to_slot);
                        return Ok(WorkSchedule::Unit(ChainAction::ForwardBatch(batch)));
                    }
                }

                chainsync::NextResponse::Await => {
                    if !self.at_tip {
                        self.at_tip = true;
                        info!("chain-follower: REACHED TIP — now following chain in real-time");
                    }
                    if batch.is_empty() {
                        tokio::time::sleep(Duration::from_secs(stage.interval_secs)).await;
                        return Ok(WorkSchedule::Idle);
                    } else {
                        // Flush the partial batch before sleeping.
                        return Ok(WorkSchedule::Unit(ChainAction::ForwardBatch(batch)));
                    }
                }
            }
        }
    }

    async fn execute(
        &mut self,
        unit: &ChainAction,
        stage: &mut ChainFollowerStage,
    ) -> Result<(), WorkerError> {
        match unit {
            ChainAction::ForwardBatch(headers) => {
                if headers.is_empty() {
                    return Ok(());
                }

                let start = Point::Specific(headers[0].0, headers[0].1.clone());
                let end = Point::Specific(
                    headers.last().unwrap().0,
                    headers.last().unwrap().1.clone(),
                );

                let client = self.blockfetch_peer.as_mut().unwrap().blockfetch();

                match client.request_range((start, end)).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("chain-follower: request_range failed: {:?} — reconnecting", e);
                        return Err(WorkerError::Restart);
                    }
                }

                loop {
                    match tokio::time::timeout(
                        Duration::from_secs(60),
                        client.recv_while_streaming(),
                    )
                    .await
                    {
                        Ok(Ok(Some(body))) => {
                            let block = match MultiEraBlock::decode(&body) {
                                Ok(b) => b,
                                Err(e) => {
                                    warn!("chain-follower: block decode failed: {}", e);
                                    continue;
                                }
                            };

                            let slot = block.slot();
                            let events = parser::extract_governance(&body);
                            let epoch = slot_to_epoch(slot);
                            let timestamp = slot_to_timestamp(slot);

                            let parsed = ParsedBlock {
                                slot,
                                epoch,
                                timestamp,
                                events,
                            };

                            if let Err(e) = db::persist_parsed_block(&stage.pool, &parsed).await {
                                error!("chain-follower: persist failed at slot {}: {}", slot, e);
                                return Err(WorkerError::Retry);
                            }

                            stage
                                .output
                                .send(ChainEvent::Block(parsed))
                                .await
                                .map_err(|_| WorkerError::Send)?;

                            stage.blocks_followed.inc(1);
                        }
                        Ok(Ok(None)) => break, // range fully received
                        Ok(Err(e)) => {
                            warn!("chain-follower: stream error: {:?} — reconnecting", e);
                            return Err(WorkerError::Restart);
                        }
                        Err(_) => {
                            warn!("chain-follower: stream timed out — reconnecting");
                            return Err(WorkerError::Restart);
                        }
                    }
                }

                // Checkpoint at the end of the batch.
                let (last_slot, last_hash) = headers.last().unwrap();
                let hash_hex = hex::encode(last_hash);
                if let Err(e) =
                    db::update_checkpoint(&stage.pool, &stage.worker_id, *last_slot, &hash_hex)
                        .await
                {
                    warn!("chain-follower: checkpoint update failed: {}", e);
                }
            }

            ChainAction::Rollback { to_slot } => {
                warn!("chain-follower: rollback to slot {}", to_slot);

                if let Err(e) = db::delete_after_slot(&stage.pool, *to_slot).await {
                    error!("chain-follower: rollback delete failed: {}", e);
                    return Err(WorkerError::Retry);
                }

                if let Err(e) =
                    db::update_checkpoint_slot(&stage.pool, &stage.worker_id, *to_slot).await
                {
                    warn!("chain-follower: checkpoint rollback failed: {}", e);
                }

                stage
                    .output
                    .send(ChainEvent::Rollback { to_slot: *to_slot })
                    .await
                    .map_err(|_| WorkerError::Send)?;

                stage.rollbacks_seen.inc(1);
            }
        }

        Ok(())
    }

    async fn teardown(&mut self) -> Result<(), WorkerError> {
        if let Some(peer) = self.chainsync_peer.take() {
            peer.abort().await;
        }
        if let Some(peer) = self.blockfetch_peer.take() {
            peer.abort().await;
        }
        Ok(())
    }
}
