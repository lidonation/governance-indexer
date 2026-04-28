use std::time::Duration;

use async_trait::async_trait;
use hex;
use pallas::ledger::traverse::MultiEraHeader;
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

// ---------------------------------------------------------------------------
// Unit type: what schedule() hands to execute()
// ---------------------------------------------------------------------------

pub enum ChainAction {
    Forward { slot: u64, hash: Vec<u8> },
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
        let (mut cs_peer, bf_peer) =
            Self::connect(stage).await.or_restart()?;

        // Find intersection with our last known point
        let intersection = cs_peer
            .chainsync()
            .find_intersect(vec![stage.start_point.clone()])
            .await
            .or_restart()?;

        info!("chain-follower: intersection at {:?}", intersection);

        Ok(ChainFollowerWorker {
            chainsync_peer: Some(cs_peer),
            blockfetch_peer: Some(bf_peer),
        })
    }

    /// Calls request_next() and returns what schedule() found.
    /// Returns Idle when at tip (so we sleep before checking again).
    async fn schedule(
        &mut self,
        stage: &mut ChainFollowerStage,
    ) -> Result<WorkSchedule<ChainAction>, WorkerError> {
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
                        let slot = header.slot();
                        let hash = header.hash().to_vec();
                        Ok(WorkSchedule::Unit(ChainAction::Forward { slot, hash }))
                    }
                    Err(e) => {
                        warn!("chain-follower: header decode failed: {}", e);
                        // Skip this block — it might be a Byron-era edge case.
                        Ok(WorkSchedule::Idle)
                    }
                }
            }
            chainsync::NextResponse::RollBackward(point, _tip) => {
                let to_slot = match &point {
                    Point::Specific(s, _) => *s,
                    Point::Origin => 0,
                };
                Ok(WorkSchedule::Unit(ChainAction::Rollback { to_slot }))
            }
            chainsync::NextResponse::Await => {
                // At tip — sleep before polling again.
                tokio::time::sleep(Duration::from_secs(stage.interval_secs)).await;
                Ok(WorkSchedule::Idle)
            }
        }
    }

    async fn execute(
        &mut self,
        unit: &ChainAction,
        stage: &mut ChainFollowerStage,
    ) -> Result<(), WorkerError> {
        match unit {
            ChainAction::Forward { slot, hash } => {
                let point = Point::Specific(*slot, hash.clone());
                let hash_hex = hex::encode(hash);

                match self
                    .blockfetch_peer
                    .as_mut()
                    .unwrap()
                    .blockfetch()
                    .fetch_single(point.clone())
                    .await
                {
                    Ok(body) => {
                        let events = parser::extract_governance(&body);
                        let epoch = slot_to_epoch(*slot);
                        let timestamp = slot_to_timestamp(*slot);

                        let parsed = ParsedBlock {
                            slot: *slot,
                            epoch,
                            timestamp,
                            events,
                        };

                        // Persist checkpoint atomically with events inside db call.
                        if let Err(e) = db::persist_parsed_block(&stage.pool, &parsed).await {
                            error!("chain-follower: persist failed at slot {}: {}", slot, e);
                            return Err(WorkerError::Retry);
                        }

                        // Update checkpoint for continuous worker.
                        if let Err(e) =
                            db::update_checkpoint(&stage.pool, &stage.worker_id, *slot, &hash_hex)
                                .await
                        {
                            warn!("chain-follower: checkpoint update failed: {}", e);
                        }

                        // Forward the ChainEvent downstream.
                        stage
                            .output
                            .send(ChainEvent::Block(parsed))
                            .await
                            .map_err(|_| WorkerError::Send)?;

                        stage.blocks_followed.inc(1);
                    }
                    Err(e) => {
                        warn!("chain-follower: fetch_single failed at slot {}: {}", slot, e);
                        // Don't fail the stage — missed blocks will be caught on reconnect.
                    }
                }
            }

            ChainAction::Rollback { to_slot } => {
                warn!("chain-follower: rollback to slot {}", to_slot);

                if let Err(e) = db::delete_after_slot(&stage.pool, *to_slot).await {
                    error!("chain-follower: rollback delete failed: {}", e);
                    return Err(WorkerError::Retry);
                }

                // Update checkpoint to rollback point.
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
