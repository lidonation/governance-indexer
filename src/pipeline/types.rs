use chrono::{DateTime, Utc};
use pallas::network::miniprotocols::Point;

use crate::parser::GovEvent;

/// A range of blocks to fetch during historical sync.
#[derive(Clone)]
pub struct EpochRange {
    pub start: Point,
    pub end: Point,
    pub worker_id: String,
}

/// A decoded block with its body bytes ready for parsing.
#[derive(Clone)]
pub struct RawBlock {
    pub slot: u64,
    pub body: Vec<u8>,
    pub epoch: u64,
    pub timestamp: DateTime<Utc>,
}

/// A block with governance events extracted, ready for DB persistence.
pub struct ParsedBlock {
    pub slot: u64,
    pub epoch: u64,
    pub timestamp: DateTime<Utc>,
    pub events: Vec<GovEvent>,
}

/// The unit type flowing into EventSink from both historical and continuous pipelines.
pub enum ChainEvent {
    Block(ParsedBlock),
    Rollback { to_slot: u64 },
}
