pub mod postgres;
pub mod webhook;

use async_trait::async_trait;
use std::sync::Arc;

use crate::pipeline::types::ParsedBlock;

/// Implemented by every event destination (Postgres, Webhook, etc).
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn handle_block(&self, block: &ParsedBlock) -> anyhow::Result<()>;
    async fn handle_rollback(&self, to_slot: u64) -> anyhow::Result<()>;
}

/// A collection of sinks that receives every event.
pub type SinkSet = Vec<Arc<dyn EventSink>>;
