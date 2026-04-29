use async_trait::async_trait;
use sqlx::PgPool;
use tracing::error;

use crate::db;
use crate::pipeline::types::ParsedBlock;
use crate::sinks::EventSink;

pub struct PostgresSink {
    pool: PgPool,
}

impl PostgresSink {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventSink for PostgresSink {
    async fn handle_block(&self, block: &ParsedBlock) -> anyhow::Result<()> {
        db::persist_parsed_block(&self.pool, block).await
    }

    async fn handle_rollback(&self, to_slot: u64) -> anyhow::Result<()> {
        let deleted = db::delete_after_slot(&self.pool, to_slot).await?;
        if deleted > 0 {
            error!("postgres-sink: rollback deleted {} events after slot {}", deleted, to_slot);
        }
        Ok(())
    }
}
