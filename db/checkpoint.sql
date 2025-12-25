-- Sync Checkpointing (added 2025-12-24)
CREATE TABLE IF NOT EXISTS sync_checkpoint (
    worker_id TEXT PRIMARY KEY,
    last_slot BIGINT NOT NULL,
    last_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
