-- Denormalization migration (added 2025-12-25)
-- Purpose: Extract stake_address into its own column for blazing fast delegation lookups.

ALTER TABLE drep_timeline_event ADD COLUMN IF NOT EXISTS stake_address TEXT;
ALTER TABLE drep_timeline_event ADD COLUMN IF NOT EXISTS previous_drep TEXT;

-- Index for the lookup: "Who was this user delegated to before?"
CREATE INDEX IF NOT EXISTS idx_drep_timeline_stake_lookup 
ON drep_timeline_event (stake_address, timestamp DESC) 
WHERE event_type = 'delegation';

-- Drop the old expensive JSONB functional index
DROP INDEX IF EXISTS idx_drep_timeline_stake_address;
