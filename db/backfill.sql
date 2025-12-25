-- Backfill migration: Populate stake_address and previous_drep from metadata JSONB
-- This is critical for the idx_drep_timeline_stake_lookup index to be used.

-- 1. Backfill stake_address for delegation events
UPDATE drep_timeline_event
SET stake_address = metadata->>'stake_address'
WHERE event_type = 'delegation' 
  AND stake_address IS NULL;

-- 2. Backfill previous_drep for delegation events
UPDATE drep_timeline_event
SET previous_drep = metadata->>'previous_drep'
WHERE event_type = 'delegation'
  AND previous_drep IS NULL;

-- 3. Verify index usage with a dummy plan check (not a real query, just a note for the dev)
-- EXPLAIN ANALYZE SELECT previous_drep FROM drep_timeline_event 
-- WHERE stake_address = 'some_stake_address' AND event_type = 'delegation' 
-- ORDER BY timestamp DESC LIMIT 1;
