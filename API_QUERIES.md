# API Queries for 1694.io Integration

## Database Connection

**Connection String:**
```
postgres://govindex:govindex@localhost:5433/govindex
```

---

## DRep List Queries

### 1. Get All Active DReps (Paginated)
```sql
SELECT 
    view as drep_id,
    drep_type,
    active,
    retired,
    is_active_voter,
    voting_power_ada,
    delegation_vote_count,
    governance_vote_count,
    last_vote_epoch,
    metadata_url,
    given_name,
    image_url,
    updated_at
FROM drep_frontend_snapshot
WHERE retired = false AND is_active_voter = true
ORDER BY governance_vote_count DESC
LIMIT $1 OFFSET $2;
```

### 2. Get DRep Count by Status
```sql
SELECT 
    COUNT(*) FILTER (WHERE retired = false AND is_active_voter = true) as active_voters,
    COUNT(*) FILTER (WHERE retired = false AND is_active_voter = false) as inactive,
    COUNT(*) FILTER (WHERE retired = true) as retired,
    COUNT(*) as total
FROM drep_frontend_snapshot;
```

### 3. Search DReps
```sql
SELECT 
    view as drep_id,
    drep_type,
    governance_vote_count,
    delegation_vote_count,
    is_active_voter,
    metadata_url,
    given_name
FROM drep_frontend_snapshot
WHERE view ILIKE $1 OR given_name ILIKE $1
ORDER BY governance_vote_count DESC
LIMIT 20;
```

---

## DRep Timeline Queries

### 4. Get Timeline for Specific DRep
```sql
SELECT 
    id,
    drep_view,
    type,
    timestamp,
    epoch_no,
    payload
FROM drep_timeline_event
WHERE drep_view = $1
ORDER BY timestamp DESC
LIMIT $2 OFFSET $3;
```

### 5. Get Recent Activity Across All DReps
```sql
SELECT 
    drep_view,
    type,
    timestamp,
    epoch_no,
    payload
FROM drep_timeline_event
ORDER BY timestamp DESC
LIMIT $1 OFFSET $2;
```

### 6. Get Activity by Type
```sql
SELECT 
    drep_view,
    timestamp,
    epoch_no,
    payload
FROM drep_timeline_event
WHERE drep_view = $1 AND type = $2
ORDER BY timestamp DESC;
```

---

## Governance Action Queries

### 7. Get All Proposals
```sql
SELECT 
    action_id,
    tx_hash,
    action_type,
    action_details,
    status,
    proposal_epoch,
    proposal_timestamp,
    expiration_epoch,
    deposit_lovelace,
    metadata_url
FROM governance_action
ORDER BY proposal_timestamp DESC
LIMIT $1 OFFSET $2;
```

### 8. Get Votes for a Proposal
```sql
SELECT 
    voter_credential as drep_id,
    voter_type,
    vote,
    vote_timestamp,
    vote_epoch,
    rationale_url
FROM governance_vote
WHERE action_id = $1
ORDER BY vote_timestamp DESC;
```

### 9. Get DRep Voting History
```sql
SELECT 
    gv.action_id,
    gv.vote,
    gv.vote_timestamp,
    gv.vote_epoch,
    ga.action_type,
    ga.action_details,
    ga.metadata_url as proposal_metadata
FROM governance_vote gv
JOIN governance_action ga ON gv.action_id = ga.action_id
WHERE gv.voter_credential = $1
ORDER BY gv.vote_timestamp DESC;
```

---

## Statistics Queries

### 10. DRep Activity Stats
```sql
SELECT 
    type,
    COUNT(*) as count,
    MAX(timestamp) as most_recent
FROM drep_timeline_event
WHERE drep_view = $1
GROUP BY type;
```

### 11. Delegation Distribution
```sql
SELECT 
    payload->>'target_drep' as target_drep,
    COUNT(*) as delegation_count
FROM drep_timeline_event
WHERE type = 'delegation'
GROUP BY payload->>'target_drep'
ORDER BY delegation_count DESC
LIMIT 20;
```

---

## Example API Endpoints (Node.js/Express)

```javascript
// Example using pg library
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: 'postgres://govindex:govindex@localhost:5433/govindex'
});

// GET /api/dreps - List all DReps
app.get('/api/dreps', async (req, res) => {
  const { page = 1, limit = 20, filter = 'active' } = req.query;
  const offset = (page - 1) * limit;
  
  let whereClause = '';
  if (filter === 'active') {
    whereClause = 'WHERE retired = false AND is_active_voter = true';
  } else if (filter === 'inactive') {
    whereClause = 'WHERE retired = false AND is_active_voter = false';
  } else if (filter === 'retired') {
    whereClause = 'WHERE retired = true';
  }
  
  const result = await pool.query(`
    SELECT 
      view as drep_id,
      drep_type,
      governance_vote_count,
      delegation_vote_count,
      is_active_voter,
      last_vote_epoch,
      metadata_url,
      given_name
    FROM drep_frontend_snapshot
    ${whereClause}
    ORDER BY governance_vote_count DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset]);
  
  res.json(result.rows);
});

// GET /api/dreps/:id/timeline - Get DRep timeline
app.get('/api/dreps/:id/timeline', async (req, res) => {
  const { id } = req.params;
  const { page = 1, limit = 50 } = req.query;
  const offset = (page - 1) * limit;
  
  const result = await pool.query(`
    SELECT 
      type,
      timestamp,
      epoch_no,
      payload
    FROM drep_timeline_event
    WHERE drep_view = $1
    ORDER BY timestamp DESC
    LIMIT $2 OFFSET $3
  `, [id, limit, offset]);
  
  res.json(result.rows);
});

// GET /api/proposals - List proposals
app.get('/api/proposals', async (req, res) => {
  const { page = 1, limit = 20 } = req.query;
  const offset = (page - 1) * limit;
  
  const result = await pool.query(`
    SELECT 
      action_id,
      action_type,
      status,
      proposal_timestamp,
      expiration_epoch,
      metadata_url
    FROM governance_action
    ORDER BY proposal_timestamp DESC
    LIMIT $1 OFFSET $2
  `, [limit, offset]);
  
  res.json(result.rows);
});
```

---

## Testing Queries

Run these to verify data:

```bash
# Count DReps by status
psql postgres://govindex:govindex@localhost:5433/govindex -c "
SELECT 
  COUNT(*) FILTER (WHERE retired = false AND is_active_voter = true) as active,
  COUNT(*) FILTER (WHERE retired = false AND is_active_voter = false) as inactive,
  COUNT(*) FILTER (WHERE retired = true) as retired
FROM drep_frontend_snapshot;"

# Get top 5 DReps
psql postgres://govindex:govindex@localhost:5433/govindex -c "
SELECT view, governance_vote_count, delegation_vote_count 
FROM drep_frontend_snapshot 
ORDER BY governance_vote_count DESC 
LIMIT 5;"

# Get recent timeline events
psql postgres://govindex:govindex@localhost:5433/govindex -c "
SELECT drep_view, type, timestamp 
FROM drep_timeline_event 
ORDER BY timestamp DESC 
LIMIT 10;"
```
