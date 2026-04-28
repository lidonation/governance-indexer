# Reconciler Plan — Data Discrepancy Fixes

Inventory of known data discrepancies that have accumulated since the indexer first ran, their root causes, fix strategies, and the architecture for an automated reconciler deployed as a K8s CronJob.

---

## Discrepancy Inventory

### 1. `drep_id` set on constant-DRep votes (SQL-only fix)

**Root cause:** The vote insert in `db/mod.rs` filters out constant DRep voters with:
```rust
if !voter_id.starts_with("drep-always-") {
    // set drep_id
}
```
But `format_voter()` in `parser/mod.rs` encodes them with **underscores**: `drep_always_abstain`, `drep_always_no_confidence`. The `starts_with("drep-always-")` check with a **hyphen** never matches. Every vote cast by a constant DRep has `drep_id` set to the literal string `"drep_always_abstain"` or `"drep_always_no_confidence"` instead of NULL.

**Impact:** All rows where `event_type = 'vote'` AND `drep_id IN ('drep_always_abstain', 'drep_always_no_confidence')`.

**Fix (SQL, no re-sync needed):**
```sql
UPDATE drep_timeline_event
SET drep_id = NULL
WHERE event_type = 'vote'
  AND drep_id IN ('drep_always_abstain', 'drep_always_no_confidence');
```

---

### 2. Wrong `previous_drep` on delegation events (SQL window function fix)

**Root cause:** During parallel historical sync, all workers share one global `Mutex<LruCache>` for `stake_address → drep_id` lookups. Workers process different epoch ranges concurrently in non-chronological order. A worker on epoch 440 may write to the cache before a worker on epoch 350 reads it for the same stake address, producing a `previous_drep` from the future.

**Impact:** Any `event_type = 'delegation'` row where the `previous_drep` column does not match the immediately preceding delegation for that stake address ordered by slot. This affects all historical delegations — the extent depends on how much epoch ranges overlapped in the parallel run. In the worst case, every historical delegation has a wrong `previous_drep`.

**Fix (SQL window function — authoritative, no re-sync needed):**

Slot order is chain-canonical. The correct `previous_drep` for each delegation is the `drep_id` of the immediately prior delegation for the same stake address ordered by slot.

```sql
-- Preview first (dry run)
SELECT
    id,
    stake_address,
    drep_id,
    previous_drep AS current_previous_drep,
    LAG(drep_id) OVER (
        PARTITION BY stake_address
        ORDER BY slot
    ) AS correct_previous_drep
FROM drep_timeline_event
WHERE event_type = 'delegation'
ORDER BY stake_address, slot
LIMIT 100;

-- Apply fix
WITH corrected AS (
    SELECT
        id,
        LAG(drep_id) OVER (
            PARTITION BY stake_address
            ORDER BY slot
        ) AS correct_previous_drep
    FROM drep_timeline_event
    WHERE event_type = 'delegation'
)
UPDATE drep_timeline_event dte
SET previous_drep = corrected.correct_previous_drep
FROM corrected
WHERE dte.id = corrected.id
  AND dte.event_type = 'delegation'
  AND (dte.previous_drep IS DISTINCT FROM corrected.correct_previous_drep);
```

**Note:** The first delegation for each stake address correctly gets `previous_drep = NULL` from the LAG window (no prior row).

---

### 3. Orphaned events from chain rollbacks (relay re-scan fix)

**Root cause:** `monitor/mod.rs` handles `RollBackward` with a log warning and continues. It does not delete events whose slot is past the rollback point. On shallow forks (common on mainnet), events from 1–3 orphaned blocks remain in the DB permanently. The `ON CONFLICT DO NOTHING` idempotency means the re-applied canonical blocks don't overwrite them.

**Impact:** Unknown count. Each rollback event during continuous sync leaves up to ~3 blocks of orphaned governance events (registrations, votes, etc. that reference transactions that never made it to the canonical chain). These look like valid events to consumers but are not on-chain.

**Fix strategy:** Cross-reference each `(slot, tx_hash)` pair in the DB against Blockfrost's confirmed transaction endpoint. Any tx_hash that Blockfrost doesn't recognise (404 or not in a confirmed block) is orphaned and should be deleted.

```
For each distinct slot in drep_timeline_event:
  1. Call GET https://cardano-mainnet.blockfrost.io/api/v0/txs/{tx_hash}
  2. If 404 → DELETE FROM drep_timeline_event WHERE tx_hash = ?
  3. Rate-limit to ~5 req/s (Blockfrost free tier)
```

This is the most expensive fix (one HTTP call per distinct tx_hash). Run it once over the historical dataset. Future rollbacks are handled by the fixed monitor (after the rollback bug is patched).

---

### 4. Empty `vote_rationale` on all votes (Blockfrost backfill — optional)

**Root cause:** `db/mod.rs` hardcodes `"vote_rationale": ""` in the vote metadata JSONB. CIP-1694 allows voters to attach an anchor URL + hash to their vote. The parser extracts the anchor from the block but it is never threaded through to the insert.

**Impact:** All vote events have empty rationale. For DReps and CC members who included rationale anchors, this data is missing from the index.

**Fix:** Once the code path is fixed (parser → insert), backfill historical votes by fetching each vote tx from Blockfrost and extracting the anchor. This is lower priority than fixes 1–3 and can wait until the code bug is patched first.

---

## Fix Priority

| # | Fix | Effort | Risk | Do First? |
|---|-----|--------|------|-----------|
| 1 | NULL out constant-DRep `drep_id` | SQL, 1 query | Low | Yes — immediate |
| 2 | Recompute `previous_drep` via LAG | SQL, 1 query | Low | Yes — immediate |
| 3 | Delete orphaned rollback events | HTTP + SQL | Medium | Yes — one-time batch |
| 4 | Backfill `vote_rationale` | HTTP + SQL | Low | After code fix |

---

## Reconciler Architecture

The reconciler is a separate binary subcommand (`indexer reconcile`) deployed as a **Kubernetes CronJob**. It does not share a pod with the main indexer. It runs the SQL-only fixes on every execution and the Blockfrost re-scan fixes on a configurable schedule or flag.

### Gasket Pipeline Design

The reconciler is built as a gasket staged pipeline. Each stage is a `gasket::Worker` with typed input/output. Gasket handles backpressure, retries, and error reporting.

```
┌──────────────────────────────────────────────────────────────────┐
│                      Reconciler Pipeline                          │
│                                                                   │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────┐  │
│  │ Audit Stage │───▶│ Fetch Stage  │───▶│   Patch Stage      │  │
│  │             │    │              │    │                    │  │
│  │ Query DB    │    │ Blockfrost   │    │ DELETE orphans     │  │
│  │ for suspect │    │ tx lookup    │    │ UPDATE drep_id     │  │
│  │ records     │    │ per tx_hash  │    │ UPDATE previous_   │  │
│  │             │    │              │    │   drep             │  │
│  └─────────────┘    └──────────────┘    └────────────────────┘  │
│                                                                   │
│  SQL-only fixes bypass the pipeline and run directly on startup  │
└──────────────────────────────────────────────────────────────────┘
```

### Stage Definitions

```rust
// Stage 1: Emit suspect tx_hashes from the DB
struct AuditStage {
    pool: PgPool,
    output: OutputPort<SuspectTx>,
}

struct SuspectTx {
    tx_hash: String,
    slot: u64,
    reason: SuspectReason,
}

enum SuspectReason {
    PossibleRollback,       // slot is within a known rollback window
    OrphanedFork,           // slot > last confirmed checkpoint before monitor restart
}

// Stage 2: Verify tx against Blockfrost
struct FetchStage {
    client: BlockfrostClient,
    input: InputPort<SuspectTx>,
    output: OutputPort<VerifiedTx>,
}

enum VerifiedTx {
    Confirmed(String),           // tx_hash is on-chain, keep
    Orphaned(String),            // tx_hash not found, delete
}

// Stage 3: Apply DB patches
struct PatchStage {
    pool: PgPool,
    input: InputPort<VerifiedTx>,
    metrics: ReconcilerMetrics,
}
```

### New CLI subcommand

```
indexer reconcile [OPTIONS]

Options:
  --sql-only          Run SQL-only fixes (fixes 1 + 2), skip Blockfrost scan
  --scan-orphans      Also run Blockfrost orphan detection (fix 3)
  --dry-run           Print what would be changed, apply nothing
  --since-slot SLOT   Only check events after this slot (for incremental runs)
  --blockfrost-key KEY  Blockfrost project ID (or BLOCKFROST_PROJECT_ID env var)
```

---

## Kubernetes CronJob Deployment

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: governance-indexer-reconciler
  namespace: voltaire
spec:
  schedule: "0 2 * * *"     # 02:00 UTC nightly
  concurrencyPolicy: Forbid   # never run two reconcilers at once
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          imagePullSecrets:
            - name: gitlab-registry-token
          containers:
            - name: reconciler
              image: "registry.2lovelaces.io/voltaire/governance-indexer:prod"
              command: ["indexer", "reconcile", "--sql-only"]
              env:
                - name: DATABASE_URL
                  value: "{{ .Values.governanceIndexer.databaseUrl }}"
                - name: RUST_LOG
                  value: "info"
              resources:
                limits:
                  memory: 256Mi
                  cpu: 250m
                requests:
                  memory: 128Mi
                  cpu: 100m
```

Add to the Helm chart values:
```yaml
reconciler:
  enabled: true
  schedule: "0 2 * * *"
  sqlOnly: true
  scanOrphans: false        # enable after blockfrost key is configured
  blockfrostKey: ""         # set via K8s secret
```

---

## Immediate Actions (Run Now)

These two SQL statements can be run directly against production right now, no code changes needed:

```sql
-- Fix 1: NULL constant-DRep voter drep_ids
BEGIN;
UPDATE drep_timeline_event
SET drep_id = NULL
WHERE event_type = 'vote'
  AND drep_id IN ('drep_always_abstain', 'drep_always_no_confidence');
-- check rowcount before committing
COMMIT;

-- Fix 2: Recompute previous_drep from slot-ordered window
BEGIN;
WITH corrected AS (
    SELECT
        id,
        LAG(drep_id) OVER (
            PARTITION BY stake_address
            ORDER BY slot
        ) AS correct_previous_drep
    FROM drep_timeline_event
    WHERE event_type = 'delegation'
)
UPDATE drep_timeline_event dte
SET previous_drep = corrected.correct_previous_drep
FROM corrected
WHERE dte.id = corrected.id
  AND dte.event_type = 'delegation'
  AND (dte.previous_drep IS DISTINCT FROM corrected.correct_previous_drep);
-- check rowcount before committing
COMMIT;
```

---

## Files to Create

```
src/
  reconciler/
    mod.rs          — CLI entry point, runs SQL fixes + kicks off gasket pipeline
    audit.rs        — AuditStage: query DB for suspect records
    fetch.rs        — FetchStage: Blockfrost verification per tx_hash
    patch.rs        — PatchStage: apply deletes/updates
    blockfrost.rs   — Blockfrost HTTP client wrapper
```

Add to `main.rs` `Commands` enum:
```rust
/// Fix known data discrepancies (run as CronJob or on-demand)
Reconcile {
    #[arg(long)] sql_only: bool,
    #[arg(long)] scan_orphans: bool,
    #[arg(long)] dry_run: bool,
    #[arg(long)] since_slot: Option<u64>,
    #[arg(long, env = "BLOCKFROST_PROJECT_ID")] blockfrost_key: Option<String>,
}
```

Add to `Cargo.toml` (already has `gasket = "0.9"`):
```toml
# no new deps needed — reqwest already present for Blockfrost calls
```
