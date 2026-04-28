# Governance Indexer — Claude Context

## Project

Cardano governance event indexer implementing CIP-1694 (Voltaire). Tracks DRep registrations, delegations, proposals, and votes from the Conway era forward. Written in Rust, deployed on Kubernetes via Helm.

## Stack

| Layer | Tool |
|---|---|
| Language | Rust (nightly, edition 2024) |
| Async | tokio (full features) |
| Chain sync | Pallas 0.33 (N2N protocol, blockfetch, chainsync) |
| Pipeline | Gasket 0.9 (SEDA staged pipelines, used in reconciler) |
| Database | PostgreSQL 16 via sqlx 0.7 |
| Encoding | bech32 0.9 (DRep/stake/pool addresses) |
| Config | dotenvy + manual env-var priority logic |
| Deploy | Docker → GitLab registry → Kubernetes (Helm) |

## Key Files

```
src/
  main.rs          — CLI subcommands, historical + continuous sync orchestration
  config.rs        — Env-var loading with priority: real env > .env file > defaults
  db/mod.rs        — sqlx pool, event inserts, checkpoint CRUD, delegation LRU cache
  scanner/mod.rs   — Pallas blockfetch worker, parallel epoch range processing
  monitor/mod.rs   — Real-time chainsync after historical sync completes
  parser/mod.rs    — Conway cert + tx extraction, bech32 encoding
  parser/gov_serializer.rs — GovAction → JSON details (ParameterChange, HardFork, etc.)
  metrics.rs       — AtomicU64 counters, background reporting task
  metadata/mod.rs  — EMPTY — scaffolding, not implemented

db/
  init.sql         — drep_timeline_event + sync_checkpoint schema + indexes
  denormalize.sql  — Adds stake_address/previous_drep columns for faster lookups
  checkpoint.sql   — sync_checkpoint table definition
```

## Database Schema

Single event table `drep_timeline_event` with JSONB `metadata` column. All governance event types (registration, retirement, delegation, vote, proposal) share one table. Unique constraint on `(tx_hash, tx_index)`.

Separate `sync_checkpoint` table keyed by `worker_id` (`historical_0`, `historical_1`, ..., `continuous`).

## CLI Subcommands

```sh
indexer start          # Run historical sync then continuous monitor
indexer sync-history   # Historical sync only
indexer status         # Print checkpoint and event stats
indexer wipe           # TRUNCATE both tables (destructive, no prompt)
indexer reset          # Clear checkpoints only
indexer stop/pause/continue  # Signal running process via PID file
```

## Configuration

All config comes from environment variables. The `.env` file is **local dev only** — never baked into Docker images. In production, Kubernetes injects vars via the deployment manifest.

Key vars:
- `DATABASE_URL` — required, must start with `postgres://`
- `RELAY_ADDRESSES` — comma-separated list of Cardano relay hosts:ports
- `RELAY_ADDRESS` — single relay (merged with RELAY_ADDRESSES list)
- `NETWORK_MAGIC` — 764824073 (mainnet), 1 (preprod), 2 (preview)
- `MAX_CONCURRENT_WORKERS` — parallel historical sync workers (default 10)
- `BATCH_SIZE` — events per DB batch (default 1000)

## Build

```sh
# Local
cargo build --release

# Docker (no .env needed — vars injected at runtime)
docker build -t governance-indexer .

# Docker Compose (local dev with postgres)
docker compose up
```

## Reconciler

See [reconciler-plan.md](reconciler-plan.md) for the full discrepancy inventory and fix plan. Short version:

- **Run immediately (SQL):** Fix 1 (NULL constant-DRep `drep_id` on votes), Fix 2 (recompute `previous_drep` via LAG window)
- **Planned:** `indexer reconcile` subcommand as a gasket pipeline — Audit → Fetch (Blockfrost) → Patch
- **K8s:** Deploy as a CronJob nightly alongside the main indexer deployment

## Known Issues / Active TODOs

- `monitor/mod.rs`: RollBackward logs a warning but does NOT delete orphaned events. Shallow forks leave stale data. Needs `DELETE WHERE slot > rollback_slot`.
- `vote_rationale` is hardcoded as `""` in every vote insert — CIP-1694 anchor on votes is never stored.
- `metadata/mod.rs` is empty — module declaration exists but no implementation.
- Delegation LRU cache is a global `Mutex<LruCache>` — all parallel workers contend on one lock.
- `slot_to_timestamp()` and `slot_to_epoch()` are copy-pasted in both `scanner` and `monitor` — should live in a shared utils module.
- DRep filter in vote insert uses `starts_with("drep-always-")` with hyphens, but `format_voter()` outputs underscores (`drep_always_abstain`) — filter is currently a no-op.
- Nightly toolchain is a floating tag in Dockerfile. Add `rust-toolchain.toml` to pin it.

## Conventions

- No `.env` in Docker images — credentials come from K8s secrets / deployment env vars
- `ON CONFLICT (tx_hash, tx_index) DO NOTHING` on all event inserts — idempotent, re-indexing won't overwrite
- Bech32 encoding always in parser layer — DB stores human-readable addresses, not raw bytes
- Checkpoints written every 1000 blocks during historical sync, every block during continuous sync
- Cardano slot constants live in `config::cardano` — never hardcode slot math inline
