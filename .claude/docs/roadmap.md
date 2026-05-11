# Governance Indexer — Roadmap

Tracks iterative SDD loops for the governance-indexer.

## In Progress
- *None currently active. Pull the next item from the backlog.*

## Backlog (needs spec before dev starts)

### Bugs / Correctness
- **Rollback event deletion**: `monitor/mod.rs` logs `RollBackward` but does not delete orphaned events newer than the rollback slot. Shallow forks leave stale data. Needs `DELETE FROM drep_timeline_event WHERE slot > $rollback_slot`.
- **Vote DRep filter broken**: `starts_with("drep-always-")` uses hyphens but `format_voter()` outputs underscores (`drep_always_abstain`) — filter is a no-op, always-abstain/always-no-confidence votes are incorrectly inserted.
- **Vote rationale not stored**: `vote_rationale` is hardcoded `""` on every insert. The CIP-1694 anchor on vote transactions is never fetched or stored.

### Refactors / Tech Debt
- **Shared slot utilities**: `slot_to_timestamp()` and `slot_to_epoch()` are copy-pasted in both `scanner` and `monitor`. Move to `src/utils.rs`.
- **Delegation LRU contention**: Global `Mutex<LruCache>` is shared across all parallel historical workers. Replace with per-worker caches or a lock-free structure.
- **Pin nightly toolchain**: Dockerfile uses a floating nightly tag. Add `rust-toolchain.toml` (already exists in repo) to the Docker build so the image is reproducible.

### Features
- **`metadata/mod.rs` implementation**: Module declaration exists but is empty. Define scope in a spec before implementing.
- **`indexer reconcile` subcommand**: Gasket pipeline — Audit → Fetch (Blockfrost) → Patch — to fix historical discrepancies. See `reconciler-plan.md` for the full inventory.
- **Reconciler CronJob**: Deploy the reconciler as a Kubernetes CronJob running nightly alongside the main indexer.
- **Testing framework**: Add `cargo test` integration with a test Postgres instance (via `sqlx::test` or `testcontainers`) to support TDD workflow.

## Completed
- **Delegation Transfer (Undelegation) Events**: Stateless indexer emits raw `delegation` events; `TimelineWatcherWorker` in queue-backend synthesizes `undelegation` events for the previous DRep. (Spec: see `www-1694/docs/specs/01-delegation-transfer-events.md`)
- **Governance Indexer Stabilization**: Fixed rollback loops and corrected Preview epoch anchors.
