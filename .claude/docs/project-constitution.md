# Governance Indexer — Project Constitution

This document is the foundational source of truth for the governance-indexer. AI Agents must read and adhere to these principles during all coding sessions.

## Mission

The governance-indexer is a high-throughput Cardano blockchain event indexer implementing CIP-1694 (Voltaire). It tracks DRep registrations, delegations, proposals, and votes from the Conway era forward and writes structured event records to PostgreSQL for consumption by the 1694.io platform.

## Tech Stack

| Layer | Tool |
|---|---|
| Language | Rust (nightly, edition 2024) |
| Async runtime | tokio (full features) |
| Chain sync | Pallas 0.33 (N2N protocol, blockfetch, chainsync) |
| Pipeline | Gasket 0.9 (SEDA staged pipelines) |
| Database | PostgreSQL 17 via sqlx 0.7 |
| Encoding | bech32 0.9 |
| Config | dotenvy + env-var priority logic |
| Deploy | Docker → GitLab registry → Kubernetes (Helm) |

## Architectural Principles

1. **Stateless parsing** — The Rust indexer must remain stateless. It extracts raw events from blocks and writes them. Cross-event state lookups (e.g. "what DRep was this stake key delegated to before?") belong in the NestJS queue-backend (`TimelineWatcherWorker`), not here.
2. **Idempotent inserts** — All event inserts use `ON CONFLICT DO NOTHING`. Re-indexing a range is always safe.
3. **Checkpoint-driven progress** — Progress is tracked per worker in `sync_checkpoint`. Never assume the DB is consistent without a valid checkpoint.
4. **No credentials in images** — `.env` is local dev only. Production credentials come from Kubernetes secrets via the deployment manifest.
5. **Bech32 at the boundary** — Raw bytes are decoded to human-readable bech32 in the parser layer. The DB stores only human-readable addresses.

## Spec-Driven Development (SDD) Workflow

We do not use "vibe coding". All changes follow a strict SDD methodology.

1. **Specs Over Assumptions**: Never guess missing requirements. If a requirement is missing from a spec in `docs/specs/`, stop and ask for clarification or help draft the spec. Never implement from a description alone — a spec must exist first.
2. **Strict TDD**:
   - Read the spec.
   - Write failing tests *first* based strictly on the spec's Test Cases.
   - Run `cargo test` to confirm they fail.
   - Implement the minimum code required to make them pass.
3. **Continuous Verification**: After tests pass, re-read the spec top-to-bottom to verify no requirement was missed.
4. **Roadmap Maintenance**: Update `docs/roadmap.md` to move features from backlog to completed once verified.

## Execution Standards

- **Minimal logging**: Use `tracing` macros at appropriate levels. `info!` for major milestones only. `warn!` for recoverable issues. `error!` for fatal failures. No per-block or per-event spam in steady state.
- **No obvious comments**: Code should be self-documenting. Only comment non-obvious invariants, Cardano-specific constants, or workarounds for upstream bugs.
- **No placeholders**: Implement full logic. No `todo!()` macros in merged code unless the spec explicitly defers the item.
- **Slot math in one place**: All slot/epoch/timestamp conversion constants live in `config::cardano`. Never hardcode slot arithmetic inline.
