# Cardano Governance Indexer

## 1. Overview
The Governance Indexer is a high-performance system designed to index, store, and serve Cardano governance data in compliance with the **CIP-1694 Voltaire specification**. It acts as a bridge between the Cardano blockchain and the www.1694.io frontend, providing real-time data on DReps, governance actions, and voting activity.

## 2. Architecture (Rust Indexer)

The production system is built in **Rust** using the [Pallas](https://github.com/txpipe/pallas) library to communicate directly with Cardano nodes via Ouroboros mini-protocols (Node-to-Node).

```mermaid
graph LR
    Node[Cardano Node] -->|Ouroboros N2N| Indexer[Rust Indexer]
    Indexer -->|Decodes Blocks| Parser[Gov Parser]
    Parser -->|Extracts Events| DB[(PostgreSQL)]
    DB -->|Queries| API[Frontend API]
```

### Components
*   **Scanner**: Connects to Cardano relays using Pallas. Supports **Parallel Historical Sync** (fast catch-up using workers) and **Continuous Sync** (monitoring chain tip).
*   **Parser**: Extracts governance-specific certificates (`regDrepCert`, `voteDelegCert`, etc.) and transaction bodies (`txvote`, `txprop`) from raw block data.
*   **Storage**: Normalizes data into a PostgreSQL schema optimized for timeline queries.

---

## 3. Governance Specifications & Standards

The indexer logic is strictly based on the following formal specifications and standards.

### Core Business Logic
*   **[Conway Ledger Formal Specification](https://github.com/IntersectMBO/cardano-ledger/tree/master/eras/conway/impl/README.md)**: Defines the rules for Delegate Representatives (DReps), Governance Actions, and Voting.
    *   **GovActionID**: Unique ID (`TxId + Index`).
    *   **Voter Roles**: CC (Constitutional Committee), DRep, SPO (Stake Pool Operator).
    *   **Action Types**: NoConfidence, UpdateCommittee, NewConstitution, TriggerHF, ChangePParams, TreasuryWdrl, Info.

### Relevant CIPs (Cardano Improvement Proposals)
*   **[CIP-1694](https://github.com/cardano-foundation/CIPs/tree/master/CIP-1694)**: On-Chain Decentralized Governance.
*   **[CIP-105](https://github.com/cardano-foundation/CIPs/tree/master/CIP-105)**: DRep ID Format.
*   **[CIP-119](https://github.com/cardano-foundation/CIPs/tree/master/CIP-119)**: DRep Metadata Standards (JSON-LD).
*   **[CIP-129](https://github.com/cardano-foundation/CIPs/tree/master/CIP-129)**: Governance Entity Bech32 Encoding.

---

## 4. Data Models & Schema

The database is structured to support the **DRep Timeline** feature, which aggregates all events related to a DRep in chronological order.

### Database Tables (PostgreSQL)

#### `drep_timeline_event`
The central table for all DRep activities.

| Column | Type | Description |
|Args|---|---|
| `event_type` | `text` | `registration`, `delegation`, `voting_activity`, `retirement` |
| `timestamp` | `timestamptz` | Block time of the event |
| `epoch` | `bigint` | Cardano epoch number |
| `slot` | `bigint` | Absolute slot number |
| `tx_hash` | `text` | Transaction hash |
| `drep_id` | `text` | DRep ID (CIP-105/129) |
| `metadata` | `jsonb` | Event-specific payload (see below) |

### Event Payloads (JSONB)

**1. Registration**
```json
{
  "drep_id": "drep1...",
  "deposit": "500000000",
  "anchor_url": "https://...",
  "anchor_hash": "..."
}
```

**2. Delegation**
```json
{
  "stake_address": "stake1...",
  "target_drep": "drep1...",
  "previous_drep": "drep1... (or null)",
  "added_power": true // true if delegation TO, false if FROM
}
```

**3. Voting Activity**
```json
{
  "vote": "Yes/No/Abstain",
  "gov_action_proposal_id": "tx_hash",
  "vote_rationale": "Reason derived from anchor url..."
}
```

---

## 5. Configuration & Deployment

### Environment Variables
Configure the indexer using a `.env` file or Docker environment variables.

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL Connection String | `postgres://user:pass@localhost:5432/db` |
| `RELAY_ADDRESSES` | Comma-separated list of Cardano relays | `backbone.mainnet...:3001` |
| `NETWORK_MAGIC` | Network identifier (Mainnet=764824073, Preview=2) | `764824073` |
| `MAX_CONCURRENT_WORKERS` | Parallel workers for history sync | `10` |
| `BATCH_SIZE` | DB transaction batch size | `1000` |

### Running with Docker
```bash
docker run -d \
  -e DATABASE_URL=postgres://user:pass@db:5432/govindex \
  -e RELAY_ADDRESSES=relay1.com:3000,relay2.com:3000 \
  gov-indexer:latest
```

### Local Development (Rust)
```bash
# Assuming you are in the rust-indexer directory
cargo run -- start
```
Use `cargo run -- --help` to see all available commands (`wipe`, `reset`, `status`).

---

## 6. Legacy References
*   Archived Python Prototype Spec: `[PHASE1_COMPLETE.md]` (Deleted)
*   Original ETL Guide: `[IMPLEMENTATION_GUIDE.md]` (Deleted)
*   *Note: These files have been consolidated into this document or deprecated in favor of the Rust implementation.*
