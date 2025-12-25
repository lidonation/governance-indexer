# Cardano Governance Indexer (Rust Prototype)

This is a prototype implementation of a high-performance governance indexer for Cardano (CIP-1694). It is designed to extract DRep registrations, retirements, votes, proposals, and delegations directly from a Cardano node using the Ouroboros mini-protocols.

## Architecture

The system consists of:
- A Rust-based indexer using the Pallas library.
- A PostgreSQL database for persistent storage.
- A hybrid synchronization strategy: parallel historical catch-up and real-time monitoring.

## System Requirements

- Rust (latest stable or nightly)
- PostgreSQL 16+
- Docker and Docker Compose (optional, for containerized deployment)

## Setup and Configuration

Configure the system using a `.env` file or environment variables.

### Database Settings
- `DATABASE_URL`: PostgreSQL connection string.
- `DB_MAX_CONNECTIONS`: Maximum pool size (default: 30).

### Network Settings
- `RELAY_ADDRESS`: Address of a Cardano relay (e.g., `backbone.mainnet.emurgornd.com:3001`).
- `NETWORK_MAGIC`: Network identifier (e.g., 764824073 for Mainnet).

### Processing Settings
- `MAX_CONCURRENT_WORKERS`: Number of parallel workers for historical sync.
- `BATCH_SIZE`: Number of events per database transaction.

## Usage

The indexer provides a native CLI for management.

### Starting the Indexer
```bash
cargo run -- start
```

### Process Control
Management commands use a `.indexer.pid` file to track the running instance.
```bash
cargo run -- stop      # Gracefully terminate the process
cargo run -- pause     # Suspend execution
cargo run -- continue  # Resume execution
```

### Data Management
```bash
cargo run -- status    # View event counts and sync progress
cargo run -- wipe      # Permanently delete all indexed data and checkpoints
cargo run -- reset     # Wipe data and start a fresh synchronization
```

## Docker Deployment

To run the entire stack using Docker:

```bash
# Start database and indexer
docker-compose up -d

# View logs
docker-compose logs -f indexer

# Stop all services
docker-compose down
```

## Status and Limitations

This project is a prototype. Current features include:
- Denormalized schema for high-performance DRep timeline lookups.
- LRU caching for delegation state.
- Checkpointing for resilient synchronization across restarts.
- Professional color-coded logging for monitoring.

Note: The indexer currently targets the Voltair preview and Mainnet networks based on provided boundary hashes.
