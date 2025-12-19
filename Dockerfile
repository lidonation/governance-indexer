# Build stage
FROM rustlang/rust:nightly-slim AS builder

WORKDIR /app

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY slot_hashes.csv ./

# Build for release
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/rust-indexer /app/rust-indexer

# Copy the slot_hashes.csv file
COPY --from=builder /app/slot_hashes.csv /app/slot_hashes.csv

# Set environment
ENV RUST_LOG=info

# Run the binary
CMD ["/app/rust-indexer"]
