# Governance Indexer — local dev shortcuts
#
# Workflow:
#   make build   — compile a Linux release binary using the production builder
#                  image (rustlang/rust:nightly-slim). Artifacts land in a
#                  Docker volume, not the macOS host FS, so there are no
#                  platform incompatibilities.
#   make up      — start postgres + indexer (runs the pre-built binary)
#   make <cmd>   — run a one-off indexer command against the live stack

.PHONY: build up up-detach down logs shell \
        status monitor reconcile-dry reconcile \
        clean help

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BUILD_IMAGE := rustlang/rust:nightly-slim

# Must match the 'name:' entries in docker-compose.yml
CARGO_REGISTRY := governance-indexer-cargo-registry
CARGO_GIT      := governance-indexer-cargo-git
BUILD_CACHE    := governance-indexer-build

# Path to the binary inside the build-cache volume (used by docker compose run)
BIN := /build/target/release/indexer

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

## build   Compile a Linux release binary inside rustlang/rust:nightly-slim.
##         Artifacts are cached in Docker volumes — only changed files recompile.
build:
	docker run --rm \
		-v "$(CURDIR):/app" \
		-v "$(CARGO_REGISTRY):/usr/local/cargo/registry" \
		-v "$(CARGO_GIT):/usr/local/cargo/git" \
		-v "$(BUILD_CACHE):/app/target" \
		-w /app \
		$(BUILD_IMAGE) \
		sh -c "apt-get update -qq && \
		       apt-get install -y -qq pkg-config libssl-dev 2>/dev/null && \
		       cargo build --release"

# ---------------------------------------------------------------------------
# Stack lifecycle
# ---------------------------------------------------------------------------

## up      Start postgres + indexer (binary must be built first).
up:
	docker compose up

## up-d    Start in the background.
up-detach:
	docker compose up -d

## down    Stop and remove containers.
down:
	docker compose down

## logs    Stream indexer logs.
logs:
	docker compose logs -f indexer

## shell   Open a bash shell inside the running indexer container.
shell:
	docker compose exec indexer bash

# ---------------------------------------------------------------------------
# Indexer commands  (each spins up a fresh container against the pre-built binary)
# ---------------------------------------------------------------------------

## status  Print event counts and last sync checkpoint.
status:
	docker compose run --rm indexer $(BIN) status

## monitor Start continuous sync only (from last checkpoint).
monitor:
	docker compose run --rm indexer $(BIN) monitor

## reconcile-dry  Preview reconciler SQL fixes without writing anything.
reconcile-dry:
	docker compose run --rm indexer $(BIN) reconcile --sql-only --dry-run

## reconcile      Apply reconciler SQL fixes (fix 1 + fix 2).
reconcile:
	docker compose run --rm indexer $(BIN) reconcile --sql-only

## wipe    Truncate drep_timeline_event and sync_checkpoint (no prompt).
wipe:
	docker compose run --rm indexer $(BIN) wipe --force

## reset   Wipe all data then start a fresh historical + continuous sync.
reset:
	docker compose run --rm indexer $(BIN) reset --force

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

## clean   Delete build artifact and cargo cache volumes. Next build is cold.
clean:
	docker volume rm $(BUILD_CACHE) $(CARGO_REGISTRY) $(CARGO_GIT) 2>/dev/null || true

## help    List available targets.
help:
	@grep -E '^##' Makefile | sed 's/^## /  /'
