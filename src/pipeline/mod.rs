pub mod stages;
pub mod types;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use anyhow::Context;
use pallas::network::miniprotocols::Point;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::Config;
use crate::pipeline::stages::{
    block_fetcher::HistoricalFetcherStage,
    chain_follower::ChainFollowerStage,
    event_sink::EventSinkStage,
    governance_parser::GovernanceParserStage,
};
use crate::pipeline::types::EpochRange;
use crate::sinks::{SinkSet, postgres::PostgresSink, webhook::WebhookSink};
use gasket::metrics::Counter;
use gasket::runtime::{Policy, TetherState, spawn_stage};
use gasket::retries;

// ---------------------------------------------------------------------------
// Sink construction
// ---------------------------------------------------------------------------

pub fn build_sinks(config: &Config, pool: PgPool) -> SinkSet {
    let mut sinks: SinkSet = Vec::new();

    if config.sinks.postgres_enabled {
        info!("sink: postgres enabled");
        sinks.push(Arc::new(PostgresSink::new(pool, "continuous")));
    }

    if config.sinks.webhook_enabled {
        if let Some(url) = &config.sinks.webhook_url {
            info!("sink: webhook enabled → {}", url);
            sinks.push(Arc::new(WebhookSink::new(
                url.clone(),
                config.sinks.webhook_secret.clone(),
            )));
        } else {
            warn!("sink: SINK_WEBHOOK_ENABLED=true but SINK_WEBHOOK_URL is not set — webhook disabled");
        }
    }

    if sinks.is_empty() {
        warn!("no sinks configured — events will be parsed but not persisted");
    }

    sinks
}

// ---------------------------------------------------------------------------
// Retry policies
// ---------------------------------------------------------------------------

// Historical fetcher runs execute() for hours (spawns N tasks, joins all).
// tick_timeout must be None or gasket kills the worker mid-sync.
fn historical_fetch_policy() -> Policy {
    Policy {
        tick_timeout: None,
        bootstrap_retry: retries::Policy {
            max_retries: usize::MAX,
            backoff_unit: Duration::from_secs(5),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(120),
            dismissible: false,
        },
        work_retry: retries::Policy {
            max_retries: usize::MAX,
            backoff_unit: Duration::from_secs(5),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(120),
            dismissible: false,
        },
        teardown_retry: retries::Policy::no_retry(),
    }
}

// Continuous chain follower: each tick is bounded (60s request_next timeout + sleep).
fn network_policy() -> Policy {
    Policy {
        tick_timeout: Some(Duration::from_secs(180)),
        bootstrap_retry: retries::Policy {
            max_retries: usize::MAX,
            backoff_unit: Duration::from_secs(5),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(120),
            dismissible: false,
        },
        work_retry: retries::Policy {
            max_retries: usize::MAX,
            backoff_unit: Duration::from_secs(5),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(120),
            dismissible: false,
        },
        teardown_retry: retries::Policy::no_retry(),
    }
}

fn db_policy() -> Policy {
    Policy {
        tick_timeout: Some(Duration::from_secs(120)),
        bootstrap_retry: retries::Policy::no_retry(),
        work_retry: retries::Policy {
            max_retries: usize::MAX,
            backoff_unit: Duration::from_secs(2),
            backoff_factor: 2,
            max_backoff: Duration::from_secs(60),
            dismissible: false,
        },
        teardown_retry: retries::Policy::no_retry(),
    }
}

fn pure_policy() -> Policy {
    Policy {
        tick_timeout: None,
        bootstrap_retry: retries::Policy::no_retry(),
        work_retry: retries::Policy::no_retry(),
        teardown_retry: retries::Policy::no_retry(),
    }
}

// ---------------------------------------------------------------------------
// Historical sync pipeline
// ---------------------------------------------------------------------------

/// Partition epoch ranges evenly across N worker slots.
fn partition_ranges(all_ranges: Vec<EpochRange>, n: usize) -> Vec<Vec<EpochRange>> {
    let n = n.max(1);
    let mut partitions: Vec<Vec<EpochRange>> = (0..n).map(|_| Vec::new()).collect();

    for (i, range) in all_ranges.into_iter().enumerate() {
        partitions[i % n].push(range);
    }

    partitions
}

/// Build epoch ranges from the slot_hashes CSV data.
pub fn build_epoch_ranges(slot_hashes: &[(u64, String)]) -> anyhow::Result<Vec<EpochRange>> {
    if slot_hashes.len() < 2 {
        anyhow::bail!("need at least 2 slot hashes to define ranges");
    }

    let mut ranges = Vec::new();

    for (i, window) in slot_hashes.windows(2).enumerate() {
        let (start_slot, start_hash_hex) = &window[0];
        let (end_slot, end_hash_hex) = &window[1];

        let start_hash = hex::decode(start_hash_hex)
            .with_context(|| format!("invalid hex in slot_hashes at index {}", i))?;
        let end_hash = hex::decode(end_hash_hex)
            .with_context(|| format!("invalid hex in slot_hashes at index {}", i + 1))?;

        ranges.push(EpochRange {
            start: Point::Specific(*start_slot, start_hash),
            end: Point::Specific(*end_slot, end_hash),
            worker_id: format!("historical_{}", i),
        });
    }

    Ok(ranges)
}

/// Run historical sync: fetch all epoch ranges, parse events, persist to DB.
///
/// Blocks until all data has been processed or SIGTERM is received.
/// Uses a custom wait loop that waits for the EventSink (last stage) to finish,
/// rather than stopping on the first stage to end.
pub fn run_historical_pipeline(
    config: &Config,
    pool: PgPool,
    sinks: SinkSet,
    ranges: Vec<EpochRange>,
) -> anyhow::Result<()> {
    // Skip ranges already completed in a previous run.
    // Called from spawn_blocking so block_on is safe here.
    let handle = tokio::runtime::Handle::current();
    let completed = handle.block_on(crate::db::get_historical_checkpoints(&pool))?;

    let total = ranges.len();
    let ranges: Vec<EpochRange> = ranges
        .into_iter()
        .filter(|r| {
            let done_slot = completed.get(&r.worker_id).copied().unwrap_or(0);
            let end_slot = match &r.end {
                Point::Specific(s, _) => *s,
                Point::Origin => 0,
            };
            let skip = done_slot >= end_slot;
            if skip {
                info!("resume: skipping completed range {} (slot {})", r.worker_id, done_slot);
            }
            !skip
        })
        .collect();

    if ranges.is_empty() {
        info!("all {} historical ranges already completed — skipping to continuous sync", total);
        return Ok(());
    }

    info!("{}/{} historical ranges remaining", ranges.len(), total);

    let n = config.processing.max_concurrent_workers;
    let partitions = partition_ranges(ranges, n);

    // Large buffers so N fetcher workers don't stall waiting for the single parser/sink.
    // Channel: HistoricalFetcher → GovernanceParser
    let (raw_tx, raw_rx) = mpsc::channel(1024);
    // Channel: GovernanceParser → EventSink (much smaller — only blocks with events pass through)
    let (event_tx, event_rx) = mpsc::channel(256);

    let fetcher = HistoricalFetcherStage {
        partitioned_ranges: partitions,
        relay_addresses: config.network.relays.clone(),
        magic: config.network.magic as u64,
        output: raw_tx,
        pool: pool.clone(),
        blocks_fetched: Counter::default(),
        ranges_completed: Counter::default(),
    };

    let parser = GovernanceParserStage {
        input: raw_rx,
        output: event_tx,
        blocks_parsed: Counter::default(),
        events_found: Counter::default(),
    };

    let sink = EventSinkStage {
        sinks,
        input: event_rx,
        blocks_committed: Counter::default(),
        events_persisted: Counter::default(),
        rollbacks_handled: Counter::default(),
    };

    // Spawn stages — each runs in its own thread with a single-threaded tokio runtime.
    let fetcher_tether = spawn_stage(fetcher, historical_fetch_policy());
    let parser_tether = spawn_stage(parser, pure_policy());
    let sink_tether = spawn_stage(sink, db_policy());

    let term = register_term_signal();

    // Wait for EventSink (the last stage) to finish naturally, or for SIGTERM.
    // We use `all` not `any` so the pipeline drains fully before we exit.
    info!("historical pipeline running...");

    loop {
        if term.load(Ordering::Relaxed) {
            warn!("SIGTERM received — dismissing historical pipeline");
            break;
        }

        let sink_done = matches!(
            sink_tether.check_state(),
            TetherState::Dropped | TetherState::Alive(gasket::runtime::StagePhase::Ended)
        );

        if sink_done {
            info!("historical pipeline: EventSink complete");
            break;
        }

        std::thread::sleep(Duration::from_millis(500));
    }

    // Dismiss any stages still running (order: upstream first).
    for tether in [&fetcher_tether, &parser_tether, &sink_tether] {
        tether.dismiss_stage().ok();
    }

    fetcher_tether.join_stage();
    parser_tether.join_stage();
    sink_tether.join_stage();

    info!("historical pipeline complete");
    Ok(())
}

// ---------------------------------------------------------------------------
// Continuous sync pipeline
// ---------------------------------------------------------------------------

/// Run continuous sync: follow the chain tip, parse and persist new blocks.
///
/// Blocks indefinitely until SIGTERM.
pub fn run_continuous_pipeline(
    config: &Config,
    pool: PgPool,
    sinks: SinkSet,
    start_point: Point,
) -> anyhow::Result<()> {
    let (event_tx, event_rx) = mpsc::channel(64);

    let follower = ChainFollowerStage {
        relay_addresses: config.network.relays.clone(),
        magic: config.network.magic as u64,
        start_point,
        interval_secs: config.processing.continuous_sync_interval_secs,
        pool: pool.clone(),
        worker_id: "continuous".to_string(),
        output: event_tx,
        blocks_followed: Counter::default(),
        rollbacks_seen: Counter::default(),
    };

    let sink = EventSinkStage {
        sinks,
        input: event_rx,
        blocks_committed: Counter::default(),
        events_persisted: Counter::default(),
        rollbacks_handled: Counter::default(),
    };

    let follower_tether = spawn_stage(follower, network_policy());
    let sink_tether = spawn_stage(sink, db_policy());

    let term = register_term_signal();

    info!("continuous pipeline running...");

    loop {
        if term.load(Ordering::Relaxed) {
            warn!("SIGTERM received — dismissing continuous pipeline");
            break;
        }

        let any_ended = [&follower_tether, &sink_tether].iter().any(|t| {
            matches!(
                t.check_state(),
                TetherState::Dropped | TetherState::Alive(gasket::runtime::StagePhase::Ended)
            )
        });

        if any_ended {
            warn!("continuous pipeline: a stage ended unexpectedly");
            break;
        }

        std::thread::sleep(Duration::from_millis(1500));
    }

    for tether in [&follower_tether, &sink_tether] {
        tether.dismiss_stage().ok();
    }

    follower_tether.join_stage();
    sink_tether.join_stage();

    info!("continuous pipeline stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// SIGTERM registration (shared between pipelines)
// ---------------------------------------------------------------------------

fn register_term_signal() -> Arc<AtomicBool> {
    let term = Arc::new(AtomicBool::new(false));
    for sig in signal_hook::consts::TERM_SIGNALS {
        signal_hook::flag::register(*sig, Arc::clone(&term))
            .expect("failed to register signal hook");
    }
    term
}
