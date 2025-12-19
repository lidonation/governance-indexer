use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub blocks_processed: Arc<AtomicU64>,
    pub events_processed: Arc<AtomicU64>,
    pub database_operations: Arc<AtomicU64>,
    pub network_connections: Arc<AtomicU64>,
    pub errors: Arc<AtomicU64>,
    pub start_time: Instant,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            blocks_processed: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            database_operations: Arc::new(AtomicU64::new(0)),
            network_connections: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

    pub fn get_blocks_processed(&self) -> u64 {
        self.blocks_processed.load(Ordering::Relaxed)
    }

    pub fn get_events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn get_database_operations(&self) -> u64 {
        self.database_operations.load(Ordering::Relaxed)
    }

    pub fn get_network_connections(&self) -> u64 {
        self.network_connections.load(Ordering::Relaxed)
    }

    pub fn get_errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    pub fn get_uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn get_blocks_per_second(&self) -> f64 {
        let elapsed = self.get_uptime().as_secs_f64();
        if elapsed > 0.0 {
            self.get_blocks_processed() as f64 / elapsed
        } else {
            0.0
        }
    }

    pub fn get_events_per_second(&self) -> f64 {
        let elapsed = self.get_uptime().as_secs_f64();
        if elapsed > 0.0 {
            self.get_events_processed() as f64 / elapsed
        } else {
            0.0
        }
    }

    pub fn log_stats(&self) {
        let uptime = self.get_uptime();
        let blocks = self.get_blocks_processed();
        let events = self.get_events_processed();
        let db_ops = self.get_database_operations();
        let connections = self.get_network_connections();
        let errors = self.get_errors();

        info!("📊 Performance Statistics:");
        info!("  ⏱️  Uptime: {:?}", uptime);
        info!("  🗂️  Blocks processed: {} ({:.2}/sec)", blocks, self.get_blocks_per_second());
        info!("  📋 Events processed: {} ({:.2}/sec)", events, self.get_events_per_second());
        info!("  💾 Database operations: {}", db_ops);
        info!("  🔗 Network connections: {}", connections);
        info!("  ❌ Errors: {}", errors);

        if errors > 0 {
            let error_rate = (errors as f64 / (blocks.max(1) as f64)) * 100.0;
            if error_rate > 5.0 {
                warn!("⚠️  High error rate: {:.2}%", error_rate);
            }
        }
    }

    /// Start a background task that logs metrics every interval
    pub fn start_reporting_task(self, report_interval_secs: u64) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(report_interval_secs));

            loop {
                interval_timer.tick().await;
                self.log_stats();
            }
        })
    }
}



