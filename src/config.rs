// use anyhow::Context; // Removed unused import
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    pub relay_address: String,
    pub connection_timeout_secs: u64,
    pub magic: u32,
    pub retry_attempts: u32,
    pub retry_delay_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessingConfig {
    pub max_concurrent_workers: usize,
    pub batch_size: usize,
    pub continuous_sync_interval_secs: u64,
    pub slot_hashes_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub network: NetworkConfig,
    pub processing: ProcessingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                url: "postgres://postgres:postgres@localhost:5432/cexplorer".to_string(),
                max_connections: 20,
                min_connections: 5,
                acquire_timeout_secs: 30,
                idle_timeout_secs: 300,
                max_lifetime_secs: 600,
            },
            network: NetworkConfig {
                relay_address: "backbone.mainnet.emurgornd.com:3001".to_string(),
                connection_timeout_secs: 10,
                magic: 764824073,
                retry_attempts: 3,
                retry_delay_secs: 5,
            },
            processing: ProcessingConfig {
                max_concurrent_workers: 20,
                batch_size: 1000,
                continuous_sync_interval_secs: 20,
                slot_hashes_file: "slot_hashes.csv".to_string(),
            },
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let mut config = Self::default();

        // Helper to get and trim env var
        let get_env = |key: &str| -> Option<String> {
            let val = std::env::var(key)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());

            if let Some(ref v) = val {
                if key.contains("URL") || key.contains("PASSWORD") {
                    tracing::debug!("Found environment variable: {}=********", key);
                } else {
                    tracing::debug!("Found environment variable: {}={}", key, v);
                }
            }
            val
        };

        // Database configuration
        if let Some(url) = get_env("DATABASE_URL") {
            tracing::info!("DATABASE_URL: loading from environment/dotenv");
            config.database.url = url;
        } else {
            tracing::warn!("DATABASE_URL: not found in environment, using hardcoded default");
        }

        if let Some(max_conn) = get_env("DB_MAX_CONNECTIONS") {
            tracing::info!(
                "DB_MAX_CONNECTIONS: loading from environment/dotenv ({})",
                max_conn
            );
            config.database.max_connections = max_conn
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid DB_MAX_CONNECTIONS: {}", max_conn))?;
        }
        if let Some(min_conn) = get_env("DB_MIN_CONNECTIONS") {
            config.database.min_connections = min_conn
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid DB_MIN_CONNECTIONS: {}", min_conn))?;
        }
        if let Some(timeout) = get_env("DB_ACQUIRE_TIMEOUT_SECS") {
            config.database.acquire_timeout_secs = timeout
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid DB_ACQUIRE_TIMEOUT_SECS: {}", timeout))?;
        }

        // Network configuration
        if let Some(relay) = get_env("RELAY_ADDRESS") {
            tracing::info!("RELAY_ADDRESS: loading from environment/dotenv ({})", relay);
            config.network.relay_address = relay;
        } else {
            tracing::warn!(
                "RELAY_ADDRESS: not found in environment, using hardcoded default: {}",
                config.network.relay_address
            );
        }

        if let Some(timeout) = get_env("CONNECTION_TIMEOUT_SECS") {
            config.network.connection_timeout_secs = timeout
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid CONNECTION_TIMEOUT_SECS: {}", timeout))?;
        }
        if let Some(magic) = get_env("NETWORK_MAGIC") {
            config.network.magic = magic
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid NETWORK_MAGIC: {}", magic))?;
        }
        if let Some(attempts) = get_env("RETRY_ATTEMPTS") {
            config.network.retry_attempts = attempts
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid RETRY_ATTEMPTS: {}", attempts))?;
        }

        // Processing configuration
        if let Some(workers) = get_env("MAX_CONCURRENT_WORKERS") {
            tracing::info!(
                "MAX_CONCURRENT_WORKERS: loading from environment/dotenv ({})",
                workers
            );
            config.processing.max_concurrent_workers = workers
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid MAX_CONCURRENT_WORKERS: {}", workers))?;
        }
        if let Some(batch_size) = get_env("BATCH_SIZE") {
            config.processing.batch_size = batch_size
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid BATCH_SIZE: {}", batch_size))?;
        }
        if let Some(interval) = get_env("CONTINUOUS_SYNC_INTERVAL_SECS") {
            config.processing.continuous_sync_interval_secs = interval.parse().map_err(|_| {
                anyhow::anyhow!("Invalid CONTINUOUS_SYNC_INTERVAL_SECS: {}", interval)
            })?;
        }
        if let Some(file) = get_env("SLOT_HASHES_FILE") {
            config.processing.slot_hashes_file = file;
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if !self.database.url.starts_with("postgres://")
            && !self.database.url.starts_with("postgresql://")
        {
            return Err(anyhow::anyhow!(
                "DATABASE_URL must start with postgres:// or postgresql:// (got: {})",
                self.database.url
            ));
        }

        if self.database.max_connections < self.database.min_connections {
            return Err(anyhow::anyhow!(
                "Database max_connections ({}) must be >= min_connections ({})",
                self.database.max_connections,
                self.database.min_connections
            ));
        }

        Ok(())
    }

    pub fn log_summary(&self) {
        // Mask database password
        let url = if let Ok(mut url) = url::Url::parse(&self.database.url) {
            if url.password().is_some() {
                let _ = url.set_password(Some("********"));
            }
            url.to_string()
        } else {
            "invalid-url".to_string()
        };

        tracing::info!("Configuration Summary:");
        tracing::info!("  Database URL: {}", url);
        tracing::info!(
            "  DB Pool: max={}, min={}",
            self.database.max_connections,
            self.database.min_connections
        );
        tracing::info!("  Relay: {}", self.network.relay_address);
        tracing::info!("  Workers: {}", self.processing.max_concurrent_workers);
        tracing::info!("  Batch Size: {}", self.processing.batch_size);
    }
}

// Cardano-specific constants
pub mod cardano {
    pub const SHELLEY_START_SLOT: u64 = 4492800;
    pub const SHELLEY_START_TIME: i64 = 1596059091; // 2020-07-29 21:44:51 UTC
    pub const SLOT_LENGTH_SECONDS: u64 = 1;
    pub const EPOCH_LENGTH_SLOTS: u64 = 432000;
    pub const SHELLEY_START_EPOCH: u64 = 208;
}
