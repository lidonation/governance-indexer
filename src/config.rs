// use anyhow::Context; // Removed unused import
use serde::Deserialize;
use std::env;

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
                max_concurrent_workers: 10,
                batch_size: 100,
                continuous_sync_interval_secs: 20,
                slot_hashes_file: "slot_hashes.csv".to_string(),
            },
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let mut config = Self::default();

        // Database configuration
        if let Ok(url) = env::var("DATABASE_URL") {
            config.database.url = url;
        }
        if let Ok(max_conn) = env::var("DB_MAX_CONNECTIONS") {
            config.database.max_connections = max_conn.parse()?;
        }
        if let Ok(min_conn) = env::var("DB_MIN_CONNECTIONS") {
            config.database.min_connections = min_conn.parse()?;
        }
        if let Ok(timeout) = env::var("DB_ACQUIRE_TIMEOUT_SECS") {
            config.database.acquire_timeout_secs = timeout.parse()?;
        }
        

        // Network configuration
        if let Ok(relay) = env::var("RELAY_ADDRESS") {
            config.network.relay_address = relay;
        }
        if let Ok(timeout) = env::var("CONNECTION_TIMEOUT_SECS") {
            config.network.connection_timeout_secs = timeout.parse()?;
        }
        if let Ok(magic) = env::var("NETWORK_MAGIC") {
            config.network.magic = magic.parse()?;
        }
        if let Ok(attempts) = env::var("RETRY_ATTEMPTS") {
            config.network.retry_attempts = attempts.parse()?;
        }

        // Processing configuration
        if let Ok(workers) = env::var("MAX_CONCURRENT_WORKERS") {
            config.processing.max_concurrent_workers = workers.parse()?;
        }
        if let Ok(batch_size) = env::var("BATCH_SIZE") {
            config.processing.batch_size = batch_size.parse()?;
        }
        if let Ok(interval) = env::var("CONTINUOUS_SYNC_INTERVAL_SECS") {
            config.processing.continuous_sync_interval_secs = interval.parse()?;
        }
        if let Ok(file) = env::var("SLOT_HASHES_FILE") {
            config.processing.slot_hashes_file = file;
        }

        Ok(config)
    }

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        if self.database.max_connections < self.database.min_connections {
            return Err(anyhow::anyhow!(
                "Database max_connections must be >= min_connections"
            ));
        }

        if self.processing.max_concurrent_workers == 0 {
            return Err(anyhow::anyhow!(
                "max_concurrent_workers must be greater than 0"
            ));
        }

        if self.processing.batch_size == 0 {
            return Err(anyhow::anyhow!("batch_size must be greater than 0"));
        }

        Ok(())
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