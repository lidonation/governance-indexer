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
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_retry_delay_secs")]
    pub retry_delay_secs: u64,
}

fn default_retry_attempts() -> u32 {
    0
}

fn default_retry_delay_secs() -> u64 {
    5
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    pub relay_address: String,
    pub relays: Vec<String>,
    pub connection_timeout_secs: u64,
    pub magic: u32,
    pub retry_attempts: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProcessingConfig {
    pub max_concurrent_workers: usize,
    pub batch_size: usize,
    pub continuous_sync_interval_secs: u64,
    pub slot_hashes_file: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinksConfig {
    pub postgres_enabled: bool,
    pub webhook_enabled: bool,
    pub webhook_url: Option<String>,
    pub webhook_secret: Option<String>,
}

impl Default for SinksConfig {
    fn default() -> Self {
        Self {
            postgres_enabled: true,
            webhook_enabled: false,
            webhook_url: None,
            webhook_secret: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub network: NetworkConfig,
    pub processing: ProcessingConfig,
    pub sinks: SinksConfig,
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
                retry_attempts: 0,
                retry_delay_secs: 5,
            },
            network: NetworkConfig {
                relay_address: "backbone.mainnet.emurgornd.com:3001".to_string(),
                relays: vec![
                    "backbone.mainnet.emurgornd.com:3001".to_string(),
                    "relays-new.cardano-mainnet.iohk.io:3001".to_string(),
                    "relay1.lidonation.com:3011".to_string(),
                ],
                connection_timeout_secs: 10,
                magic: 764824073,
                retry_attempts: 0,
            },
            processing: ProcessingConfig {
                max_concurrent_workers: 10,
                batch_size: 1000,
                continuous_sync_interval_secs: 20,
                slot_hashes_file: "slot_hashes.csv".to_string(),
            },
            sinks: SinksConfig::default(),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, anyhow::Error> {
        let mut config = Self::default();

        // 1. Manually load .env file into a Map for priority handling
        // This allows us to use .env values even if the environment has an EMPTY variable shadowing it.
        let mut env_file_vars = std::collections::HashMap::new();
        if let Ok(iter) = dotenvy::from_path_iter(".env") {
            for item in iter {
                if let Ok((k, v)) = item {
                    let k = k.trim().to_string();
                    let v = v.trim().to_string();
                    if !v.is_empty() {
                        env_file_vars.insert(k, v);
                    }
                }
            }
            tracing::info!(
                "Loaded {} non-empty variables from .env file",
                env_file_vars.len()
            );
        }

        // Helper to get and trim env var with override logic
        let get_env = |key: &str| -> Option<String> {
            // Priority 1: Real Environment Variable (must be non-empty)
            if let Ok(v) = std::env::var(key) {
                let trimmed = v.trim();
                if !trimmed.is_empty() {
                    if key.contains("URL") || key.contains("PASSWORD") {
                        tracing::info!("Found variable {} in environment: (hidden)", key);
                    } else {
                        tracing::info!("Found variable {} in environment: {}", key, trimmed);
                    }
                    return Some(trimmed.to_string());
                }
                tracing::warn!("Variable {} is present but EMPTY in environment", key);
            }

            // Priority 2: Baked-in .env file (even if shadowed in env by empty string)
            if let Some(v) = env_file_vars.get(key) {
                if key.contains("URL") || key.contains("PASSWORD") {
                    tracing::info!("Found variable {} in .env file: (hidden)", key);
                } else {
                    tracing::info!("Found variable {} in .env file: {}", key, v);
                }
                return Some(v.clone());
            }

            // Priority 3: BOM-aware search in all environment variables
            for (k, v) in std::env::vars() {
                if k.contains(key) && k.len() <= key.len() + 4 {
                    let trimmed = v.trim();
                    if !trimmed.is_empty() {
                        tracing::info!(
                            "Found variable {} (as {:?}) in environment fallback",
                            key,
                            k
                        );
                        return Some(trimmed.to_string());
                    }
                }
            }

            tracing::debug!("Variable {} not found in any source", key);
            None
        };

        // Database configuration
        if let Some(url) = get_env("DATABASE_URL") {
            config.database.url = url;
        } else {
            tracing::error!("❌ DATABASE_URL: NOT FOUND in any source. Using hardcoded fallback!");
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
        if let Some(attempts) = get_env("DB_RETRY_ATTEMPTS") {
            config.database.retry_attempts = attempts
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid DB_RETRY_ATTEMPTS: {}", attempts))?;
        }
        if let Some(delay) = get_env("DB_RETRY_DELAY_SECS") {
            config.database.retry_delay_secs = delay
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid DB_RETRY_DELAY_SECS: {}", delay))?;
        }

        // Network configuration
        // 1. Try RELAY_ADDRESSES first (comma separated)
        let mut relays = Vec::new();
        if let Some(relay_list) = get_env("RELAY_ADDRESSES") {
            tracing::info!(
                "RELAY_ADDRESSES: loading from environment/dotenv ({})",
                relay_list
            );
            relays = relay_list
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        // 2. If RELAY_ADDRESS is set, add it (if not already present)
        if let Some(relay) = get_env("RELAY_ADDRESS") {
            tracing::info!("RELAY_ADDRESS: loading from environment/dotenv ({})", relay);
            // If it contains commas, treat it as a list too (backward compatibility)
            if relay.contains(',') {
                let extra: Vec<String> = relay
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                for r in extra {
                    if !relays.contains(&r) {
                        relays.push(r);
                    }
                }
            } else if !relays.contains(&relay) {
                relays.insert(0, relay); // Prepend single relay_address as primary
            }
        }

        if relays.is_empty() {
            tracing::warn!(
                "RELAY_ADDRESS: not found in environment, using hardcoded default: {}",
                config.network.relay_address
            );
            relays.push(config.network.relay_address.clone());
        }

        // Update config
        if let Some(first) = relays.first() {
            config.network.relay_address = first.clone();
        }
        config.network.relays = relays;

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
        if let Some(retry_attempts) = get_env("RETRY_ATTEMPTS") {
            config.network.retry_attempts = retry_attempts
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid RETRY_ATTEMPTS: {}", retry_attempts))?;
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

        // Sink configuration
        if let Some(v) = get_env("SINK_POSTGRES_ENABLED") {
            config.sinks.postgres_enabled = v.to_lowercase() != "false" && v != "0";
        }
        if let Some(v) = get_env("SINK_WEBHOOK_ENABLED") {
            config.sinks.webhook_enabled = v.to_lowercase() == "true" || v == "1";
        }
        if let Some(url) = get_env("SINK_WEBHOOK_URL") {
            config.sinks.webhook_url = Some(url);
        }
        if let Some(secret) = get_env("SINK_WEBHOOK_SECRET") {
            config.sinks.webhook_secret = Some(secret);
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
        tracing::info!(
            "  DB Retry: attempts={} (0=infinite), delay={}s",
            self.database.retry_attempts,
            self.database.retry_delay_secs
        );
        tracing::info!("  Relay: {}", self.network.relay_address);
        tracing::info!("  Workers: {}", self.processing.max_concurrent_workers);
        tracing::info!("  Batch Size: {}", self.processing.batch_size);
        tracing::info!("  Sinks: postgres={}, webhook={}", self.sinks.postgres_enabled, self.sinks.webhook_enabled);
        if self.sinks.webhook_enabled {
            if let Some(url) = &self.sinks.webhook_url {
                tracing::info!("  Webhook URL: {}", url);
            }
        }
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
