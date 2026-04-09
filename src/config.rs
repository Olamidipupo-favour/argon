use anyhow::Result;
use serde::Deserialize;

use crate::types::normalize_address;

fn default_poll_interval_ms() -> u64 {
    400
}

fn default_min_trade_usd() -> f64 {
    50.0
}

fn default_confidence_threshold() -> f64 {
    0.6
}

#[derive(Debug, Clone, Deserialize)]
pub struct WatcherConfig {
    pub tracked_addresses: Vec<String>,
    #[serde(default = "default_min_trade_usd")]
    pub min_trade_usd: f64,
    #[serde(default = "default_confidence_threshold")]
    pub confidence_threshold: f64,
    pub rpc_url: String,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub watcher: WatcherConfig,
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let mut cfg: AppConfig = config::Config::builder()
            .add_source(config::File::with_name("config"))
            .add_source(config::Environment::with_prefix("ARGON").separator("__"))
            .build()?
            .try_deserialize()?;

        cfg.watcher.tracked_addresses = cfg
            .watcher
            .tracked_addresses
            .iter()
            .map(|addr| normalize_address(addr))
            .collect();

        Ok(cfg)
    }
}
