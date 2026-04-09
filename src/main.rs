mod config;
mod engine;
mod ingestion;
mod output;
mod types;

use std::time::Duration;

use anyhow::Result;
use tracing::{info, warn};

use crate::config::AppConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "argon=info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cfg = AppConfig::load()?;
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .build()?;

    let (gamma_cache, gamma_sender) = ingestion::gamma::start_gamma_task(client.clone());
    let (sender, receiver) = engine::channel(1024);
    let output = output::OutputHub::new().await?;

    let tracked = cfg.watcher.tracked_addresses.clone();

    tokio::spawn(ingestion::ws_clob::run_ws_clob(tracked.clone(), sender.clone()));
    tokio::spawn(ingestion::rest_clob::run_rest_clob(
        tracked.clone(),
        sender.clone(),
        client.clone(),
        Duration::from_millis(cfg.watcher.poll_interval_ms),
    ));
    let rpc_url = cfg.watcher.rpc_url.clone();
    let onchain_tracked = tracked.clone();
    let onchain_sender = sender.clone();
    tokio::spawn(async move {
        let _ = ingestion::onchain::run_onchain(&rpc_url, onchain_tracked, onchain_sender).await;
    });

    let engine = engine::SignalEngine::new(
        receiver,
        output,
        gamma_cache,
        Some(gamma_sender),
        cfg.watcher.clone(),
    );
    tokio::spawn(async move {
        engine.run().await;
    });

    info!("argon signaler running");
    if let Err(err) = tokio::signal::ctrl_c().await {
        warn!(error = ?err, "failed to listen for ctrl-c");
    }

    Ok(())
}
