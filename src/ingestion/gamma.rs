use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::{DashMap, DashSet};
use reqwest::Client;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, warn};

use crate::types::MarketMeta;

const GAMMA_ENDPOINT: &str = "https://gamma-api.polymarket.com/markets";

pub fn start_gamma_task(
    client: Client,
) -> (Arc<DashMap<String, MarketMeta>>, mpsc::UnboundedSender<String>) {
    let cache = Arc::new(DashMap::new());
    let tokens = Arc::new(DashSet::new());
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    let cache_clone = Arc::clone(&cache);
    let tokens_clone = Arc::clone(&tokens);

    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                Some(token_id) = rx.recv() => {
                    tokens_clone.insert(token_id);
                }
                _ = ticker.tick() => {
                    if let Err(err) = refresh_metadata(&client, &tokens_clone, &cache_clone).await {
                        warn!(error = ?err, "gamma refresh failed");
                    }
                }
            }
        }
    });

    (cache, tx)
}

async fn refresh_metadata(
    client: &Client,
    tokens: &DashSet<String>,
    cache: &DashMap<String, MarketMeta>,
) -> Result<()> {
    let token_ids: Vec<String> = tokens.iter().map(|entry| entry.key().clone()).collect();
    if token_ids.is_empty() {
        return Ok(());
    }

    let query = token_ids.join(",");
    let url = format!("{}?clob_token_ids={}", GAMMA_ENDPOINT, query);
    let resp = client.get(url).send().await?;
    let body: Value = resp.json().await?;

    let Some(markets) = body.as_array() else {
        return Ok(());
    };

    for market in markets {
        let slug = market.get("slug").and_then(|v| v.as_str()).unwrap_or("");
        let question = market
            .get("question")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let end_date = market
            .get("end_date")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let Some(clob_tokens) = market.get("clob_token_ids").and_then(|v| v.as_array()) else {
            continue;
        };

        for token in clob_tokens {
            if let Some(token_id) = token.as_str() {
                cache.insert(
                    token_id.to_string(),
                    MarketMeta {
                        slug: slug.to_string(),
                        question: question.to_string(),
                        end_date: end_date.to_string(),
                    },
                );
            }
        }
    }

    debug!(count = cache.len(), "gamma cache refreshed");
    Ok(())
}
