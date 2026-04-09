use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde_json::Value;
use tokio::time::{interval, timeout};
use tracing::{debug, warn};

use crate::engine::DropOldestSender;
use crate::types::{normalize_address, now_ms, Address, RawTradeEvent, Side, Source};

const REST_BASE: &str = "https://clob.polymarket.com";

pub async fn run_rest_clob(
    tracked: Vec<Address>,
    sender: DropOldestSender<RawTradeEvent>,
    client: Client,
    poll_interval: Duration,
) -> Result<()> {
    for addr in tracked {
        let sender = sender.clone();
        let client = client.clone();
        tokio::spawn(async move {
            let mut ticker = interval(poll_interval);
            loop {
                ticker.tick().await;
                if let Err(err) = poll_address(&addr, &sender, &client).await {
                    warn!(address = addr.as_str(), error = ?err, "rest poll failed");
                }
            }
        });
    }

    Ok(())
}

async fn poll_address(addr: &str, sender: &DropOldestSender<RawTradeEvent>, client: &Client) -> Result<()> {
    let url = format!("{}/trades?maker_address={}&limit=20", REST_BASE, addr);
    let resp = client.get(url).send().await?;
    let trades: Value = resp.json().await?;

    let Some(items) = trades.as_array() else {
        return Ok(());
    };

    for item in items {
        if let Some(event) = parse_trade(item, addr) {
            if !event.token_id.is_empty() {
                let _ = fetch_order_book(client, &event.token_id).await;
            }
            sender.send(event).await;
        }
    }

    Ok(())
}

fn parse_trade(item: &Value, addr: &str) -> Option<RawTradeEvent> {
    let maker = item
        .get("maker_address")
        .or_else(|| item.get("maker"))
        .and_then(|v| v.as_str())
        .map(normalize_address);

    let taker = item
        .get("taker_address")
        .or_else(|| item.get("taker"))
        .and_then(|v| v.as_str())
        .map(normalize_address);

    let address = if maker.as_deref() == Some(addr) {
        maker.clone()?
    } else if taker.as_deref() == Some(addr) {
        taker.clone()?
    } else {
        return None;
    };

    let token_id = item
        .get("token_id")
        .or_else(|| item.get("tokenId"))
        .or_else(|| item.get("outcome_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let side = item
        .get("side")
        .and_then(|v| v.as_str())
        .map(|s| match s.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => Side::Buy,
        })
        .unwrap_or(Side::Buy);

    let price = item
        .get("price")
        .or_else(|| item.get("price_usd"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let size_usd = item
        .get("size_usd")
        .or_else(|| item.get("notional"))
        .or_else(|| item.get("size"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let timestamp_ms = item
        .get("timestamp_ms")
        .or_else(|| item.get("timestamp"))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_ms);

    let order_hash = item
        .get("order_hash")
        .or_else(|| item.get("orderHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tx_hash = item
        .get("tx_hash")
        .or_else(|| item.get("txHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(RawTradeEvent {
        source: Source::Rest,
        address,
        token_id,
        side,
        price,
        size_usd,
        timestamp_ms,
        tx_hash,
        order_hash,
    })
}

async fn fetch_order_book(client: &Client, token_id: &str) -> Result<()> {
    let url = format!("{}/order-book/{}", REST_BASE, token_id);
    let request = client.get(url);

    let response = timeout(Duration::from_millis(600), request.send()).await;
    match response {
        Ok(Ok(resp)) => {
            let _body: Value = resp.json().await.unwrap_or(Value::Null);
            debug!(token_id = token_id, "order book refreshed");
        }
        Ok(Err(err)) => {
            debug!(token_id = token_id, error = ?err, "order book fetch failed");
        }
        Err(_) => {
            debug!(token_id = token_id, "order book fetch timed out");
        }
    }

    Ok(())
}
