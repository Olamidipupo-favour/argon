use std::time::Duration;

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

use crate::engine::DropOldestSender;
use crate::types::{normalize_address, now_ms, Address, RawTradeEvent, Side, Source};

const WS_ENDPOINT: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

pub async fn run_ws_clob(
    tracked: Vec<Address>,
    sender: DropOldestSender<RawTradeEvent>,
) -> Result<()> {
    let mut backoff_ms = 500u64;

    loop {
        match connect_async(WS_ENDPOINT).await {
            Ok((mut ws, _)) => {
                info!(endpoint = WS_ENDPOINT, "ws connected");
                backoff_ms = 500;

                for addr in &tracked {
                    let sub = serde_json::json!({
                        "type": "subscribe",
                        "channel": "user_order",
                        "user_address": addr,
                    });
                    ws.send(Message::Text(sub.to_string())).await.ok();
                }

                while let Some(msg) = ws.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            if let Some(event) = parse_trade(&text, &tracked) {
                                sender.send(event).await;
                            }
                        }
                        Ok(Message::Binary(bin)) => {
                            if let Ok(text) = String::from_utf8(bin) {
                                if let Some(event) = parse_trade(&text, &tracked) {
                                    sender.send(event).await;
                                }
                            }
                        }
                        Ok(Message::Close(_)) => {
                            warn!("ws closed");
                            break;
                        }
                        Err(err) => {
                            warn!(error = ?err, "ws error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => {
                warn!(error = ?err, "ws connect failed");
            }
        }

        sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

fn parse_trade(text: &str, tracked: &[Address]) -> Option<RawTradeEvent> {
    let value: Value = serde_json::from_str(text).ok()?;
    let event_type = value
        .get("type")
        .and_then(|v| v.as_str())
        .or_else(|| value.get("event").and_then(|v| v.as_str()))
        .unwrap_or("");

    if event_type != "trade" {
        return None;
    }

    let data = value.get("data").cloned().unwrap_or(value.clone());

    let maker = data
        .get("maker")
        .or_else(|| data.get("maker_address"))
        .and_then(|v| v.as_str())
        .map(normalize_address);

    let taker = data
        .get("taker")
        .or_else(|| data.get("taker_address"))
        .and_then(|v| v.as_str())
        .map(normalize_address);

    let address = maker
        .as_ref()
        .filter(|addr| tracked.contains(addr))
        .cloned()
        .or_else(|| taker.as_ref().filter(|addr| tracked.contains(addr)).cloned())?;

    let token_id = data
        .get("token_id")
        .or_else(|| data.get("tokenId"))
        .or_else(|| data.get("outcome_id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if token_id.is_empty() {
        return None;
    }

    let side = data
        .get("side")
        .and_then(|v| v.as_str())
        .map(|s| match s.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            _ => Side::Buy,
        })
        .unwrap_or(Side::Buy);

    let price = data
        .get("price")
        .or_else(|| data.get("price_usd"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let size_usd = data
        .get("size_usd")
        .or_else(|| data.get("notional"))
        .or_else(|| data.get("size"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let timestamp_ms = data
        .get("timestamp_ms")
        .or_else(|| data.get("timestamp"))
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_ms);

    let order_hash = data
        .get("order_hash")
        .or_else(|| data.get("orderHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let tx_hash = data
        .get("tx_hash")
        .or_else(|| data.get("txHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(RawTradeEvent {
        source: Source::Ws,
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
