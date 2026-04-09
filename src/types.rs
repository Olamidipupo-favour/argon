use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Address = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Source {
    Ws,
    Rest,
    OnChain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTradeEvent {
    pub source: Source,
    pub address: Address,
    pub token_id: String,
    pub side: Side,
    pub price: f64,
    pub size_usd: f64,
    pub timestamp_ms: u64,
    pub tx_hash: Option<String>,
    pub order_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuySignal {
    pub id: Uuid,
    pub emitted_at_ms: u64,
    pub token_id: String,
    pub market_slug: Option<String>,
    pub market_question: Option<String>,
    pub side: Side,
    pub price: f64,
    pub suggested_size_usd: f64,
    pub confidence: f64,
    pub sources: Vec<Source>,
    pub lead_address: Address,
    pub order_hash: Option<String>,
    pub tx_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketMeta {
    pub slug: String,
    pub question: String,
    pub end_date: String,
}

pub fn normalize_address(addr: &str) -> Address {
    let trimmed = addr.trim().to_lowercase();
    if trimmed.starts_with("0x") {
        trimmed
    } else {
        format!("0x{}", trimmed)
    }
}

pub fn now_ms() -> u64 {
    let now = std::time::SystemTime::now();
    now.duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
