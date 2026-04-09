use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use ethers::abi::RawLog;
use ethers::contract::{abigen, EthLogDecode};
use ethers::providers::{Middleware, Provider, Ws};
use ethers::types::{Filter, H256};
use ethers::utils::keccak256;
use futures_util::StreamExt;
use tracing::{info, warn};

use crate::engine::DropOldestSender;
use crate::types::{now_ms, Address, RawTradeEvent, Side, Source};

const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

abigen!(
    CtfExchange,
    r#"[
        event OrderFilled(bytes32 orderHash, address maker, address taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled, uint256 fee)
        event OrdersMatched(bytes32 takerOrderHash, address takerOrderMaker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmountFilled, uint256 takerAmountFilled)
    ]"#
);

pub async fn run_onchain(
    rpc_url: &str,
    tracked: Vec<Address>,
    sender: DropOldestSender<RawTradeEvent>,
) -> Result<()> {
    let tracked_set: HashSet<Address> = tracked.into_iter().collect();
    let provider = Provider::<Ws>::connect(rpc_url).await?;
    let provider = Arc::new(provider);

    let order_filled_sig = H256::from_slice(&keccak256(
        "OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)",
    ));
    let orders_matched_sig = H256::from_slice(&keccak256(
        "OrdersMatched(bytes32,address,uint256,uint256,uint256,uint256)",
    ));

    let filter = Filter::new()
        .address(CTF_EXCHANGE.parse::<ethers::types::Address>()?)
        .topic0(vec![order_filled_sig, orders_matched_sig]);

    let mut stream = provider.subscribe_logs(&filter).await?;
    info!("onchain subscription started");

    while let Some(log) = stream.next().await {
        match decode_log(log, &tracked_set) {
            Ok(Some(event)) => {
                sender.send(event).await;
            }
            Ok(None) => {}
            Err(err) => {
                warn!(error = ?err, "onchain decode failed");
            }
        }
    }

    Ok(())
}

fn decode_log(
    log: ethers::types::Log,
    tracked: &HashSet<Address>,
) -> Result<Option<RawTradeEvent>> {
    let raw_log = RawLog {
        topics: log.topics.clone(),
        data: log.data.to_vec(),
    };

    let decoded = CtfExchangeEvents::decode_log(&raw_log)?;

    match decoded {
        CtfExchangeEvents::OrderFilledFilter(event) => {
            let maker = format!("{:#x}", event.maker);
            let taker = format!("{:#x}", event.taker);

            let maker_tracked = tracked.contains(&maker);
            let tracked_addr = if maker_tracked {
                Some(maker.clone())
            } else if tracked.contains(&taker) {
                Some(taker.clone())
            } else {
                None
            };

            let Some(address) = tracked_addr else {
                return Ok(None);
            };

            let maker_asset = event.maker_asset_id;
            let taker_asset = event.taker_asset_id;
            let token_id = if maker_asset.is_zero() {
                taker_asset.to_string()
            } else {
                maker_asset.to_string()
            };

            let maker_amount = event.maker_amount_filled.as_u128() as f64;
            let taker_amount = event.taker_amount_filled.as_u128() as f64;

            let price = if maker_amount > 0.0 {
                taker_amount / maker_amount
            } else {
                0.0
            };

            let size_usd = if maker_asset.is_zero() {
                maker_amount / 1_000_000.0
            } else if taker_asset.is_zero() {
                taker_amount / 1_000_000.0
            } else {
                maker_amount / 1_000_000.0
            };

            let side = if maker_tracked {
                Side::Buy
            } else if maker_asset.is_zero() {
                Side::Buy
            } else {
                Side::Sell
            };

            let tx_hash = log.transaction_hash.map(|h| format!("{:#x}", h));
            let order_hash = Some(format!("{:#x}", H256::from(event.order_hash)));

            Ok(Some(RawTradeEvent {
                source: Source::OnChain,
                address,
                token_id,
                side,
                price,
                size_usd,
                timestamp_ms: now_ms(),
                tx_hash,
                order_hash,
            }))
        }
        CtfExchangeEvents::OrdersMatchedFilter(event) => {
            let taker_maker = format!("{:#x}", event.taker_order_maker);
            if !tracked.contains(&taker_maker) {
                return Ok(None);
            }

            let maker_asset = event.maker_asset_id;
            let taker_asset = event.taker_asset_id;
            let token_id = if maker_asset.is_zero() {
                taker_asset.to_string()
            } else {
                maker_asset.to_string()
            };

            let maker_amount = event.maker_amount_filled.as_u128() as f64;
            let taker_amount = event.taker_amount_filled.as_u128() as f64;

            let price = if maker_amount > 0.0 {
                taker_amount / maker_amount
            } else {
                0.0
            };

            let size_usd = if maker_asset.is_zero() {
                maker_amount / 1_000_000.0
            } else if taker_asset.is_zero() {
                taker_amount / 1_000_000.0
            } else {
                maker_amount / 1_000_000.0
            };

            let side = if maker_asset.is_zero() {
                Side::Buy
            } else {
                Side::Sell
            };

            let tx_hash = log.transaction_hash.map(|h| format!("{:#x}", h));
            let order_hash = Some(format!("{:#x}", H256::from(event.taker_order_hash)));

            Ok(Some(RawTradeEvent {
                source: Source::OnChain,
                address: taker_maker,
                token_id,
                side,
                price,
                size_usd,
                timestamp_ms: now_ms(),
                tx_hash,
                order_hash,
            }))
        }
    }
}
