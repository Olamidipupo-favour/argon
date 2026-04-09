pub mod dedup;
pub mod scorer;

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, Mutex};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::WatcherConfig;
use crate::output::OutputHub;
use crate::types::{now_ms, Address, BuySignal, MarketMeta, RawTradeEvent, Source};

#[derive(Clone)]
pub struct DropOldestSender<T> {
    tx: mpsc::Sender<T>,
    rx: Arc<Mutex<mpsc::Receiver<T>>>,
    capacity: usize,
}

pub struct DropOldestReceiver<T> {
    rx: Arc<Mutex<mpsc::Receiver<T>>>,
}

pub fn channel<T>(capacity: usize) -> (DropOldestSender<T>, DropOldestReceiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    let rx = Arc::new(Mutex::new(rx));

    (
        DropOldestSender {
            tx,
            rx: Arc::clone(&rx),
            capacity,
        },
        DropOldestReceiver { rx },
    )
}

impl<T> DropOldestSender<T> {
    pub async fn send(&self, item: T) {
        match self.tx.try_send(item) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(item)) => {
                warn!(capacity = self.capacity, "event channel full, dropping oldest");
                let mut rx = self.rx.lock().await;
                let _ = rx.try_recv();
                drop(rx);
                if let Err(err) = self.tx.try_send(item) {
                    warn!(error = ?err, "event channel still full, dropping event");
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("event channel closed");
            }
        }
    }
}

impl<T> DropOldestReceiver<T> {
    pub async fn recv(&self) -> Option<T> {
        let mut rx = self.rx.lock().await;
        rx.recv().await
    }
}

struct Aggregate {
    first_event: RawTradeEvent,
    sources: HashSet<Source>,
    lead_address: Address,
    first_seen_ms: u64,
    emitted: bool,
}

pub struct SignalEngine {
    receiver: DropOldestReceiver<RawTradeEvent>,
    output: OutputHub,
    deduper: dedup::Deduper,
    aggregates: DashMap<String, Aggregate>,
    token_activity: DashMap<String, VecDeque<(Address, u64)>>,
    gamma_cache: Arc<DashMap<String, MarketMeta>>,
    gamma_sender: Option<mpsc::UnboundedSender<String>>,
    config: WatcherConfig,
}

impl SignalEngine {
    pub fn new(
        receiver: DropOldestReceiver<RawTradeEvent>,
        output: OutputHub,
        gamma_cache: Arc<DashMap<String, MarketMeta>>,
        gamma_sender: Option<mpsc::UnboundedSender<String>>,
        config: WatcherConfig,
    ) -> Self {
        Self {
            receiver,
            output,
            deduper: dedup::Deduper::new(5_000),
            aggregates: DashMap::new(),
            token_activity: DashMap::new(),
            gamma_cache,
            gamma_sender,
            config,
        }
    }

    pub async fn run(&self) {
        info!("signal engine started");

        while let Some(event) = self.receiver.recv().await {
            if event.size_usd < self.config.min_trade_usd {
                continue;
            }

            let now = now_ms();
            let key = event
                .order_hash
                .clone()
                .or(event.tx_hash.clone())
                .unwrap_or_else(|| format!("{}:{}:{}", event.address, event.token_id, event.timestamp_ms));

            let already_seen = self.deduper.check_and_update(&key, now);
            self.cleanup_aggregates(now);

            let mut aggregate = self.aggregates.entry(key.clone()).or_insert_with(|| Aggregate {
                first_event: event.clone(),
                sources: HashSet::from([event.source]),
                lead_address: event.address.clone(),
                first_seen_ms: now,
                emitted: false,
            });

            aggregate.sources.insert(event.source);

            let (lag_detected, multiple_addresses) =
                self.update_token_activity(&event.token_id, &event.address, now);

            if let Some(sender) = &self.gamma_sender {
                let _ = sender.send(event.token_id.clone());
            }

            let sources_count = aggregate.sources.len();
            let sources: Vec<Source> = aggregate.sources.iter().copied().collect();
            let only_rest = sources_count == 1 && aggregate.sources.contains(&Source::Rest);
            let onchain_confirmed = aggregate.sources.contains(&Source::OnChain);

            let confidence = scorer::compute_confidence(
                sources_count,
                event.size_usd,
                multiple_addresses,
                onchain_confirmed,
                only_rest,
                lag_detected,
            );

            if confidence < self.config.confidence_threshold {
                continue;
            }

            if already_seen && aggregate.emitted {
                continue;
            }

            if aggregate.emitted {
                continue;
            }

            let meta = self.gamma_cache.get(&event.token_id).map(|entry| entry.clone());

            let signal = BuySignal {
                id: Uuid::new_v4(),
                emitted_at_ms: now,
                token_id: event.token_id.clone(),
                market_slug: meta.as_ref().map(|m| m.slug.clone()),
                market_question: meta.as_ref().map(|m| m.question.clone()),
                side: event.side,
                price: event.price,
                suggested_size_usd: event.size_usd * 0.5,
                confidence,
                sources,
                lead_address: aggregate.lead_address.clone(),
                order_hash: event.order_hash.clone(),
                tx_hash: event.tx_hash.clone(),
            };

            aggregate.emitted = true;
            self.output.emit(signal).await;
        }
    }

    fn update_token_activity(
        &self,
        token_id: &str,
        address: &Address,
        now: u64,
    ) -> (bool, bool) {
        let window_ms = 200;
        let mut entry = self
            .token_activity
            .entry(token_id.to_string())
            .or_insert_with(|| VecDeque::new());

        while let Some((_, ts)) = entry.front() {
            if now.saturating_sub(*ts) > window_ms {
                entry.pop_front();
            } else {
                break;
            }
        }

        let seen_recent_any = !entry.is_empty();
        entry.push_back((address.clone(), now));

        let mut unique = HashSet::new();
        for (addr, _) in entry.iter() {
            unique.insert(addr);
        }

        let multiple_addresses = unique.len() >= 2;
        (seen_recent_any, multiple_addresses)
    }

    fn cleanup_aggregates(&self, now: u64) {
        let window_ms = 5_000;
        self.aggregates.retain(|_, agg| now.saturating_sub(agg.first_seen_ms) <= window_ms);
    }
}
