pub mod broadcast;
pub mod redis_sink;
pub mod stdout;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::types::BuySignal;

#[derive(Clone)]
pub struct OutputHub {
    pub broadcaster: broadcast::Sender<BuySignal>,
    redis: Option<Arc<redis_sink::RedisSink>>,
}

impl OutputHub {
    pub async fn new() -> Result<Self> {
        let (broadcaster, _) = broadcast::channel(256);
        let redis = redis_sink::RedisSink::new_from_env().await?;
        Ok(Self {
            broadcaster,
            redis: redis.map(Arc::new),
        })
    }

    pub async fn emit(&self, signal: BuySignal) {
        let _ = self.broadcaster.send(signal.clone());

        let signal_stdout = signal.clone();
        tokio::spawn(async move {
            let _ = stdout::emit_stdout(&signal_stdout).await;
        });

        if let Some(redis) = &self.redis {
            let redis = Arc::clone(redis);
            tokio::spawn(async move {
                let _ = redis.publish(&signal).await;
            });
        }
    }
}
