use std::env;
use std::sync::Arc;

use anyhow::Result;
use redis::AsyncCommands;
use tokio::sync::Mutex;
use tracing::warn;

use crate::types::BuySignal;

pub struct RedisSink {
    conn: Arc<Mutex<redis::aio::MultiplexedConnection>>,
}

impl RedisSink {
    pub async fn new_from_env() -> Result<Option<Self>> {
        let url = env::var("REDIS_URL").ok();
        let Some(url) = url else {
            return Ok(None);
        };

        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;

        Ok(Some(Self {
            conn: Arc::new(Mutex::new(conn)),
        }))
    }

    pub async fn publish(&self, signal: &BuySignal) -> Result<()> {
        let payload = serde_json::to_string(signal)?;
        let mut conn = self.conn.lock().await;
        let result: redis::RedisResult<()> = conn.publish("polymarket:signals", payload).await;
        if let Err(err) = result {
            warn!(error = ?err, "redis publish failed");
        }
        Ok(())
    }
}
