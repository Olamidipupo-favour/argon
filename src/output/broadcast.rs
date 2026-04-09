use tokio::sync::broadcast;

use crate::types::BuySignal;

pub fn channel() -> broadcast::Sender<BuySignal> {
    let (sender, _) = broadcast::channel(256);
    sender
}
