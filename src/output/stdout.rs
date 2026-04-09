use anyhow::Result;
use tokio::io::{self, AsyncWriteExt};

use crate::types::BuySignal;

pub async fn emit_stdout(signal: &BuySignal) -> Result<()> {
    let line = serde_json::to_string(signal)?;
    let mut stdout = io::stdout();
    stdout.write_all(line.as_bytes()).await?;
    stdout.write_all(b"\n").await?;
    Ok(())
}
