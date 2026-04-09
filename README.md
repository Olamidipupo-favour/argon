# Argon

High-performance Polymarket lag-arb watcher that emits structured buy signals.

## Setup

- Update `config.toml` with tracked addresses and RPC URL.
- Optional Redis output: set `REDIS_URL`.

## Run

```bash
cargo run --release
```

## Stdout piping examples

```bash
# Write signals to a file
cargo run --release | tee signals.ndjson

# Pipe signals to a downstream executor process
cargo run --release | ./executor --stdin
```
