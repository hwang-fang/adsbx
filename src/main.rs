mod aggregator;
mod app;
mod config;
mod cpr;
mod decode;
mod dedup;
mod domain;
mod downsampler;
mod engine;
mod metrics;
mod receiver;
mod state;
mod time;
mod wire;
mod writer;

use anyhow::Result;
use clap::Parser;
use config::{Cli, Config, Mode};
use metrics::Metrics;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::from_cli(cli)?;
    let metrics = Metrics::new();

    tracing::info!(
        "starting adsbx: mode={:?} sensors={:?} block_size_ms={} dedup_ttl_ms={}",
        config.mode,
        config.sensors,
        config.block_size_ms,
        config.dedup_ttl_ms,
    );

    match config.mode {
        Mode::Realtime => app::run_realtime(config, metrics).await,
        Mode::Recompute => app::run_recompute(config, metrics).await,
    }
}
