#![forbid(unsafe_code)]

mod audio;
mod config;
mod ipc;
mod pipeline;
mod translation;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

const WS_ADDR: &str = "127.0.0.1:9876";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() >= 4 && args[1] == "--check-model" {
        return pipeline::check_model(&args[3], &args[2]);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    tracing::info!("whiscap-backend starting");

    let pipeline = pipeline::PipelineHandle::new();
    let ws_server = ipc::start_server(WS_ADDR, pipeline.clone()).await?;

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");

    ws_server.shutdown().await;
    Ok(())
}
