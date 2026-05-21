#![forbid(unsafe_code)]

mod audio;
mod config;
mod ipc;
mod pipeline;
mod translation;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

const WS_HOST: &str = "127.0.0.1";
const WS_BASE_PORT: u16 = 9876;

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

    tracing::info!("oncaptions-backend starting");

    let pipeline = pipeline::PipelineHandle::new();
    let (ws_server, _port) = ipc::start_server(WS_HOST, WS_BASE_PORT, pipeline.clone()).await?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = ws_server.wait_shutdown() => {
            tracing::info!("shutdown via ws");
        }
    }
    tracing::info!("shutting down");

    pipeline.stop().await;
    ws_server.shutdown().await;

    let _ = std::fs::remove_file("/tmp/oncaptions-port");

    Ok(())
}
