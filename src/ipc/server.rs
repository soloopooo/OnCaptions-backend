use crate::audio::enumerator;
use crate::config::{AudioBackend, PipelineConfig, PipelineMode, VadParams};
use crate::pipeline::{PipelineError, PipelineHandle, PipelineStatus, TranscriptionEvent};
use crate::translation::{TranslationConfig, TranslationResult};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::Message;

pub struct WsServer {
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Deserialize)]
struct IncomingMessage {
    #[serde(rename = "type")]
    msg_type: String,
    model_dir: Option<String>,
    language: Option<String>,
    translate: Option<bool>,
    audio_backend: Option<String>,
    device_name: Option<String>,
    #[serde(default)]
    translation: Option<TranslationConfig>,
    pipeline_mode: Option<String>,
    vad_threshold: Option<f32>,
    vad_min_silence_duration: Option<f32>,
    vad_min_speech_duration: Option<f32>,
    vad_max_speech_duration: Option<f32>,
}

pub async fn start_server(
    base_addr: &str,
    base_port: u16,
    pipeline: PipelineHandle,
) -> Result<(Arc<WsServer>, u16)> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let max_port = base_port + 24;
    let mut listener = None;
    let mut port = base_port;
    for p in base_port..=max_port {
        let addr = format!("{base_addr}:{p}");
        match TcpListener::bind(&addr).await {
            Ok(l) => {
                listener = Some(l);
                port = p;
                break;
            }
            Err(_) => continue,
        }
    }
    let listener = listener.ok_or_else(|| {
        anyhow::anyhow!("no available port in {base_port}..={max_port}")
    })?;

    let port_content = port.to_string();
    if let Err(e) = std::fs::write("/tmp/oncaptions-port", &port_content) {
        tracing::warn!("failed to write /tmp/oncaptions-port: {e}");
    }

    let server = Arc::new(WsServer { shutdown_tx });
    let sv = server.clone();

    let mut shutdown_rx = shutdown_rx;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow_and_update() {
                        tracing::info!("ws server: shutting down accept loop");
                        break;
                    }
                }
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            tracing::info!("ws client connected: {peer}");
                            let pl = pipeline.clone();
                            let stx = sv.shutdown_tx.clone();
                            tokio::spawn(handle_connection(stream, pl, stx));
                        }
                        Err(e) => {
                            tracing::error!("accept error: {e}");
                        }
                    }
                }
            }
        }
    });

    tracing::info!("ws server listening on {base_addr}:{port}");
    Ok((server, port))
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    pipeline: PipelineHandle,
    shutdown_tx: watch::Sender<bool>,
) {
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("ws handshake failed");
    let (mut tx, mut rx) = ws_stream.split();
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<TranscriptionEvent>();
    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel::<PipelineError>();
    let (status_tx, mut status_rx) = tokio::sync::mpsc::unbounded_channel::<PipelineStatus>();
    let (translation_tx, mut translation_rx) =
        tokio::sync::mpsc::unbounded_channel::<TranslationResult>();

    let tr_tx = translation_tx.clone();
    loop {
        tokio::select! {
            msg = rx.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        match handle_message(&text, &pipeline, &event_tx, &tr_tx, &error_tx, &status_tx, &shutdown_tx).await {
                            Ok(Some(response)) => {
                                if response.get("type") == Some(&serde_json::Value::String("shutdown".into())) {
                                    let msg = response.to_string();
                                    let _ = tx.send(Message::Text(msg)).await;
                                    break;
                                }
                                let msg = response.to_string();
                                let _ = tx.send(Message::Text(msg)).await;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                tracing::error!("handle msg: {e:?}");
                                let err = serde_json::json!({
                                    "type": "error",
                                    "code": "invalid_message",
                                    "message": e.to_string(),
                                });
                                let msg = err.to_string();
                                let _ = tx.send(Message::Text(msg)).await;
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            Some(evt) = event_rx.recv() => {
                let msg = serde_json::json!({
                    "type": "transcription",
                    "text": evt.text,
                    "tokens": evt.tokens,
                    "is_final": evt.is_final,
                });
                let msg = msg.to_string();
                let _ = tx.send(Message::Text(msg)).await;
            }
            Some(status) = status_rx.recv() => {
                let msg = serde_json::json!({
                    "type": "pipeline_status",
                    "state": status.state,
                });
                let _ = tx.send(Message::Text(msg.to_string())).await;
            }
            Some(err) = error_rx.recv() => {
                let msg = serde_json::json!({
                    "type": "error",
                    "code": err.code,
                    "message": err.message,
                });
                let _ = tx.send(Message::Text(msg.to_string())).await;
                let status = serde_json::json!({
                    "type": "pipeline_status",
                    "state": "error",
                });
                let _ = tx.send(Message::Text(status.to_string())).await;
            }
            Some(tr) = translation_rx.recv() => {
                let msg = serde_json::json!({
                    "type": "translation",
                    "original": tr.original,
                    "provider_name": tr.provider_name,
                    "provider_display": tr.provider_display,
                    "text": tr.text,
                    "target_lang": tr.target_lang,
                });
                let _ = tx.send(Message::Text(msg.to_string())).await;
            }
            else => break,
        }
    }
}

async fn handle_message(
    text: &str,
    pipeline: &PipelineHandle,
    event_tx: &tokio::sync::mpsc::UnboundedSender<TranscriptionEvent>,
    translation_tx: &tokio::sync::mpsc::UnboundedSender<TranslationResult>,
    error_tx: &tokio::sync::mpsc::UnboundedSender<PipelineError>,
    status_tx: &tokio::sync::mpsc::UnboundedSender<PipelineStatus>,
    shutdown_tx: &watch::Sender<bool>,
) -> Result<Option<serde_json::Value>> {
    let msg: IncomingMessage = serde_json::from_str(text)?;

    if msg.msg_type == "start_pipeline" {
        tracing::debug!("start_pipeline params: {text}");
    }

    match msg.msg_type.as_str() {
        "start_pipeline" => {
            let backend = msg
                .audio_backend
                .as_deref()
                .map(parse_backend)
                .unwrap_or(Ok(AudioBackend::Alsa))?;

            let model_dir = msg.model_dir.unwrap_or_default();
            if !Path::new(&model_dir).is_absolute() {
                anyhow::bail!("model_dir must be absolute");
            }

            let pipeline_mode = msg
                .pipeline_mode
                .as_deref()
                .and_then(PipelineMode::from_str)
                .unwrap_or(PipelineMode::Streaming);

            let vad_params = VadParams {
                threshold: msg.vad_threshold.unwrap_or(0.4),
                min_silence_duration: msg.vad_min_silence_duration.unwrap_or(0.8),
                min_speech_duration: msg.vad_min_speech_duration.unwrap_or(0.25),
                max_speech_duration: msg.vad_max_speech_duration.unwrap_or(15.0),
            };

            let config = PipelineConfig {
                model_dir,
                language: msg.language.unwrap_or_else(|| "auto".into()),
                translate: msg.translate.unwrap_or(false),
                audio_backend: backend,
                device_name: msg.device_name,
                translation: msg.translation.unwrap_or_default(),
                pipeline_mode,
                vad_params,
            };

            let evt_tx = event_tx.clone();
            let tr_tx = translation_tx.clone();
            let err_tx = error_tx.clone();
            let st_tx = status_tx.clone();
            if let Err(e) = pipeline.start(config, evt_tx, tr_tx, err_tx, st_tx) {
                return Ok(Some(serde_json::json!({
                    "type": "error",
                    "code": "start_failed",
                    "message": e.to_string(),
                })));
            }

            Ok(Some(serde_json::json!({
                "type": "pipeline_status",
                "state": "running",
            })))
        }

        "stop_pipeline" => {
            pipeline.stop();
            Ok(Some(serde_json::json!({
                "type": "pipeline_status",
                "state": "stopped",
            })))
        }

        "list_devices" => {
            let devices = enumerator::enumerate_devices();
            Ok(Some(serde_json::json!({
                "type": "device_list",
                "devices": devices,
            })))
        }

        "shutdown" => {
            tracing::info!("shutdown requested via ws");
            let _ = shutdown_tx.send(true);
            Ok(Some(serde_json::json!({
                "type": "shutdown",
                "ack": true,
            })))
        }

        other => {
            anyhow::bail!("unknown message type: {other}");
        }
    }
}

fn parse_backend(s: &str) -> Result<AudioBackend> {
    match s {
        "alsa" => Ok(AudioBackend::Alsa),
        "pulse" => Ok(AudioBackend::Pulse),
        "jack" => Ok(AudioBackend::Jack),
        "pipewire" => Ok(AudioBackend::PipeWire),
        _ => anyhow::bail!("unknown audio backend: {s}"),
    }
}

impl WsServer {
    pub async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    pub async fn wait_shutdown(&self) {
        let mut rx = self.shutdown_tx.subscribe();
        let _ = rx.changed().await;
    }
}
