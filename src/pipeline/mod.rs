use crate::audio::{AudioCapture, AudioStreamError};
use crate::config::{ModelPaths, PipelineConfig, PipelineMode};
use crate::translation::{self, TranslationResult};
use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

#[derive(Debug, Clone, serde::Serialize)]
pub struct TranscriptionEvent {
    pub text: String,
    pub tokens: Vec<TokenInfo>,
    pub is_final: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TokenInfo {
    pub t: String,
    pub p: f32,
}

#[derive(Debug, Clone)]
pub struct PipelineError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PipelineStatus {
    pub state: String,
}

#[derive(Clone)]
pub struct PipelineHandle {
    inner: Arc<Mutex<PipelineInner>>,
}

struct PipelineInner {
    running: bool,
    shutdown_tx: Option<watch::Sender<bool>>,
    state: PipelineState,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    Idle,
    Running,
    Error(String),
}

impl PipelineHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineInner {
                running: false,
                shutdown_tx: None,
                state: PipelineState::Idle,
            })),
        }
    }

    pub fn start(
        &self,
        config: PipelineConfig,
        event_tx: mpsc::UnboundedSender<TranscriptionEvent>,
        translation_tx: mpsc::UnboundedSender<TranslationResult>,
        error_tx: mpsc::UnboundedSender<PipelineError>,
        status_tx: mpsc::UnboundedSender<PipelineStatus>,
    ) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.running {
            anyhow::bail!("pipeline already running");
        }
        inner.state = PipelineState::Running;
        inner.running = true;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        inner.shutdown_tx = Some(shutdown_tx);

        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = run_pipeline(config, event_tx, translation_tx, error_tx, status_tx, shutdown_rx).await {
                tracing::error!("pipeline failed: {e:?}");
            }
            let mut inner = this.inner.lock().unwrap();
            inner.running = false;
            inner.shutdown_tx = None;
            inner.state = PipelineState::Idle;
        });

        Ok(())
    }

    pub fn stop(&self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(tx) = inner.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        inner.running = false;
        inner.state = PipelineState::Idle;
    }

    pub fn is_running(&self) -> bool {
        self.inner.lock().unwrap().running
    }
}

async fn run_pipeline(
    config: PipelineConfig,
    event_tx: mpsc::UnboundedSender<TranscriptionEvent>,
    translation_tx: mpsc::UnboundedSender<TranslationResult>,
    error_tx: mpsc::UnboundedSender<PipelineError>,
    status_tx: mpsc::UnboundedSender<PipelineStatus>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    // Validate model in a subprocess first (C++ code may abort on wrong model type)
    let mode_str = match config.pipeline_mode {
        PipelineMode::Streaming => "streaming",
        PipelineMode::VadOffline => "vad_offline",
    };
    if let Err(e) = validate_model(mode_str, &config.model_dir) {
        let msg = format!("model validation failed: {e}");
        tracing::error!("{msg}");
        let _ = error_tx.send(PipelineError {
            code: "model_load_failed".into(),
            message: msg,
        });
        return Ok(());
    }

    // Audio capture setup
    let buffer: Arc<Mutex<VecDeque<f32>>> =
        Arc::new(Mutex::new(VecDeque::with_capacity(2048)));
    let cb_buffer = buffer.clone();
    let mut capture = AudioCapture::new();
    let (audio_err_tx, mut audio_err_rx) = tokio::sync::mpsc::channel::<AudioStreamError>(16);
    let sample_hz = match capture.start(
        &config.audio_backend,
        config.device_name.as_deref(),
        Box::new(move |samples| {
            let mut buf = cb_buffer.lock().unwrap();
            buf.extend(samples);
        }),
        audio_err_tx,
    ) {
        Ok(hz) => hz,
        Err(e) => {
            let _ = error_tx.send(PipelineError {
                code: "audio_capture_failed".into(),
                message: format!("audio capture failed: {e}"),
            });
            return Err(e);
        }
    };

    let (result_tx, mut result_rx) =
        mpsc::unbounded_channel::<(String, bool)>();
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = shutdown.clone();

    let model_paths = config.model_paths();
    let translation_enabled = config.translation.enabled;
    let source_lang = config.language.clone();
    let translation_cfg = config.translation.clone();
    let vad_params = config.vad_params;

    let (model_start_tx, mut model_start_rx) = mpsc::unbounded_channel::<Result<()>>();

    match config.pipeline_mode {
        PipelineMode::Streaming => run_streaming(sample_hz, buffer, model_paths, result_tx, shutdown_clone, error_tx.clone(), model_start_tx),
        PipelineMode::VadOffline => {
            let vad_path = find_vad_model()
                .unwrap_or_else(|| PathBuf::from("models/silero_vad/silero_vad.onnx"));
            run_vad_offline(buffer, vad_path, vad_params, model_paths, result_tx, shutdown_clone, error_tx.clone(), model_start_tx);
        }
    }

    // Wait for model to start or fail
    match model_start_rx.recv().await {
        Some(Err(e)) => {
            let msg = format!("model load failed: {e}");
            tracing::error!("{msg}");
            let _ = error_tx.send(PipelineError {
                code: "model_load_failed".into(),
                message: msg,
            });
            let _ = capture.stop();
            shutdown.store(true, Ordering::Relaxed);
            return Ok(());
        }
        None => {
            let msg = "model thread exited unexpectedly".to_string();
            tracing::error!("{msg}");
            let _ = error_tx.send(PipelineError {
                code: "model_load_failed".into(),
                message: msg,
            });
            let _ = capture.stop();
            shutdown.store(true, Ordering::Relaxed);
            return Ok(());
        }
        _ => {}
    }

    // Translation thread
    let (translation_req_tx, mut translation_req_rx) =
        mpsc::unbounded_channel::<(String, String)>();
    let tile_shutdown = shutdown.clone();

    std::thread::spawn(move || {
        if !translation_enabled {
            return;
        }
        let cache_dir = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
            .unwrap_or_else(|| PathBuf::from("."));
        let cache = translation::TranslationCache::new(cache_dir);
        let translators = translation::create_translators(&translation_cfg, cache);

        loop {
            if tile_shutdown.load(Ordering::Relaxed) {
                break;
            }
            let (text, source_lang) = match translation_req_rx.blocking_recv() {
                Some(t) => t,
                None => break,
            };
            for t in &translators {
                match t.translate(&text, &source_lang) {
                    Ok(translated) => {
                        let _ = translation_tx.send(TranslationResult {
                            original: text.clone(),
                            provider_name: t.name().to_string(),
                            provider_display: t.display_name().to_string(),
                            text: translated,
                            target_lang: t.target_lang().to_string(),
                        });
                    }
                    Err(e) => {
                        tracing::error!("translation failed ({}): {e}", t.name());
                    }
                }
            }
        }
    });

    // Main async event loop
    loop {
        tokio::select! {
            Some((text, is_final)) = result_rx.recv() => {
                if is_final && translation_enabled {
                    let _ = translation_req_tx.send((text.clone(), source_lang.clone()));
                }
                let _ = event_tx.send(TranscriptionEvent {
                    text,
                    tokens: Vec::new(),
                    is_final,
                });
            }
            Some(audio_err) = audio_err_rx.recv() => {
                match audio_err {
                    AudioStreamError::DeviceLost => {
                        let _ = error_tx.send(PipelineError {
                            code: "device_lost".into(),
                            message: "audio device disconnected".into(),
                        });
                        let _ = capture.stop();
                        break;
                    }
                    AudioStreamError::Xrun => {
                        tracing::warn!("audio xrun, continuing");
                    }
                    AudioStreamError::RealtimeDenied => {
                        tracing::warn!("audio realtime denied, continuing");
                    }
                    AudioStreamError::Other(msg) => {
                        tracing::error!("audio error: {msg}");
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
            }
        }
    }

    shutdown.store(true, Ordering::Relaxed);
    let _ = capture.stop();
    tracing::info!("pipeline stopped cleanly");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_streaming(
    sample_hz: u32,
    buffer: Arc<Mutex<VecDeque<f32>>>,
    model_paths: crate::config::ModelPaths,
    result_tx: mpsc::UnboundedSender<(String, bool)>,
    shutdown: Arc<AtomicBool>,
    _error_tx: mpsc::UnboundedSender<PipelineError>,
    model_start_tx: mpsc::UnboundedSender<Result<()>>,
) {
    std::thread::spawn(move || {
        use sherpa_onnx::{OnlineRecognizer, OnlineRecognizerConfig};

        let mut recognizer_config = OnlineRecognizerConfig::default();
        recognizer_config.model_config.transducer.encoder = Some(model_paths.encoder);
        recognizer_config.model_config.transducer.decoder = Some(model_paths.decoder);
        recognizer_config.model_config.transducer.joiner = Some(model_paths.joiner);
        recognizer_config.model_config.tokens = Some(model_paths.tokens);
        recognizer_config.model_config.provider = Some("cpu".to_string());
        recognizer_config.enable_endpoint = true;
        recognizer_config.decoding_method = Some("greedy_search".to_string());

        let recognizer = match std::panic::catch_unwind(|| OnlineRecognizer::create(&recognizer_config)) {
            Ok(Some(r)) => r,
            Ok(None) => {
                let _ = model_start_tx.send(Err(anyhow::anyhow!(
                    "sherpa-onnx online model load failed (check model_dir points to a streaming model)"
                )));
                return;
            }
            Err(panic) => {
                let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown error (model format mismatch?)".into()
                };
                let _ = model_start_tx.send(Err(anyhow::anyhow!("model panic: {msg}")));
                return;
            }
        };
        let _ = model_start_tx.send(Ok(()));

        let stream = recognizer.create_stream();
        let mut audio_buf: Vec<f32> = Vec::new();
        let chunk_size: usize = 3200;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            let new_samples: Vec<f32> = {
                let mut buf = buffer.lock().unwrap();
                buf.drain(..).collect()
            };

            if new_samples.is_empty() {
                std::thread::sleep(Duration::from_millis(20));
                continue;
            }

            audio_buf.extend(new_samples);

            while audio_buf.len() >= chunk_size {
                let chunk: Vec<f32> = audio_buf.drain(..chunk_size).collect();
                stream.accept_waveform(sample_hz as i32, &chunk);

                while recognizer.is_ready(&stream) {
                    recognizer.decode(&stream);

                    if let Some(result) = recognizer.get_result(&stream) {
                        let text = result.text;
                        if !text.is_empty() {
                            let _ = result_tx.send((text, false));
                        }
                    }

                    if recognizer.is_endpoint(&stream) {
                        if let Some(result) = recognizer.get_result(&stream)
                            && !result.text.is_empty()
                        {
                            let _ = result_tx.send((result.text, true));
                        }
                        recognizer.reset(&stream);
                    }
                }
            }
        }
    });
}

fn panic_msg(context: &str, panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        format!("{context} panicked: {s}")
    } else if let Some(s) = panic.downcast_ref::<String>() {
        format!("{context} panicked: {s}")
    } else {
        format!("{context} panicked (unknown cause, likely model format mismatch)")
    }
}

fn find_vad_model() -> Option<PathBuf> {
    if let Ok(cwd) = std::env::current_dir() {
        let p = cwd.join("models/silero_vad/silero_vad.onnx");
        if p.exists() {
            return Some(p);
        }
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        for up in &["", "..", "../..", "../../.."] {
            let p = dir.join(up).join("models/silero_vad/silero_vad.onnx");
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn run_vad_offline(
    buffer: Arc<Mutex<VecDeque<f32>>>,
    vad_model_path: PathBuf,
    vad_params: crate::config::VadParams,
    model_paths: crate::config::ModelPaths,
    result_tx: mpsc::UnboundedSender<(String, bool)>,
    shutdown: Arc<AtomicBool>,
    _error_tx: mpsc::UnboundedSender<PipelineError>,
    model_start_tx: mpsc::UnboundedSender<Result<()>>,
) {
    std::thread::spawn(move || {
        use sherpa_onnx::{
            OfflineRecognizer, OfflineRecognizerConfig, SileroVadModelConfig, VadModelConfig,
            VoiceActivityDetector,
        };

        // Create VAD
        let vad_path_str = vad_model_path.to_string_lossy().to_string();
        let vad_config = VadModelConfig {
            silero_vad: SileroVadModelConfig {
                model: Some(vad_path_str),
                threshold: vad_params.threshold,
                min_silence_duration: vad_params.min_silence_duration,
                min_speech_duration: vad_params.min_speech_duration,
                window_size: 512,
                max_speech_duration: vad_params.max_speech_duration,
            },
            ten_vad: Default::default(),
            sample_rate: 16000,
            num_threads: 1,
            provider: Some("cpu".into()),
            debug: false,
        };
        let vad = match std::panic::catch_unwind(|| VoiceActivityDetector::create(&vad_config, 30.0)) {
            Ok(Some(v)) => v,
            Ok(None) => {
                let _ = model_start_tx.send(Err(anyhow::anyhow!(
                    "Silero VAD model load failed (check models/silero_vad/silero_vad.onnx exists)"
                )));
                return;
            }
            Err(panic) => {
                let msg = panic_msg("VAD model", &panic);
                let _ = model_start_tx.send(Err(anyhow::anyhow!("{msg}")));
                return;
            }
        };

        // Create OfflineRecognizer
        let mut offline_config = OfflineRecognizerConfig::default();
        offline_config.model_config.transducer.encoder = Some(model_paths.encoder);
        offline_config.model_config.transducer.decoder = Some(model_paths.decoder);
        offline_config.model_config.transducer.joiner = Some(model_paths.joiner);
        offline_config.model_config.tokens = Some(model_paths.tokens);
        offline_config.model_config.provider = Some("cpu".to_string());
        offline_config.decoding_method = Some("greedy_search".to_string());

        let recognizer = match std::panic::catch_unwind(|| OfflineRecognizer::create(&offline_config)) {
            Ok(Some(r)) => r,
            Ok(None) => {
                let _ = model_start_tx.send(Err(anyhow::anyhow!(
                    "OfflineRecognizer model load failed (check model_dir has encoder/decoder/joiner ONNX files)"
                )));
                return;
            }
            Err(panic) => {
                let msg = panic_msg("ASR model", &panic);
                let _ = model_start_tx.send(Err(anyhow::anyhow!("{msg}")));
                return;
            }
        };
        let _ = model_start_tx.send(Ok(()));

        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            let new_samples: Vec<f32> = {
                let mut buf = buffer.lock().unwrap();
                buf.drain(..).collect()
            };

            if !new_samples.is_empty() {
                vad.accept_waveform(&new_samples);
            }

            while let Some(segment) = vad.front() {
                let samples = segment.samples().to_vec();
                vad.pop();

                if !samples.is_empty() {
                    let stream = recognizer.create_stream();
                    stream.accept_waveform(16_000, &samples);
                    recognizer.decode(&stream);

                    if let Some(result) = stream.get_result()
                        && !result.text.is_empty()
                    {
                        let _ = result_tx.send((result.text, true));
                    }
                }
            }

            if new_samples.is_empty() {
                std::thread::sleep(Duration::from_millis(20));
            }
        }

        // Flush remaining VAD segments on shutdown
        vad.flush();
        while let Some(segment) = vad.front() {
            let samples = segment.samples().to_vec();
            vad.pop();
            if !samples.is_empty() {
                let stream = recognizer.create_stream();
                stream.accept_waveform(16_000, &samples);
                recognizer.decode(&stream);
                if let Some(result) = stream.get_result()
                    && !result.text.is_empty()
                {
                    let _ = result_tx.send((result.text, true));
                }
            }
        }
    });
}

/// Entry point for `--check-model` subprocess mode.
/// Tries to load the model and exits with 0 on success.
pub fn check_model(model_dir: &str, mode: &str) -> Result<()> {
    let paths = ModelPaths {
        encoder: format!("{model_dir}/encoder.onnx"),
        decoder: format!("{model_dir}/decoder.onnx"),
        joiner: format!("{model_dir}/joiner.onnx"),
        tokens: format!("{model_dir}/tokens.txt"),
    };

    for (name, p) in [("encoder.onnx", &paths.encoder), ("decoder.onnx", &paths.decoder),
                       ("joiner.onnx", &paths.joiner), ("tokens.txt", &paths.tokens)] {
        if !Path::new(p).exists() {
            eprintln!("missing model file: {name} at {p}");
            std::process::exit(1);
        }
    }

    match mode {
        "streaming" => {
            let mut cfg = sherpa_onnx::OnlineRecognizerConfig::default();
            cfg.model_config.transducer.encoder = Some(paths.encoder);
            cfg.model_config.transducer.decoder = Some(paths.decoder);
            cfg.model_config.transducer.joiner = Some(paths.joiner);
            cfg.model_config.tokens = Some(paths.tokens);
            cfg.model_config.provider = Some("cpu".to_string());
            cfg.enable_endpoint = true;
            cfg.decoding_method = Some("greedy_search".to_string());
            if sherpa_onnx::OnlineRecognizer::create(&cfg).is_none() {
                eprintln!("OnlineRecognizer: model load failed (wrong model type?)");
                std::process::exit(1);
            }
        }
        "vad_offline" => {
            let mut cfg = sherpa_onnx::OfflineRecognizerConfig::default();
            cfg.model_config.transducer.encoder = Some(paths.encoder);
            cfg.model_config.transducer.decoder = Some(paths.decoder);
            cfg.model_config.transducer.joiner = Some(paths.joiner);
            cfg.model_config.tokens = Some(paths.tokens);
            cfg.model_config.provider = Some("cpu".to_string());
            cfg.decoding_method = Some("greedy_search".to_string());
            if sherpa_onnx::OfflineRecognizer::create(&cfg).is_none() {
                eprintln!("OfflineRecognizer: model load failed (wrong model type?)");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("unknown mode: {mode}");
            std::process::exit(1);
        }
    }
    std::process::exit(0);
}

/// Spawn a subprocess to validate the model without risking C++ abort in the main process.
pub fn validate_model(mode: &str, model_dir: &str) -> Result<()> {
    let exe = std::env::current_exe()
        .map_err(|e| anyhow!("cannot get exe path: {e}"))?;
    let output = std::process::Command::new(&exe)
        .args(["--check-model", mode, model_dir])
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| anyhow!("failed to spawn validation subprocess: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let msg = if stderr.is_empty() {
            format!("model validation failed (exit code {})", output.status)
        } else {
            stderr.trim().to_string()
        };
        return Err(anyhow!("{msg}"));
    }
    Ok(())
}
