use crate::translation::TranslationConfig;

#[derive(Debug, Clone)]
pub struct ModelPaths {
    pub encoder: String,
    pub decoder: String,
    pub joiner: String,
    pub tokens: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PipelineMode {
    Streaming,
    VadOffline,
}

impl PipelineMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "streaming" => Some(Self::Streaming),
            "vad_offline" => Some(Self::VadOffline),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::VadOffline => "vad_offline",
        }
    }
}

#[derive(Debug, Clone)]
pub struct VadParams {
    pub threshold: f32,
    pub min_silence_duration: f32,
    pub min_speech_duration: f32,
    pub max_speech_duration: f32,
}

impl Default for VadParams {
    fn default() -> Self {
        Self {
            threshold: 0.4,
            min_silence_duration: 0.8,
            min_speech_duration: 0.25,
            max_speech_duration: 15.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub model_dir: String,
    pub language: String,
    pub translate: bool,
    pub audio_backend: AudioBackend,
    pub device_name: Option<String>,
    pub translation: TranslationConfig,
    pub pipeline_mode: PipelineMode,
    pub vad_params: VadParams,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            model_dir: String::new(),
            language: "auto".into(),
            translate: false,
            audio_backend: AudioBackend::PipeWire,
            device_name: None,
            translation: TranslationConfig::default(),
            pipeline_mode: PipelineMode::Streaming,
            vad_params: VadParams::default(),
        }
    }
}

impl PipelineConfig {
    pub fn model_paths(&self) -> ModelPaths {
        ModelPaths {
            encoder: format!("{}/encoder.onnx", self.model_dir),
            decoder: format!("{}/decoder.onnx", self.model_dir),
            joiner: format!("{}/joiner.onnx", self.model_dir),
            tokens: format!("{}/tokens.txt", self.model_dir),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AudioBackend {
    Alsa,
    Pulse,
    Jack,
    PipeWire,
}
