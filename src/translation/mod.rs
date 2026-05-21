mod cache;
mod openai;

use anyhow::Result;
use serde::{Deserialize, Serialize};

pub use cache::TranslationCache;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranslationProviderConfig {
    pub name: String,
    pub display_name: String,
    pub provider_type: String,
    pub enabled: bool,
    pub api_url: String,
    pub api_key: String,
    pub model: String,
    pub target_lang: String,
    #[serde(default = "default_system_prompt")]
    pub system_prompt: String,
}

fn default_system_prompt() -> String {
    "Translate the following text from {source_lang} to {target_lang}. Only output the translation, nothing else.".into()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TranslationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub providers: Vec<TranslationProviderConfig>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TranslationResult {
    pub original: String,
    pub provider_name: String,
    pub provider_display: String,
    pub text: String,
    pub target_lang: String,
}

pub trait Translator: Send {
    fn name(&self) -> &str;
    fn display_name(&self) -> &str;
    fn target_lang(&self) -> &str;
    fn translate(&self, text: &str, source_lang: &str) -> Result<String>;
}

pub fn create_translators(
    config: &TranslationConfig,
    cache: TranslationCache,
) -> Vec<Box<dyn Translator>> {
    config
        .providers
        .iter()
        .filter(|p| p.enabled)
        .map(|p| -> Box<dyn Translator> {
            match p.provider_type.as_str() {
                "openai" => Box::new(openai::OpenAITranslator::new(p, cache.clone())),
                other => {
                    tracing::warn!("unknown translator type: {other}, falling back to openai");
                    Box::new(openai::OpenAITranslator::new(p, cache.clone()))
                }
            }
        })
        .collect()
}
