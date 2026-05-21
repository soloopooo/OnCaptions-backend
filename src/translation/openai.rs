use crate::translation::{TranslationCache, TranslationProviderConfig, Translator};
use anyhow::Result;
use serde_json::json;

pub struct OpenAITranslator {
    name: String,
    display_name: String,
    target_lang: String,
    api_url: String,
    api_key: String,
    model: String,
    system_prompt: String,
    cache: TranslationCache,
}

impl OpenAITranslator {
    pub fn new(config: &TranslationProviderConfig, cache: TranslationCache) -> Self {
        let api_url = config.api_url.trim_end_matches('/').to_string();
        Self {
            name: config.name.clone(),
            display_name: config.display_name.clone(),
            target_lang: config.target_lang.clone(),
            api_url,
            api_key: config.api_key.clone(),
            model: config.model.clone(),
            system_prompt: config.system_prompt.clone(),
            cache,
        }
    }

    fn endpoint(&self) -> String {
        let url = &self.api_url;
        if url.ends_with("/chat/completions") {
            url.clone()
        } else {
            format!("{}/chat/completions", url.trim_end_matches('/'))
        }
    }
}

impl Translator for OpenAITranslator {
    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn target_lang(&self) -> &str {
        &self.target_lang
    }

    fn translate(&self, text: &str, source_lang: &str) -> Result<String> {
        if let Some(cached) = self.cache.get(text, &self.name, &self.target_lang) {
            tracing::debug!("translation cache hit: {text} -> {cached}");
            return Ok(cached);
        }

        let prompt = self
            .system_prompt
            .replace("{source_lang}", source_lang)
            .replace("{target_lang}", &self.target_lang);

        let body = json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": text}
            ],
            "temperature": 0.0,
            "max_tokens": 2048,
        });

        let response = ureq::post(&self.endpoint())
            .set("Content-Type", "application/json")
            .set("Authorization", &format!("Bearer {}", self.api_key))
            .send_json(&body)
            .map_err(|e| anyhow::anyhow!("OpenAI request failed: {e}"))?;

        let resp_json: serde_json::Value = response
            .into_json()
            .map_err(|e| anyhow::anyhow!("bad JSON response: {e}"))?;

        let translated = resp_json["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("unexpected OpenAI response format"))?
            .trim()
            .to_string();

        self.cache
            .put(text, &self.name, &self.target_lang, &translated);

        Ok(translated)
    }
}
