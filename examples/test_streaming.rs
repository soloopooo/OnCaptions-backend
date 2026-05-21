// Quick test: load sherpa-onnx streaming model, feed WAV, print output
use sherpa_onnx::{OnlineRecognizer, OnlineRecognizerConfig, Wave};

fn main() {
    let model_dir = std::env::args().nth(1).expect("usage: test_streaming <model_dir> [wav]");
    let wav_path = std::env::args().nth(2).unwrap_or_else(|| {
        format!("{}/test_wavs/0.wav", model_dir)
    });

    let mut config = OnlineRecognizerConfig::default();
    config.model_config.transducer.encoder = Some(format!("{}/encoder.onnx", model_dir));
    config.model_config.transducer.decoder = Some(format!("{}/decoder.onnx", model_dir));
    config.model_config.transducer.joiner = Some(format!("{}/joiner.onnx", model_dir));
    config.model_config.tokens = Some(format!("{}/tokens.txt", model_dir));
    config.model_config.provider = Some("cpu".to_string());
    config.enable_endpoint = true;
    config.decoding_method = Some("greedy_search".to_string());

    println!("Loading model from {}...", model_dir);
    let recognizer = OnlineRecognizer::create(&config)
        .expect("Failed to create OnlineRecognizer");
    println!("Model loaded successfully!");

    let wave = Wave::read(&wav_path).expect("Failed to read WAV");
    println!("WAV: {} samples, {:.2}s, {}Hz",
        wave.num_samples(),
        wave.num_samples() as f32 / wave.sample_rate() as f32,
        wave.sample_rate(),
    );

    let stream = recognizer.create_stream();
    let chunk_size: usize = 3200; // 200ms at 16kHz

    for chunk in wave.samples().chunks(chunk_size) {
        stream.accept_waveform(wave.sample_rate(), chunk);

        while recognizer.is_ready(&stream) {
            recognizer.decode(&stream);

            if let Some(result) = recognizer.get_result(&stream) {
                if !result.text.is_empty() {
                    println!("[partial] {}", result.text);
                }
            }

            if recognizer.is_endpoint(&stream) {
                if let Some(result) = recognizer.get_result(&stream) {
                    if !result.text.is_empty() {
                        println!("[final]   {}", result.text);
                    }
                }
                recognizer.reset(&stream);
            }
        }
    }

    // Flush: send remaining audio
    let tail = vec![0.0f32; (wave.sample_rate() as f32 * 0.3) as usize];
    stream.accept_waveform(wave.sample_rate(), &tail);
    stream.input_finished();

    while recognizer.is_ready(&stream) {
        recognizer.decode(&stream);
        if let Some(result) = recognizer.get_result(&stream) {
            if !result.text.is_empty() {
                println!("[final]   {}", result.text);
            }
        }
    }

    println!("Done.");
}
