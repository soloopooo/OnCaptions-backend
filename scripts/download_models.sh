#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

download_model() {
    local name="$1"
    local dir="$2"
    echo "==> Downloading $name ..."
    wget -q "https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/${name}.tar.bz2"
    tar xf "${name}.tar.bz2"
    rm "${name}.tar.bz2"
    # create standard symlinks
    cd "${dir}"
    for f in encoder-*.onnx; do ln -sf "$f" encoder.onnx 2>/dev/null || true; done
    for f in decoder-*.onnx; do ln -sf "$f" decoder.onnx 2>/dev/null || true; done
    for f in joiner-*.onnx; do ln -sf "$f" joiner.onnx 2>/dev/null || true; done
    cd - >/dev/null
    echo "done"
}

mkdir -p models

# Silero VAD (required for VadOffline mode)
echo "==> Downloading Silero VAD ..."
wget -q https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/silero_vad.onnx -O models/silero_vad/silero_vad.onnx

# ReazonSpeech offline Japanese (VadOffline mode)
download_model sherpa-onnx-zipformer-ja-reazonspeech-2024-08-01 models/ja-reazonspeech

# Streaming Japanese (Streaming mode)
download_model sherpa-onnx-zipformer-ja-reazonspeech-2024-08-01 models/ja

echo "==> All models downloaded."
echo
echo "Models directory:"
ls -lh models/*/
