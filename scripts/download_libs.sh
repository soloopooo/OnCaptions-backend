#!/usr/bin/env bash
set -euo pipefail

SHERPA_VER="v1.13.2"
SHERPA_FILE="sherpa-onnx-${SHERPA_VER}-linux-x64-static-lib.tar.xz"
SHERPA_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/${SHERPA_VER}/${SHERPA_FILE}"
LIB_DIR="sherpa-onnx-libs/sherpa-onnx-${SHERPA_VER}-linux-x64-static-lib/lib"
CHECK_FILE="${LIB_DIR}/libonnxruntime.a"

cd "$(dirname "$0")/.."

if [ -f "$CHECK_FILE" ]; then
    echo "[download_libs] sherpa-onnx static libs already present"
    exit 0
fi

echo "[download_libs] downloading ${SHERPA_FILE}..."
mkdir -p sherpa-onnx-libs
curl -fL "$SHERPA_URL" | tar xJ -C sherpa-onnx-libs/

if [ -f "$CHECK_FILE" ]; then
    echo "[download_libs] done"
else
    echo "[download_libs] extract failed: ${CHECK_FILE} not found"
    exit 1
fi
