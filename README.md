# oncaptions-backend (sherpa-onnx)

> 模型文件不包含在 git 仓库中。首次使用需手动下载模型（见下方「模型」章节）。

流式语音转写后端。支持两种 Pipeline Mode：

- **Streaming** — `OnlineRecognizer`（Zipformer Transducer），逐词输出，低延迟
- **VadOffline** — Silero VAD + `OfflineRecognizer`（ReazonSpeech Zipformer），VAD 攒段 → 整段推理，高精度

与前端通过 WebSocket（127.0.0.1:9876）通信。

## 前置条件

- Rust 1.75+（edition 2024）
- PipeWire 运行时（`libpipewire`）
- sherpa-onnx 预编译库

## 目录结构

```
backend-sherpa/
├── src/
│   ├── main.rs              # 入口 + --check-model 子进程验证
│   ├── config/mod.rs         # PipelineConfig, PipelineMode, VadParams
│   ├── ipc/server.rs         # WebSocket 消息处理
│   ├── pipeline/mod.rs       # Pipeline 状态机 + Streaming/VadOffline 实现
│   ├── audio/
│   │   ├── capture.rs        # cpal PipeWire 音频采集
│   │   ├── enumerator.rs     # 设备枚举（PulseAudio → cpal fallback）
│   │   └── mod.rs
│   └── translation/          # 翻译模块（OpenAI API）
├── models/                   # 模型目录（见下方）
├── sherpa-onnx-libs/         # sherpa-onnx 预编译库
└── examples/                 # 测试示例
```

## 前置条件

- Rust 1.75+（edition 2024）
- PipeWire 运行时（`libpipewire`）
- sherpa-onnx 预编译库（已包含在 `sherpa-onnx-libs/`）

## 构建

```bash
cd backend-sherpa

# 首次构建需要指定 sherpa-onnx 库路径
SHERPA_ONNX_LIB_DIR="$PWD/sherpa-onnx-libs/sherpa-onnx-v1.13.2-linux-x64-static-lib/lib" cargo build
```

你也可以在 `.cargo/config.toml` 中固化该路径（注意该文件已从 git 中排除，不会上传）：

```toml
[env]
SHERPA_ONNX_LIB_DIR = "/home/user/oncaptions/backend-sherpa/sherpa-onnx-libs/sherpa-onnx-v1.13.2-linux-x64-static-lib/lib"
```

之后每次构建只需：

```bash
cargo build
```

## 运行

```bash
RUST_LOG=debug cargo run
```

日志级别：`RUST_LOG=info`（默认）、`RUST_LOG=debug`（详细）、`RUST_LOG=error`（仅错误）。

## 模型

### 内置模型（已下载）

| 目录 | 类型 | 语言 | 模式 |
|------|------|------|------|
| `models/ja/` | Streaming Zipformer int8 | 日语 | Streaming |
| `models/multi/` | Streaming Zipformer int8 | 多语言（含日） | Streaming |
| `models/zh-en/` | Streaming Zipformer | 中英 | Streaming |
| `models/parakeet/` | NeMo Parakeet int8 | 英语 | Streaming |
| `models/sherpa-zipformer-en/` | Streaming Zipformer | 英语 | Streaming |
| `models/sensevoice/` | SenseVoice | 中英日韩粤 | VadOffline |
| `models/ja-reazonspeech/` | **Offline Zipformer int8** | 日语 | **VadOffline** |
| `models/silero_vad/` | Silero VAD ONNX | — | VadOffline |

### 一键下载脚本

```bash
# 下载所有推荐模型
bash scripts/download_models.sh

# 或按需下载
# Streaming 日语
wget https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-zipformer-ja-reazonspeech-2024-08-01.tar.bz2
tar xf sherpa-onnx-zipformer-ja-reazonspeech-2024-08-01.tar.bz2
rm sherpa-onnx-zipformer-ja-reazonspeech-2024-08-01.tar.bz2

# 创建标准文件名 symlink（model_paths() 要求 encoder.onnx/decoder.onnx/joiner.onnx）
cd models/<your-model-dir>
ln -s encoder-*.onnx encoder.onnx
ln -s decoder-*.onnx decoder.onnx
ln -s joiner-*.onnx joiner.onnx
```

### 模型路径约定

`model_paths()` 从 `model_dir` 加载以下文件：
- `{model_dir}/encoder.onnx`
- `{model_dir}/decoder.onnx`
- `{model_dir}/joiner.onnx`
- `{model_dir}/tokens.txt`

VAD 模型路径固定为 `models/silero_vad/silero_vad.onnx`（CWD 或 exe 相对路径查找）。

## 运行模式

### Streaming 模式（默认）

前端不传 `pipeline_mode` 或传 `"streaming"`：

```json
{
  "type": "start_pipeline",
  "model_dir": "/abs/path/to/models/ja",
  "language": "ja",
  "audio_backend": "pipewire",
  "pipeline_mode": "streaming"
}
```

`OnlineRecognizer` 逐词输出，`is_final` 在 endpoint 检测到时为 true。

### VadOffline 模式

```json
{
  "type": "start_pipeline",
  "model_dir": "/abs/path/to/models/ja-reazonspeech",
  "pipeline_mode": "vad_offline",
  "language": "ja",
  "audio_backend": "pipewire",
  "vad_threshold": 0.4,
  "vad_min_silence_duration": 0.8,
  "vad_min_speech_duration": 0.25,
  "vad_max_speech_duration": 15.0
}
```

1. 音频 → Silero VAD 检测语音段
2. 每段 → `OfflineRecognizer` 全段推理
3. 输出 `is_final: true`（段级延迟，日语 1–3s）

## WS 协议

```
→ start_pipeline:  { type, model_dir, language?, audio_backend?, device_name?,
                     pipeline_mode?, vad_*?, translation? }
→ stop_pipeline:   { type: "stop_pipeline" }
→ list_devices:    { type: "list_devices" }

← transcription:   { type: "transcription", text, tokens?, is_final }
← translations:    { type: "translation", original, text, ... }
← pipeline_status: { type: "pipeline_status", state: "running"|"stopped"|"error" }
← error:           { type: "error", code, message }
← device_list:     { type: "device_list", devices: [...] }
```

## 测试

```bash
# 单元测试
cargo test

# 快速测试某 Streaming 模型
cargo run --example test_streaming -- models/ja test_wavs/ja.wav

# 手动发 WS 消息测试
python3 test_ws.py
```

## 架构要点

- `#![forbid(unsafe_code)]` — 应用层零 unsafe
- 音频采集：`std::thread` 中跑 cpal PipeWire 回调 → `Arc<Mutex<VecDeque>>`
- ASR 推理：`std::thread` + `std::sync::mpsc` 与 async 主循环通信
- 模型加载：`std::process::Command` 子进程预校验（防 C++ abort 杀主进程）
- 翻译：独立线程池，缓存 SQLite
