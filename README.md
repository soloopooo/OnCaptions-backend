# oncaptions-backend (sherpa-onnx)

> 模型文件不包含在 git 仓库中。首次使用需手动下载（见下方「模型」章节）。

流式语音转写后端。支持两种 Pipeline Mode：

- **Streaming** — `OnlineRecognizer`（Zipformer Transducer），逐词输出，低延迟
- **VadOffline** — Silero VAD + `OfflineRecognizer`（ReazonSpeech Zipformer），VAD 攒段 → 整段推理，高精度

与前端通过 WebSocket（127.0.0.1 端口自动 fallback 9876~9899）通信。

## 前置条件

- Rust 1.85+（edition 2024）
- PipeWire 运行时（`libpipewire`）
- sherpa-onnx 预编译库（`scripts/download_libs.sh` 自动下载）

## 目录结构

```
oncaptions-backend-sherpa/
├── src/
│   ├── main.rs              # 入口 + --check-model 子进程验证
│   ├── config/mod.rs         # PipelineConfig, PipelineMode, VadParams
│   ├── ipc/server.rs         # WebSocket 消息处理 + 端口 fallback
│   ├── pipeline/mod.rs       # Pipeline 状态机 + Streaming/VadOffline 实现
│   ├── audio/
│   │   ├── capture.rs        # cpal PipeWire 音频采集
│   │   ├── enumerator.rs     # 设备枚举（PulseAudio → cpal fallback）
│   │   └── mod.rs
│   └── translation/          # 翻译模块（OpenAI API）
├── scripts/
│   ├── download_libs.sh      # 下载 sherpa-onnx 静态库
│   └── download_models.sh    # 下载 ASR 模型
├── models/                   # 模型目录（手动放置）
├── sherpa-onnx-libs/         # 静态库（gitignored，脚本下载）
└── examples/                 # 测试示例
```

## 构建

```bash
cd oncaptions-backend-sherpa

# 首次构建：下载 sherpa-onnx 静态库
bash scripts/download_libs.sh

# 构建后端
SHERPA_ONNX_LIB_DIR="$PWD/sherpa-onnx-libs/sherpa-onnx-v1.13.2-linux-x64-static-lib/lib" cargo build --release
```

推荐通过前端 `npm run build:backend` 构建（自动跑 download + cargo build + 拷贝到 `backend-bin/`）。

## 运行

```bash
RUST_LOG=debug cargo run
```

日志级别：`RUST_LOG=info`（默认）、`RUST_LOG=debug`（详细）、`RUST_LOG=error`（仅错误）。

## 模型

模型需手动下载，不在仓库中（`models/` 已 gitignored）。

### 一键下载脚本

```bash
bash scripts/download_models.sh
```

下载的模型放入 `models/` 目录，保证目录结构如下：

```
models/<your-model-dir>/
├── encoder.onnx
├── decoder.onnx
├── joiner.onnx
└── tokens.txt
```

各模型目录名示例：`models/ja/`、`models/zh-en/`、`models/ja-reazonspeech/`、`models/silero_vad/`。

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
→ shutdown:        { type: "shutdown" }       // 优雅关闭

← transcription:   { type, text, tokens?, is_final }
← translation:     { type, original, text, provider_display, target_lang }
← pipeline_status: { type, state: "running"|"stopped"|"error" }
← error:           { type, error, code, message }
← device_list:     { type, devices: [...] }
← shutdown:        { type: "shutdown", ack: true }
```

> `language` 已在 WS 消息中标记为可选字段，前端不再发送。

## 端口分配

后端启动时尝试绑定 9876~9899，成功后写入 `/tmp/oncaptions-port`。前端通过该文件获取实际端口。

## 测试

```bash
# 手动发 WS 消息测试
python3 test_ws.py
```

## 架构要点

- `#![forbid(unsafe_code)]` — 应用层零 unsafe
- 音频采集：`std::thread` 中跑 cpal PipeWire 回调 → `Arc<Mutex<VecDeque>>`
- ASR 推理：`std::thread` + `std::sync::mpsc` 与 async 主循环通信
- 模型加载：`std::process::Command` 子进程预校验（防 C++ abort 杀主进程）
- 翻译：独立线程池，缓存 SQLite
- 优雅关闭：WS `shutdown` 消息 → pipeline stop + oneshot 等待 → server shutdown
- 端口 fallback：循环 bind 9876~9899，写 `/tmp/oncaptions-port`
