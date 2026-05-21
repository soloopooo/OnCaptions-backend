use crate::config::AudioBackend;
use anyhow::{Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug, Clone)]
pub enum AudioStreamError {
    DeviceLost,
    Xrun,
    RealtimeDenied,
    Other(String),
}

pub struct AudioCapture {
    join_handle: Option<thread::JoinHandle<()>>,
    shutdown_flag: Option<Arc<Mutex<bool>>>,
}

impl AudioCapture {
    pub fn new() -> Self {
        Self {
            join_handle: None,
            shutdown_flag: None,
        }
    }

    pub fn start(
        &mut self,
        backend: &AudioBackend,
        device_name: Option<&str>,
        mut callback: Box<dyn FnMut(Vec<f32>) + Send>,
        error_tx: tokio::sync::mpsc::Sender<AudioStreamError>,
    ) -> Result<u32> {
        let backend = *backend;
        let device_name = device_name.map(String::from);
        let (sample_tx, sample_rx) = std::sync::mpsc::channel::<Vec<f32>>();
        let shutdown = Arc::new(Mutex::new(false));
        let shutdown_clone = shutdown.clone();

        let handle = thread::spawn(move || {
            let result =
                run_capture(backend, device_name.as_deref(), sample_tx, shutdown_clone, error_tx);
            if let Err(e) = result {
                tracing::error!("audio capture thread failed: {e:?}");
            }
        });

        let first_chunk = sample_rx
            .recv()
            .context("audio thread exited before sending first chunk")?;
        let sample_rate = first_chunk[0] as u32;

        thread::spawn(move || {
            for chunk in sample_rx {
                callback(chunk);
            }
        });

        self.join_handle = Some(handle);
        self.shutdown_flag = Some(shutdown);

        Ok(sample_rate)
    }

    pub fn stop(&mut self) -> Result<()> {
        if let Some(ref flag) = self.shutdown_flag {
            let mut f = flag.lock().unwrap();
            *f = true;
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
        Ok(())
    }
}

fn map_stream_error(err: &cpal::Error) -> AudioStreamError {
    use cpal::ErrorKind;
    match err.kind() {
        ErrorKind::DeviceNotAvailable | ErrorKind::DeviceChanged | ErrorKind::DeviceBusy => {
            AudioStreamError::DeviceLost
        }
        ErrorKind::RealtimeDenied => AudioStreamError::RealtimeDenied,
        _ => {
            let desc = format!("{err}");
            if desc.contains("xrun") || desc.contains("underrun") || desc.contains("overrun") {
                AudioStreamError::Xrun
            } else if desc.contains("no target node available") {
                AudioStreamError::DeviceLost
            } else {
                AudioStreamError::Other(desc)
            }
        }
    }
}

fn linear_resample(input: &[f32], from_rate: u32, to_rate: u32) -> Vec<f32> {
    if from_rate == to_rate || input.is_empty() {
        return input.to_vec();
    }
    let ratio = to_rate as f64 / from_rate as f64;
    let out_len = (input.len() as f64 * ratio).ceil() as usize;
    let mut out = Vec::with_capacity(out_len);
    for i in 0..out_len {
        let src_pos = i as f64 / ratio;
        let src_idx = src_pos as usize;
        if src_idx + 1 < input.len() {
            let frac = src_pos - src_idx as f64;
            out.push((input[src_idx] as f64 * (1.0 - frac) + input[src_idx + 1] as f64 * frac) as f32);
        } else {
            out.push(input[src_idx]);
        }
    }
    out
}

fn is_monitor_device(device: &cpal::Device) -> bool {
    device.description().ok().is_some_and(|d| {
        d.direction() == cpal::DeviceDirection::Duplex && d.supports_input()
    })
}

fn select_device(
    host: &cpal::Host,
    backend: AudioBackend,
    device_name: Option<&str>,
) -> Result<cpal::Device> {
    if let Some(name) = device_name {
        for device in host.devices()? {
            let id_str = device.id()?.to_string();
            let device_part = id_str.split_once(':').map(|(_, rest)| rest).unwrap_or(&id_str);
            let name_clean = name.split_once(':').map(|(_, rest)| rest).unwrap_or(name);
            if !(id_str == name || id_str == name_clean
                || device_part == name || device_part == name_clean)
            {
                continue;
            }
            if device.supports_input() {
                // For PipeWire, prefer Duplex (monitor) devices over source-only devices.
                // If the matched device is a source rather than a monitor, keep looking.
                if backend == AudioBackend::PipeWire && !is_monitor_device(&device) {
                    continue;
                }
                tracing::info!("selected device by id: {name}");
                return Ok(device);
            }
        }
        anyhow::bail!("device '{name}' not found or doesn't support input");
    }

    match backend {
        AudioBackend::PipeWire => {
            for device in host.devices()? {
                if is_monitor_device(&device) {
                    let desc = device.description()?;
                    let disp = desc
                        .extended()
                        .first()
                        .map(String::as_str)
                        .unwrap_or_else(|| desc.name());
                    tracing::info!("selected pipewire sink monitor: {disp}");
                    return Ok(device);
                }
            }
            let device = host
                .default_input_device()
                .context("no input device available")?;
            let desc = device.description()?;
            let disp = desc
                .extended()
                .first()
                .map(String::as_str)
                .unwrap_or_else(|| desc.name());
            tracing::info!("no duplex device found, using default input: {disp}");
            Ok(device)
        }
        _ => host.default_input_device().context("no input device available"),
    }
}

fn run_capture(
    backend: AudioBackend,
    device_name: Option<&str>,
    sample_tx: std::sync::mpsc::Sender<Vec<f32>>,
    shutdown: Arc<Mutex<bool>>,
    error_tx: tokio::sync::mpsc::Sender<AudioStreamError>,
) -> Result<()> {
    let host: cpal::Host = if matches!(backend, AudioBackend::PipeWire) {
        let id = cpal::available_hosts()
            .into_iter()
            .find(|id| id.name().eq_ignore_ascii_case("PipeWire"))
            .context("pipewire host not available (cpal PipeWire backend not compiled or libpipewire not found)")?;
        cpal::host_from_id(id)?
    } else {
        cpal::default_host()
    };

    let device = select_device(&host, backend, device_name)?;
    let config = device.default_input_config()?;

    let sample_rate = config.sample_rate();
    let channels = config.channels() as usize;
    let fmt = config.sample_format();

    let target_sr = if sample_rate != 16000 {
        tracing::info!("resampling audio from {sample_rate} Hz to 16000 Hz");
        16000u32
    } else {
        sample_rate
    };

    let _ = sample_tx.send(vec![target_sr as f32]);

    let s0 = shutdown.clone();
    let s1 = shutdown.clone();
    let s2 = shutdown.clone();
    let s3 = shutdown.clone();
    let err_tx0 = error_tx.clone();
    let err_tx1 = error_tx.clone();
    let err_tx2 = error_tx.clone();

    let stream = match fmt {
        cpal::SampleFormat::F32 => {
            let et = err_tx0;
            device.build_input_stream::<f32, _, _>(
                config.config(),
                move |data: &[f32], _: &cpal::InputCallbackInfo| {
                    if *s0.lock().unwrap() {
                        return;
                    }
                    let mut mono = Vec::with_capacity(data.len() / channels);
                    for frame in data.chunks(channels) {
                        mono.push(frame[0]);
                    }
                    let resampled = linear_resample(&mono, sample_rate, target_sr);
                    let _ = sample_tx.send(resampled);
                },
                move |err| {
                    let ae = map_stream_error(&err);
                    tracing::error!("audio stream error: {err} ({ae:?})");
                    let _ = et.blocking_send(ae);
                },
                None::<std::time::Duration>,
            )?
        }
        cpal::SampleFormat::I16 => {
            let et = err_tx1;
            device.build_input_stream::<i16, _, _>(
                config.config(),
                move |data: &[i16], _: &cpal::InputCallbackInfo| {
                    if *s1.lock().unwrap() {
                        return;
                    }
                    let mut mono = Vec::with_capacity(data.len() / channels);
                    for frame in data.chunks(channels) {
                        mono.push(frame[0] as f32 / i16::MAX as f32);
                    }
                    let resampled = linear_resample(&mono, sample_rate, target_sr);
                    let _ = sample_tx.send(resampled);
                },
                move |err| {
                    let ae = map_stream_error(&err);
                    tracing::error!("audio stream error: {err} ({ae:?})");
                    let _ = et.blocking_send(ae);
                },
                None::<std::time::Duration>,
            )?
        }
        cpal::SampleFormat::U16 => {
            let et = err_tx2;
            device.build_input_stream::<u16, _, _>(
                config.config(),
                move |data: &[u16], _: &cpal::InputCallbackInfo| {
                    if *s2.lock().unwrap() {
                        return;
                    }
                    let mut mono = Vec::with_capacity(data.len() / channels);
                    for frame in data.chunks(channels) {
                        mono.push((frame[0] as f32 / u16::MAX as f32) * 2.0 - 1.0);
                    }
                    let resampled = linear_resample(&mono, sample_rate, target_sr);
                    let _ = sample_tx.send(resampled);
                },
                move |err| {
                    let ae = map_stream_error(&err);
                    tracing::error!("audio stream error: {err} ({ae:?})");
                    let _ = et.blocking_send(ae);
                },
                None::<std::time::Duration>,
            )?
        }
        other => anyhow::bail!("unsupported sample format: {other}"),
    };

    stream.play()?;
    tracing::info!(
        "audio capture started on {} ({} Hz, {} channels, format={})",
        device.description()?.name(),
        sample_rate,
        channels,
        fmt
    );

    loop {
        thread::sleep(std::time::Duration::from_millis(100));
        if *s3.lock().unwrap() {
            break;
        }
    }

    drop(stream);
    Ok(())
}
