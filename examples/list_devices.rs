#![forbid(unsafe_code)]

use cpal::traits::{DeviceTrait, HostTrait};
use pulsectl::controllers::DeviceControl;

#[derive(Debug)]
struct DeviceInfo {
    inner_name: String,
    display_name: String,
    is_monitor: bool,
}

fn enumerate_pulse() -> Option<Vec<DeviceInfo>> {
    let mut handler = pulsectl::controllers::SourceController::create().ok()?;
    handler.get_server_info().ok()?;
    let devices = handler.list_devices().ok()?;

    let mut result: Vec<DeviceInfo> = Vec::new();
    let mut monitors: Vec<DeviceInfo> = Vec::new();

    for dev in devices {
        let name = dev.name.as_deref()?;
        let description = dev.description.as_deref().unwrap_or(name);

        let inner_name = if dev.monitor.is_some() {
            name.strip_suffix(".monitor").unwrap_or(name).to_string()
        } else {
            name.to_string()
        };
        let is_monitor = dev.monitor.is_some();

        let info = DeviceInfo {
            inner_name,
            display_name: description.to_string(),
            is_monitor,
        };

        if is_monitor {
            monitors.push(info);
        } else {
            result.push(info);
        }
    }

    result.extend(monitors);
    Some(result)
}

fn enumerate_cpal_pipewire() -> Vec<DeviceInfo> {
    let pipewire_id = cpal::available_hosts()
        .into_iter()
        .find(|id| id.name().eq_ignore_ascii_case("PipeWire"))
        .expect("pipewire host not available");

    let host = match cpal::host_from_id(pipewire_id) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("failed to get pipewire host: {e}");
            return Vec::new();
        }
    };

    let default_input = host.default_input_device();
    let default_name = default_input
        .as_ref()
        .and_then(|d| d.id().ok())
        .map(|id| id.to_string());

    let mut result: Vec<DeviceInfo> = Vec::new();
    let mut monitors: Vec<DeviceInfo> = Vec::new();

    let Ok(devices) = host.devices() else {
        return result;
    };

    for device in devices {
        let desc = match device.description() {
            Ok(d) => d,
            Err(_) => continue,
        };

        let id_str = match device.id() {
            Ok(id) => id.to_string(),
            Err(_) => continue,
        };

        let is_monitor = desc.direction() == cpal::DeviceDirection::Duplex && desc.supports_input();
        let is_default = default_name.as_deref() == Some(&id_str);

        let display_name = desc
            .extended()
            .first()
            .map(String::as_str)
            .unwrap_or_else(|| desc.name());

        let info = DeviceInfo {
            inner_name: id_str,
            display_name: display_name.to_string(),
            is_monitor,
        };

        if is_default {
            result.insert(0, info);
        } else if is_monitor {
            monitors.push(info);
        } else {
            result.push(info);
        }
    }

    result.extend(monitors);
    result
}

fn enumerate_devices() -> Vec<DeviceInfo> {
    let pa = enumerate_pulse();
    if let Some(devices) = pa {
        println!("[used PulseAudio backend]");
        return devices;
    }
    println!("[PulseAudio unavailable, fallen back to cpal PipeWire]");
    enumerate_cpal_pipewire()
}

fn main() {
    let devices = enumerate_devices();
    println!("=== Enumerated devices ===");
    for d in &devices {
        println!(
            "  inner=\"{}\" display=\"{}\" {}",
            d.inner_name,
            d.display_name,
            if d.is_monitor { "[monitor]" } else { "" },
        );
    }
    println!("\ntotal: {}", devices.len());
}
