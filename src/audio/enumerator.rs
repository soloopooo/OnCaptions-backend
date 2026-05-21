use cpal::traits::{DeviceTrait, HostTrait};
use pulsectl::controllers::DeviceControl;

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeviceInfo {
    pub inner_name: String,
    pub display_name: String,
    pub is_monitor: bool,
}

pub fn enumerate_devices() -> Vec<DeviceInfo> {
    enumerate_pulse().unwrap_or_else(|| {
        tracing::info!("PulseAudio not available, falling back to cpal PipeWire enumeration");
        enumerate_cpal_pipewire()
    })
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

        // Strip .monitor suffix for monitor devices to get the bare PipeWire node name,
        // which matches cpal device.id()?.to_string() for both PipeWire and PulseAudio backends.
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
            tracing::error!("failed to get pipewire host: {e}");
            return Vec::new();
        }
    };

    let default_input = host.default_input_device();
    let default_name: Option<String> = default_input
        .as_ref()
        .and_then(|d| d.id().ok())
        .map(|id| {
            let full = id.to_string();
            full.split_once(':')
                .map(|(_, rest)| rest.to_string())
                .unwrap_or(full)
        });

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
        // Strip the "{hostname}:" prefix to get the bare device ID
        let bare_id = id_str.split_once(':').map(|(_, rest)| rest).unwrap_or(&id_str);

        let is_monitor = desc.direction() == cpal::DeviceDirection::Duplex && desc.supports_input();
        let is_default = default_name.as_deref() == Some(bare_id);

        let display_name = desc
            .extended()
            .first()
            .map(String::as_str)
            .unwrap_or_else(|| desc.name());

        let info = DeviceInfo {
            inner_name: bare_id.to_string(),
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
