// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Configuration for the io-thread-controller daemon.

use std::io;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::util::Path;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("invalid configuration value: {0}")]
    InvalidValue(String),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

/// Tunable parameters for the controller.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Which registered scaling engine drives decisions.
    pub engine: String,
    /// Directory to look for `<engine>.json` engine-specific
    /// config files.
    pub engine_config_dir: Path,
    /// Directory containing backend-owned configuration files.
    pub backend_config_dir: Path,
    /// JSON file containing the managed and unmanaged VM lists.
    ///
    /// The default is below `/run`, so state survives daemon restarts but not a
    /// host reboot.
    pub vm_state_path: Path,
    /// Lowest worker count accepted from a scaling engine.
    #[serde(default = "default_min_thread_count")]
    pub min_thread_count: u32,
    /// Highest worker count accepted from a scaling engine.
    #[serde(default = "default_max_thread_count")]
    pub max_thread_count: u32,
    /// Host CPU utilisation at or above which ordinary scale-up is blocked in
    /// the [0.0, 1.0] range.
    ///
    /// Zero disables this guard.
    #[serde(
        default = "default_host_cpu_scale_up_ceiling",
        rename = "host_cpu_scale_up_ceiling_percent",
        deserialize_with = "deserialize_percent",
        serialize_with = "serialize_percent"
    )]
    pub host_cpu_scale_up_ceiling: f64,
    /// Per-VM delay after ordinary successful scale actions.
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: f64,
    /// Poll cadence in seconds. The controller refreshes
    /// per-instance state on each tick and calls the active
    /// scaling engine for a decision.
    pub scale_poll_secs: f64,
    /// Emit a `vm=<id>` status line per tracked instance on
    /// every tick (default `true`).
    #[serde(default = "default_true")]
    pub enable_per_vm_status_line: bool,
    /// Emit a single aggregate status line summarising every
    /// tracked instance on every tick (default `true`).
    #[serde(default = "default_true")]
    pub enable_aggregate_status_line: bool,
    /// Emit a one-time legend for status-line fields at startup.
    #[serde(default)]
    pub print_status_header: bool,
    /// When true, log the scaling verdict but skip the actuation
    /// call to `set_thread_count`.
    #[serde(default)]
    pub dry_run: bool,
}

fn default_true() -> bool {
    true
}

fn default_min_thread_count() -> u32 {
    1
}

fn default_max_thread_count() -> u32 {
    8
}

fn default_host_cpu_scale_up_ceiling() -> f64 {
    0.9
}

fn default_cooldown_secs() -> f64 {
    30.0
}

/// Deserialise a human-readable percent to a floating point value.
pub(crate) fn deserialize_percent<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<f64, D::Error> {
    Ok(f64::deserialize(deserializer)? / 100.0)
}

/// Serialise a floating point fraction as a human-readable percent.
pub(crate) fn serialize_percent<S: Serializer>(
    value: &f64,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.serialize_f64(value * 100.0)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            engine: "threshold".to_string(),
            engine_config_dir: Path::new("/etc/io-thread-controller.d/engines"),
            backend_config_dir: Path::new("/etc/io-thread-controller.d/backends"),
            scale_poll_secs: 10.0,
            min_thread_count: 2,
            max_thread_count: 8,
            host_cpu_scale_up_ceiling: 0.9,
            cooldown_secs: 30.0,
            vm_state_path: Path::new("/run/io-thread-controller/vm-ownership.json"),
            enable_per_vm_status_line: true,
            enable_aggregate_status_line: true,
            print_status_header: false,
            dry_run: false,
        }
    }
}

/// Load a JSON config file.
pub fn load_config<T: for<'de> Deserialize<'de>>(path: impl AsRef<Path>) -> Result<T, ConfigError> {
    let path = path.as_ref();
    let data = std::fs::read_to_string(path)?;
    let val: T = serde_json::from_str(&data)?;
    Ok(val)
}

// TODO add a RawConfig or ValidatedConfig to ensure an unvalidated config
// cannot be constructed outside this module.
/// Ensures configuration is internally coherent.
pub fn validate_config(cfg: &Config) -> Result<(), ConfigError> {
    if !cfg.scale_poll_secs.is_finite() || cfg.scale_poll_secs <= 0.0 {
        return Err(ConfigError::InvalidValue(
            "scale_poll_secs must be > 0".to_string(),
        ));
    }
    if cfg.engine.is_empty() {
        return Err(ConfigError::InvalidValue(
            "engine must be non-empty".to_string(),
        ));
    }
    if !(cfg.min_thread_count > 0 && cfg.min_thread_count <= cfg.max_thread_count) {
        return Err(ConfigError::InvalidValue(
            "thread-count bounds must satisfy 0 < min_thread_count <= max_thread_count".to_string(),
        ));
    }
    if !(cfg.host_cpu_scale_up_ceiling.is_finite()
        && (0.0..=1.0).contains(&cfg.host_cpu_scale_up_ceiling))
    {
        return Err(ConfigError::InvalidValue(
            "host_cpu_scale_up_ceiling_percent must be in [0, 100]".to_string(),
        ));
    }
    if !(cfg.cooldown_secs.is_finite() && cfg.cooldown_secs >= 0.0) {
        return Err(ConfigError::InvalidValue(
            "cooldown_secs must be finite and non-negative".to_string(),
        ));
    }
    Ok(())
}

/// Serialise the default config as indented JSON for CLI
/// inspection via `--dump-config`.
pub fn dump_default_config() -> String {
    serde_json::to_string_pretty(&Config::default()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn nearly_eq(left: f64, right: f64) -> bool {
        (left - right).abs() <= 1e-6 * (1.0 + left.abs().max(right.abs()))
    }

    fn arb_path_string() -> impl Strategy<Value = String> {
        prop_oneof![
            "[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}",
            "[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}".prop_map(|tail| format!("/{tail}")),
        ]
    }

    /// Test that default `Config` validates and uses the threshold
    /// engine with a 10s poll.
    #[test]
    fn default_config_is_valid() {
        let cfg = Config::default();
        assert_eq!(cfg.engine, "threshold");
        assert!((cfg.scale_poll_secs - 10.0).abs() < f64::EPSILON);
        validate_config(&cfg).unwrap();
    }

    proptest! {
        #[test]
        fn load_config_round_trips_every_field(
            engine in "[a-z0-9]{0,12}",
            engine_config_dir in arb_path_string(),
            backend_config_dir in arb_path_string(),
            vm_state_path in arb_path_string(),
            min_thread_count in any::<u32>(),
            max_thread_count in any::<u32>(),
            host_cpu_scale_up_ceiling in -1_000.0..1_000.0f64,
            cooldown_secs in -1_000.0..1_000.0f64,
            scale_poll_secs in -1_000.0..1_000.0f64,
            enable_per_vm_status_line in any::<bool>(),
            enable_aggregate_status_line in any::<bool>(),
            print_status_header in any::<bool>(),
            dry_run in any::<bool>(),
        ) {
            let cfg = Config {
                engine,
                engine_config_dir: Path::new(&engine_config_dir),
                backend_config_dir: Path::new(&backend_config_dir),
                vm_state_path: Path::new(&vm_state_path),
                min_thread_count,
                max_thread_count,
                host_cpu_scale_up_ceiling,
                cooldown_secs,
                scale_poll_secs,
                enable_per_vm_status_line,
                enable_aggregate_status_line,
                print_status_header,
                dry_run,
            };
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("config.json");
            std::fs::write(&path, serde_json::to_string(&cfg).unwrap()).unwrap();
            let loaded: Config = load_config(Path::new(path.to_str().unwrap())).unwrap();

            prop_assert_eq!(loaded.engine, cfg.engine);
            prop_assert_eq!(
                loaded.engine_config_dir.as_os_str(),
                cfg.engine_config_dir.as_os_str()
            );
            prop_assert_eq!(
                loaded.backend_config_dir.as_os_str(),
                cfg.backend_config_dir.as_os_str()
            );
            prop_assert_eq!(loaded.vm_state_path.as_os_str(), cfg.vm_state_path.as_os_str());
            prop_assert_eq!(loaded.min_thread_count, cfg.min_thread_count);
            prop_assert_eq!(loaded.max_thread_count, cfg.max_thread_count);
            prop_assert!(nearly_eq(
                loaded.host_cpu_scale_up_ceiling,
                cfg.host_cpu_scale_up_ceiling
            ));
            prop_assert!(nearly_eq(loaded.cooldown_secs, cfg.cooldown_secs));
            prop_assert!(nearly_eq(loaded.scale_poll_secs, cfg.scale_poll_secs));
            prop_assert_eq!(loaded.enable_per_vm_status_line, cfg.enable_per_vm_status_line);
            prop_assert_eq!(
                loaded.enable_aggregate_status_line,
                cfg.enable_aggregate_status_line
            );
            prop_assert_eq!(loaded.print_status_header, cfg.print_status_header);
            prop_assert_eq!(loaded.dry_run, cfg.dry_run);
        }
    }

    /// Test that loading JSON with an unknown field returns
    /// `ConfigError::SerdeJson`.
    #[test]
    fn load_config_rejects_unknown_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            r#"{
                "engine": "threshold",
                "engine_config_dir": "/engines",
                "backend_config_dir": "/backends",
                "scale_poll_secs": 1.0,
                "not_a_real_field": true
            }"#,
        )
        .unwrap();

        let err = load_config::<Config>(Path::new(path.to_str().unwrap())).unwrap_err();
        assert!(matches!(err, ConfigError::SerdeJson(_)));
    }

    /// Test that serde defaults for min/max threads, host CPU ceiling,
    /// and cooldown match `Config::default()`.
    #[test]
    fn controller_policy_defaults_round_trip() {
        let cfg: Config = serde_json::from_str(r#"{"engine": "foo", "engine_config_dir": "/path/to/engines", "backend_config_dir": "/path/to/backends", "scale_poll_secs": 42, "vm_state_path": "/path/to/vm-state.json"}"#).unwrap();
        assert_eq!(cfg.min_thread_count, 1);
        assert_eq!(cfg.max_thread_count, 8);
        assert!((cfg.host_cpu_scale_up_ceiling - 0.9).abs() < f64::EPSILON);
        assert_eq!(cfg.cooldown_secs, 30.0);

        let serialized = serde_json::to_string(&cfg).unwrap();
        assert!(serialized.contains(r#""host_cpu_scale_up_ceiling_percent":90.0"#));
    }
}
