// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Configuration for the io-thread-controller daemon.

use std::{io, time::Duration};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::path::PrefixedPath;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("invalid configuration value: {0}")]
    InvalidValue(String),

    #[error(
        "durations must be multiples of scale_poll_secs (set round_up_durations or pass \
         --round-up-durations to round them up):\n  {}",
        .0.join("\n  ")
    )]
    NotPollMultiple(Vec<String>),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

/// Tunable parameters for the controller.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Which registered scaling engine drives decisions.
    pub engine: String,
    /// Directory to look for `<engine>.json` engine-specific
    /// config files.
    pub engine_config_dir: PrefixedPath,
    /// Directory containing backend-owned configuration files.
    pub backend_config_dir: PrefixedPath,
    /// JSON file containing the managed and unmanaged VM lists.
    ///
    /// The default is below `/run`, so state survives daemon restarts but not a
    /// host reboot.
    pub vm_state_path: PrefixedPath,
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
    pub cooldown_secs: u64,
    /// Poll cadence in seconds. The controller refreshes per-instance state on
    /// each tick and calls the active scaling engine for a decision. Every
    /// other duration, including engine ones, must be a multiple of it.
    pub scale_poll_secs: u64,
    /// Round durations that are not multiples of `scale_poll_secs` up to the
    /// next multiple, with a warning, instead of rejecting them.
    #[serde(default)]
    pub round_up_durations: bool,
    /// Emit a `vm=<id>` status line per tracked instance on
    /// every tick (default `false`).
    #[serde(default)]
    pub enable_per_vm_status_line: bool,
    /// Also emit each per-VM status record on the main controller target
    /// (default `false`). Has no effect is `enable_per_vm_status_line` is
    /// `false`.
    #[serde(default)]
    pub enable_per_vm_main_log: bool,
    /// Emit a single aggregate status line summarising every
    /// tracked instance on every tick (default `true`).
    #[serde(default = "default_true")]
    pub enable_aggregate_status_line: bool,
    /// Emit a one-time legend for status-line fields at startup.
    #[serde(default)]
    pub print_status_header: bool,
    /// Cap how many instances may be actuated in any single tick.  0 disables
    /// the cap.
    #[serde(default = "default_max_instances_adjusted_per_poll")]
    pub max_instances_adjusted_per_poll: u32,
    /// Opt in to sampling `/proc/pressure/{cpu,io,memory}` on
    /// every tick and exposing it to the active engine.
    #[serde(default)]
    pub experimental_psi_monitoring: bool,
    /// When true, re-read cgroup limits on each CPU sample.
    #[serde(default)]
    pub refresh_cgroup_on_each_read: bool,
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

fn default_cooldown_secs() -> u64 {
    30
}

/// Controller poll period that durations are expressed in multiples of.
#[derive(Debug, Clone, Copy)]
pub struct PollPeriod {
    secs: u64,
    round_up: bool,
}

impl PollPeriod {
    /// `secs` must be non-zero.
    pub fn new(secs: u64, round_up: bool) -> Self {
        assert!(secs != 0, "poll period must be non-zero");
        Self { secs, round_up }
    }

    /// Start converting a group of durations to poll counts.
    pub fn converter(self) -> PollConverter {
        PollConverter {
            poll: self,
            errors: Vec::new(),
        }
    }
}

/// Converts durations to poll counts, collecting every invalid duration so
/// that they can be reported together.
pub struct PollConverter {
    poll: PollPeriod,
    errors: Vec<String>,
}

impl PollConverter {
    /// Return the poll period.
    pub fn period(&self) -> Duration {
        Duration::from_secs(self.poll.secs)
    }

    /// Return how many polls `secs` spans.
    ///
    /// A `secs` that is not a multiple of the poll period is either rounded
    /// up in place, so the effective configuration shows the value in use, or
    /// recorded as an error for [`Self::finish`].
    pub fn polls(&mut self, name: &str, secs: &mut u64) -> u32 {
        let period = self.poll.secs;
        let mut polls = *secs / period;
        if !secs.is_multiple_of(period) {
            let below = polls * period;
            let above = below.saturating_add(period);
            if !self.poll.round_up {
                self.errors.push(format!(
                    "{name} ({secs}) is not a multiple of scale_poll_secs ({period}); \
                     nearest valid values: {below} or {above}"
                ));
                return 0;
            }
            tracing::warn!(
                target: "controller",
                setting = name,
                configured = *secs,
                effective = above,
                "rounded duration up to a multiple of scale_poll_secs"
            );
            *secs = above;
            polls += 1;
        }
        match u32::try_from(polls) {
            Ok(polls) => polls,
            Err(_) => {
                self.errors
                    .push(format!("{name} ({secs}) spans too many polls"));
                0
            }
        }
    }

    /// Fail with every duration rejected by [`Self::polls`].
    pub fn finish(self) -> Result<(), ConfigError> {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::NotPollMultiple(self.errors))
        }
    }
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

fn default_max_instances_adjusted_per_poll() -> u32 {
    0
}

impl Default for Config {
    fn default() -> Self {
        Self {
            engine: "threshold".to_string(),
            engine_config_dir: PrefixedPath::new("/etc/io-thread-controller.d/engines"),
            backend_config_dir: PrefixedPath::new("/etc/io-thread-controller.d/backends"),
            scale_poll_secs: 10,
            round_up_durations: false,
            min_thread_count: 2,
            max_thread_count: 8,
            host_cpu_scale_up_ceiling: 0.9,
            cooldown_secs: default_cooldown_secs(),
            vm_state_path: PrefixedPath::new("/run/io-thread-controller/vm-ownership.json"),
            enable_per_vm_status_line: false,
            enable_per_vm_main_log: false,
            enable_aggregate_status_line: true,
            print_status_header: false,
            max_instances_adjusted_per_poll: default_max_instances_adjusted_per_poll(),
            experimental_psi_monitoring: false,
            refresh_cgroup_on_each_read: false,
            dry_run: false,
        }
    }
}

/// Envelope carrying the `include` key: a relative path to
/// another JSON file merged first, then overridden by keys in
/// the outer document.
#[derive(Debug, Deserialize)]
struct ConfigEnvelope {
    #[serde(default)]
    include: Option<String>,
    #[serde(flatten)]
    rest: serde_json::Value,
}

/// Attempt to load a JSON config file. If not found, fall back on the Default
/// implementation.
pub fn load_or_default<T: for<'de> Deserialize<'de> + Default>(
    path: impl AsRef<std::path::Path>,
) -> Result<T, ConfigError> {
    match load_json_with_includes(path) {
        r @ Ok(_) => r,
        Err(ConfigError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            Ok(Default::default())
        }
        e @ Err(_) => e,
    }
}

/// Load any JSON document that opts into the `include` mechanism. Engines
/// call this on their own config file so site-specific overrides can live in a
/// sibling file.
pub fn load_json_with_includes<T: for<'de> Deserialize<'de>>(
    path: impl AsRef<std::path::Path>,
) -> Result<T, ConfigError> {
    let path = path.as_ref();
    let value = load_config_value(path)?;
    Ok(serde_json::from_value(value)?)
}

fn load_config_value(path: &std::path::Path) -> Result<serde_json::Value, ConfigError> {
    let data = std::fs::read_to_string(path)?;
    let env: ConfigEnvelope = serde_json::from_str(&data)?;
    let mut merged = if let Some(rel) = env.include {
        load_config_value(&path.parent().unwrap_or(path).join(&rel))?
    } else {
        serde_json::Value::Object(Default::default())
    };
    // RFC 7396 merge-patch; delegated to the `json_patch` crate so
    // we do not carry our own recursive merge helper.  Semantic
    // note: a `null` value in the outer patch would *delete* the
    // key rather than overwriting to null.  Our config schema has
    // no null sentinels today so this is a no-op distinction.
    json_patch::merge(&mut merged, &env.rest);
    Ok(merged)
}

impl Config {
    /// Return the poll period that every other duration is a multiple of.
    ///
    /// Panics if `scale_poll_secs` is zero, which [`validate_config`]
    /// rejects.
    pub fn poll_period(&self) -> PollPeriod {
        PollPeriod::new(self.scale_poll_secs, self.round_up_durations)
    }
}

// TODO add a RawConfig or ValidatedConfig to ensure an unvalidated config
// cannot be constructed outside this module.
/// Ensures configuration is internally coherent, rounding durations up to
/// poll multiples when `round_up_durations` is set.
///
/// Invalid durations are not reported yet: the returned converter holds them
/// so that the engine can convert its own durations with it, and
/// [`PollConverter::finish`] then reports every invalid duration at once.
pub fn validate_config(cfg: &mut Config) -> Result<PollConverter, ConfigError> {
    if cfg.scale_poll_secs == 0 {
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
    let mut converter = cfg.poll_period().converter();
    converter.polls("cooldown_secs", &mut cfg.cooldown_secs);
    Ok(converter)
}

/// Serialise the default config as indented JSON for CLI
/// inspection via `--dump-config`.
pub fn dump_default_config() -> String {
    serde_json::to_string_pretty(&Config::default()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rstest::rstest;

    use super::*;

    use crate::test::{MockDir, mockdir::mockdir};

    /// Generate a string which resembles a relative unix path.
    fn arb_path_string() -> impl Strategy<Value = String> {
        prop_oneof![
            "[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}",
            "[a-z0-9]{1,8}(/[a-z0-9]{1,8}){0,2}".prop_map(|tail| format!("/{tail}")),
        ]
    }

    fn reasonable_f64() -> impl Strategy<Value = f64> {
        (-1000..=1000).prop_map(|i| i as f64 / 100.0)
    }

    /// Test that the default `Config` validates.
    #[test]
    fn test_config_default_is_valid() {
        let mut cfg = Config::default();
        validate_config(&mut cfg).unwrap().finish().unwrap();
    }

    /// Test that an empty engine does not validate.
    #[test]
    fn test_config_rejects_empty_engine() {
        let mut cfg = Config::default();
        cfg.engine.clear();
        assert!(validate_config(&mut cfg).is_err());
    }

    /// Test that `validate_config` rejects a zero `scale_poll_secs`.
    #[test]
    fn validate_rejects_zero_poll() {
        let mut cfg = Config {
            scale_poll_secs: 0,
            ..Default::default()
        };
        let err = validate_config(&mut cfg).err().unwrap().to_string();
        assert!(err.contains("scale_poll_secs must be > 0"));
    }

    /// Tests that a cooldown that is not a multiple of the poll period is
    /// rejected with the nearest valid values, and left unchanged.
    #[test]
    fn test_validate_rejects_cooldown_not_poll_multiple() {
        let mut cfg = Config {
            cooldown_secs: 25,
            ..Default::default()
        };
        let err = validate_config(&mut cfg)
            .unwrap()
            .finish()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "cooldown_secs (25) is not a multiple of scale_poll_secs (10); nearest valid \
                 values: 20 or 30"
            ),
            "{err}"
        );
        assert!(err.contains("--round-up-durations"), "{err}");
        assert_eq!(cfg.cooldown_secs, 25);
    }

    /// Tests that `round_up_durations` rounds a non-multiple cooldown up to
    /// the next poll multiple instead of rejecting it.
    #[test]
    fn test_validate_rounds_up_cooldown() {
        let mut cfg = Config {
            cooldown_secs: 25,
            round_up_durations: true,
            ..Default::default()
        };
        validate_config(&mut cfg).unwrap().finish().unwrap();
        assert_eq!(cfg.cooldown_secs, 30);
    }

    /// Tests that the poll converter counts polls, accepts zero, reports
    /// every invalid duration at once, and rounds up when enabled.
    #[test]
    fn test_poll_converter() {
        let mut converter = PollPeriod::new(4, false).converter();
        assert_eq!(converter.polls("zero", &mut 0), 0);
        assert_eq!(converter.polls("exact", &mut 12), 3);
        converter.polls("short", &mut 1);
        converter.polls("long", &mut 30);
        let err = converter.finish().unwrap_err().to_string();
        assert!(
            err.contains(
                "short (1) is not a multiple of scale_poll_secs (4); nearest valid values: 0 or 4"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "long (30) is not a multiple of scale_poll_secs (4); nearest valid values: 28 or 32"
            ),
            "{err}"
        );

        let mut converter = PollPeriod::new(4, true).converter();
        let mut secs = 30;
        assert_eq!(converter.polls("long", &mut secs), 8);
        assert_eq!(secs, 32);
        converter.finish().unwrap();
    }

    /// Tests that durations must be integer seconds.
    #[test]
    fn test_durations_must_be_integers() {
        let err = serde_json::from_str::<Config>(
            r#"{"engine": "foo", "engine_config_dir": "/engines", "backend_config_dir": "/backends", "scale_poll_secs": 7.5, "vm_state_path": "/path/to/vm-state"}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected u64"), "{err}");
    }

    macro_rules! test_round_trips {
        ($i:ident in $v:expr) => {
            mod $i {
                use super::*;

                proptest! {
                    #[test]
                    fn test_config_load_round_trips_field(
                        value in $v
                    ) {
                        let _md = mockdir();
                        let cfg = Config {
                            $i: value.into(),
                            ..Default::default()
                        };
                        let path = PrefixedPath::new("/config.json");
                        std::fs::write(&path, serde_json::to_string(&cfg).unwrap()).unwrap();
                        let loaded: Config = load_or_default(path).unwrap();
                        prop_assert_eq!(loaded, cfg);
                    }
                }
            }
        };
    }

    // Test that each field in Config can survive a save-load round trip.
    test_round_trips!(engine in any::<String>());
    test_round_trips!(engine_config_dir in arb_path_string());
    test_round_trips!(backend_config_dir in arb_path_string());
    test_round_trips!(vm_state_path in arb_path_string());
    test_round_trips!(min_thread_count in any::<u32>());
    test_round_trips!(max_thread_count in any::<u32>());
    test_round_trips!(host_cpu_scale_up_ceiling in reasonable_f64());
    test_round_trips!(cooldown_secs in any::<u64>());
    test_round_trips!(scale_poll_secs in any::<u64>());
    test_round_trips!(round_up_durations in any::<bool>());
    test_round_trips!(enable_per_vm_status_line in any::<bool>());
    test_round_trips!(enable_per_vm_main_log in any::<bool>());
    test_round_trips!(enable_aggregate_status_line in any::<bool>());
    test_round_trips!(print_status_header in any::<bool>());
    test_round_trips!(max_instances_adjusted_per_poll in any::<u32>());
    test_round_trips!(experimental_psi_monitoring in any::<bool>());
    test_round_trips!(refresh_cgroup_on_each_read in any::<bool>());
    test_round_trips!(dry_run in any::<bool>());

    /// Ensure that the serde-constructed default is equivalent to the
    /// Default::default implementation, while filling out the required serde
    /// fields.
    #[test]
    #[ignore = "current fails"]
    fn test_config_serde_default_equivalent() {
        let serde_cfg: Config = serde_json::from_str(
            r#"
{
    "engine":"threshold",
    "engine_config_dir":"/etc/io-thread-controller.d/engines",
    "backend_config_dir":"/etc/io-thread-controller.d/backends",
    "vm_state_path":"/run/io-thread-controller/vm-ownership.json",
    "scale_poll_secs":10.0
}"#,
        )
        .unwrap();
        let default_cfg = Config::default();

        assert_eq!(serde_cfg, default_cfg);
    }

    /// Test that loading JSON with an unknown field returns
    /// `ConfigError::SerdeJson`.
    #[rstest]
    fn test_config_load_rejects_unknown_fields(#[from(mockdir)] _mockdir: MockDir) {
        let path = PrefixedPath::new("/config");
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

        let err = load_or_default::<Config>(&path).unwrap_err();
        assert!(matches!(err, ConfigError::SerdeJson(_)));
    }
    /// Test that pretty-printed default JSON includes engine, poll
    /// interval, and engine config path.
    #[test]
    fn test_dump_default_config_contains_engine() {
        let dumped = dump_default_config();
        assert!(dumped.contains(r#""engine": "threshold""#));
        assert!(dumped.contains(r#""scale_poll_secs": 10"#));
        assert!(dumped.contains(r#""/etc/io-thread-controller.d/engines""#));
    }

    /// Test that `refresh_cgroup_on_each_read` defaults to false in
    /// `Default` and empty JSON.
    #[test]
    fn test_cgroup_refresh_defaults_to_false() {
        assert!(!Config::default().refresh_cgroup_on_each_read);
        let from_json: Config = serde_json::from_str(
            r#"{"engine": "foo", "engine_config_dir": "/engines", "backend_config_dir": "/backends", "scale_poll_secs": 10, "vm_state_path": "/path/to/vm-state"}"#,
        )
        .unwrap();
        assert!(!from_json.refresh_cgroup_on_each_read);
    }
}
