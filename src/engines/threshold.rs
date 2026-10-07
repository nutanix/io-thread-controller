// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Threshold engine: size a worker pool to carry the observed CPU load.

use std::{collections::HashMap, sync::Arc, time::Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    config::{ConfigError, deserialize_percent, serialize_percent},
    engines::{
        AppliedOutcome, EngineError, EngineRegistration, EngineTickContext, ScaleAction,
        ScalingEngine,
    },
    instance::Instance,
    util::Path,
};

/// Registry and configuration name of the threshold engine.
pub const ENGINE_NAME: &str = "threshold";

fn build_threshold_engine(dir: &Path) -> Result<Box<dyn ScalingEngine>, EngineError> {
    Ok(Box::new(ThresholdEngine::from_config_dir(dir)?))
}

#[linkme::distributed_slice(super::ENGINES)]
static THRESHOLD_ENGINE: EngineRegistration = EngineRegistration {
    name: ENGINE_NAME,
    build: build_threshold_engine,
};

/// Scale, sustain, and post-action validation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdConfig {
    /// Average per-worker CPU percent above which another worker is proposed.
    #[serde(
        default = "default_scale_up_threshold",
        rename = "scale_up_threshold_percent",
        deserialize_with = "deserialize_percent",
        serialize_with = "serialize_percent"
    )]
    pub scale_up_threshold: f64,
    /// Consecutive eligible polls required before scale-down.
    #[serde(default = "default_scale_down_sustain_polls")]
    pub scale_down_sustain_polls: u32,
    /// Maximum number of threads than can be reduced in a scale down operation.
    /// 0 means unlimited.
    #[serde(default = "default_max_scale_down_step")]
    pub max_scale_down_step: u32,
    /// Minimum IOPS gain required after scale-up; zero disables validation.
    #[serde(
        default = "default_scale_up_min_gain",
        rename = "scale_up_min_gain_percent",
        deserialize_with = "deserialize_percent",
        serialize_with = "serialize_percent"
    )]
    pub scale_up_min_gain: f64,
    /// Maximum tolerated IOPS loss after scale-down; zero disables validation.
    #[serde(
        default = "default_scale_down_revert_drop",
        rename = "scale_down_revert_drop_percent",
        deserialize_with = "deserialize_percent",
        serialize_with = "serialize_percent"
    )]
    pub scale_down_revert_drop: f64,
    /// Complete samples to wait before validating a successful action.
    #[serde(default = "default_scale_validation_sample_polls")]
    pub scale_validation_sample_polls: u32,
    /// Suppress scale-up after the VM's cgroup reports new throttled CPU time.
    #[serde(default = "default_true")]
    pub block_scale_up_when_cgroup_throttled: bool,
}

fn default_scale_up_threshold() -> f64 {
    0.95
}

fn default_scale_down_sustain_polls() -> u32 {
    3
}

fn default_max_scale_down_step() -> u32 {
    0
}

fn default_scale_up_min_gain() -> f64 {
    0.05
}
fn default_scale_down_revert_drop() -> f64 {
    0.05
}
fn default_scale_validation_sample_polls() -> u32 {
    2
}
fn default_true() -> bool {
    true
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            scale_up_threshold: default_scale_up_threshold(),
            scale_down_sustain_polls: default_scale_down_sustain_polls(),
            max_scale_down_step: default_max_scale_down_step(),
            scale_up_min_gain: default_scale_up_min_gain(),
            scale_down_revert_drop: default_scale_down_revert_drop(),
            scale_validation_sample_polls: default_scale_validation_sample_polls(),
            block_scale_up_when_cgroup_throttled: true,
        }
    }
}

impl ThresholdConfig {
    /// Reject unusable percentages and thread limits.
    fn validate(&self) -> Result<(), ConfigError> {
        if !(self.scale_up_threshold.is_finite()
            && self.scale_up_threshold > 0.0
            && self.scale_up_threshold <= 1.0)
        {
            return Err(ConfigError::InvalidValue(
                "scale_up_threshold_percent must be in (0, 100]".to_string(),
            ));
        }
        if !(self.scale_up_min_gain.is_finite() && (0.0..=1.0).contains(&self.scale_up_min_gain)) {
            return Err(ConfigError::InvalidValue(
                "scale_up_min_gain_percent must be in [0, 100]".to_string(),
            ));
        }
        if !(self.scale_down_revert_drop.is_finite()
            && (0.0..=1.0).contains(&self.scale_down_revert_drop))
        {
            return Err(ConfigError::InvalidValue(
                "scale_down_revert_drop_percent must be in [0, 100]".to_string(),
            ));
        }
        Ok(())
    }
}

/// One successful action waiting for post-action IOPS validation.
#[derive(Debug, Clone, Copy)]
struct PendingValidation {
    /// Action whose performance effect is being checked.
    action: ScaleAction,
    /// Worker count to restore after a regression.
    baseline_thread_count: u32,
    /// IOPS observed during the pre-scale baseline window.
    baseline_iops: u64,
    /// Start IO count and time of validation window.
    validation_start: RateWindow,
    /// Complete observations still required before evaluation.
    polls_remaining: u32,
}

/// Start of an IOPS-rate measurement window.
#[derive(Debug, Clone, Copy)]
struct RateWindow {
    io_count: u64,
    at: Instant,
}

/// Per-VM counters owned by this engine.
#[derive(Clone)]
struct InstanceState {
    /// Original controller record retained so lifecycle-owned state can refer
    /// to the same VM without duplicating its identity or backend handle.
    // FIXME This is probably not needed. Only .evaluate() uses it, and that already takes
    // a &Arc<Instance>. It seems like this contradicts the controller owning inventory design.
    instance: Arc<Instance>,
    /// Number of consecutive polls eligible for scale-down.
    low_util_polls: u32,
    /// Successful action waiting for its performance validation sample.
    pending_validation: Option<PendingValidation>,
    /// Start of the post-settlement baseline window (after last validation).
    baseline_window_start: Option<RateWindow>,
    /// Most recent baseline-window IOPS rate, refreshed each evaluate tick.
    current_baseline_iops: u64,
}

impl InstanceState {
    /// Create lifecycle state for the exact controller instance.
    fn new(instance: Arc<Instance>) -> Self {
        Self {
            instance,
            low_util_polls: 0,
            pending_validation: None,
            baseline_window_start: None,
            current_baseline_iops: 0,
        }
    }

    /// Retain the baseline needed to validate a completed scale action.
    fn start_validation(
        &mut self,
        action: ScaleAction,
        previous_thread_count: u32,
        baseline_iops: u64,
        validation_start: RateWindow,
        polls_remaining: u32,
    ) {
        self.pending_validation = Some(PendingValidation {
            action,
            baseline_thread_count: previous_thread_count,
            baseline_iops,
            validation_start,
            polls_remaining,
        });
    }

    /// Begin a new baseline window after settlement (or a non-validated scale).
    fn reset_baseline_window(&mut self, start: RateWindow) {
        self.baseline_window_start = Some(start);
        self.current_baseline_iops = 0;
    }

    /// Refresh the baseline-window IOPS rate ending at `end`.
    fn refresh_baseline_rate(&mut self, end: RateWindow) -> u64 {
        let rate = match self.baseline_window_start {
            None => {
                self.baseline_window_start = Some(end);
                0
            }
            Some(start) => iops_rate(start, end),
        };
        self.current_baseline_iops = rate;
        rate
    }
}

/// Average ops/sec between two cumulative counter samples.
fn iops_rate(start: RateWindow, end: RateWindow) -> u64 {
    let Some(elapsed) = end.at.checked_duration_since(start.at) else {
        return 0;
    };
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0;
    }
    let delta = end.io_count.saturating_sub(start.io_count) as f64;
    (delta / secs).round() as u64
}

/// Scaling engine driven by CPU utilisation thresholds.
pub struct ThresholdEngine {
    cfg: ThresholdConfig,

    // TODO the controller already maintains this mapping, an alternative would
    // be for this engine to define a private type and keep a reference to
    // to InstanceState.
    state: Mutex<HashMap<String, InstanceState>>,
}

impl ThresholdEngine {
    /// Construct an engine from validated settings.
    pub fn new(cfg: ThresholdConfig) -> Self {
        Self {
            cfg,
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Load `threshold.json`, falling back to built-in defaults when absent.
    pub fn from_config_dir(dir: &Path) -> Result<Self, EngineError> {
        let path = Path::new(&dir.join(format!("{ENGINE_NAME}.json")));
        // TODO TOCTOU, blindly load and return default if ENOENT
        let cfg: ThresholdConfig = if path.exists() {
            crate::config::load_json_with_includes(path)?
        } else {
            ThresholdConfig::default()
        };
        cfg.validate()?;
        Ok(Self::new(cfg))
    }

    /// Return the engine's effective configuration.
    pub fn config(&self) -> &ThresholdConfig {
        &self.cfg
    }

    /// Calculate the smallest pool that keeps average load at the up threshold.
    fn scale_down_target(
        &self,
        per_thread_util: f64,
        thread_count: u32,
        min_thread_count: u32,
    ) -> u32 {
        let total_load = per_thread_util * f64::from(thread_count);
        let required = (total_load / self.cfg.scale_up_threshold)
            .ceil()
            .clamp(1.0, f64::from(u32::MAX)) as u32;
        let target = required.max(min_thread_count);
        if target >= thread_count {
            return thread_count;
        }
        if self.cfg.max_scale_down_step == 0 {
            target
        } else {
            target.max(
                thread_count
                    .saturating_sub(self.cfg.max_scale_down_step)
                    .max(min_thread_count),
            )
        }
    }

    /// Validate a settled action and return its revert target on regression.
    fn validation_revert_target(
        &self,
        instance_id: &str,
        thread_count: u32,
        observed_iops: u64,
        per_thread_util: f64,
        pending: PendingValidation,
    ) -> Option<u32> {
        let (required_iops, failed) = match pending.action {
            ScaleAction::Up(_) => {
                let required = pending.baseline_iops as f64 * (1.0 + self.cfg.scale_up_min_gain);
                (
                    required,
                    self.cfg.scale_up_min_gain > 0.0
                        && pending.baseline_iops > 0
                        && (observed_iops as f64) < required,
                )
            }
            ScaleAction::Down(_) => {
                // Scale-down was driven by low util. If util is still low, an
                // IOPS collapse means demand left — keep the smaller pool.
                if per_thread_util <= self.cfg.scale_up_threshold {
                    return None;
                }
                let required =
                    pending.baseline_iops as f64 * (1.0 - self.cfg.scale_down_revert_drop);
                (
                    required,
                    self.cfg.scale_down_revert_drop > 0.0
                        && pending.baseline_iops > 0
                        && (observed_iops as f64) < required,
                )
            }
            ScaleAction::None | ScaleAction::Revert(_) => return None,
        };
        if !failed {
            return None;
        }
        log_performance_revert(
            instance_id,
            &pending.action.to_string(),
            thread_count,
            pending.baseline_thread_count,
            pending.baseline_iops,
            observed_iops,
            required_iops,
        );
        Some(pending.baseline_thread_count)
    }

    /// Seconds until a post-scale IOPS sample is evaluated.
    fn validation_after_secs(&self, scale_poll_secs: f64) -> f64 {
        // Countdown consumes `scale_validation_sample_polls` ticks, then the
        // next tick performs the comparison.
        (f64::from(self.cfg.scale_validation_sample_polls) + 1.0) * scale_poll_secs
    }
}

/// Announce why post-action validation is rolling a worker count back.
fn log_performance_revert(
    instance_id: &str,
    reverted_action: &str,
    thread_count: u32,
    target: u32,
    baseline_iops: u64,
    observed_iops: u64,
    required_iops: f64,
) {
    let observed_change_percent = if baseline_iops == 0 {
        0.0
    } else {
        (observed_iops as f64 - baseline_iops as f64) * 100.0 / baseline_iops as f64
    };
    tracing::info!(
        target: "engine",
        vm = %instance_id,
        action = "revert",
        reverted_action = %reverted_action,
        reason = "performance-validation-failed",
        baseline_iops,
        observed_iops,
        required_iops,
        observed_change_percent,
        thr = %format!("{thread_count}->{target}"),
        "performance validation failed; reverting previous scale"
    );
}

/// Emit one operator-visible scale-up decision with validation expectations.
fn log_scale_up_decision(
    instance: &Instance,
    per_thread_util: f64,
    thread_count: u32,
    target: u32,
    current_iops: u64,
    min_target_iops: u64,
    validation_after_secs: f64,
) {
    tracing::info!(
        target: "engine",
        event = "scale_up",
        vm = instance.to_string(),
        util = per_thread_util,
        thr = %format!("{thread_count}->{target}"),
        current_iops,
        min_target_iops,
        validation_after_secs,
        "threshold scaling decision"
    );
}

/// Emit one operator-visible scale-down decision.
fn log_scale_down_decision(
    instance: &Instance,
    per_thread_util: f64,
    thread_count: u32,
    target: u32,
) {
    tracing::info!(
        target: "engine",
        event = "scale_down",
        vm = instance.to_string(),
        util = per_thread_util,
        thr = %format!("{thread_count}->{target}"),
        "threshold scaling decision"
    );
}

#[async_trait]
impl ScalingEngine for ThresholdEngine {
    fn name(&self) -> &'static str {
        ENGINE_NAME
    }

    fn dump_config(&self) -> serde_json::Value {
        serde_json::to_value(&self.cfg).unwrap_or(serde_json::Value::Null)
    }

    async fn evaluate(&self, instance: &Arc<Instance>, context: &EngineTickContext) -> ScaleAction {
        let (per_thread_util, thread_count, io_count_total, throttled_recently) = {
            let status = instance.status.read().await;
            (
                status.per_thread_util,
                status.thread_count,
                match &status.perf {
                    Some(perf) => perf.total_io_count(),
                    None => 0,
                },
                status.throttled_usec_delta > 0,
            )
        };
        let down_target =
            self.scale_down_target(per_thread_util, thread_count, context.min_thread_count);
        let mut state = self.state.lock().await;
        let instance_state = state
            .entry(instance.id.clone())
            .or_insert_with(|| InstanceState::new(Arc::clone(instance)));

        let sample = RateWindow {
            io_count: io_count_total,
            at: context.now,
        };

        if let Some(mut pending) = instance_state.pending_validation.take() {
            if pending.polls_remaining > 0 {
                pending.polls_remaining -= 1;
                instance_state.pending_validation = Some(pending);
                return ScaleAction::None;
            }
            let observed_iops = iops_rate(pending.validation_start, sample);
            let revert_target = self.validation_revert_target(
                &instance_state.instance.id,
                thread_count,
                observed_iops,
                per_thread_util,
                pending,
            );
            // Whether retained or reverted, the next baseline excludes this
            // validation window. Defer further scaling until the new baseline
            // window has at least one full tick of samples.
            instance_state.reset_baseline_window(sample);
            if let Some(target) = revert_target {
                return ScaleAction::Revert(target);
            }
            return ScaleAction::None;
        }

        instance_state.refresh_baseline_rate(sample);

        if down_target < thread_count {
            instance_state.low_util_polls += 1;
        } else {
            instance_state.low_util_polls = 0;
        }

        if thread_count < context.max_thread_count && per_thread_util > self.cfg.scale_up_threshold
        {
            if self.cfg.block_scale_up_when_cgroup_throttled && throttled_recently {
                tracing::info!(
                    target: "controller",
                    id = %instance.id,
                    "scale-up suppressed by cgroup CPU throttling"
                );
                return ScaleAction::None;
            }
            // FIXME The min seems redundant given the thread count will always be less than
            // or equal to the max_thread_count here.
            let target = (thread_count + 1).min(context.max_thread_count);
            let current_iops = instance_state.current_baseline_iops;
            let (min_target_iops, validation_after_secs) = if self.cfg.scale_up_min_gain > 0.0 {
                (
                    ((current_iops as f64) * (1.0 + self.cfg.scale_up_min_gain)).ceil() as u64,
                    self.validation_after_secs(context.scale_poll_secs),
                )
            } else {
                (current_iops, 0.0)
            };
            log_scale_up_decision(
                instance,
                per_thread_util,
                thread_count,
                target,
                current_iops,
                min_target_iops,
                validation_after_secs,
            );
            return ScaleAction::Up(target);
        }

        if down_target < thread_count
            && instance_state.low_util_polls >= self.cfg.scale_down_sustain_polls
        {
            let action = ScaleAction::Down(down_target);
            log_scale_down_decision(instance, per_thread_util, thread_count, down_target);
            action
        } else {
            ScaleAction::None
        }
    }

    async fn on_applied(&self, instance_id: &str, outcome: AppliedOutcome) {
        let (action, previous_thread_count, previous_io_count, applied_at) = match outcome {
            AppliedOutcome::Success {
                action,
                prev_thread_count,
                prev_io_count_total,
                applied_at,
            }
            | AppliedOutcome::DryRun {
                action,
                prev_thread_count,
                prev_io_count_total,
                applied_at,
            } => (action, prev_thread_count, prev_io_count_total, applied_at),
            AppliedOutcome::Blocked { .. } | AppliedOutcome::Failed { .. } => return,
        };
        let mut state = self.state.lock().await;
        let Some(instance_state) = state.get_mut(instance_id) else {
            return;
        };
        let applied = RateWindow {
            io_count: previous_io_count,
            at: applied_at,
        };
        match action {
            ScaleAction::Up(_) if self.cfg.scale_up_min_gain > 0.0 => {
                // Reset sustain only after the controller accepted the action;
                // a merely proposed or blocked downscale must retain the
                // evidence accumulated by the sustain policy.
                instance_state.low_util_polls = 0;
                instance_state.start_validation(
                    action,
                    previous_thread_count,
                    instance_state.current_baseline_iops,
                    applied,
                    self.cfg.scale_validation_sample_polls,
                );
            }
            ScaleAction::Down(_) if self.cfg.scale_down_revert_drop > 0.0 => {
                // Successful actuation consumes the sustained low-utilisation
                // run; subsequent downscaling must establish a fresh run.
                instance_state.low_util_polls = 0;
                instance_state.start_validation(
                    action,
                    previous_thread_count,
                    instance_state.current_baseline_iops,
                    applied,
                    self.cfg.scale_validation_sample_polls,
                );
            }
            ScaleAction::Up(_) => {
                instance_state.low_util_polls = 0;
                instance_state.reset_baseline_window(applied);
            }
            ScaleAction::Down(_) => {
                instance_state.low_util_polls = 0;
                instance_state.reset_baseline_window(applied);
            }
            ScaleAction::None | ScaleAction::Revert(_) => {}
        }
    }

    async fn on_instance_added(&self, instance: &Arc<Instance>) {
        let mut state = self.state.lock().await;
        state
            .entry(instance.id.clone())
            .or_insert_with(|| InstanceState::new(Arc::clone(instance)));
    }

    async fn on_instance_removed(&self, instance_id: &str) {
        self.state.lock().await.remove(instance_id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use rstest::{fixture, rstest};
    use test_log::test;

    use super::{ThresholdConfig, ThresholdEngine, log_performance_revert};
    use crate::{
        backends::BackendClientError,
        engines::{AppliedOutcome, EngineTickContext, ScaleAction, ScalingEngine},
        instance::{
            Instance, InstanceClient, InstancePerfSample, InstanceStatus, ThreadPoolSnapshot,
        },
        util::Path,
    };

    #[derive(Clone)]
    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct SnapshotClient;

    #[async_trait]
    impl InstanceClient for SnapshotClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: 1,
                vcpu_count: 8,
                perf: None,
                per_thread_util: Some(0.0),
            })
        }

        async fn close(&self) {}
    }

    #[fixture]
    async fn instance() -> Arc<Instance> {
        let instance = Arc::new(Instance::new(
            "vm-1".to_string(),
            Path::new(""),
            0,
            SnapshotClient,
        ));
        let mut status = instance.status.write().await;
        *status = InstanceStatus::default();
        status.perf = Some(InstancePerfSample::default());
        drop(status);
        instance
    }

    fn context_at(now: Instant) -> EngineTickContext {
        EngineTickContext {
            now,
            scale_poll_secs: 10.0,
            ..EngineTickContext::default()
        }
    }

    async fn set_observation(
        instance: &Arc<Instance>,
        thread_count: u32,
        per_thread_util: f64,
        io_count: u64,
    ) {
        let mut status = instance.status.write().await;
        status.thread_count = thread_count;
        status.per_thread_util = per_thread_util;
        status.perf.as_mut().unwrap().read_io_count = io_count;
    }

    /// Establish a baseline window of `baseline_iops` ops/sec ending at `t1`.
    async fn seed_baseline_rate(
        engine: &ThresholdEngine,
        instance: &Arc<Instance>,
        thread_count: u32,
        baseline_iops: u64,
        t0: Instant,
        t1: Instant,
    ) {
        engine.on_instance_added(instance).await;
        // Park util exactly at the up threshold so neither up nor down fires.
        let steady_util = engine.cfg.scale_up_threshold;
        set_observation(instance, thread_count, steady_util, 0).await;
        assert_eq!(
            engine.evaluate(instance, &context_at(t0)).await,
            ScaleAction::None
        );
        set_observation(instance, thread_count, steady_util, baseline_iops).await;
        assert_eq!(
            engine.evaluate(instance, &context_at(t1)).await,
            ScaleAction::None
        );
    }

    /// Test that performance-revert log line includes VM id and the
    /// reverted action.
    #[test]
    fn performance_revert_log_contains_decision_inputs() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || BufferWriter(Arc::clone(&writer_output)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        log_performance_revert("vm-1", "up", 6, 5, 155_000, 122_000, 162_750.0);

        let rendered = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("performance validation failed; reverting previous scale"));
        assert!(rendered.contains("vm=vm-1"));
        assert!(rendered.contains("reverted_action=up"));
        assert!(rendered.contains("baseline_iops=155000"));
        assert!(rendered.contains("observed_iops=122000"));
        assert!(rendered.contains("required_iops=162750"));
        assert!(rendered.contains("thr=6->5"));
    }

    #[fixture]
    fn engine() -> ThresholdEngine {
        ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 0,
            ..Default::default()
        })
    }

    /// Test that an IOPS-rate drop after scale-up triggers revert.
    #[rstest]
    #[tokio::test]
    async fn scale_up_revert_fires_on_iops_rate_drop(
        engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, &instance, 5, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(6),
                    prev_thread_count: 5,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // Validation window: only 50k ops over 1s → 50k IOPS (< 105k required).
        set_observation(&instance, 6, 0.6, 150_000).await;

        assert_eq!(
            engine.evaluate(&instance, &context_at(t_validate)).await,
            ScaleAction::Revert(5)
        );
    }

    /// Test that flat post-scale IOPS/rate during validation triggers
    /// revert.
    #[rstest]
    #[tokio::test]
    async fn scale_up_flat_rate_reverts(
        engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, &instance, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // +1% ops over the validation second → still below 5% gain.
        set_observation(&instance, 4, 0.6, 101_000).await;

        assert_eq!(
            engine.evaluate(&instance, &context_at(t_validate)).await,
            ScaleAction::Revert(3)
        );
    }

    /// Test that a sufficient post-scale IOPS-rate gain keeps the scale-up.
    #[rstest]
    #[tokio::test]
    async fn scale_up_rate_gain_keeps_scale(
        engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, &instance, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // +10% ops/sec during validation → above 5% min gain.
        set_observation(&instance, 4, 0.6, 210_000).await;

        assert_eq!(
            engine.evaluate(&instance, &context_at(t_validate)).await,
            ScaleAction::None
        );
    }

    /// Test that scale-down validation does not revert when util stays low
    /// (demand left; IOPS collapse is expected).
    #[rstest]
    #[tokio::test]
    async fn scale_down_keeps_shrink_when_util_stays_low(#[future] instance: Arc<Instance>) {
        let instance = instance.await;
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_down_revert_drop: 0.05,
            scale_validation_sample_polls: 0,
            scale_down_sustain_polls: 1,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, &instance, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Down(1),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // IOPS collapsed, but util remains below the up threshold.
        set_observation(&instance, 1, 0.0, 100_001).await;
        assert_eq!(
            engine.evaluate(&instance, &context_at(t_validate)).await,
            ScaleAction::None
        );
    }

    /// Test that scale-down validation reverts when util is still high (demand
    /// remains) but IOPS fell beyond the tolerated drop.
    #[rstest]
    #[tokio::test]
    async fn scale_down_reverts_when_util_stays_high_and_iops_drop(
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_down_revert_drop: 0.05,
            scale_validation_sample_polls: 0,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, &instance, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Down(2),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // Demand still present (util above up threshold) but IOPS collapsed.
        set_observation(&instance, 2, 0.95, 110_000).await;
        assert_eq!(
            engine.evaluate(&instance, &context_at(t_validate)).await,
            ScaleAction::Revert(4)
        );
    }

    /// Test that percent fields serde as human percents on the wire and
    /// fractions in memory.
    #[test]
    fn percent_wire_format_round_trips() {
        let config: ThresholdConfig =
            serde_json::from_str(r#"{"scale_up_min_gain_percent":10}"#).unwrap();
        assert!((config.scale_up_min_gain - 0.10).abs() < f64::EPSILON);

        let serialized = serde_json::to_string(&config).unwrap();
        assert!(serialized.contains(r#""scale_up_min_gain_percent":10.0"#));
        assert!(
            serde_json::from_str::<ThresholdConfig>(r#"{"scale_up_revert_drop_percent":10}"#)
                .is_err()
        );
    }

    /// Test that while a prior scale-up is pending validation, further
    /// scale-ups are suppressed.
    #[rstest]
    #[test(tokio::test)]
    async fn no_scale_up_during_pending_validation(
        engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;

        seed_baseline_rate(&engine, &instance, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(5),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;

        set_observation(&instance, 5, 0.88, 220_000).await;

        for i in 0..engine.cfg.scale_validation_sample_polls {
            assert_eq!(
                engine
                    .evaluate(
                        &instance,
                        &context_at(t_apply + Duration::from_secs(u64::from(i) + 1))
                    )
                    .await,
                ScaleAction::None
            );
        }

        // Validation tick settles the scale-up without proposing another action.
        let t_settle = t_apply + Duration::from_secs(1);
        assert_eq!(
            engine.evaluate(&instance, &context_at(t_settle)).await,
            ScaleAction::None
        );

        // Next tick with sustained high util may scale again.
        set_observation(&instance, 5, 0.88, 330_000).await;
        assert_eq!(
            engine
                .evaluate(&instance, &context_at(t_settle + Duration::from_secs(1)))
                .await,
            ScaleAction::Up(6)
        );
    }

    /// Test that a scale up is revert after the validation period if
    /// performance doesn't increase much.
    #[rstest]
    #[test(tokio::test)]
    async fn pending_validation_reverts_regressive_scale(
        _engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 2,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;

        seed_baseline_rate(&engine, &instance, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                &instance.id,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(5),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        set_observation(&instance, 5, 0.88, 140_000).await;

        for i in 0..engine.cfg.scale_validation_sample_polls {
            assert_eq!(
                engine
                    .evaluate(
                        &instance,
                        &context_at(t_apply + Duration::from_secs(u64::from(i) + 1))
                    )
                    .await,
                ScaleAction::None
            );
        }
        // 40k ops over ~3s ≈ 13k IOPS ≪ 105k required.
        assert_eq!(
            engine
                .evaluate(&instance, &context_at(t_apply + Duration::from_secs(3)))
                .await,
            ScaleAction::Revert(4)
        );
    }

    /// Test that scale-up decision logs current/min IOPS and validation delay.
    #[rstest]
    #[tokio::test]
    async fn scale_up_log_includes_iops_targets(
        engine: ThresholdEngine,
        #[future] instance: Arc<Instance>,
    ) {
        let instance = instance.await;
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer_output = Arc::clone(&output);
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_target(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || BufferWriter(Arc::clone(&writer_output)))
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        seed_baseline_rate(&engine, &instance, 2, 100_000, t0, t1).await;
        set_observation(&instance, 2, 0.9, 100_000).await;

        assert_eq!(
            engine.evaluate(&instance, &context_at(t1)).await,
            ScaleAction::Up(3)
        );

        let rendered = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("event=\"scale_up\"") || rendered.contains("event=scale_up"));
        assert!(rendered.contains("current_iops=100000"));
        assert!(rendered.contains("min_target_iops=105000"));
        assert!(rendered.contains("validation_after_secs=10"));
    }

    /// Test that a missing `threshold.json` still builds an engine with
    /// defaults.
    #[test]
    fn missing_config_file_uses_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            ThresholdEngine::from_config_dir(&Path::new(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(engine.name(), super::ENGINE_NAME);
        let _ = engine.config();
    }

    /// Test that CPU utilisation below scale-up/down thresholds yields
    /// Hold/`None`.
    #[rstest]
    #[tokio::test]
    async fn evaluate_holds_when_util_below_thresholds(#[future] instance: Arc<Instance>) {
        let instance = instance.await;
        let engine = ThresholdEngine::new(ThresholdConfig::default());
        let action = engine
            .evaluate(&instance, &context_at(Instant::now()))
            .await;
        assert_eq!(action, ScaleAction::None);
    }
}
