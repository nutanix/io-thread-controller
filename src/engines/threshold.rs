// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Threshold engine: size a worker pool to carry the observed CPU load.

use std::{collections::HashMap, time::Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{
    config::{self, ConfigError, deserialize_percent, serialize_percent},
    engines::{
        AppliedOutcome, EngineError, EngineInstanceSnapshot, EngineRegistration, EngineTickContext,
        ScaleAction, ScalingEngine,
    },
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
    /// Additional validation polls per target thread after a scale action.
    #[serde(default)]
    pub scale_validation_sample_polls_per_thread: u32,
    /// Suppress scale-up after the VM's cgroup reports new throttled CPU time.
    #[serde(default = "default_true")]
    pub block_scale_up_when_cgroup_throttled: bool,
    /// Polls after a failed scale-up revert during which metrics are ignored.
    #[serde(default = "default_post_revert_up_grace_polls")]
    pub post_revert_up_grace_polls: u32,
    /// Polls after grace spent measuring a settled baseline while scale-up is
    /// still blocked.
    #[serde(default = "default_post_revert_up_recovery_polls")]
    pub post_revert_up_recovery_polls: u32,
    /// Cap on the post-revert hold (grace plus recovery), which doubles with
    /// each consecutive failed scale-up. Zero disables the backoff.
    #[serde(default = "default_post_revert_up_backoff_max_polls")]
    pub post_revert_up_backoff_max_polls: u32,
    /// Cap on the low-utilisation polls required before scale-down, which
    /// doubles with each consecutive failed scale-down. Zero disables the
    /// backoff.
    #[serde(default = "default_scale_down_backoff_max_polls")]
    pub scale_down_backoff_max_polls: u32,
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
fn default_post_revert_up_grace_polls() -> u32 {
    1
}
fn default_post_revert_up_recovery_polls() -> u32 {
    2
}
fn default_post_revert_up_backoff_max_polls() -> u32 {
    30
}
fn default_scale_down_backoff_max_polls() -> u32 {
    120
}

/// `base` doubled `doublings` times, capped at `max` (never below `base`).
/// A zero `max` disables the backoff and returns `base`.
fn backoff_polls(base: u32, doublings: u32, max: u32) -> u32 {
    if max == 0 {
        return base;
    }
    base.saturating_mul(2u32.saturating_pow(doublings))
        .min(max.max(base))
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
            scale_validation_sample_polls_per_thread: 0,
            block_scale_up_when_cgroup_throttled: true,
            post_revert_up_grace_polls: default_post_revert_up_grace_polls(),
            post_revert_up_recovery_polls: default_post_revert_up_recovery_polls(),
            post_revert_up_backoff_max_polls: default_post_revert_up_backoff_max_polls(),
            scale_down_backoff_max_polls: default_scale_down_backoff_max_polls(),
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
    validation_start: IoSample,
    /// Complete observations still required before evaluation.
    polls_remaining: u32,
}

/// Start of an IOPS-rate measurement window.
#[derive(Debug, Clone, Copy)]
struct IoSample {
    io_count: u64,
    at: Instant,
}

/// Hold that blocks scale-up after a failed scale-up was reverted.
#[derive(Debug, Clone, Copy)]
enum PostRevertUpHold {
    /// Metrics ignored while the pool settles after the revert.
    Grace { polls_remaining: u32 },
    /// Scale-up still blocked while a settled baseline rate is measured.
    Recovery {
        polls_remaining: u32,
        window_start: IoSample,
    },
}
/// Per-VM counters owned by this engine.
#[derive(Clone)]
struct InstanceState {
    /// Number of consecutive polls eligible for scale-down.
    low_util_polls: u32,
    /// Successful action waiting for its performance validation sample.
    pending_validation: Option<PendingValidation>,
    /// Start of the post-settlement baseline window (after last validation).
    baseline_window_start: Option<IoSample>,
    /// Most recent baseline-window IOPS rate, refreshed each evaluate tick.
    current_baseline_iops: u64,
    /// After a failed scale-up revert, suppress further ups through grace then
    /// recovery while a settled baseline is established.
    post_revert_up_hold: Option<PostRevertUpHold>,
    /// Scale-ups reverted by validation since the last kept scale-up or
    /// applied scale-down.
    consecutive_failed_ups: u32,
    /// Scale-downs reverted by validation since the last kept scale-down or
    /// applied scale-up.
    consecutive_failed_downs: u32,
}

impl InstanceState {
    /// Create lifecycle state for one controller-owned instance.
    fn new() -> Self {
        Self {
            low_util_polls: 0,
            pending_validation: None,
            baseline_window_start: None,
            current_baseline_iops: 0,
            post_revert_up_hold: None,
            consecutive_failed_ups: 0,
            consecutive_failed_downs: 0,
        }
    }

    /// Retain the baseline needed to validate a completed scale action.
    fn start_validation(
        &mut self,
        action: ScaleAction,
        previous_thread_count: u32,
        baseline_iops: u64,
        validation_start: IoSample,
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
    fn reset_baseline_window(&mut self, start: IoSample) {
        self.baseline_window_start = Some(start);
        self.current_baseline_iops = 0;
    }

    /// Consume the evidence that led to an applied scale action.
    ///
    /// Only called once the controller accepted the action; a merely proposed
    /// or blocked action must retain the evidence accumulated by the sustain
    /// policy, and subsequent scaling must establish a fresh run.
    fn reset_after_applied(&mut self, action: ScaleAction) {
        match action {
            ScaleAction::Up(_) => self.consecutive_failed_downs = 0,
            ScaleAction::Down(_) => {
                self.post_revert_up_hold = None;
                self.consecutive_failed_ups = 0;
            }
            ScaleAction::None | ScaleAction::Revert(_) => return,
        }
        self.low_util_polls = 0;
    }

    /// Refresh the baseline-window IOPS rate ending at `end`.
    fn refresh_baseline_rate(&mut self, end: IoSample) -> u64 {
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

    /// Start grace→recovery hold after a failed scale-up revert.
    fn begin_post_revert_up_hold(&mut self, grace_polls: u32) {
        self.baseline_window_start = None;
        self.current_baseline_iops = 0;
        self.post_revert_up_hold = Some(PostRevertUpHold::Grace {
            polls_remaining: grace_polls,
        });
    }

    /// Advance grace/recovery state. Returns true while scale-up must stay
    /// blocked.
    fn advance_post_revert_up_hold(&mut self, sample: IoSample, recovery_polls: u32) -> bool {
        let Some(hold) = self.post_revert_up_hold.take() else {
            return false;
        };
        match hold {
            PostRevertUpHold::Grace { polls_remaining } => {
                if polls_remaining > 0 {
                    self.post_revert_up_hold = Some(PostRevertUpHold::Grace {
                        polls_remaining: polls_remaining - 1,
                    });
                    return true;
                }
                // Grace finished: anchor the recovery baseline window.
                if recovery_polls == 0 {
                    self.reset_baseline_window(sample);
                    return false;
                }
                self.post_revert_up_hold = Some(PostRevertUpHold::Recovery {
                    polls_remaining: recovery_polls,
                    window_start: sample,
                });
                true
            }
            PostRevertUpHold::Recovery {
                polls_remaining,
                window_start,
            } => {
                let rate = iops_rate(window_start, sample);
                self.current_baseline_iops = rate;
                if polls_remaining > 1 {
                    self.post_revert_up_hold = Some(PostRevertUpHold::Recovery {
                        polls_remaining: polls_remaining - 1,
                        window_start,
                    });
                    return true;
                }
                // Final recovery sample: publish the window and allow scale-up.
                self.baseline_window_start = Some(window_start);
                self.current_baseline_iops = rate;
                false
            }
        }
    }
}

/// Average ops/sec between two cumulative counter samples.
fn iops_rate(start: IoSample, end: IoSample) -> u64 {
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
        let path = dir.join(format!("{ENGINE_NAME}.json"));
        let cfg: ThresholdConfig = config::load_config_or_default(&path)?;
        cfg.validate()?;
        Ok(Self::new(cfg))
    }

    /// Return the engine's effective configuration.
    pub fn config(&self) -> &ThresholdConfig {
        &self.cfg
    }

    /// Return the validation polls for an action's resulting thread count.
    fn validation_sample_polls(&self, target_thread_count: u32) -> u32 {
        self.cfg.scale_validation_sample_polls.saturating_add(
            self.cfg
                .scale_validation_sample_polls_per_thread
                .saturating_mul(target_thread_count),
        )
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

    /// Grace polls for the post-revert hold after `failures` consecutive
    /// failed scale-ups. The backoff extends grace so the recovery baseline
    /// window keeps its configured length.
    fn post_revert_up_grace_polls(&self, failures: u32) -> u32 {
        let recovery = self.cfg.post_revert_up_recovery_polls;
        let hold = backoff_polls(
            self.cfg.post_revert_up_grace_polls.saturating_add(recovery),
            failures.saturating_sub(1),
            self.cfg.post_revert_up_backoff_max_polls,
        );
        hold.saturating_sub(recovery)
    }

    /// Low-utilisation polls required before scale-down after `failures`
    /// consecutive failed scale-downs.
    fn scale_down_sustain_polls(&self, failures: u32) -> u32 {
        backoff_polls(
            self.cfg.scale_down_sustain_polls,
            failures,
            self.cfg.scale_down_backoff_max_polls,
        )
    }

    /// Seconds until a post-scale IOPS sample is evaluated.
    fn validation_after_secs(&self, scale_poll_secs: f64, target_thread_count: u32) -> f64 {
        // Countdown consumes the effective validation polls, then the next
        // tick performs the comparison.
        // TODO use Duration
        (f64::from(self.validation_sample_polls(target_thread_count)) + 1.0) * scale_poll_secs
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
    instance: &EngineInstanceSnapshot,
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
        vm = %instance.id,
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
    instance: &EngineInstanceSnapshot,
    per_thread_util: f64,
    thread_count: u32,
    target: u32,
) {
    tracing::info!(
        target: "engine",
        event = "scale_down",
        vm = %instance.id,
        util = per_thread_util,
        thr = %format!("{thread_count}->{target}"),
        "threshold scaling decision"
    );
}

/// Announce that scale-up is held after a failed-up revert.
fn log_post_revert_up_hold(
    instance_id: &str,
    consecutive_failures: u32,
    grace_polls: u32,
    recovery_polls: u32,
) {
    tracing::info!(
        target: "engine",
        event = "post_revert_up_hold",
        vm = %instance_id,
        consecutive_failures,
        grace_polls,
        recovery_polls,
        "blocking scale-up after failed scale-up revert"
    );
}

/// Announce the sustain requirement for the next scale-down after a
/// failed-down revert.
fn log_scale_down_backoff(instance_id: &str, consecutive_failures: u32, sustain_polls: u32) {
    tracing::info!(
        target: "engine",
        event = "scale_down_backoff",
        vm = %instance_id,
        consecutive_failures,
        sustain_polls,
        "raising scale-down sustain after failed scale-down revert"
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

    async fn evaluate(
        &self,
        instance: &EngineInstanceSnapshot,
        context: &EngineTickContext,
    ) -> ScaleAction {
        if !instance.cpu_available {
            return ScaleAction::None;
        }
        let per_thread_util = instance.per_thread_util;
        let thread_count = instance.thread_count;
        let io_count_total = instance.io_count_total;
        let throttled_recently = instance.throttled_recently;
        let down_target =
            self.scale_down_target(per_thread_util, thread_count, context.min_thread_count);
        let mut state = self.state.lock().await;
        let instance_state = state
            .entry(instance.id.clone())
            .or_insert_with(InstanceState::new);

        let sample = IoSample {
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
            let failed_up = matches!(pending.action, ScaleAction::Up(_));
            let revert_target = self.validation_revert_target(
                &instance.id,
                thread_count,
                observed_iops,
                per_thread_util,
                pending,
            );
            // Whether retained or reverted, the next baseline excludes this
            // validation window. Defer further scaling until the new baseline
            // window has at least one full tick of samples.
            instance_state.reset_baseline_window(sample);
            let Some(target) = revert_target else {
                if failed_up {
                    instance_state.consecutive_failed_ups = 0;
                } else {
                    instance_state.consecutive_failed_downs = 0;
                }
                return ScaleAction::None;
            };
            if failed_up {
                instance_state.consecutive_failed_ups =
                    instance_state.consecutive_failed_ups.saturating_add(1);
                let failures = instance_state.consecutive_failed_ups;
                let grace_polls = self.post_revert_up_grace_polls(failures);
                instance_state.begin_post_revert_up_hold(grace_polls);
                log_post_revert_up_hold(
                    &instance.id,
                    failures,
                    grace_polls,
                    self.cfg.post_revert_up_recovery_polls,
                );
            } else {
                instance_state.consecutive_failed_downs =
                    instance_state.consecutive_failed_downs.saturating_add(1);
                let failures = instance_state.consecutive_failed_downs;
                log_scale_down_backoff(
                    &instance.id,
                    failures,
                    self.scale_down_sustain_polls(failures),
                );
            }
            return ScaleAction::Revert(target);
        }

        let block_scale_up = instance_state
            .advance_post_revert_up_hold(sample, self.cfg.post_revert_up_recovery_polls);
        // Grace ignores metrics; recovery updates the rate inside advance.
        // Once the hold ends (or if there was none), refresh the normal baseline.
        if !block_scale_up {
            instance_state.refresh_baseline_rate(sample);
        }

        if down_target < thread_count {
            instance_state.low_util_polls += 1;
        } else {
            instance_state.low_util_polls = 0;
        }

        if !block_scale_up
            && thread_count < context.max_thread_count
            && per_thread_util > self.cfg.scale_up_threshold
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
                    self.validation_after_secs(context.scale_poll_secs, target),
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
            && instance_state.low_util_polls
                >= self.scale_down_sustain_polls(instance_state.consecutive_failed_downs)
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
        let applied = IoSample {
            io_count: previous_io_count,
            at: applied_at,
        };
        let validate = match action {
            ScaleAction::Up(_) => self.cfg.scale_up_min_gain > 0.0,
            ScaleAction::Down(_) => self.cfg.scale_down_revert_drop > 0.0,
            ScaleAction::None | ScaleAction::Revert(_) => return,
        };
        instance_state.reset_after_applied(action);
        if validate {
            instance_state.start_validation(
                action,
                previous_thread_count,
                instance_state.current_baseline_iops,
                applied,
                self.validation_sample_polls(action.target().unwrap_or(previous_thread_count)),
            );
        } else {
            instance_state.reset_baseline_window(applied);
        }
    }

    async fn on_instance_added(&self, instance_id: &str) {
        let mut state = self.state.lock().await;
        state
            .entry(instance_id.to_string())
            .or_insert_with(InstanceState::new);
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

    use rstest::{fixture, rstest};
    use test_log::test;

    use super::{
        InstanceState, PostRevertUpHold, ThresholdConfig, ThresholdEngine, backoff_polls,
        log_performance_revert,
    };
    use crate::{
        engines::{
            AppliedOutcome, EngineInstanceSnapshot, EngineTickContext, ScaleAction, ScalingEngine,
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

    const VM_ID: &str = "vm-1";

    fn observation(
        thread_count: u32,
        per_thread_util: f64,
        io_count_total: u64,
    ) -> EngineInstanceSnapshot {
        EngineInstanceSnapshot {
            id: VM_ID.to_string(),
            thread_count,
            vcpu_count: 8,
            per_thread_util,
            io_count_total,
            scaling_allowed: true,
            cpu_available: true,
            ..Default::default()
        }
    }

    fn unavailable_observation() -> EngineInstanceSnapshot {
        EngineInstanceSnapshot {
            id: VM_ID.to_string(),
            thread_count: 4,
            vcpu_count: 8,
            per_thread_util: 1.0,
            scaling_allowed: true,
            ..Default::default()
        }
    }

    fn context_at(now: Instant) -> EngineTickContext {
        EngineTickContext {
            now,
            scale_poll_secs: 10.0,
            ..EngineTickContext::default()
        }
    }

    /// Establish a baseline window of `baseline_iops` ops/sec ending at `t1`.
    async fn seed_baseline_rate(
        engine: &ThresholdEngine,
        thread_count: u32,
        baseline_iops: u64,
        t0: Instant,
        t1: Instant,
    ) {
        engine.on_instance_added(VM_ID).await;
        // Park util exactly at the up threshold so neither up nor down fires.
        let steady_util = engine.cfg.scale_up_threshold;
        let first = observation(thread_count, steady_util, 0);
        assert_eq!(
            engine.evaluate(&first, &context_at(t0)).await,
            ScaleAction::None
        );
        let second = observation(thread_count, steady_util, baseline_iops);
        assert_eq!(
            engine.evaluate(&second, &context_at(t1)).await,
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
    async fn test_scale_up_revert_fires_on_iops_rate_drop(engine: ThresholdEngine) {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, 5, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(6),
                    prev_thread_count: 5,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // Validation window: only 50k ops over 1s → 50k IOPS (< 105k required).
        let observed = observation(6, 0.6, 150_000);

        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::Revert(5)
        );
    }

    /// Test that flat post-scale IOPS/rate during validation triggers
    /// revert.
    #[rstest]
    #[tokio::test]
    async fn test_scale_up_flat_rate_reverts(engine: ThresholdEngine) {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // +1% ops over the validation second → still below 5% gain.
        let observed = observation(4, 0.6, 101_000);

        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::Revert(3)
        );
    }

    /// Test that a sufficient post-scale IOPS-rate gain keeps the scale-up.
    #[rstest]
    #[tokio::test]
    async fn test_scale_up_rate_gain_keeps_scale(engine: ThresholdEngine) {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // +10% ops/sec during validation → above 5% min gain.
        let observed = observation(4, 0.6, 210_000);

        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::None
        );
    }

    /// Test that a failed scale-up enters grace then recovery: scale-up is
    /// blocked while a settled baseline is measured, then the next up uses
    /// that recovery-window IOPS (not a depressed post-revert blip).
    #[tokio::test]
    async fn test_failed_scale_up_grace_then_recovery_baseline() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 0,
            post_revert_up_grace_polls: 1,
            post_revert_up_recovery_polls: 2,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        let observed = observation(4, 0.95, 101_000);
        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::Revert(3)
        );

        // Grace: util blip / depressed IOPS must not produce a scale-up.
        let observed = observation(3, 0.4, 150_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_validate + Duration::from_secs(1)))
                .await,
            ScaleAction::None
        );

        // Recovery tick 1: enter recovery window (anchor), still blocked.
        let observed = observation(3, 0.95, 150_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_validate + Duration::from_secs(2)))
                .await,
            ScaleAction::None
        );

        // Recovery tick 2: measure ~100k IOPS over 1s, still blocked.
        let observed = observation(3, 0.95, 250_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_validate + Duration::from_secs(3)))
                .await,
            ScaleAction::None
        );

        // Hold complete: scale-up allowed; baseline is the recovery-window rate
        // (~100k), so min_target reflects that — not the grace-period blip.
        let observed = observation(3, 0.95, 350_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_validate + Duration::from_secs(4)))
                .await,
            ScaleAction::Up(4)
        );
        {
            let state = engine.state.lock().await;
            let instance_state = state.get(VM_ID).unwrap();
            // Recovery anchored at 150k @ +2s through 350k @ +4s → 100k IOPS.
            assert_eq!(instance_state.current_baseline_iops, 100_000);
            assert!(instance_state.post_revert_up_hold.is_none());
        }
    }

    /// Test that scale-down remains allowed during the post-revert up hold.
    #[tokio::test]
    async fn test_scale_down_allowed_during_post_revert_up_hold() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 0,
            scale_down_sustain_polls: 1,
            post_revert_up_grace_polls: 2,
            post_revert_up_recovery_polls: 2,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;
        let t_validate = t1 + Duration::from_secs(1);

        seed_baseline_rate(&engine, 3, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(4),
                    prev_thread_count: 3,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        let observed = observation(4, 0.95, 101_000);
        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::Revert(3)
        );

        // During grace, low util can still scale down.
        let observed = observation(3, 0.0, 101_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_validate + Duration::from_secs(1)))
                .await,
            ScaleAction::Down(2)
        );
    }
    /// Test that scale-down validation does not revert when util stays low
    /// (demand left; IOPS collapse is expected).
    #[tokio::test]
    async fn test_scale_down_keeps_shrink_when_util_stays_low() {
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

        seed_baseline_rate(&engine, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Down(1),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // IOPS collapsed, but util remains below the up threshold.
        let observed = observation(1, 0.0, 100_001);
        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::None
        );
    }

    /// Test that scale-down validation reverts when util is still high (demand
    /// remains) but IOPS fell beyond the tolerated drop.
    #[tokio::test]
    async fn test_scale_down_reverts_when_util_stays_high_and_iops_drop() {
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

        seed_baseline_rate(&engine, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Down(2),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        // Demand still present (util above up threshold) but IOPS collapsed.
        let observed = observation(2, 0.95, 110_000);
        assert_eq!(
            engine.evaluate(&observed, &context_at(t_validate)).await,
            ScaleAction::Revert(4)
        );
    }

    /// Test that backoff doubles from the base, caps at the maximum, never
    /// drops below the base, and is disabled by a zero maximum.
    #[test]
    fn test_backoff_polls_doubles_and_caps() {
        let got: Vec<u32> = (0..6).map(|n| backoff_polls(3, n, 30)).collect();
        assert_eq!(got, vec![3, 6, 12, 24, 30, 30]);
        assert_eq!(backoff_polls(3, 100, 30), 30);
        assert_eq!(backoff_polls(3, 4, 0), 3);
        assert_eq!(backoff_polls(12, 2, 5), 12);
    }

    /// Apply a scale action at `at` against a 100k IOPS baseline and validate
    /// it one second later with `io_delta` completed operations.
    async fn apply_and_validate(
        engine: &ThresholdEngine,
        action: ScaleAction,
        prev_thread_count: u32,
        io_delta: u64,
        at: Instant,
    ) -> ScaleAction {
        engine
            .state
            .lock()
            .await
            .get_mut(VM_ID)
            .unwrap()
            .current_baseline_iops = 100_000;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action,
                    prev_thread_count,
                    prev_io_count_total: 0,
                    applied_at: at,
                },
            )
            .await;
        let target = action.target().unwrap();
        let observed = observation(target, 0.95, io_delta);
        engine
            .evaluate(&observed, &context_at(at + Duration::from_secs(1)))
            .await
    }

    /// Test that each consecutive failed scale-up doubles the post-revert
    /// hold up to the cap, extending grace while recovery stays fixed, and
    /// that a kept scale-up resets the backoff.
    #[tokio::test]
    async fn test_failed_scale_ups_back_off_until_kept() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 0,
            post_revert_up_grace_polls: 1,
            post_revert_up_recovery_polls: 2,
            post_revert_up_backoff_max_polls: 12,
            ..Default::default()
        });
        engine.on_instance_added(VM_ID).await;
        let start = Instant::now();

        // Holds of 3, 6, 12, 12 polls minus the 2 recovery polls.
        for (i, expected_grace) in [1, 4, 10, 10].into_iter().enumerate() {
            let at = start + Duration::from_secs(10 * i as u64);
            assert_eq!(
                apply_and_validate(&engine, ScaleAction::Up(4), 3, 1_000, at).await,
                ScaleAction::Revert(3)
            );
            let state = engine.state.lock().await;
            let instance_state = state.get(VM_ID).unwrap();
            assert_eq!(instance_state.consecutive_failed_ups, i as u32 + 1);
            assert!(matches!(
                instance_state.post_revert_up_hold,
                Some(PostRevertUpHold::Grace { polls_remaining }) if polls_remaining == expected_grace
            ));
        }

        let at = start + Duration::from_secs(100);
        assert_eq!(
            apply_and_validate(&engine, ScaleAction::Up(4), 3, 200_000, at).await,
            ScaleAction::None
        );
        let state = engine.state.lock().await;
        assert_eq!(state.get(VM_ID).unwrap().consecutive_failed_ups, 0);
        drop(state);
        assert_eq!(engine.post_revert_up_grace_polls(1), 1);
    }

    /// Test that each consecutive failed scale-down doubles the sustain
    /// requirement up to the cap, and that an applied scale-up resets it.
    #[tokio::test]
    async fn test_failed_scale_downs_back_off_until_scale_up() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_down_revert_drop: 0.05,
            scale_validation_sample_polls: 0,
            scale_down_sustain_polls: 3,
            scale_down_backoff_max_polls: 20,
            ..Default::default()
        });
        engine.on_instance_added(VM_ID).await;
        let start = Instant::now();

        for (i, expected_sustain) in [6, 12, 20].into_iter().enumerate() {
            let at = start + Duration::from_secs(10 * i as u64);
            assert_eq!(
                apply_and_validate(&engine, ScaleAction::Down(2), 4, 1_000, at).await,
                ScaleAction::Revert(4)
            );
            let failures = engine
                .state
                .lock()
                .await
                .get(VM_ID)
                .unwrap()
                .consecutive_failed_downs;
            assert_eq!(failures, i as u32 + 1);
            assert_eq!(engine.scale_down_sustain_polls(failures), expected_sustain);
        }

        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(5),
                    prev_thread_count: 4,
                    prev_io_count_total: 0,
                    applied_at: start + Duration::from_secs(100),
                },
            )
            .await;
        let state = engine.state.lock().await;
        assert_eq!(state.get(VM_ID).unwrap().consecutive_failed_downs, 0);
    }

    /// Test that an applied action resets only the evidence for its own
    /// direction, and that non-scaling actions reset nothing.
    #[rstest]
    #[case::up(ScaleAction::Up(4), 0, 2, 0, true)]
    #[case::down(ScaleAction::Down(2), 0, 0, 3, false)]
    #[case::none(ScaleAction::None, 5, 2, 3, true)]
    #[case::revert(ScaleAction::Revert(3), 5, 2, 3, true)]
    #[tokio::test]
    async fn test_reset_after_applied(
        #[case] action: ScaleAction,
        #[case] low_util_polls: u32,
        #[case] consecutive_failed_ups: u32,
        #[case] consecutive_failed_downs: u32,
        #[case] post_revert_up_hold: bool,
    ) {
        let mut instance_state = InstanceState::new();
        instance_state.low_util_polls = 5;
        instance_state.consecutive_failed_ups = 2;
        instance_state.consecutive_failed_downs = 3;
        instance_state.begin_post_revert_up_hold(1);

        instance_state.reset_after_applied(action);

        assert_eq!(instance_state.low_util_polls, low_util_polls);
        assert_eq!(
            instance_state.consecutive_failed_ups,
            consecutive_failed_ups
        );
        assert_eq!(
            instance_state.consecutive_failed_downs,
            consecutive_failed_downs
        );
        assert_eq!(
            instance_state.post_revert_up_hold.is_some(),
            post_revert_up_hold
        );
    }

    /// Test that percent fields serde as human percents on the wire and
    /// fractions in memory.
    #[test]
    fn test_percent_wire_format_round_trips() {
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

    #[test]
    fn test_validation_polls_increase_with_target_thread_count() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_validation_sample_polls: 2,
            scale_validation_sample_polls_per_thread: 3,
            ..Default::default()
        });

        assert_eq!(engine.validation_sample_polls(1), 5);
        assert_eq!(engine.validation_sample_polls(4), 14);
    }

    /// Test that while a prior scale-up is pending validation, further
    /// scale-ups are suppressed.
    #[rstest]
    #[test(tokio::test)]
    async fn test_no_scale_up_during_pending_validation(engine: ThresholdEngine) {
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;

        seed_baseline_rate(&engine, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(5),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;

        let observed = observation(5, 0.88, 220_000);

        for i in 0..engine.cfg.scale_validation_sample_polls {
            assert_eq!(
                engine
                    .evaluate(
                        &observed,
                        &context_at(t_apply + Duration::from_secs(u64::from(i) + 1))
                    )
                    .await,
                ScaleAction::None
            );
        }

        // Validation tick settles the scale-up without proposing another action.
        let t_settle = t_apply + Duration::from_secs(1);
        assert_eq!(
            engine.evaluate(&observed, &context_at(t_settle)).await,
            ScaleAction::None
        );

        // Next tick with sustained high util may scale again.
        let observed = observation(5, 0.88, 330_000);
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_settle + Duration::from_secs(1)))
                .await,
            ScaleAction::Up(6)
        );
    }

    /// Test that a scale up is revert after the validation period if
    /// performance doesn't increase much.
    #[test(tokio::test)]
    async fn test_pending_validation_reverts_regressive_scale() {
        let engine = ThresholdEngine::new(ThresholdConfig {
            scale_up_threshold: 0.8,
            scale_up_min_gain: 0.05,
            scale_validation_sample_polls: 2,
            ..Default::default()
        });
        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(1);
        let t_apply = t1;

        seed_baseline_rate(&engine, 4, 100_000, t0, t1).await;
        engine
            .on_applied(
                VM_ID,
                AppliedOutcome::Success {
                    action: ScaleAction::Up(5),
                    prev_thread_count: 4,
                    prev_io_count_total: 100_000,
                    applied_at: t_apply,
                },
            )
            .await;
        let observed = observation(5, 0.88, 140_000);

        for i in 0..engine.cfg.scale_validation_sample_polls {
            assert_eq!(
                engine
                    .evaluate(
                        &observed,
                        &context_at(t_apply + Duration::from_secs(u64::from(i) + 1))
                    )
                    .await,
                ScaleAction::None
            );
        }
        // 40k ops over ~3s ≈ 13k IOPS ≪ 105k required.
        assert_eq!(
            engine
                .evaluate(&observed, &context_at(t_apply + Duration::from_secs(3)))
                .await,
            ScaleAction::Revert(4)
        );
    }

    /// Test that scale-up decision logs current/min IOPS and validation delay.
    #[rstest]
    #[tokio::test]
    async fn test_scale_up_log_includes_iops_targets(engine: ThresholdEngine) {
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
        seed_baseline_rate(&engine, 2, 100_000, t0, t1).await;
        let observed = observation(2, 0.9, 100_000);

        assert_eq!(
            engine.evaluate(&observed, &context_at(t1)).await,
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
    fn test_missing_config_file_uses_defaults() {
        let dir = tempfile::tempdir().unwrap();
        let engine =
            ThresholdEngine::from_config_dir(&Path::new(dir.path().to_str().unwrap())).unwrap();
        assert_eq!(engine.name(), super::ENGINE_NAME);
        let _ = engine.config();
    }

    /// Test that CPU utilisation below scale-up/down thresholds yields
    /// Hold/`None`.
    #[tokio::test]
    async fn test_evaluate_holds_when_util_below_thresholds() {
        let engine = ThresholdEngine::new(ThresholdConfig::default());
        let observed = observation(4, 0.75, 0);
        let action = engine
            .evaluate(&observed, &context_at(Instant::now()))
            .await;
        assert_eq!(action, ScaleAction::None);
    }

    #[tokio::test]
    async fn test_evaluate_holds_until_cpu_is_available() {
        let engine = ThresholdEngine::new(ThresholdConfig::default());
        let action = engine
            .evaluate(&unavailable_observation(), &context_at(Instant::now()))
            .await;
        assert_eq!(action, ScaleAction::None);
    }
}
