// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Pluggable fleet scaling decisions.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use futures_util::future::join_all;
use linkme::distributed_slice;
use thiserror::Error;

use crate::{backends::BackendClientError, config::ConfigError, instance::Instance, util::Path};

#[cfg(feature = "threshold-engine")]
pub mod threshold;

/// One in-tree (or out-of-tree) engine factory registered on [`ENGINES`].
pub struct EngineRegistration {
    /// Stable name matched against `Config::engine`.
    pub name: &'static str,
    /// Build the engine from `<engine_config_dir>`.
    pub build: fn(&Path) -> Result<Box<dyn ScalingEngine>, EngineError>,
}

/// Linked-in engine factories. Feature-gated modules append themselves here.
#[distributed_slice]
pub static ENGINES: [EngineRegistration] = [..];

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Engine(#[from] ConfigError),

    #[error("no engine: {0}")]
    NoSuchEngine(String),
}

/// Scaling operation selected by an engine.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ScaleAction {
    /// Keep the current worker count.
    #[default]
    None,
    /// Grow to the carried worker count.
    Up(u32),
    /// Shrink to the carried worker count.
    Down(u32),
    /// Restore the carried worker count after failed validation.
    Revert(u32),
}

impl ScaleAction {
    /// Return the requested worker count, or `None` for a hold.
    pub fn target(self) -> Option<u32> {
        match self {
            Self::None => None,
            Self::Up(target) | Self::Down(target) | Self::Revert(target) => Some(target),
        }
    }
}

impl std::fmt::Display for ScaleAction {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::Up(_) => "up",
            Self::Down(_) => "down",
            Self::Revert(_) => "revert",
        };
        formatter.write_str(s)
    }
}

/// Host-level context shared by every evaluation in one tick.
#[derive(Debug, Clone, Copy)]
pub struct EngineTickContext {
    /// When the tick started.
    pub now: Instant,
    // FIXME why do we redefine min_thread_count, max_thread_count, host_cpu_util, they already
    // exist in Config
    /// Controller-wide minimum worker count.
    pub min_thread_count: u32,
    /// Controller-wide maximum worker count.
    pub max_thread_count: u32,
    /// Host-wide CPU utilisation (0.0-1.0) from successive `/proc/stat`
    /// samples; zero until two samples exist.
    pub host_cpu_util: f64,
    /// Monotonically increasing tick sequence number.
    pub tick_index: u64,
}

/// One VM's entry in a fleet-wide plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceDecision {
    /// Stable VM identifier.
    pub instance_id: String,
    /// Verdict for this VM.
    pub decision: ScaleAction,
}

impl InstanceDecision {
    /// Construct one plan entry.
    pub fn new(instance_id: impl Into<String>, decision: ScaleAction) -> Self {
        Self {
            instance_id: instance_id.into(),
            decision,
        }
    }
}

/// Controller guard that rejected an otherwise valid scale action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlockedReason {
    /// A process-local manual scaling command owns the pool for now.
    #[error("manual override is sticky")]
    ManualOverride,
    /// Backend policy classified the initial pool as operator-managed.
    #[error("VM is not managed by the controller")]
    UnmanagedVm,
    /// The requested pool would be below the configured minimum.
    #[error("target is below controller minimum")]
    TargetBelowMinimum,
    /// The requested pool would exceed the configured maximum.
    #[error("target exceeds controller maximum")]
    TargetExceedsMaximum,
    /// The requested pool would exceed the guest's vCPU count.
    #[error("target exceeds vCPU count")]
    TargetExceedsVcpuCount,
    /// Host CPU usage leaves no configured headroom for ordinary scale-up.
    #[error("host CPU is at or above scale-up ceiling")]
    HostCpuCeiling,
    /// A previous ordinary scale action is still cooling down.
    #[error("VM is in post-action cooldown")]
    Cooldown,
}

/// Result reported to an engine after actuation.
#[derive(Debug)]
pub enum AppliedOutcome {
    /// The backend accepted the action.
    Success {
        /// Applied action and target.
        action: ScaleAction,
        /// Count before actuation.
        prev_thread_count: u32,
        /// Cumulative backend I/O ops before actuation.
        prev_io_count_total: u64,
    },
    /// Actuation was intentionally skipped.
    DryRun {
        /// Proposed action and target.
        action: ScaleAction,
        /// Count before the proposal.
        prev_thread_count: u32,
        /// Cumulative backend I/O ops before the proposal.
        prev_io_count_total: u64,
    },
    /// A controller guard deliberately prevented backend actuation.
    Blocked {
        /// Proposed action and target.
        action: ScaleAction,
        /// Controller policy that prevented the operation.
        reason: BlockedReason,
    },
    /// The controller attempted actuation, but the backend returned an error.
    Failed {
        /// Rejected action and target.
        action: ScaleAction,
        /// Error returned by the backend.
        error: BackendClientError,
    },
}

/// Scaling algorithm called after every VM has been refreshed.
#[async_trait]
pub trait ScalingEngine: Send + Sync {
    /// Stable engine name used by configuration.
    fn name(&self) -> &'static str;

    /// Return the effective engine-specific configuration for diagnostics.
    fn dump_config(&self) -> serde_json::Value;

    /// Evaluate one VM.
    ///
    /// The controller needs to pass `Instances` to the engine for it to
    /// evaluate and decide.
    async fn evaluate(&self, instance: &Arc<Instance>, context: &EngineTickContext) -> ScaleAction;

    /// Evaluate eligible VMs concurrently and collect one coherent plan.
    async fn evaluate_fleet(
        &self,
        instances: &[Arc<Instance>],
        context: &EngineTickContext,
    ) -> Vec<InstanceDecision> {
        // FIXME concurrent evaluation might not be a good idea, an engine might want
        // to first look at all the instances first and then make decisions
        join_all(instances.iter().map(|instance| async move {
            let status = instance.status.read().await;
            if status.manual_scaling_sticky || !status.scaling_allowed {
                return None;
            }
            Some(InstanceDecision::new(
                &instance.id,
                self.evaluate(instance, context).await,
            ))
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }

    /// Report what happened to a proposed action.
    ///
    /// Stateful engines need this feedback because a decision alone does not
    /// prove that applying it will necessarily result in the VM's thread count
    /// changing as instructed. [`AppliedOutcome::Blocked`] distinguishes a
    /// controller policy guard from [`AppliedOutcome::Failed`], where backend
    /// actuation was attempted and returned an error. Dry runs report the
    /// hypothetical action without claiming that the backend changed.
    async fn on_applied(&self, _instance_id: &str, _outcome: AppliedOutcome) {}

    /// Notify the engine when a VM is added.
    async fn on_instance_added(&self, _instance: &Arc<Instance>) {}

    /// Notify the engine when a VM is removed.
    async fn on_instance_removed(&self, _instance_id: &str) {}
}

/// Load the selected built-in engine by name.
pub fn load_registered_engine(
    engine_config_dir: &Path,
    name: &str,
) -> Result<Box<dyn ScalingEngine>, EngineError> {
    for registration in ENGINES {
        if registration.name == name {
            return (registration.build)(engine_config_dir);
        }
    }
    Err(EngineError::NoSuchEngine(name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that registry loads the threshold engine by name and errors
    /// on unknown engines.
    #[test]
    fn load_registered_engine_finds_threshold_and_rejects_unknown() {
        let dir = Path::new("/tmp/missing-engines");
        let engine = load_registered_engine(&dir, "threshold").unwrap();
        assert_eq!(engine.name(), "threshold");

        match load_registered_engine(&dir, "no-such-engine") {
            Err(EngineError::NoSuchEngine(name)) => assert_eq!(name, "no-such-engine"),
            Ok(_) => panic!("expected NoSuchEngine"),
            Err(other) => panic!("expected NoSuchEngine, got {other}"),
        }
    }
}
