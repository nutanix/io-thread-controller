// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Engine-agnostic fleet inventory, sampling, and actuation.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::future::join_all;
use procfs::{CurrentSI, ProcError};
use serde::{Deserialize, Serialize};
use statistical::median;
use thiserror::Error;

use crate::{
    backends::{BackendClientError, IoThreadProperties, VqMapping},
    config::Config,
    dbus::DbusRequest,
    engines::{
        AppliedOutcome, BlockedReason, EngineTickContext, PsiSample, ScaleAction, ScalingEngine,
    },
    instance::{CgroupError, Instance, InstanceStatus},
    rolling::format_1_5_15,
    state::{StateError, VmOwnership, VmStateStore},
};
/// Wire-facing container for the D-Bus `GetSnapshot` reply.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotPayload {
    /// One entry per tracked instance.
    pub vms: Vec<SnapshotVm>,
}

/// One tracked instance's freshest state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotVm {
    /// Instance / vm identifier.
    pub id: String,
    /// Guest vCPU count reported by the backend.
    pub vcpu_count: u32,
    /// Backend worker PID.
    pub pid: i32,
    /// Current effective thread-pool size.
    pub thread_count: u32,
    /// Whether the most recent refresh succeeded.
    pub alive: bool,
    /// Whether the backend supplied trustworthy performance data.
    pub perf_available: bool,
    /// Average utilisation per worker, as a 0..1 fraction.
    pub per_thread_util: f64,
    /// Whether a sticky manual override suppresses automatic scaling.
    pub manual_scaling_sticky: bool,
    /// Per-tick read operation rate.
    pub read_iops: u64,
    /// Per-tick write operation rate.
    pub write_iops: u64,
    /// Per-tick non-read/write operation rate.
    pub other_iops: u64,
    /// Per-tick read bandwidth in bytes per second.
    pub read_bw_bps: u64,
    /// Per-tick write bandwidth in bytes per second.
    pub write_bw_bps: u64,
    /// Read-latency histogram digest, when available.
    #[serde(default)]
    pub read_latency_us: Option<SnapshotLatency>,
    /// Write-latency histogram digest, when available.
    #[serde(default)]
    pub write_latency_us: Option<SnapshotLatency>,
    /// Number of queues represented by `per_vq_depth`.
    #[serde(default)]
    pub num_queues: Option<u32>,
    /// One in-flight depth reading per virtqueue.
    #[serde(default)]
    pub per_vq_depth: Option<Vec<u64>>,
    /// Aggregate virtqueue depth.
    #[serde(default)]
    pub qd_total: Option<u64>,
    /// Average virtqueue depth multiplied by 100.
    #[serde(default)]
    pub qd_avg_x100: Option<u64>,
    /// Median virtqueue depth.
    #[serde(default)]
    pub qd_median: Option<u64>,
    /// Mean per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_avg: Option<u64>,
    /// Median per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_median: Option<u64>,
    /// Sum of per-worker CPU utilisation, in percent.
    #[serde(default)]
    pub cpu_pct_total: Option<u64>,
}

/// Serialised latency histogram digest.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotLatency {
    /// Median latency in microseconds.
    pub p50: u64,
    /// 95th-percentile latency in microseconds.
    pub p95: u64,
    /// 99th-percentile latency in microseconds.
    pub p99: u64,
    /// Histogram-derived arithmetic mean in microseconds.
    pub avg: u64,
}

/// Average, median, and aggregate utilisation across sampled workers.
#[derive(Debug, Clone, Copy)]
struct CpuStats {
    /// Mean per-worker utilisation in percent.
    avg_pct: u64,
    /// Median per-worker utilisation in percent.
    median_pct: u64,
    /// Sum of per-worker utilisation percentages.
    total_pct: u64,
}

/// Summarise per-worker utilisation, falling back to aggregate samples.
fn compute_cpu_stats(s: &crate::instance::InstanceStatus) -> CpuStats {
    if !s.per_worker_util.is_empty() {
        let total = s.per_worker_util.iter().sum::<f64>();
        let median = median(&s.per_worker_util);
        return CpuStats {
            avg_pct: (total / s.per_worker_util.len() as f64 * 100.0).round() as u64,
            median_pct: (median * 100.0).round() as u64,
            total_pct: (total * 100.0).round() as u64,
        };
    }

    CpuStats {
        avg_pct: (s.per_thread_util * 100.0).round() as u64,
        median_pct: (s.per_thread_util * 100.0).round() as u64,
        total_pct: (s.per_thread_util * s.thread_count as f64 * 100.0).round() as u64,
    }
}

#[derive(Debug, Error)]
pub enum ControllerError {
    #[error(transparent)]
    BackendClient(#[from] BackendClientError),

    #[error(transparent)]
    Cgroup(#[from] CgroupError),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Proc(#[from] ProcError),

    #[error(transparent)]
    State(#[from] StateError),

    #[error("Set thread count error: {0}")]
    ThreadCountError(String),

    #[error("VM error: {0}")]
    VmError(String),
}

/// Construct a [`Duration`] from whole minutes.
const fn from_mins(minutes: u64) -> Duration {
    Duration::from_secs(minutes * 60)
}

/// Rolling windows displayed in uptime-style status fields.
const STATUS_WINDOWS: [Duration; 3] = [from_mins(1), from_mins(5), from_mins(15)];

/// Host-wide cumulative CPU counters from `/proc/stat`.
#[derive(Debug, Default, Clone, Copy)]
struct HostCpuSample {
    busy_ticks: u64,
    total_ticks: u64,
}

/// Live VM inventory and per-tick driver.
pub struct Controller {
    /// Effective daemon configuration.
    pub cfg: Config,
    /// Exclusively owned engine borrowed by concurrent evaluation futures.
    ///
    /// The controller does not need shared ownership: fleet evaluation lends
    /// `&self.engine` to concurrent calls, which is safe because
    /// [`ScalingEngine`] requires both [`Send`] and [`Sync`].
    pub engine: Box<dyn ScalingEngine>,
    /// Atomic ownership-registry store.
    pub vm_state_store: VmStateStore,
    /// Restart-surviving managed and unmanaged VM sets, loaded once.
    pub vm_ownership: VmOwnership,
    /// Tracked VMs keyed by stable identifier.
    ///
    /// The controller needs to pass Instances to the engine for it to evaluate
    /// and decide.
    // FIXME Instance probably doesn't need to be an Arc.
    pub instances: HashMap<String, Arc<Instance>>,
    /// Monotonically increasing tick sequence.
    pub tick_index: u64,
    /// Previous host CPU sample.
    last_host_cpu: Option<HostCpuSample>,
    /// Fraction of host CPU consumed since the previous tick.
    host_cpu_util: f64,
    /// Latest host pressure-stall snapshot.
    psi: PsiSample,
}

impl Controller {
    /// Construct an empty controller after loading VM ownership once.
    pub fn new(cfg: Config, engine: Box<dyn ScalingEngine>) -> Result<Self, ControllerError> {
        let vm_state_store = VmStateStore::new(cfg.vm_state_path.clone());
        let vm_ownership = vm_state_store.load()?;
        Ok(Self {
            cfg,
            engine,
            vm_state_store,
            vm_ownership,
            instances: HashMap::new(),
            tick_index: 0,
            last_host_cpu: None,
            host_cpu_util: 0.0,
            psi: PsiSample::default(),
        })
    }

    /// Reconcile a complete discovery result with the live inventory. Returns
    /// the number added and removed instances.
    pub async fn sync_instances(
        &mut self,
        discovered: Vec<Arc<Instance>>,
    ) -> Result<(usize, usize), ControllerError> {
        let seen: HashSet<_> = discovered
            .iter()
            .map(|instance| instance.id.clone())
            .collect();
        let mut added = 0;

        for instance in discovered {
            if self.instances.contains_key(&instance.id) {
                instance.client.close().await;
                continue;
            }
            let classification = self.vm_ownership.classification(&instance.id);
            let mut status = instance.status.write().await;
            status.ownership_classification = classification;
            drop(status);
            self.engine.on_instance_added(&instance).await;
            self.instances.insert(instance.id.clone(), instance);
            added += 1;
        }

        let stale: Vec<_> = self
            .instances
            .keys()
            .filter(|id| !seen.contains(*id))
            .cloned()
            .collect();
        let removed = stale.len();
        for id in stale {
            if let Some(instance) = self.instances.remove(&id) {
                instance.client.close().await;
                self.engine.on_instance_removed(&id).await;
            }
        }
        Ok((added, removed))
    }

    /// Serve one D-Bus request in-line with the tick loop.
    pub async fn handle_dbus_request(&self, request: DbusRequest) {
        match request {
            DbusRequest::SetThreadCount {
                vm,
                threads,
                sticky,
                reply,
            } => {
                let result = self
                    .handle_set_thread_count(&vm, threads, sticky)
                    .await
                    .map_err(|error| error.to_string());
                let _ = reply.send(result);
            }
            DbusRequest::SetAllThreadCounts {
                threads,
                sticky,
                reply,
            } => {
                let result = self.handle_set_all_thread_counts(threads, sticky).await;
                let _ = reply.send(result);
            }
            DbusRequest::GetSnapshot { reply } => {
                let _ = reply.send(self.handle_get_snapshot().await);
            }
            DbusRequest::GetVersion { reply } => {
                let _ = reply.send(env!("CARGO_PKG_VERSION").to_string());
            }
            DbusRequest::GetIoThreadVqMapping { vm, device, reply } => {
                let result = match self.instances.get(&vm) {
                    Some(instance) => instance
                        .client
                        .get_io_thread_vq_mapping(&device)
                        .await
                        .map_err(|error| error.to_string())
                        .and_then(|mapping| {
                            serde_json::to_string(&mapping)
                                .map_err(|error| format!("serialize mapping: {error}"))
                        }),
                    None => Err(format!("unknown VM {vm}")),
                };
                let _ = reply.send(result);
            }
            DbusRequest::GetStats { reply } => {
                let mut vms = Vec::with_capacity(self.instances.len());
                for (id, instance) in &self.instances {
                    let status = instance.status.read().await;
                    let (read_io_count, write_io_count, other_io_count) = match &status.perf {
                        Some(perf) => {
                            (perf.read_io_count, perf.write_io_count, perf.other_io_count)
                        }
                        None => (0, 0, 0),
                    };
                    vms.push(serde_json::json!({
                        "vm": id,
                        "thread_count": status.thread_count,
                        "manual_scaling_sticky": status.manual_scaling_sticky,
                        "scaling_allowed": status.scaling_allowed,
                        "vcpu_count": status.vcpu_count,
                        "per_thread_util": status.per_thread_util,
                        // FIXME omit if status.perf.is_none()?
                        "read_io_count": read_io_count,
                        "write_io_count": write_io_count,
                        "other_io_count": other_io_count,
                    }));
                }
                let snapshot = serde_json::json!({
                    "tick": self.tick_index,
                    "vms": vms,
                })
                .to_string();
                let _ = reply.send(snapshot);
            }
            DbusRequest::AddIoThread {
                vm,
                id,
                poll_max_ns,
                reply,
            } => {
                let result = match self.instances.get(&vm) {
                    Some(instance) => {
                        let properties = (poll_max_ns >= 0).then(|| IoThreadProperties {
                            poll_max_ns: Some(poll_max_ns),
                            ..Default::default()
                        });
                        instance
                            .client
                            .add_io_thread(&id, properties.as_ref())
                            .await
                            .map_err(|error| error.to_string())
                    }
                    None => Err(format!("unknown VM {vm}")),
                };
                let _ = reply.send(result);
            }
            DbusRequest::DelIoThread { vm, id, reply } => {
                let result = match self.instances.get(&vm) {
                    Some(instance) => instance
                        .client
                        .del_io_thread(&id)
                        .await
                        .map_err(|error| error.to_string()),
                    None => Err(format!("unknown VM {vm}")),
                };
                let _ = reply.send(result);
            }
            DbusRequest::SetIoThreadVqMapping {
                vm,
                device,
                mapping_json,
                reply,
            } => {
                let result = match serde_json::from_str::<Vec<VqMapping>>(&mapping_json) {
                    Ok(mapping) => match self.instances.get(&vm) {
                        Some(instance) => instance
                            .client
                            .set_io_thread_vq_mapping(&device, &mapping)
                            .await
                            .map_err(|error| error.to_string()),
                        None => Err(format!("unknown VM {vm}")),
                    },
                    Err(error) => Err(format!("parse mapping JSON: {error}")),
                };
                let _ = reply.send(result);
            }
        }
    }

    /// Apply a debug-only manual thread-count request.
    async fn handle_set_thread_count(
        &self,
        vm: &str,
        threads: u32,
        sticky: bool,
    ) -> Result<(), ControllerError> {
        let instance = self
            .instances
            .get(vm)
            .cloned()
            .ok_or_else(|| ControllerError::VmError("unknown VM {vm}".to_string()))?;
        let vcpu_count = instance.status.read().await.vcpu_count;
        if threads == 0 {
            return Err(ControllerError::ThreadCountError(
                "target thread count must be greater than zero".to_string(),
            ));
        }
        if threads > vcpu_count {
            return Err(ControllerError::ThreadCountError(
                "target thread count {threads} > guest vCPU count {vcpu_count}".to_string(),
            ));
        }

        instance.client.set_thread_count(threads).await?;
        let mut status = instance.status.write().await;
        status.thread_count = threads;
        status.manual_scaling_sticky = sticky;
        Ok(())
    }

    /// Apply a debug-only manual count to every VM known to the daemon.
    ///
    /// The daemon inventory, rather than the consumer's filtered view, defines
    /// the target set. Every VM is attempted in stable identifier order so
    /// one rejection does not hide successful updates to other VMs.
    async fn handle_set_all_thread_counts(&self, threads: u32, sticky: bool) -> (u32, String) {
        let mut ids: Vec<_> = self.instances.keys().cloned().collect();
        ids.sort_unstable();
        let attempted = ids.len();
        let mut succeeded = 0usize;
        let mut failures = Vec::new();
        for id in ids {
            match self.handle_set_thread_count(&id, threads, sticky).await {
                Ok(()) => succeeded += 1,
                Err(error) => failures.push(format!("{id}: {error}")),
            }
        }

        let failed = failures.len();
        let mut summary = format!(
            "set thread count to {threads}{} on {succeeded}/{attempted} VMs",
            if sticky { " (sticky)" } else { "" }
        );
        if !failures.is_empty() {
            summary.push_str("; failures: ");
            summary.push_str(&failures.join("; "));
        }
        (u32::try_from(failed).unwrap_or(u32::MAX), summary)
    }

    /// Assemble the machine-readable fleet snapshot.
    async fn handle_get_snapshot(&self) -> String {
        let mut payload = SnapshotPayload {
            vms: Vec::with_capacity(self.instances.len()),
        };
        let ids: Vec<_> = self.instances.keys().collect();
        for id in ids {
            let instance = &self.instances[id];
            let s = instance.status.read().await;
            let cpu_stats = compute_cpu_stats(&s);
            payload.vms.push(SnapshotVm {
                id: id.clone(),
                vcpu_count: s.vcpu_count,
                pid: instance.pid,
                thread_count: s.thread_count,
                alive: s.alive,
                perf_available: s.perf.is_some(),
                // Guard JSON serialization: NaN/inf are not valid JSON numbers.
                per_thread_util: if s.per_thread_util.is_finite() {
                    s.per_thread_util
                } else {
                    0.0
                },
                manual_scaling_sticky: s.manual_scaling_sticky,
                read_iops: s.read_iops,
                write_iops: s.write_iops,
                other_iops: s.other_iops,
                read_bw_bps: s.read_bytes_per_second,
                write_bw_bps: s.write_bytes_per_second,
                read_latency_us: s.read_latency_us.map(|latency| SnapshotLatency {
                    p50: latency.p50,
                    p95: latency.p95,
                    p99: latency.p99,
                    avg: latency.avg,
                }),
                write_latency_us: s.write_latency_us.map(|latency| SnapshotLatency {
                    p50: latency.p50,
                    p95: latency.p95,
                    p99: latency.p99,
                    avg: latency.avg,
                }),
                num_queues: None,
                per_vq_depth: None,
                qd_total: None,
                qd_avg_x100: None,
                qd_median: None,
                cpu_pct_avg: Some(cpu_stats.avg_pct),
                cpu_pct_median: Some(cpu_stats.median_pct),
                cpu_pct_total: Some(cpu_stats.total_pct),
            });
        }
        serde_json::to_string(&payload).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Refresh the fleet, evaluate one coherent plan, and apply eligible work.
    pub async fn tick(&mut self) -> Result<(), ControllerError> {
        self.tick_index = self.tick_index.wrapping_add(1);
        if let Ok(current) = read_host_cpu_sample() {
            if let Some(previous) = self.last_host_cpu {
                self.host_cpu_util = host_cpu_utilisation(previous, current);
            }
            self.last_host_cpu = Some(current);
        }
        if self.cfg.experimental_psi_monitoring {
            self.psi = read_psi_sample();
        }
        let context = EngineTickContext {
            now: Instant::now(),
            min_thread_count: self.cfg.min_thread_count,
            max_thread_count: self.cfg.max_thread_count,
            host_cpu_util: self.host_cpu_util,
            psi: self.psi,
            tick_index: self.tick_index,
        };
        let fleet: Vec<_> = self.instances.values().cloned().collect();
        let refresh_cgroup = self.cfg.refresh_cgroup_on_each_read;
        let refreshes = join_all(
            fleet
                .iter()
                .map(|instance| instance.refresh_state(refresh_cgroup)),
        )
        .await;

        for id in fleet
            .iter()
            .zip(refreshes)
            .filter(|(_, refreshed)| !refreshed)
            .map(|(instance, _)| instance.id.clone())
        {
            self.instances.remove(&id);
            self.engine.on_instance_removed(&id).await;
            // FIXME IIUC a failed instance gets dropped but we don't call
            // close()
        }

        let fleet: Vec<_> = self.instances.values().cloned().collect();
        self.persist_new_classifications(&fleet).await?;
        let plan = self.engine.evaluate_fleet(&fleet, &context).await;
        let cap = self.cfg.max_instances_adjusted_per_poll as usize;
        let mut adjusted = 0usize;
        for item in plan {
            let action = item.decision;
            if cap > 0 && adjusted >= cap {
                continue;
            }
            if self
                .apply_engine_decision(&item.instance_id, action)
                .await?
            {
                adjusted += 1;
            }
        }
        self.emit_status_lines().await;
        Ok(())
    }

    /// Commit classifications produced by first successful backend snapshots.
    async fn persist_new_classifications(
        &mut self,
        fleet: &[Arc<Instance>],
    ) -> Result<(), ControllerError> {
        let mut changed = false;
        for instance in fleet {
            let classification = instance.status.read().await.ownership_classification;
            let Some(managed) = classification else {
                continue;
            };
            match self.vm_ownership.classification(&instance.id) {
                Some(persisted) => {
                    if persisted != managed {
                        return Err(ControllerError::VmError(format!(
                            "VM {} ownership changed after classification",
                            instance.id
                        )));
                    }
                }
                None => {
                    self.vm_ownership.record(&instance.id, managed)?;
                    changed = true;
                }
            }
        }
        if changed {
            self.vm_state_store.save(&self.vm_ownership)?;
        }
        Ok(())
    }

    /// Apply one engine action after controller safety checks.
    // FIXME why do we pass the instance ID and not a reference to the instance?
    async fn apply_engine_decision(
        &self,
        instance_id: &str,
        action: ScaleAction,
    ) -> Result<bool, ControllerError> {
        let Some(target) = action.target() else {
            return Ok(false);
        };
        let Some(instance) = self.instances.get(instance_id) else {
            return Ok(false);
        };

        let status = instance.status.read().await;
        let previous_count = status.thread_count;
        let previous_io_count = match &status.perf {
            Some(perf) => perf.total_io_count(),
            None => 0,
        };
        let sticky = status.manual_scaling_sticky;
        let scaling_allowed = status.scaling_allowed();
        let vcpu_count = status.vcpu_count;
        let cooldown_until = status.cooldown_until;
        drop(status);

        if sticky {
            self.report_blocked_scale(&instance.id, action, BlockedReason::ManualOverride)
                .await;
            tracing::info!(
                target: "controller",
                event = "scale_blocked",
                reason = "manual_override",
                vm = %instance.id,
                action = %action
            );
            return Ok(false);
        }
        if !scaling_allowed {
            // FIXME why would an engine even consider an unmanaged VM?
            self.report_blocked_scale(&instance.id, action, BlockedReason::UnmanagedVm)
                .await;
            tracing::info!(
                target: "controller",
                event = "scale_blocked",
                reason = "unmanaged_vm",
                vm = %instance.id,
                action = %action
            );
            return Ok(false);
        }
        if target < self.cfg.min_thread_count {
            self.report_blocked_scale(instance_id, action, BlockedReason::TargetBelowMinimum)
                .await;
            tracing::info!(
                target: "controller",
                id = %instance.id,
                target,
                minimum = self.cfg.min_thread_count,
                "automatic scaling suppressed by controller minimum"
            );
            return Ok(false);
        }
        if target > self.cfg.max_thread_count {
            self.report_blocked_scale(instance_id, action, BlockedReason::TargetExceedsMaximum)
                .await;
            tracing::info!(
                target: "controller",
                id = %instance.id,
                target,
                maximum = self.cfg.max_thread_count,
                "automatic scaling suppressed by controller maximum"
            );
            return Ok(false);
        }
        if target > vcpu_count {
            self.report_blocked_scale(instance_id, action, BlockedReason::TargetExceedsVcpuCount)
                .await;
            tracing::info!(
                target: "controller",
                id = %instance.id,
                target,
                vcpus = vcpu_count,
                "automatic scaling suppressed by vCPU cap"
            );
            return Ok(false);
        }
        if matches!(action, ScaleAction::Up(_) | ScaleAction::Down(_))
            && cooldown_until.is_some_and(|until| Instant::now() < until)
        {
            self.report_blocked_scale(instance_id, action, BlockedReason::Cooldown)
                .await;
            tracing::info!(
                target: "controller",
                id = %instance.id,
                "automatic scaling suppressed by cooldown"
            );
            return Ok(false);
        }
        // FIXME This condition allows ScaleAction::Down(...) with an increasing target
        // to bypass the ceiling. This check should be removed or Down(...) should be
        // blocked from having targets greater than the original.
        if target > previous_count
            && matches!(action, ScaleAction::Up(_))
            && self.cfg.host_cpu_scale_up_ceiling > 0.0
            && self.host_cpu_util >= self.cfg.host_cpu_scale_up_ceiling
        {
            self.report_blocked_scale(instance_id, action, BlockedReason::HostCpuCeiling)
                .await;
            tracing::info!(
                target: "controller",
                id = %instance.id,
                host_cpu = self.host_cpu_util,
                ceiling = self.cfg.host_cpu_scale_up_ceiling,
                "automatic scaling suppressed by host CPU guard"
            );
            return Ok(false);
        }

        if self.cfg.dry_run {
            tracing::info!(
                target: "controller",
                vm = %instance.id,
                action = %action,
                target,
                "dry-run: would scale"
            );
            if matches!(action, ScaleAction::Up(_) | ScaleAction::Down(_)) {
                instance.status.write().await.cooldown_until =
                    Some(Instant::now() + Duration::from_secs_f64(self.cfg.cooldown_secs));
            }
            self.engine
                .on_applied(
                    &instance.id,
                    AppliedOutcome::DryRun {
                        action,
                        prev_thread_count: previous_count,
                        prev_io_count_total: previous_io_count,
                    },
                )
                .await;
            return Ok(true);
        }

        match instance.client.set_thread_count(target).await {
            Ok(()) => {
                let mut status = instance.status.write().await;
                status.thread_count = target;
                if matches!(action, ScaleAction::Up(_) | ScaleAction::Down(_)) {
                    status.cooldown_until =
                        Some(Instant::now() + Duration::from_secs_f64(self.cfg.cooldown_secs));
                }
                drop(status);
                self.engine
                    .on_applied(
                        &instance.id,
                        AppliedOutcome::Success {
                            action,
                            prev_thread_count: previous_count,
                            prev_io_count_total: previous_io_count,
                        },
                    )
                    .await;
                tracing::info!(
                    target: "controller",
                    event = "scale_applied",
                    vm = %instance.id,
                    action = %action,
                    target,
                    prev_thread_count = previous_count,
                    prev_io_count_total = previous_io_count
                );
                Ok(true)
            }
            Err(error) => {
                let error_text = error.to_string();
                self.engine
                    .on_applied(&instance.id, AppliedOutcome::Failed { action, error })
                    .await;
                tracing::warn!(
                    target: "controller",
                    event = "scale_failed",
                    vm = %instance.id,
                    action = %action,
                    target,
                    error = %error_text
                );
                Ok(false)
            }
        }
    }

    /// Report a controller-blocked action to the proposing engine.
    async fn report_blocked_scale(
        &self,
        instance_id: &str,
        action: ScaleAction,
        reason: BlockedReason,
    ) {
        self.engine
            .on_applied(instance_id, AppliedOutcome::Blocked { action, reason })
            .await;
    }

    /// Emit configured per-VM and aggregate status lines.
    async fn emit_status_lines(&self) {
        let mut total_threads = 0u64;
        let mut aggregate_iops = [0u64; 3];
        let mut aggregate_has_iops = [false; 3];
        let mut fleet: Vec<_> = self.instances.values().collect();
        fleet.sort_unstable_by(|left, right| left.id.cmp(&right.id));
        for instance in fleet {
            let status = instance.status.read().await;
            let iops_windows = STATUS_WINDOWS.map(|window| status.rolling.iops_over(window));
            for (index, value) in iops_windows.iter().enumerate() {
                if let Some(value) = value {
                    aggregate_iops[index] = aggregate_iops[index].saturating_add(*value);
                    aggregate_has_iops[index] = true;
                }
            }
            if self.cfg.enable_per_vm_status_line {
                Self::emit_instance_status(instance, &status, iops_windows);
            }
            total_threads += u64::from(status.thread_count);
        }

        if self.cfg.enable_aggregate_status_line {
            let aggregate =
                std::array::from_fn(|idx| aggregate_has_iops[idx].then_some(aggregate_iops[idx]));
            tracing::info!(
                target: "status",
                tracked = self.instances.len(),
                total_threads,
                iops_1_5_15m = %format_optional_cells(aggregate),
                "aggregate"
            );
        }
    }

    /// Emit one uptime-style per-VM status record.
    fn emit_instance_status(
        instance: &Instance,
        status: &InstanceStatus,
        iops_windows: [Option<u64>; 3],
    ) {
        let cpu = compute_cpu_stats(status);
        tracing::info!(
            target: "status",
            vm = instance.to_string(),
            thr = status.thread_count,
            iops = %format!(
                "{}/{}/{}",
                status.read_iops,
                status.write_iops,
                status.other_iops
            ),
            iops_1_5_15m = %format_optional_cells(iops_windows),
            bw_mb_s = %format!(
                "{}/{}",
                status.read_bytes_per_second / 1_000_000,
                status.write_bytes_per_second / 1_000_000
            ),
            cpu = %format!("{}/{}/{}", cpu.avg_pct, cpu.median_pct, cpu.total_pct),
            cpu_us_per_io_1_5_15m =
                %format_1_5_15(&status.rolling, |rolling, window| {
                    rolling.cpu_us_per_io_over(window)
                }),
            ""
        );
    }
}

/// Read CPU, I/O, and memory pressure; unavailable values become zero.
fn read_psi_sample() -> PsiSample {
    use procfs::{CpuPressure, Current, IoPressure, MemoryPressure};

    PsiSample {
        cpu_some_avg10: CpuPressure::current()
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
        io_some_avg10: IoPressure::current()
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
        memory_some_avg10: MemoryPressure::current()
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
    }
}

/// Read aggregate host CPU counters.
fn read_host_cpu_sample() -> Result<HostCpuSample, ControllerError> {
    let total = procfs::KernelStats::current()?.total;
    let idle_ticks = total.idle.saturating_add(total.iowait.unwrap_or(0));
    let total_ticks = total
        .user
        .saturating_add(total.nice)
        .saturating_add(total.system)
        .saturating_add(total.idle)
        .saturating_add(total.iowait.unwrap_or(0))
        .saturating_add(total.irq.unwrap_or(0))
        .saturating_add(total.softirq.unwrap_or(0))
        .saturating_add(total.steal.unwrap_or(0))
        .saturating_add(total.guest.unwrap_or(0))
        .saturating_add(total.guest_nice.unwrap_or(0));
    Ok(HostCpuSample {
        busy_ticks: total_ticks.saturating_sub(idle_ticks),
        total_ticks,
    })
}

/// Compute host CPU utilisation from successive cumulative samples.
fn host_cpu_utilisation(previous: HostCpuSample, current: HostCpuSample) -> f64 {
    let delta_busy = current.busy_ticks.saturating_sub(previous.busy_ticks) as f64;
    let delta_total = current.total_ticks.saturating_sub(previous.total_ticks) as f64;
    if delta_total == 0.0 {
        0.0
    } else {
        (delta_busy / delta_total).clamp(0.0, 1.0)
    }
}

/// Render the 1m/5m/15m cells, using `-` before a window has data.
fn format_optional_cells(values: [Option<u64>; 3]) -> String {
    values
        .into_iter()
        .map(|value| {
            value
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use test_log::test;

    use super::*;
    use crate::{
        backends::BackendClientError,
        engines::{EngineTickContext, ScalingEngine},
        instance::{InstanceClient, ThreadPoolSnapshot},
        util::Path,
    };

    #[cfg(feature = "threshold-engine")]
    use crate::engines::threshold::{ThresholdConfig, ThresholdEngine};

    struct VcpuLimitedClient {
        target: Arc<AtomicU32>,
    }

    #[async_trait]
    impl InstanceClient for VcpuLimitedClient {
        async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
            self.target.store(count, Ordering::Relaxed);
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.target.load(Ordering::Relaxed),
                vcpu_count: 4,
                perf: None,
                per_thread_util: None,
            })
        }

        async fn close(&self) {}
    }

    /// Test that actuation will not raise the pool above the VM vCPU
    /// count.
    #[test(tokio::test)]
    async fn actuation_enforces_vcpu_cap() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            cooldown_secs: 0.0,
            ..Default::default()
        };
        let target = Arc::new(AtomicU32::new(2));
        let client = VcpuLimitedClient {
            target: Arc::clone(&target),
        };
        let instance = Arc::new(Instance::new(
            "some-vcpu-limited-test-instance".to_string(),
            Path::new(""),
            0,
            client,
        ));
        {
            let mut status = instance.status.write().await;
            status.thread_count = 2;
            status.vcpu_count = 4;
            status.ownership_classification = Some(true);
        }
        let mut controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 4);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(5))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 4);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(3))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 3);

        instance.status.write().await.ownership_classification = Some(false);
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(2))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 3);
    }

    // FIXME This test seems like it mixes basic ScaleAction unit tests and more
    // complex multi-tick behaviour instead of having two tests exercising different
    // things.
    /// Test that actuation clamps thread targets to configured min/max
    /// and blocks scale-up when host CPU is above the ceiling.
    #[test(tokio::test)]
    async fn actuation_enforces_controller_bounds_and_host_ceiling() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            min_thread_count: 2,
            max_thread_count: 3,
            host_cpu_scale_up_ceiling: 0.5,
            cooldown_secs: 30.0,
            ..Default::default()
        };
        let target = Arc::new(AtomicU32::new(2));
        let client = VcpuLimitedClient {
            target: Arc::clone(&target),
        };
        let instance = Arc::new(Instance::new(
            "policy-guarded".to_string(),
            Path::new(""),
            0,
            client,
        ));
        {
            let mut status = instance.status.write().await;
            status.thread_count = 2;
            status.vcpu_count = 4;
        }
        let mut controller = Controller::new(
            cfg,
            Box::new(ThresholdEngine::new(ThresholdConfig::default())),
        )
        .unwrap();
        controller.host_cpu_util = 0.5;
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(3))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 2);

        controller.host_cpu_util = 0.0;
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(3))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(2))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Revert(2))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(1))
            .await
            .unwrap();
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        assert_eq!(target.load(Ordering::Relaxed), 2);
    }

    struct BulkClient {
        fail: bool,
        target: Arc<AtomicU32>,
    }

    #[async_trait]
    impl InstanceClient for BulkClient {
        async fn set_thread_count(&self, n: u32) -> Result<(), BackendClientError> {
            if self.fail {
                return Err(BackendClientError::Protocol("injected failure".to_string()));
            }
            self.target.store(n, Ordering::Relaxed);
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.target.load(Ordering::Relaxed),
                vcpu_count: 16,
                perf: None,
                per_thread_util: Some(0.0),
            })
        }

        async fn close(&self) {}
    }

    /// Test that `set_all` applies a target to every VM and reports how
    /// many failed when some clients error.
    #[tokio::test]
    async fn set_all_attempts_every_vm_and_reports_partial_failure() {
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let engine = Box::new(ThresholdEngine::new(ThresholdConfig::default()));
        let mut controller = Controller::new(cfg, engine).unwrap();
        let good_target = Arc::new(AtomicU32::new(1));
        let good = BulkClient {
            fail: false,
            target: Arc::clone(&good_target),
        };
        let bad_target = Arc::new(AtomicU32::new(1));
        let bad = BulkClient {
            fail: true,
            target: Arc::clone(&bad_target),
        };
        let good_instance = Arc::new(Instance::new("visible".to_string(), Path::new(""), 0, good));
        let bad_instance = Arc::new(Instance::new("hidden".to_string(), Path::new(""), 0, bad));
        good_instance.status.write().await.vcpu_count = 16;
        bad_instance.status.write().await.vcpu_count = 16;
        controller
            .instances
            .insert("visible".to_string(), good_instance.clone());
        controller
            .instances
            .insert("hidden".to_string(), bad_instance.clone());

        let (failed, summary) = controller.handle_set_all_thread_counts(4, true).await;

        assert_eq!(failed, 1);
        assert_eq!(good_target.load(Ordering::Relaxed), 4);
        assert_eq!(bad_target.load(Ordering::Relaxed), 1);
        assert!(good_instance.status.read().await.manual_scaling_sticky);
        assert!(!bad_instance.status.read().await.manual_scaling_sticky);
        assert!(summary.contains("1/2"));
        assert!(summary.contains("hidden: injected failure"));
    }

    struct SnapshotClient {
        threads: u32,
        closed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InstanceClient for SnapshotClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.threads,
                // FIXME these weren't required, looked like broken due to rebase
                vcpu_count: 2,
                perf: None,
                per_thread_util: None,
            })
        }

        async fn close(&self) {
            self.closed.fetch_add(1, Ordering::Relaxed);
        }
    }

    struct FailingClient;

    #[async_trait]
    impl InstanceClient for FailingClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Err(BackendClientError::Transport("boom".into()))
        }

        async fn close(&self) {}
    }

    struct SharedEngine {
        added: Arc<AtomicUsize>,
        removed: Arc<AtomicUsize>,
        evaluated: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ScalingEngine for SharedEngine {
        fn name(&self) -> &'static str {
            "shared"
        }

        fn dump_config(&self) -> serde_json::Value {
            serde_json::Value::Null
        }

        async fn evaluate(
            &self,
            _instance: &Arc<Instance>,
            _context: &EngineTickContext,
        ) -> ScaleAction {
            self.evaluated.fetch_add(1, Ordering::Relaxed);
            ScaleAction::None
        }

        async fn on_instance_added(&self, _instance: &Arc<Instance>) {
            self.added.fetch_add(1, Ordering::Relaxed);
        }

        async fn on_instance_removed(&self, _instance_id: &str) {
            self.removed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn instance(id: &str, threads: u32, closed: Arc<AtomicUsize>) -> Arc<Instance> {
        Arc::new(Instance::new(
            id.to_string(),
            Path::new(""),
            1,
            SnapshotClient { threads, closed },
        ))
    }

    /// Test that discovery sync adds new VMs, retains existing ones,
    /// and removes disappeared ones.
    #[tokio::test]
    async fn sync_instances_adds_retains_and_removes() {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let mut controller = Controller::new(
            cfg,
            Box::new(SharedEngine {
                added: Arc::clone(&added),
                removed: Arc::clone(&removed),
                evaluated: Arc::clone(&evaluated),
            }),
        )
        .unwrap();
        let closed = Arc::new(AtomicUsize::new(0));

        let first = instance("vm-a", 1, Arc::clone(&closed));
        let (n_added, n_removed) = controller
            .sync_instances(vec![Arc::clone(&first)])
            .await
            .unwrap();
        assert_eq!((n_added, n_removed), (1, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(added.load(Ordering::Relaxed), 1);

        let duplicate = instance("vm-a", 9, Arc::clone(&closed));
        let (n_added, n_removed) = controller.sync_instances(vec![duplicate]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(closed.load(Ordering::Relaxed), 1);

        let (n_added, n_removed) = controller.sync_instances(vec![]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 1));
        assert!(controller.instances.is_empty());
        assert_eq!(removed.load(Ordering::Relaxed), 1);
        assert_eq!(evaluated.load(Ordering::Relaxed), 0);
    }

    /// Test that one tick refreshes state, runs the engine, and drops
    /// instances that fail refresh.
    #[tokio::test]
    async fn tick_refreshes_evaluates_and_drops_failed_instances() {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let state_dir = tempfile::tempdir().unwrap();
        let cfg = Config {
            vm_state_path: Path::new(&state_dir.path().join("ownership.json")),
            ..Default::default()
        };
        let mut controller = Controller::new(
            cfg,
            Box::new(SharedEngine {
                added: Arc::clone(&added),
                removed: Arc::clone(&removed),
                evaluated: Arc::clone(&evaluated),
            }),
        )
        .unwrap();

        let closed = Arc::new(AtomicUsize::new(0));
        let ok = instance("ok", 4, Arc::clone(&closed));
        let bad = Arc::new(Instance::new(
            "bad".to_string(),
            Path::new(""),
            2,
            FailingClient,
        ));
        controller.sync_instances(vec![ok, bad]).await.unwrap();
        assert_eq!(added.load(Ordering::Relaxed), 2);

        controller.tick().await.unwrap();
        assert_eq!(controller.tick_index, 1);
        assert_eq!(controller.instances.len(), 1);
        assert!(controller.instances.contains_key("ok"));
        assert_eq!(
            controller.instances["ok"].status.read().await.thread_count,
            4
        );
        assert_eq!(evaluated.load(Ordering::Relaxed), 1);
        assert_eq!(removed.load(Ordering::Relaxed), 1);
    }
}
