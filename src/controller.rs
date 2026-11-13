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
use procfs::{
    CpuPressure, FromRead, FromReadSI, IoPressure, KernelStats, MemoryPressure, ProcError,
};
use serde::{Deserialize, Serialize};
use statistical::median;
use thiserror::Error;

use crate::{
    backends::{BackendClientError, IoThreadProperties, VqMapping},
    config::Config,
    dbus::DbusRequest,
    engines::{
        AppliedOutcome, BlockedReason, EngineInstanceSnapshot, EngineTickContext, PsiSample,
        ScaleAction, ScalingEngine,
    },
    instance::{CgroupError, Instance, InstanceStatus, SnapshotLatency, ThreadPoolSnapshot},
    path::PrefixedPath,
    per_vm_log,
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

/// Average, median, and aggregate utilisation across sampled workers.
#[derive(Debug, Default, Clone, Copy)]
struct CpuStats {
    /// Mean per-worker utilisation as a 0..1 fraction.
    avg_util: f64,
    /// Mean per-worker utilisation in percent.
    avg_pct: u64,
    /// Median per-worker utilisation in percent.
    median_pct: u64,
    /// Sum of per-worker utilisation percentages.
    total_pct: u64,
}

/// Summarise controller-sampled worker utilisation.
fn compute_cpu_stats(status: &InstanceStatus) -> Option<CpuStats> {
    if status.per_worker_util.is_empty() {
        return None;
    }

<<<<<<< HEAD
    let fallback_util = s.per_thread_util.unwrap_or(0.0);
    CpuStats {
        avg_pct: (fallback_util * 100.0).round() as u64,
        median_pct: (fallback_util * 100.0).round() as u64,
        total_pct: (fallback_util * s.thread_count as f64 * 100.0).round() as u64,
    }
=======
    let total = status.per_worker_util.iter().sum::<f64>();
    let median = median(&status.per_worker_util);
    let avg_util = total / status.per_worker_util.len() as f64;
    Some(CpuStats {
        avg_util,
        avg_pct: (avg_util * 100.0).round() as u64,
        median_pct: (median * 100.0).round() as u64,
        total_pct: (total * 100.0).round() as u64,
    })
>>>>>>> 77e01d2 (Pass immutable data to ScalingEngines)
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

/// Return queue count, total, fixed-point average, and median depth.
fn summarize_vq_depths(
    depths: Option<&[u64]>,
) -> (Option<u32>, Option<u64>, Option<u64>, Option<u64>) {
    let Some(depths) = depths.filter(|depths| !depths.is_empty()) else {
        return (None, None, None, None);
    };
    let count = depths.len() as u32;
    let total = depths.iter().copied().fold(0u64, u64::saturating_add);
    let average_x100 = ((u128::from(total) * 100) / depths.len() as u128) as u64;
    let median = median(
        &depths
            .iter()
            .copied()
            .map(|depth| depth as f64)
            .collect::<Vec<_>>(),
    )
    .floor() as u64;
    (Some(count), Some(total), Some(average_x100), Some(median))
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
            instance.status.write().await.ownership_classification = classification;
            self.engine.on_instance_added(&instance.id).await;
            tracing::info!(target: per_vm_log::VM, vm = %instance.id, event = "vm_added");
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
                tracing::info!(
                    target: per_vm_log::VM,
                    vm = %id,
                    event = "vm_removed",
                    reason = "not_discovered"
                );
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
                    let snapshot = instance.snapshot.read().await;
                    let status = instance.status.read().await;
                    let cpu = compute_cpu_stats(&status).unwrap_or_default();
                    let thread_count = status.thread_count;
                    let vcpu_count = snapshot.as_ref().map_or(0, |snapshot| snapshot.vcpu_count);
                    let (read_io_count, write_io_count, other_io_count) = match snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.perf.as_ref())
                    {
                        Some(perf) => {
                            (perf.read_io_count, perf.write_io_count, perf.other_io_count)
                        }
                        None => (0, 0, 0),
                    };
                    vms.push(serde_json::json!({
                        "vm": id,
                        "thread_count": thread_count,
                        "manual_scaling_sticky": status.manual_scaling_sticky,
                        "scaling_allowed": status.scaling_allowed,
<<<<<<< HEAD
                        "vcpu_count": status.vcpu_count,
                        "per_thread_util": status.per_thread_util.unwrap_or(0.0),
=======
                        "vcpu_count": vcpu_count,
                        "per_thread_util": cpu.avg_util,
>>>>>>> 77e01d2 (Pass immutable data to ScalingEngines)
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
        let vcpu_count = instance
            .snapshot
            .read()
            .await
            .as_ref()
            .map_or(0, |snapshot| snapshot.vcpu_count);
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
        instance.status.write().await.manual_scaling_sticky = sticky;
        Ok(())
    }

    /// Apply a debug-only manual count to every VM known to the daemon.
    ///
    /// The daemon inventory, rather than the consumer's filtered view, defines
    /// the target set. Every VM is attempted in stable identifier order so that
    /// there can be a reasonable expectation about the order it gets applied.
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
            let snapshot = instance.snapshot.read().await;
            let status = instance.status.read().await;
            let cpu_stats = compute_cpu_stats(&status).unwrap_or_default();
            let thread_count = status.thread_count;
            let (vcpu_count, perf) = snapshot.as_ref().map_or((0, None), |snapshot| {
                (snapshot.vcpu_count, snapshot.perf.as_ref())
            });
            let per_vq_depth = perf.and_then(|perf| perf.per_vq_depth.as_deref());
            let (num_queues, qd_total, qd_avg_x100, qd_median) = summarize_vq_depths(per_vq_depth);
            payload.vms.push(SnapshotVm {
                id: id.clone(),
                vcpu_count,
                pid: instance.pid,
<<<<<<< HEAD
                thread_count: s.thread_count,
                alive: s.alive,
                perf_available: s.perf.is_some(),
                // Guard JSON serialization: NaN/inf are not valid JSON numbers.
                per_thread_util: s.per_thread_util.filter(|v| v.is_finite()).unwrap_or(0.0),
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
=======
                thread_count,
                alive: status.alive,
                perf_available: perf.is_some(),
                per_thread_util: cpu_stats.avg_util,
                manual_scaling_sticky: status.manual_scaling_sticky,
                read_iops: status.read_iops,
                write_iops: status.write_iops,
                other_iops: status.other_iops,
                read_bw_bps: status.read_bytes_per_second,
                write_bw_bps: status.write_bytes_per_second,
                read_latency_us: perf.and_then(|perf| perf.read_latency_us),
                write_latency_us: perf.and_then(|perf| perf.write_latency_us),
>>>>>>> 77e01d2 (Pass immutable data to ScalingEngines)
                num_queues,
                per_vq_depth: per_vq_depth.map(<[u64]>::to_vec),
                qd_total,
                qd_avg_x100,
                qd_median,
                cpu_pct_avg: Some(cpu_stats.avg_pct),
                cpu_pct_median: Some(cpu_stats.median_pct),
                cpu_pct_total: Some(cpu_stats.total_pct),
            });
        }
        serde_json::to_string(&payload).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// Convert one coherent raw snapshot/status pair into engine policy input.
    async fn engine_snapshot(instance: &Instance) -> EngineInstanceSnapshot {
        let snapshot = instance.snapshot.read().await;
        let status = instance.status.read().await;
        let cpu = compute_cpu_stats(&status).unwrap_or_default();
        let thread_count = status.thread_count;
        let (vcpu_count, io_count_total) = snapshot.as_ref().map_or((0, 0), |snapshot| {
            (
                snapshot.vcpu_count,
                snapshot
                    .perf
                    .as_ref()
                    .map_or(0, |perf| perf.total_io_count()),
            )
        });
        EngineInstanceSnapshot {
            id: instance.id.clone(),
            thread_count,
            vcpu_count,
            per_thread_util: cpu.avg_util,
            cpu_available: !status.per_worker_util.is_empty(),
            io_count_total,
            throttled_recently: status.throttled_usec_delta > 0,
            scaling_allowed: status.scaling_allowed,
            manual_scaling_sticky: status.manual_scaling_sticky,
        }
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
            tracing::info!(
                target: per_vm_log::VM,
                vm = %id,
                event = "vm_removed",
                reason = "refresh_failed"
            );
            // FIXME IIUC a failed instance gets dropped but we don't call
            // close()
        }

        let fleet: Vec<_> = self.instances.values().cloned().collect();
        self.persist_new_classifications(&fleet).await?;
        let engine_snapshots =
            join_all(fleet.iter().map(|instance| Self::engine_snapshot(instance))).await;
        let decisions = self
            .engine
            .evaluate_fleet(&engine_snapshots, &context)
            .await;
        let mut planned_ids = HashSet::new();
        let mut plan = Vec::new();
        for (sequence, item) in decisions.into_iter().enumerate() {
            if !planned_ids.insert(item.instance_id.clone()) {
                tracing::warn!(
                    target: "controller",
                    vm = %item.instance_id,
                    "engine returned duplicate decisions; keeping the first"
                );
                continue;
            }
            let Some(instance) = self.instances.get(&item.instance_id) else {
                tracing::warn!(
                    target: "controller",
                    vm = %item.instance_id,
                    "engine returned a decision for an unknown VM"
                );
                continue;
            };
            let action = item.decision;
            let Some(target) = action.target() else {
                continue;
            };
            let current = instance.status.read().await.thread_count;
            if target == current {
                continue;
            }
            plan.push((target > current, sequence, item.instance_id, action));
        }
        plan.sort_by_key(|(is_up, sequence, _, _)| (*is_up, *sequence));

        let cap = self.cfg.max_instances_adjusted_per_poll as usize;
        let mut adjusted = 0usize;
        for (_, _, instance_id, action) in plan {
            if cap > 0 && adjusted >= cap {
                continue;
            }
            if self.apply_engine_decision(&instance_id, action).await? {
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

        let snapshot = instance.snapshot.read().await;
        let status = instance.status.read().await;
        let previous_count = status.thread_count;
        let previous_io_count = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.perf.as_ref())
            .map_or(0, |perf| perf.total_io_count());
        let sticky = status.manual_scaling_sticky;
        let scaling_allowed = status.scaling_allowed();
        let vcpu_count = snapshot.as_ref().map_or(0, |snapshot| snapshot.vcpu_count);
        let cooldown_until = status.cooldown_until;
        drop(status);
        drop(snapshot);

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
                event = "scale_blocked",
                reason = "target_below_minimum",
                vm = %instance.id,
                action = %action,
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
                event = "scale_blocked",
                reason = "target_exceeds_maximum",
                vm = %instance.id,
                action = %action,
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
                event = "scale_blocked",
                reason = "target_exceeds_vcpu_count",
                vm = %instance.id,
                action = %action,
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
                event = "scale_blocked",
                reason = "cooldown",
                vm = %instance.id,
                action = %action,
                "automatic scaling suppressed by cooldown"
            );
            return Ok(false);
        }
        // FIXME This condition allows ScaleAction::Down(...) with an increasing
        // target to bypass the ceiling. This check should be removed or
        // Down(...) should be blocked from having targets greater than
        // the original.
        if target > previous_count
            && matches!(action, ScaleAction::Up(_))
            && self.cfg.host_cpu_scale_up_ceiling > 0.0
            && self.host_cpu_util >= self.cfg.host_cpu_scale_up_ceiling
        {
            self.report_blocked_scale(instance_id, action, BlockedReason::HostCpuCeiling)
                .await;
            tracing::info!(
                target: "controller",
                event = "scale_blocked",
                reason = "host_cpu_ceiling",
                vm = %instance.id,
                action = %action,
                host_cpu = self.host_cpu_util,
                ceiling = self.cfg.host_cpu_scale_up_ceiling,
                "automatic scaling suppressed by host CPU guard"
            );
            return Ok(false);
        }

        if self.cfg.dry_run {
            tracing::info!(
                target: "controller",
                event = "scale_dry_run",
                vm = %instance.id,
                action = %action,
                target,
                "dry-run: would scale"
            );
            if matches!(
                action,
                ScaleAction::Up(_) | ScaleAction::Down(_) | ScaleAction::Revert(_)
            ) {
                instance.status.write().await.cooldown_until =
                    Some(Instant::now() + Duration::from_secs(self.cfg.cooldown_secs));
            }
            let applied_at = Instant::now();
            self.engine
                .on_applied(
                    &instance.id,
                    AppliedOutcome::DryRun {
                        action,
                        prev_thread_count: previous_count,
                        prev_io_count_total: previous_io_count,
                        applied_at,
                    },
                )
                .await;
            return Ok(true);
        }

        match instance.client.set_thread_count(target).await {
            Ok(()) => {
                let applied_at = Instant::now();
                let mut status = instance.status.write().await;
                if matches!(
                    action,
                    ScaleAction::Up(_) | ScaleAction::Down(_) | ScaleAction::Revert(_)
                ) {
                    status.cooldown_until =
                        Some(applied_at + Duration::from_secs(self.cfg.cooldown_secs));
                }
                drop(status);
                self.engine
                    .on_applied(
                        &instance.id,
                        AppliedOutcome::Success {
                            action,
                            prev_thread_count: previous_count,
                            prev_io_count_total: previous_io_count,
                            applied_at,
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
            let snapshot = instance.snapshot.read().await;
            let status = instance.status.read().await;
            let iops_windows = STATUS_WINDOWS.map(|window| status.rolling.iops_over(window));
            for (index, value) in iops_windows.iter().enumerate() {
                if let Some(value) = value {
                    aggregate_iops[index] = aggregate_iops[index].saturating_add(*value);
                    aggregate_has_iops[index] = true;
                }
            }
            if self.cfg.enable_per_vm_main_log {
                Self::emit_instance_status_main(instance, &status, iops_windows, snapshot.as_ref());
            }
            total_threads += u64::from(status.thread_count);
        }

        if self.cfg.enable_aggregate_status_line {
            let aggregate =
                std::array::from_fn(|idx| aggregate_has_iops[idx].then_some(aggregate_iops[idx]));
            tracing::info!(
                target: "status",
                "{} {} {}",
                self.instances.len(),
                total_threads,
                format_optional_cells(aggregate, " ")
            );
        }
    }

    #[cfg(test)]
    fn format_instance_status_line(
        status: &InstanceStatus,
        iops_windows: [Option<u64>; 3],
        snapshot: Option<&ThreadPoolSnapshot>,
    ) -> String {
        let cpu = compute_cpu_stats(status).unwrap_or_default();
        let perf = snapshot.and_then(|snapshot| snapshot.perf.as_ref());
        format!(
            "status={} {} {} {} {} {} {} {} {} {} {} {} {}",
            status.thread_count,
            status.read_iops,
            status.write_iops,
            status.other_iops,
            format_optional_cells(iops_windows, " "),
            status.read_bytes_per_second / 1_000_000,
            status.write_bytes_per_second / 1_000_000,
            cpu.avg_pct,
            cpu.median_pct,
            cpu.total_pct,
            format_latency_cell(perf.and_then(|perf| perf.read_latency_us), " "),
            format_latency_cell(perf.and_then(|perf| perf.write_latency_us), " "),
            status
                .rolling
                .format_1_5_15(|rolling, window| rolling.cpu_us_per_io_over(window), " ")
        )
    }

    /// Emit one per-VM status record on the main controller target.
    fn emit_instance_status_main(
        instance: &Instance,
        status: &InstanceStatus,
        iops_windows: [Option<u64>; 3],
        snapshot: Option<&ThreadPoolSnapshot>,
    ) {
        let thread_count = status.thread_count;
        let perf = snapshot.and_then(|snapshot| snapshot.perf.as_ref());
        tracing::info!(
            target: "controller",
            event = "vm_status",
            vm = instance.to_string(),
            thread_count,
            read_iops = status.read_iops,
            write_iops = status.write_iops,
            other_iops = status.other_iops,
            iops_1_5_15m = %format_optional_cells(iops_windows, "/"),
            read_bw_mb_s = status.read_bytes_per_second / 1_000_000,
            write_bw_mb_s = status.write_bytes_per_second / 1_000_000,
            read_latency_us = %format_latency_cell(perf.and_then(|perf| perf.read_latency_us), "/"),
            write_latency_us = %format_latency_cell(perf.and_then(|perf| perf.write_latency_us), "/")
        );
    }
}

/// Read CPU, I/O, and memory pressure; unavailable values become zero.
fn read_psi_sample() -> PsiSample {
    PsiSample {
        cpu_some_avg10: CpuPressure::from_file(PrefixedPath::new("/proc/pressure/cpu"))
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
        io_some_avg10: IoPressure::from_file(PrefixedPath::new("/proc/pressure/io"))
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
        memory_some_avg10: MemoryPressure::from_file(PrefixedPath::new("/proc/pressure/memory"))
            .map(|pressure| pressure.some.avg10 as f64)
            .unwrap_or(0.0),
    }
}

/// Read aggregate host CPU counters.
fn read_host_cpu_sample() -> Result<HostCpuSample, ControllerError> {
    let current_si = procfs::current_system_info();
    let total = KernelStats::from_file(PrefixedPath::new("/proc/stat"), current_si)?.total;

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

/// Render the 1m/5m/15m cells joined by `sep`, using `-` before a window has
/// data.
fn format_optional_cells(values: [Option<u64>; 3], sep: &str) -> String {
    values
        .into_iter()
        .map(|value| {
            value
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        })
        .collect::<Vec<_>>()
        .join(sep)
}

/// Render p50/p95/p99/avg latency cells joined by `sep`.
///
/// Missing latency still renders one `-` per cell so positional consumers see
/// a fixed number of fields.
fn format_latency_cell(latency: Option<crate::instance::SnapshotLatency>, sep: &str) -> String {
    match latency {
        Some(latency) => [latency.p50, latency.p95, latency.p99, latency.avg]
            .map(|value| value.to_string())
            .join(sep),
        None => ["-"; 4].join(sep),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    };

    use async_trait::async_trait;
    use proptest::prelude::*;
    use rstest::rstest;
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::{
        backends::BackendClientError,
        config::PollPeriod,
        engines::{EngineTickContext, ScalingEngine},
        instance::{InstanceClient, InstanceStatus, ThreadPoolSnapshot},
        per_vm_log::PerVmLog,
        test::{MockDir, mockdir::mockdir},
    };

    #[cfg(feature = "threshold-engine")]
    use crate::engines::threshold::{ThresholdConfig, ThresholdEngine};

    #[cfg(feature = "threshold-engine")]
    fn default_threshold_engine() -> ThresholdEngine {
        let mut durations = Config::default().poll_period().converter();
        let engine = ThresholdEngine::new(ThresholdConfig::default(), &mut durations).unwrap();
        durations.finish().unwrap();
        engine
    }

    #[test]
    fn cpu_stats_summarize_procfs_workers() {
        let status = InstanceStatus {
            per_worker_util: vec![0.25, 0.75],
            ..Default::default()
        };
        let stats = compute_cpu_stats(&status).unwrap();
        assert_eq!(stats.avg_util, 0.5);
        assert_eq!(stats.avg_pct, 50);
        assert_eq!(stats.median_pct, 50);
        assert_eq!(stats.total_pct, 100);
    }

    #[test]
    fn cpu_stats_are_zero_before_a_complete_procfs_interval() {
        let stats = compute_cpu_stats(&InstanceStatus::default());
        assert!(stats.is_none());
    }

    struct VcpuLimitedClient {
        /// History of worker thread counts.
        history: Mutex<Vec<u32>>,
    }

    impl VcpuLimitedClient {
        fn new(init: u32) -> Self {
            Self {
                history: Mutex::new(vec![init]),
            }
        }

        fn history(&self) -> impl std::ops::Deref<Target = Vec<u32>> {
            self.history.lock().unwrap()
        }

        fn last(&self) -> u32 {
            *self.history().last().unwrap()
        }
    }

    #[async_trait]
    impl InstanceClient for Arc<VcpuLimitedClient> {
        async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
            self.history.lock().unwrap().push(count);
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                vcpu_count: 4,
                perf: None,
            })
        }

        async fn close(&self) {}
    }

    /// Test that actuation will not raise the pool above the VM vCPU count.
    async fn set_test_state(
        instance: &Instance,
        thread_count: u32,
        vcpu_count: u32,
        ownership_classification: Option<bool>,
    ) {
        *instance.snapshot.write().await = Some(ThreadPoolSnapshot {
            vcpu_count,
            perf: None,
        });
        let mut status = instance.status.write().await;
        status.thread_count = thread_count;
        status.ownership_classification = ownership_classification;
    }

    /// Test that actuation will not raise the pool above the VM vCPU count.
    #[rstest]
    #[tokio::test]
    async fn test_controller_actuation_enforces_vcpu_cap(#[from(mockdir)] _mockdir: MockDir) {
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
            cooldown_secs: 0,
            ..Default::default()
        };
        let client = Arc::new(VcpuLimitedClient::new(2));
        let instance = Arc::new(Instance::new(
            "some-vcpu-limited-test-instance".to_string(),
            PrefixedPath::new(""),
            0,
            Arc::clone(&client),
        ));
        set_test_state(&instance, 2, 4, Some(true)).await;
        let mut controller = Controller::new(
            cfg,
            Box::new(
                ThresholdEngine::new(
                    ThresholdConfig::default(),
                    &mut PollPeriod::new(1, false).converter(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        assert_eq!(client.last(), 4);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(5))
            .await
            .unwrap();
        assert_eq!(client.last(), 4);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(3))
            .await
            .unwrap();
        assert_eq!(client.last(), 3);

        instance.status.write().await.ownership_classification = Some(false);
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(2))
            .await
            .unwrap();
        assert_eq!(client.last(), 3);

        assert_eq!(*client.history(), vec![2, 4, 3])
    }

    // FIXME This test seems like it mixes basic ScaleAction unit tests and more
    // complex multi-tick behaviour instead of having two tests exercising
    // different things.
    /// Test that actuation clamps thread targets to configured min/max
    /// and blocks scale-up when host CPU is above the ceiling.
    #[rstest]
    #[tokio::test]
    async fn test_controller_actuation_enforces_controller_bounds_and_host_ceiling(
        #[from(mockdir)] _mockdir: MockDir,
    ) {
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
            min_thread_count: 2,
            max_thread_count: 3,
            host_cpu_scale_up_ceiling: 0.5,
            cooldown_secs: 30,
            ..Default::default()
        };
        let client = Arc::new(VcpuLimitedClient::new(2));
        let instance = Arc::new(Instance::new(
            "policy-guarded".to_string(),
            PrefixedPath::new(""),
            0,
            Arc::clone(&client),
        ));
        set_test_state(&instance, 2, 4, None).await;
        let mut controller = Controller::new(
            cfg,
            Box::new(
                ThresholdEngine::new(
                    ThresholdConfig::default(),
                    &mut PollPeriod::new(1, false).converter(),
                )
                .unwrap(),
            ),
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
        assert_eq!(client.last(), 2);

        controller.host_cpu_util = 0.0;
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(3))
            .await
            .unwrap();
        assert_eq!(client.last(), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(2))
            .await
            .unwrap();
        assert_eq!(client.last(), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Revert(2))
            .await
            .unwrap();
        assert_eq!(client.last(), 2);

        controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(1))
            .await
            .unwrap();
        controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        assert_eq!(client.last(), 2);

        assert_eq!(*client.history(), vec![2])
    }

    /// Test that a performance-validation revert arms cooldown so the next
    /// ordinary up/down cannot immediately undo it (the 4→3→4 flap).
    #[cfg(feature = "threshold-engine")]
    #[rstest]
    #[tokio::test]
    async fn test_controller_revert_arms_cooldown_blocking_immediate_retry(
        #[from(mockdir)] _mockdir: MockDir,
    ) {
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
            cooldown_secs: 30,
            min_thread_count: 1,
            max_thread_count: 8,
            ..Default::default()
        };
        let client = Arc::new(VcpuLimitedClient::new(4));
        let instance = Arc::new(Instance::new(
            "revert-cooldown".to_string(),
            PrefixedPath::new(""),
            0,
            Arc::clone(&client),
        ));
        set_test_state(&instance, 4, 8, Some(true)).await;
        let mut controller = Controller::new(
            cfg,
            Box::new(
                ThresholdEngine::new(
                    ThresholdConfig::default(),
                    &mut PollPeriod::new(1, false).converter(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        let applied = controller
            .apply_engine_decision(&instance.id, ScaleAction::Revert(3))
            .await
            .unwrap();
        assert!(applied);
        assert_eq!(client.last(), 3);
        assert!(instance.status.read().await.cooldown_until.is_some());

        // Immediate re-up must be suppressed by the revert's cooldown.
        let retried = controller
            .apply_engine_decision(&instance.id, ScaleAction::Up(4))
            .await
            .unwrap();
        assert!(!retried);
        assert_eq!(client.last(), 3);

        let down_retry = controller
            .apply_engine_decision(&instance.id, ScaleAction::Down(2))
            .await
            .unwrap();
        assert!(!down_retry);
        assert_eq!(client.last(), 3);

        assert_eq!(*client.history(), vec![4, 3]);
    }

    /// Test that revert itself is not blocked by an existing cooldown, so a
    /// failed validation can still roll the pool back.
    #[cfg(feature = "threshold-engine")]
    #[rstest]
    #[tokio::test]
    async fn test_controller_revert_bypasses_existing_cooldown(#[from(mockdir)] _mockdir: MockDir) {
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
            cooldown_secs: 60,
            min_thread_count: 1,
            max_thread_count: 8,
            ..Default::default()
        };
        let client = Arc::new(VcpuLimitedClient::new(3));
        let instance = Arc::new(Instance::new(
            "revert-bypass".to_string(),
            PrefixedPath::new(""),
            0,
            Arc::clone(&client),
        ));
        set_test_state(&instance, 3, 8, Some(true)).await;
        let mut controller = Controller::new(
            cfg,
            Box::new(
                ThresholdEngine::new(
                    ThresholdConfig::default(),
                    &mut PollPeriod::new(1, false).converter(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
        controller
            .instances
            .insert(instance.id.clone(), Arc::clone(&instance));

        assert!(
            controller
                .apply_engine_decision(&instance.id, ScaleAction::Up(4))
                .await
                .unwrap()
        );
        assert_eq!(client.last(), 4);
        assert!(instance.status.read().await.cooldown_until.is_some());

        // Cooldown from the up must not trap the pool at the failed size.
        assert!(
            controller
                .apply_engine_decision(&instance.id, ScaleAction::Revert(3))
                .await
                .unwrap()
        );
        assert_eq!(client.last(), 3);
        assert_eq!(*client.history(), vec![3, 4, 3]);
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
                vcpu_count: 16,
                perf: None,
            })
        }

        async fn close(&self) {}
    }

    /// Test that `set_all` applies a target to every VM and reports how
    /// many failed when some clients error.
    #[rstest]
    #[tokio::test]
    async fn test_set_all_attempts_every_vm_and_reports_partial_failure(
        #[from(mockdir)] _mockdir: MockDir,
    ) {
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
            ..Default::default()
        };
        let engine = Box::new(default_threshold_engine());
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
        let good_instance = Arc::new(Instance::new(
            "visible".to_string(),
            PrefixedPath::new(""),
            0,
            good,
        ));
        let bad_instance = Arc::new(Instance::new(
            "hidden".to_string(),
            PrefixedPath::new(""),
            0,
            bad,
        ));
        set_test_state(&good_instance, 1, 16, None).await;
        set_test_state(&bad_instance, 1, 16, None).await;
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
        closed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl InstanceClient for SnapshotClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                // FIXME these weren't required, looked like broken due to rebase
                vcpu_count: 2,
                perf: None,
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
            _instance: &EngineInstanceSnapshot,
            _context: &EngineTickContext,
        ) -> ScaleAction {
            self.evaluated.fetch_add(1, Ordering::Relaxed);
            ScaleAction::None
        }

        async fn on_instance_added(&self, _instance_id: &str) {
            self.added.fetch_add(1, Ordering::Relaxed);
        }

        async fn on_instance_removed(&self, _instance_id: &str) {
            self.removed.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn instance(id: &str, closed: Arc<AtomicUsize>) -> Arc<Instance> {
        Arc::new(Instance::new(
            id.to_string(),
            PrefixedPath::new(""),
            1,
            SnapshotClient { closed },
        ))
    }

    /// Test that the per-VM status line has a fixed number of space-separated
    /// fields whether or not samples are available.
    #[test]
    fn test_per_vm_status_line_has_fixed_space_separated_fields() {
        let mut status = InstanceStatus::default();
        let empty = Controller::format_instance_status_line(&status, [None; 3], None);
        status.read_latency_us = Some(crate::instance::SnapshotLatency {
            p50: 1,
            p95: 2,
            p99: 3,
            avg: 4,
        });
        let full =
            Controller::format_instance_status_line(&status, [Some(1), Some(2), Some(3)], None);
        for line in [&empty, &full] {
            assert!(!line.contains('/'), "{line}");
            assert_eq!(line.split(' ').count(), 23, "{line}");
        }
        assert!(full.contains(" 1 2 3 4 - - - - "), "{full}");
    }

    /// Test that discovery sync adds new VMs, retains existing ones, and
    /// removes disappeared ones.
    #[rstest]
    #[tokio::test]
    async fn test_constroller_sync_instances_adds_retains_and_removes(
        #[from(mockdir)] _mockdir: MockDir,
    ) {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
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
        let (per_vm_log, _log_dir, _guard) = capture_per_vm_log();
        let closed = Arc::new(AtomicUsize::new(0));

        let first = instance("vm-a", Arc::clone(&closed));
        let (n_added, n_removed) = controller
            .sync_instances(vec![Arc::clone(&first)])
            .await
            .unwrap();
        assert_eq!((n_added, n_removed), (1, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(added.load(Ordering::Relaxed), 1);

        let duplicate = instance("vm-a", Arc::clone(&closed));
        let (n_added, n_removed) = controller.sync_instances(vec![duplicate]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 0));
        assert_eq!(controller.instances.len(), 1);
        assert_eq!(closed.load(Ordering::Relaxed), 1);

        let (n_added, n_removed) = controller.sync_instances(vec![]).await.unwrap();
        assert_eq!((n_added, n_removed), (0, 1));
        assert!(controller.instances.is_empty());
        assert_eq!(removed.load(Ordering::Relaxed), 1);
        assert_eq!(evaluated.load(Ordering::Relaxed), 0);
        assert_eq!(
            per_vm_log_events(&per_vm_log, "vm-a"),
            ["event=vm_added", "event=vm_removed reason=not_discovered"]
        );
    }

    /// Route this thread's per-VM events to files in a temporary directory.
    fn capture_per_vm_log() -> (
        PerVmLog,
        tempfile::TempDir,
        tracing::subscriber::DefaultGuard,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let per_vm_log = PerVmLog::default();
        per_vm_log.set_dir(Some(PrefixedPath::new(dir.path())));
        let guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(per_vm_log.clone()),
        );
        (per_vm_log, dir, guard)
    }

    /// Return the records of one VM's per-VM log that carry an `event` field,
    /// without timestamps.
    fn per_vm_log_events(per_vm_log: &PerVmLog, vm: &str) -> Vec<String> {
        std::fs::read_to_string(per_vm_log.path(vm).unwrap())
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once(' ').map(|(_, record)| record))
            .filter(|record| record.contains("event="))
            .map(str::to_string)
            .collect()
    }

    /// Test that one tick refreshes state, runs the engine, and drops instances
    /// that fail to refresh.
    #[rstest]
    #[tokio::test]
    async fn test_controller_tick_refreshes_evaluates_and_drops_failed_instances(
        #[from(mockdir)] _mockdir: MockDir,
    ) {
        let added = Arc::new(AtomicUsize::new(0));
        let removed = Arc::new(AtomicUsize::new(0));
        let evaluated = Arc::new(AtomicUsize::new(0));
        let cfg = Config {
            vm_state_path: PrefixedPath::new("/ownership.json"),
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
        let (per_vm_log, _log_dir, _guard) = capture_per_vm_log();

        let closed = Arc::new(AtomicUsize::new(0));
        let ok = instance("ok", Arc::clone(&closed));
        let bad = Arc::new(Instance::new(
            "bad".to_string(),
            PrefixedPath::new(""),
            2,
            FailingClient,
        ));
        controller.sync_instances(vec![ok, bad]).await.unwrap();
        assert_eq!(added.load(Ordering::Relaxed), 2);

        let task_dir = PrefixedPath::new("/proc/1/task/10");
        std::fs::create_dir_all(&task_dir).unwrap();
        std::fs::write(
            task_dir.join("stat"),
            "10 (worker0) S 1 1 1 0 -1 0 0 0 0 0 5 3 0 0 20 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 \
             0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n",
        )
        .unwrap();

        controller.tick().await.unwrap();
        assert_eq!(controller.tick_index, 1);
        assert_eq!(controller.instances.len(), 1);
        assert!(controller.instances.contains_key("ok"));
        assert!(controller.instances["ok"].status.read().await.thread_count > 0);
        assert_eq!(evaluated.load(Ordering::Relaxed), 1);
        assert_eq!(removed.load(Ordering::Relaxed), 1);
        assert_eq!(per_vm_log_events(&per_vm_log, "ok"), ["event=vm_added"]);
        assert_eq!(
            per_vm_log_events(&per_vm_log, "bad"),
            ["event=vm_added", "event=vm_removed reason=refresh_failed"]
        );
    }

    fn status_from_per_worker(per_worker: Vec<f64>) -> InstanceStatus {
        InstanceStatus {
            per_worker_util: per_worker,
            ..Default::default()
        }
    }

    fn status_with_fallback(
        per_worker: Vec<f64>,
        per_thread_util: f64,
        thread_count: u32,
    ) -> InstanceStatus {
        InstanceStatus {
            per_worker_util: per_worker,
            per_thread_util: Some(per_thread_util),
            thread_count,
            ..Default::default()
        }
    }

    fn status_fallback(per_thread_util: f64, thread_count: u32) -> InstanceStatus {
        status_with_fallback(Vec::new(), per_thread_util, thread_count)
    }

    // Strategy helpers: produce finite f64 in [0,1] and small worker vecs.
    fn util() -> impl Strategy<Value = f64> {
        0.0f64..=1.0f64
    }

    fn per_worker_vec() -> impl Strategy<Value = Vec<f64>> {
        prop::collection::vec(util(), 1..10)
    }

    proptest! {
        /// Test that when per_worker_util is non-empty the fallback inputs
        /// per_thread_util and thread_count are ignored.
        #[test]
        fn test_per_worker_branch_ignores_fallback(
            per_worker in per_worker_vec(),
            util_a in util(),
            util_b in util(),
            count_a in any::<u32>(),
            count_b in any::<u32>(),
        ) {
            let s_a = status_with_fallback(per_worker.clone(), util_a, count_a);
            let s_b = status_with_fallback(per_worker.clone(), util_b, count_b);
            let a = compute_cpu_stats(&s_a);
            let b = compute_cpu_stats(&s_b);
            // If the fallback inputs were taken into account, `a` and `b` would
            // differ when the fallback values are different.
            prop_assert_eq!(a.avg_pct, b.avg_pct);
            prop_assert_eq!(a.median_pct, b.median_pct);
            prop_assert_eq!(a.total_pct, b.total_pct);
        }

        /// Test that when per_worker_util is empty the output depends only on
        /// the fallback fields.
        #[test]
        fn test_fallback_avg_equals_median(
            per_thread_util in util(),
            thread_count in 0u32..32,
        ) {
            let s = status_fallback(per_thread_util, thread_count);
            let stats = compute_cpu_stats(&s);
            // Since we don't have fine-grained information, the thread
            // utilisation is treated like a `vec![per_thread_util;
            // thread_count]` meaning the following should hold.
            prop_assert_eq!(stats.avg_pct, stats.median_pct);
        }

        /// Test that the order of read thread utilisations is irrelevant.
        #[test]
        fn test_per_worker_permutation_invariance(per_worker in per_worker_vec()) {
            let s_orig = status_from_per_worker(per_worker.clone());
            let mut sorted = per_worker.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let s_sorted = status_from_per_worker(sorted.clone());
            let mut reversed = per_worker.clone();
            reversed.reverse();
            let s_reversed = status_from_per_worker(reversed);

            let orig = compute_cpu_stats(&s_orig);
            let sorted_stats = compute_cpu_stats(&s_sorted);
            let reversed_stats = compute_cpu_stats(&s_reversed);

            prop_assert_eq!(orig.avg_pct, sorted_stats.avg_pct);
            prop_assert_eq!(orig.median_pct, sorted_stats.median_pct);
            prop_assert_eq!(orig.total_pct, sorted_stats.total_pct);

            prop_assert_eq!(orig.avg_pct, reversed_stats.avg_pct);
            prop_assert_eq!(orig.median_pct, reversed_stats.median_pct);
            prop_assert_eq!(orig.total_pct, reversed_stats.total_pct);
        }

        /// Test that for a single element vector, avg == median == total.
        #[test]
        fn test_single_element_all_equal(x in util()) {
            let s = status_from_per_worker(vec![x]);
            let stats = compute_cpu_stats(&s);
            prop_assert_eq!(stats.avg_pct, stats.median_pct);
            prop_assert_eq!(stats.median_pct, stats.total_pct);
        }

        /// Test that for a two element vector, avg == median.
        #[test]
        fn test_two_elements_median_equals_avg(a in util(), b in util()) {
            let s = status_from_per_worker(vec![a, b]);
            let stats = compute_cpu_stats(&s);
            prop_assert_eq!(stats.avg_pct, stats.median_pct);
        }

        /// Test that for a uniform vector, avg == median.
        #[test]
        fn test_uniform_vector_avg_equals_median(
            x in util(),
            n in 1usize..20,
        ) {
            let per_worker = vec![x; n];
            let s = status_from_per_worker(per_worker);
            let stats = compute_cpu_stats(&s);
            prop_assert_eq!(stats.avg_pct, stats.median_pct);
        }

        /// Test that duplicating the utilization vector preserves the average
        /// and median while doubling the total.
        #[test]
        fn test_duplication_preserves_avg_median(per_worker in per_worker_vec()) {
            let s_orig = status_from_per_worker(per_worker.clone());
            let mut doubled = per_worker.clone();
            doubled.extend(per_worker.clone());
            let s_doubled = status_from_per_worker(doubled);

            let orig = compute_cpu_stats(&s_orig);
            let doubled_stats = compute_cpu_stats(&s_doubled);
            prop_assert_eq!(orig.avg_pct, doubled_stats.avg_pct);
            prop_assert_eq!(orig.median_pct, doubled_stats.median_pct);
            prop_assert!(doubled_stats.total_pct >= orig.total_pct);
        }

        /// Test that raising any worker's utilisation cannot decrease any of
        /// the counters.
        #[test]
        fn test_per_worker_monotone_in_one_element(
            per_worker in per_worker_vec(),
            delta in 0.0f64..=1.0f64,
        ) {
            // pick index deterministically from delta to keep strategy simple
            let idx = (delta * 1000.0) as usize % per_worker.len();
            let old = per_worker[idx];
            let new_val = (old + delta).min(1.0);
            prop_assume!(old < new_val);

            let mut increased = per_worker.clone();
            increased[idx] = new_val;

            let s_old = status_from_per_worker(per_worker);
            let s_new = status_from_per_worker(increased);
            let old_stats = compute_cpu_stats(&s_old);
            let new_stats = compute_cpu_stats(&s_new);

            prop_assert!(new_stats.avg_pct >= old_stats.avg_pct);
            prop_assert!(new_stats.total_pct >= old_stats.total_pct);
            prop_assert!(new_stats.median_pct >= old_stats.median_pct);
        }

        /// Test that for a well-formed utilization vector, the metrics remain
        /// in bounds.
        #[test]
        fn test_per_worker_bounds_and_total_dominance(per_worker in per_worker_vec()) {
            let s = status_from_per_worker(per_worker.clone());
            let stats = compute_cpu_stats(&s);
            let n = per_worker.len() as u64;
            prop_assert!(stats.avg_pct <= 100, "avg {} >100 for {:?}", stats.avg_pct, per_worker);
            prop_assert!(stats.median_pct <= 100, "median {} >100 for {:?}", stats.median_pct, per_worker);
            prop_assert!(stats.total_pct <= n * 100, "total {} > n*100={} for {:?}", stats.total_pct, n*100, per_worker);
            prop_assert!(stats.total_pct >= stats.avg_pct, "total {} < avg {} for {:?}", stats.total_pct, stats.avg_pct, per_worker);
            prop_assert!(stats.total_pct >= stats.median_pct, "total {} < median {} for {:?}", stats.total_pct, stats.median_pct, per_worker);
        }

        /// Test that average and median are between the min and max util
        /// samples.
        #[test]
        fn test_avg_median_between_min_max(per_worker in per_worker_vec()) {
            let s = status_from_per_worker(per_worker.clone());
            let stats = compute_cpu_stats(&s);

            let min_val = per_worker.iter().cloned().fold(f64::INFINITY, f64::min);
            let max_val = per_worker.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let min_pct = (min_val * 100.0).round() as u64;
            let max_pct = (max_val * 100.0).round() as u64;

            prop_assert!(stats.avg_pct >= min_pct && stats.avg_pct <= max_pct,
                "avg {} not in [{}, {}] for {:?}", stats.avg_pct, min_pct, max_pct, per_worker);
            prop_assert!(stats.median_pct >= min_pct && stats.median_pct <= max_pct,
                "median {} not in [{}, {}] for {:?}", stats.median_pct, min_pct, max_pct, per_worker);
        }

        /// Test that the fallback counters monotonically increase stats.
        #[test]
        fn test_fallback_total_monotone(
            per_thread_util in util(),
            count_a in 0u32..32,
            count_b in 0u32..32,
            util_lo in util(),
            util_hi in util(),
        ) {
            prop_assume!(count_a < count_b);
            prop_assume!(util_lo <= util_hi);

            let s_lo = status_fallback(per_thread_util, count_a);
            let s_hi = status_fallback(per_thread_util, count_b);
            prop_assert!(compute_cpu_stats(&s_hi).total_pct >= compute_cpu_stats(&s_lo).total_pct);

            let lo_stats = compute_cpu_stats(&s_lo);
            let hi_stats = compute_cpu_stats(&s_hi);
            prop_assert!(hi_stats.avg_pct >= lo_stats.avg_pct);
            prop_assert!(hi_stats.median_pct >= lo_stats.median_pct);
            prop_assert!(hi_stats.total_pct >= lo_stats.total_pct);
        }

        /// Test that fallback values keep the metrics in bounds.
        #[test]
        fn prop_fallback_bounds(
            per_thread_util in util(),
            thread_count in 1u32..32,
        ) {
            prop_assume!(per_thread_util > 0.0);
            let s = status_fallback(per_thread_util, thread_count);
            let stats = compute_cpu_stats(&s);
            prop_assert!(stats.avg_pct <= 100);
            prop_assert!(stats.median_pct <= 100);
            prop_assert!(stats.total_pct <= thread_count as u64 * 100);
            prop_assert!(stats.total_pct >= stats.avg_pct);
            prop_assert!(stats.total_pct >= stats.median_pct);
        }
    }

    fn vq_small() -> impl Strategy<Value = Vec<u64>> {
        prop::collection::vec(0u64..10_000, 1..20)
    }

    fn vq_bounded() -> impl Strategy<Value = Vec<u64>> {
        prop::collection::vec(0u64..1_000_000, 1..32)
    }

    /// Test that empty and None inputs return None.
    #[test]
    fn test_vq_none_and_empty_yield_all_none() {
        assert_eq!(summarize_vq_depths(None), (None, None, None, None));

        let empty: Vec<u64> = vec![];
        assert_eq!(summarize_vq_depths(Some(&empty)), (None, None, None, None));
    }

    proptest! {
        /// Test that a non-empty input always yields all-Some and count == len.
        #[test]
        fn test_vq_nonempty_yields_all_some_and_count_is_len(
            depths in vq_small(),
        ) {
            let (count, total, avg, median) = summarize_vq_depths(Some(&depths));
            prop_assert!(count.is_some());
            prop_assert!(total.is_some());
            prop_assert!(avg.is_some());
            prop_assert!(median.is_some());
            prop_assert_eq!(count.unwrap() as usize, depths.len());

            // Appending one element must increase count by exactly one.
            let mut extended = depths.clone();
            extended.push(42);
            let (count2, _, _, _) = summarize_vq_depths(Some(&extended));
            prop_assert_eq!(count2.unwrap(), count.unwrap() + 1);
        }

        /// Test that a single element vector is the identity for all four
        /// outputs.
        #[test]
        fn test_vq_singleton_is_identity(v in 0u64..1_000_000) {
            let (count, total, avg, median) = summarize_vq_depths(Some(&[v]));
            prop_assert_eq!(count, Some(1));
            prop_assert_eq!(total, Some(v));
            // average_x100 = v * 100 / 1
            prop_assert_eq!(avg, Some(v * 100));
            prop_assert_eq!(median, Some(v));
        }

        /// Test that a uniform vector has predictable total/avg/median.
        #[test]
        fn test_vq_uniform_vector_properties(
            v in 0u64..10_000,
            n in 1usize..20,
        ) {
            let depths = vec![v; n];
            let (count, total, avg, median) = summarize_vq_depths(Some(&depths));
            prop_assert_eq!(count, Some(n as u32));

            let expected_total = v * n as u64;
            prop_assert_eq!(total, Some(expected_total));
            prop_assert_eq!(avg, Some(v * 100));
            prop_assert_eq!(median, Some(v));
        }

        /// Test that two elements combine with saturating add and the median
        /// is floor((a+b)/2).
        #[test]
        fn test_vq_two_elements_total_and_median(
            a in 0u64..1_000_000,
            b in 0u64..1_000_000,
        ) {
            let (count, total, avg, median) = summarize_vq_depths(Some(&[a, b]));
            prop_assert_eq!(count, Some(2));
            prop_assert_eq!(total, Some(a.saturating_add(b)));
            // average_x100 = (a+b)*100/2
            let expected_avg = ((u128::from(a.saturating_add(b)) * 100) / 2) as u64;
            prop_assert_eq!(avg, Some(expected_avg));
            let expected_median = ((a as f64 + b as f64) / 2.0).floor() as u64;
            prop_assert_eq!(median, Some(expected_median));
        }

        /// Test that the result is invariant under permutation.
        #[test]
        fn test_vq_permutation_invariance(depths in vq_bounded()) {
            let (c0, t0, a0, m0) = summarize_vq_depths(Some(&depths));
            let mut sorted = depths.clone();
            sorted.sort_unstable();
            let (c1, t1, a1, m1) = summarize_vq_depths(Some(&sorted));
            prop_assert_eq!(c0, c1);
            prop_assert_eq!(t0, t1);
            prop_assert_eq!(a0, a1);
            prop_assert_eq!(m0, m1);

            let mut reversed = depths.clone();
            reversed.reverse();
            let (c2, t2, a2, m2) = summarize_vq_depths(Some(&reversed));
            prop_assert_eq!(c0, c2);
            prop_assert_eq!(t0, t2);
            prop_assert_eq!(a0, a2);
            prop_assert_eq!(m0, m2);
        }

        /// Test that duplicating any vector preserves avg/median and doubles
        /// total via saturating arithmetic.
        #[test]
        fn test_vq_duplication_preserves_avg_median(depths in vq_small()) {
            let (c0, t0, a0, m0) = summarize_vq_depths(Some(&depths));
            let mut doubled = depths.clone();
            doubled.extend(depths.clone());
            let (c1, t1, a1, m1) = summarize_vq_depths(Some(&doubled));
            prop_assert_eq!(c1.unwrap(), c0.unwrap() * 2);
            prop_assert_eq!(t1, Some(t0.unwrap().saturating_mul(2)));
            prop_assert_eq!(a0, a1);
            prop_assert_eq!(m0, m1);
        }

        /// Test that total is compositional via saturating addition.
        #[test]
        fn test_vq_concatenation_total_is_saturating_sum(
            a in vq_small(),
            b in vq_small(),
        ) {
            let (t_a, ) = {
                let (_, t, _, _) = summarize_vq_depths(Some(&a));
                (t.unwrap(),)
            };
            let (t_b, ) = {
                let (_, t, _, _) = summarize_vq_depths(Some(&b));
                (t.unwrap(),)
            };
            let mut concat = a.clone();
            concat.extend(b.clone());
            let (c_concat, t_concat, _, _) = summarize_vq_depths(Some(&concat));
            prop_assert_eq!(t_concat.unwrap(), t_a.saturating_add(t_b));
            prop_assert_eq!(c_concat.unwrap() as usize, a.len() + b.len());
        }

        /// Test that appending an element updates count and total predictably.
        #[test]
        fn test_vq_append_increments_count_and_total(
            depths in vq_small(),
            x in 0u64..10_000,
        ) {
            let (_, t0, _, _) = summarize_vq_depths(Some(&depths));
            let mut extended = depths.clone();
            extended.push(x);
            let (c1, t1, _, _) = summarize_vq_depths(Some(&extended));
            prop_assert_eq!(c1.unwrap() as usize, depths.len() + 1);
            prop_assert_eq!(t1.unwrap(), t0.unwrap().saturating_add(x));
        }

        /// Test that avg_x100 lies between min*100 and max*100.
        #[test]
        fn test_vq_average_between_min_max(depths in vq_bounded()) {
            let (_, _, avg, _) = summarize_vq_depths(Some(&depths));
            let avg = avg.unwrap();
            let min = *depths.iter().min().unwrap();
            let max = *depths.iter().max().unwrap();
            prop_assert!(avg >= min * 100, "avg {} < min*100 {} for {:?}", avg, min*100, depths);
            prop_assert!(avg <= max * 100, "avg {} > max*100 {} for {:?}", avg, max*100, depths);
        }

        /// Test that total dominates each element and determines avg_x100 zero
        /// case.
        #[test]
        fn test_vq_total_dominates_elements(depths in vq_small()) {
            let (_, total, _, _) = summarize_vq_depths(Some(&depths));
            let total = total.unwrap();
            let max = *depths.iter().max().unwrap();
            prop_assert!(total >= max, "total {} < max {} for {:?}", total, max, depths);
            for &v in &depths {
                prop_assert!(total >= v, "total {} < element {} for {:?}", total, v, depths);
            }
            let all_zero = depths.iter().all(|&v| v == 0);
            prop_assert_eq!(total == 0, all_zero);
        }

        /// Test that median lies between min and max and never exceeds total.
        #[test]
        fn test_vq_median_between_min_max_and_less_than_total(depths in vq_small()) {
            let (_, total, _, median) = summarize_vq_depths(Some(&depths));
            let total = total.unwrap();
            let median = median.unwrap();
            let min = *depths.iter().min().unwrap();
            let max = *depths.iter().max().unwrap();
            prop_assert!(median >= min, "median {} < min {} for {:?}", median, min, depths);
            prop_assert!(median <= max, "median {} > max {} for {:?}", median, max, depths);
            // For non-negative depths, total is at least max, so median <= total.
            // This also holds when total is saturated to MAX.
            prop_assert!(median <= total, "median {} > total {} for {:?}", median, total, depths);
        }

        /// Test that total and median are monotone when raising one element.
        #[test]
        fn test_vq_monotone_when_raising_one_element(
            depths in vq_bounded(),
            delta in 1u64..10_000,
        ) {
            let idx = depths.len() / 2; // deterministic index
            let old = depths[idx];
            let new_val = old.saturating_add(delta);
            prop_assume!(new_val != old);
            let mut raised = depths.clone();
            raised[idx] = new_val;

            let (_, t_old, a_old, m_old) = summarize_vq_depths(Some(&depths));
            let (_, t_new, a_new, m_new) = summarize_vq_depths(Some(&raised));

            prop_assert!(t_new.unwrap() >= t_old.unwrap(),
                "total not monotone: {} -> {} for {:?} delta {}", t_old.unwrap(), t_new.unwrap(), depths, delta);
            prop_assert!(a_new.unwrap() >= a_old.unwrap(),
                "avg not monotone: {} -> {} for {:?} delta {}", a_old.unwrap(), a_new.unwrap(), depths, delta);
            // Median should not decrease when any element increases.
            prop_assert!(m_new.unwrap() >= m_old.unwrap(),
                "median not monotone: {} -> {} for {:?} delta {}", m_old.unwrap(), m_new.unwrap(), depths, delta);
        }

        /// Test that saturating addition is used for total and does not wrap.
        #[test]
        fn test_vq_saturating_total_does_not_wrap(
            prefix in prop::collection::vec(0u64..1_000, 0..5),
        ) {
            // Two large values that would wrap on normal addition.
            let depths = {
                let mut v = prefix.clone();
                v.push(u64::MAX);
                v.push(1);
                v
            };
            let (_, total, _, _) = summarize_vq_depths(Some(&depths));
            prop_assert_eq!(total, Some(u64::MAX));
        }
    }

    fn cpu_sample(busy: u64, total: u64) -> HostCpuSample {
        HostCpuSample {
            busy_ticks: busy,
            total_ticks: total,
        }
    }

    /// Test that host CPU utilisation is the clamped busy/total delta ratio.
    #[rstest]
    #[case::zero_total(cpu_sample(10, 10), cpu_sample(10, 10), 0.0)]
    #[case::counter_reset(cpu_sample(5, 20), cpu_sample(2, 10), 0.0)]
    #[case::zero_busy(cpu_sample(0, 0), cpu_sample(0, 10), 0.0)]
    #[case::full(cpu_sample(0, 0), cpu_sample(10, 10), 1.0)]
    #[case::half(cpu_sample(0, 0), cpu_sample(5, 10), 0.5)]
    #[case::clamp(cpu_sample(0, 0), cpu_sample(20, 10), 1.0)]
    fn test_host_cpu_utilisation(
        #[case] prev: HostCpuSample,
        #[case] cur: HostCpuSample,
        #[case] expected: f64,
    ) {
        assert_eq!(host_cpu_utilisation(prev, cur), expected);
    }

    /// Contents of a valid pressure file.
    fn psi_content(avg10: f32) -> String {
        format!(
            "some avg10={avg10:.2} avg60=0.00 avg300=0.00 total=100\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
        )
    }

    /// Write a valid pressure file.
    fn write_pressure_file(path: PrefixedPath, avg10: f32) {
        let p: &std::path::Path = &path;
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(p, psi_content(avg10)).unwrap();
    }

    proptest! {
        /// Test that `read_psi_sample` round-trips arbitrary pressure values
        /// through the file format.
        #[test]
        fn test_read_psi_sample_proptest_roundtrip(
            cpu in 0.0f32..100.0,
            io in 0.0f32..100.0,
            mem in 0.0f32..100.0,
        ) {
            let _mockdir = MockDir::new();
            write_pressure_file(PrefixedPath::new("/proc/pressure/cpu"), cpu);
            write_pressure_file(PrefixedPath::new("/proc/pressure/io"), io);
            write_pressure_file(PrefixedPath::new("/proc/pressure/memory"), mem);

            let sample = read_psi_sample();
            // psi_content formats with two decimals; procfs parses as f32 then casts to f64
            let expected_cpu = format!("{cpu:.2}").parse::<f32>().unwrap() as f64;
            let expected_io = format!("{io:.2}").parse::<f32>().unwrap() as f64;
            let expected_mem = format!("{mem:.2}").parse::<f32>().unwrap() as f64;
            prop_assert!((sample.cpu_some_avg10 - expected_cpu).abs() < 1e-6);
            prop_assert!((sample.io_some_avg10 - expected_io).abs() < 1e-6);
            prop_assert!((sample.memory_some_avg10 - expected_mem).abs() < 1e-6);
        }

        /// Test that `read_psi_sample` handles randomly present or absent
        /// pressure files with arbitrary values.
        #[test]
        fn test_read_psi_sample_proptest_optional_files(
            cpu_opt in proptest::option::of(0.0f32..100.0),
            io_opt in proptest::option::of(0.0f32..100.0),
            mem_opt in proptest::option::of(0.0f32..100.0),
        ) {
            let _mockdir = MockDir::new();
            if let Some(v) = cpu_opt {
                write_pressure_file(PrefixedPath::new("/proc/pressure/cpu"), v);
            }
            if let Some(v) = io_opt {
                write_pressure_file(PrefixedPath::new("/proc/pressure/io"), v);
            }
            if let Some(v) = mem_opt {
                write_pressure_file(PrefixedPath::new("/proc/pressure/memory"), v);
            }

            let sample = read_psi_sample();
            let expected = |opt: Option<f32>| opt.map(|v| format!("{v:.2}").parse::<f32>().unwrap() as f64).unwrap_or(0.0);
            prop_assert!((sample.cpu_some_avg10 - expected(cpu_opt)).abs() < 1e-6);
            prop_assert!((sample.io_some_avg10 - expected(io_opt)).abs() < 1e-6);
            prop_assert!((sample.memory_some_avg10 - expected(mem_opt)).abs() < 1e-6);
        }
    }
}
