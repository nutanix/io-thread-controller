// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Backend-neutral data model for one VM and its backend-owned client.
//!
//! [`Instance`] is a record, not a backend abstraction: it holds identity,
//! current state, and one [`InstanceClient`] that controls exactly that VM.
//! A [`crate::backends::Backend`] is the fleet-level adapter that owns
//! backend-wide configuration and discovers zero or more such records.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::LazyLock,
    time::Instant,
};

use async_trait::async_trait;
use procfs::{ProcError, process::Process};
use regex::Regex;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    backends::{BackendClientError, IoThreadProperties, VqMapping},
    rolling::RollingMetrics,
    util::Path,
};

#[derive(Debug, Error)]
pub enum InstanceError {
    #[error(transparent)]
    Regex(#[from] regex::Error),
}

/// Operations and observations supplied by one backend VM client.
#[async_trait]
pub trait InstanceClient: Send + Sync {
    /// Set this VM's I/O worker count.
    async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError>;

    /// Fetch the current worker-pool snapshot.
    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError>;

    /// Classify whether automatic scaling owns a VM's initial worker pool.
    ///
    /// The controller calls this once after the first usable snapshot of a
    /// previously unknown VM and persists the result across daemon restarts.
    /// The default keeps existing backend implementations permissive.
    fn initial_pool_is_managed(&self, _thread_count: u32, _vcpu_count: u32) -> bool {
        true
    }

    /// Close or invalidate the underlying transport.
    async fn close(&self);

    /// Create a named IOThread.
    async fn add_io_thread(
        &self,
        _id: &str,
        _properties: Option<&IoThreadProperties>,
    ) -> Result<(), BackendClientError> {
        Err(BackendClientError::NotSupported("add IOThread".to_string()))
    }

    /// Delete a named IOThread.
    async fn del_io_thread(&self, _id: &str) -> Result<(), BackendClientError> {
        Err(BackendClientError::NotSupported(
            "delete IOThread".to_string(),
        ))
    }

    /// Replace a device's virtqueue-to-IOThread mapping.
    async fn set_io_thread_vq_mapping(
        &self,
        _device: &str,
        _mapping: &[VqMapping],
    ) -> Result<(), BackendClientError> {
        Err(BackendClientError::NotSupported(
            "set IOThread virtqueue mapping".to_string(),
        ))
    }

    /// Fetch a device's virtqueue-to-IOThread mapping.
    async fn get_io_thread_vq_mapping(
        &self,
        _device: &str,
    ) -> Result<Vec<VqMapping>, BackendClientError> {
        Err(BackendClientError::NotSupported(
            "get IOThread virtqueue mapping".to_string(),
        ))
    }
}

#[async_trait]
impl<T> InstanceClient for Box<T>
where
    T: InstanceClient + ?Sized,
{
    async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
        (**self).set_thread_count(count).await
    }

    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
        (**self).get_thread_pool_snapshot().await
    }

    fn initial_pool_is_managed(&self, thread_count: u32, vcpu_count: u32) -> bool {
        (**self).initial_pool_is_managed(thread_count, vcpu_count)
    }

    async fn close(&self) {
        (**self).close().await;
    }
}

/// One VM tracked by the controller.
pub struct Instance {
    /// Stable identifier used by inventory, logs, and engine plans.
    pub id: String,
    /// Backend-owned path or URI identifying this VM's transport.
    pub sock_path: Path,
    /// Backend process ID.
    pub pid: i32,
    /// Backend-selected task names included in fallback CPU sampling.
    pub thread_name_filter: ThreadNameFilter,
    /// Per-VM backend implementation.
    pub client: Box<dyn InstanceClient>,
    /// Latest snapshot shared by refresh, evaluation, and status output.
    ///
    /// The lock permits all VM refresh and evaluation futures to share their
    /// records safely.
    pub status: RwLock<InstanceStatus>,
}

#[derive(Debug, Error)]
pub enum CgroupError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("missing file `{0}`")]
    MissingFile(String),

    #[error("missing field `{0}`")]
    MissingField(String),

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Proc(#[from] ProcError),
}
#[derive(Debug, thiserror::Error)]
enum CpuSampleError {
    #[error("no usable backend task CPU samples")]
    NoCpuSamples,

    #[error(transparent)]
    Proc(#[from] procfs::ProcError),
}
/// The constant tick rate used for process stats from /proc.
static TICKS_PER_SECOND: LazyLock<f64> = LazyLock::new(|| procfs::ticks_per_second() as f64);

impl Instance {
    /// Construct a VM record with an empty initial status.
    ///
    /// id: the VM ID
    /// sock_path: /path/to/sock
    /// pid: PID of the storage backend
    /// client: the client handling this instance
    pub fn new(
        id: String,
        sock_path: Path,
        pid: i32,
        client: impl InstanceClient + 'static,
    ) -> Self {
        Self {
            id,
            sock_path,
            pid,
            thread_name_filter: ThreadNameFilter::default(),
            client: Box::new(client),
            status: RwLock::new(InstanceStatus {
                scaling_allowed: true,
                ..Default::default()
            }),
        }
    }

    /// Replace the default all-task CPU sampling filter.
    pub fn with_thread_name_filter(mut self, filter: ThreadNameFilter) -> Self {
        self.thread_name_filter = filter;
        self
    }

    /// Refresh one VM and mark its client broken on failure.
    #[tracing::instrument(skip(self), fields(id = %self.id))]
    pub async fn refresh_state(&self, refresh_cgroup: bool) -> bool {
        match self.client.get_thread_pool_snapshot().await {
            Ok(snapshot) => {
                self.apply_thread_pool_snapshot(snapshot, refresh_cgroup)
                    .await
            }
            Err(error) => {
                tracing::warn!(
                    target: "controller",
                    %error,
                    "refresh failed; removing instance"
                );
                self.status.write().await.alive = false;
                self.client.close().await;
                false
            }
        }
    }

    /// Apply one successful thread-pool snapshot to this instance.
    async fn apply_thread_pool_snapshot(
        &self,
        snapshot: ThreadPoolSnapshot,
        refresh_cgroup: bool,
    ) -> bool {
        let cpu = if snapshot.per_thread_util.is_none() {
            match read_cpu_sample(self.pid, &self.thread_name_filter) {
                Ok(sample) => Some(sample),
                Err(error) => {
                    tracing::warn!(
                        target: "controller",
                        %error,
                        "failed to sample backend task CPU"
                    );
                    None
                }
            }
        } else {
            None
        };
        let previous_cgroup = self.status.read().await.cgroup;
        let cgroup = if refresh_cgroup || previous_cgroup.is_none() {
            read_cgroup_sample(self.pid).ok()
        } else {
            previous_cgroup
        };
        let now = Instant::now();
        let mut status = self.status.write().await;
        if status.ownership_classification.is_none() {
            status.ownership_classification = Some(
                self.client
                    .initial_pool_is_managed(snapshot.thread_count, snapshot.vcpu_count),
            );
        }
        Self::update_perf_rates(&mut status, &snapshot.perf, now);
        status.thread_count = snapshot.thread_count;
        status.vcpu_count = snapshot.vcpu_count;
        status.perf = snapshot.perf.clone();
        status.read_latency_us = snapshot.perf.as_ref().and_then(|perf| perf.read_latency_us);
        status.write_latency_us = snapshot
            .perf
            .as_ref()
            .and_then(|perf| perf.write_latency_us);
        status.alive = true;
        let backend_util = snapshot.per_thread_util.map(|util| util.clamp(0.0, 1.0));
        if let Some(backend_util) = backend_util {
            status.per_worker_util.clear();
            status.last_worker_names = None;
            status.per_thread_util = backend_util;
        } else if let (Some(previous), Some(current)) =
            (status.last_cpu_sample.as_ref(), cpu.as_ref())
        {
            let wall_ticks = current
                .sampled_at
                .checked_duration_since(previous.sampled_at)
                .map(|elapsed| elapsed.as_secs_f64() * *TICKS_PER_SECOND)
                .filter(|ticks| *ticks > 0.0);
            if let Some(wall_ticks) = wall_ticks {
                let worker_util =
                    compute_per_worker_util(&previous.per_worker, &current.per_worker, wall_ticks);
                update_worker_utilisation(&mut status, worker_util);
            }
        }
        let io_ops_total = match snapshot.perf {
            Some(perf) => perf
                .read_io_count
                .saturating_add(perf.write_io_count)
                .saturating_add(perf.other_io_count),
            None => 0,
        };
        if let Some(per_thread_util) = backend_util {
            status.rolling.push_from_backend_util(
                now,
                io_ops_total,
                per_thread_util,
                snapshot.thread_count,
            );
        } else if let Some(current) = cpu.as_ref() {
            status.rolling.push_from_procfs_delta(
                now,
                io_ops_total,
                current.cpu_ticks,
                *TICKS_PER_SECOND,
            );
        }
        status.last_cpu_sample = if backend_util.is_none() { cpu } else { None };
        status.throttled_usec_delta = match (status.cgroup, cgroup) {
            (Some(previous), Some(current)) => current
                .throttled_usec
                .saturating_sub(previous.throttled_usec),
            _ => 0,
        };
        status.cgroup = cgroup;
        true
    }

    /// Refresh per-tick rates from cumulative backend counters.
    fn update_perf_rates(
        status: &mut InstanceStatus,
        perf: &Option<InstancePerfSample>,
        now: Instant,
    ) {
        let (read_io_count, write_io_count, other_io_count, read_bytes_total, write_bytes_total) =
            match perf {
                Some(perf) => (
                    perf.read_io_count,
                    perf.write_io_count,
                    perf.other_io_count,
                    perf.read_bytes_total,
                    perf.write_bytes_total,
                ),
                None => (0, 0, 0, 0, 0),
            };
        if let Some(previous_time) = status.previous_perf_time {
            let elapsed_ns = now
                .checked_duration_since(previous_time)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            if elapsed_ns > 0 {
                let rate = |current: u64, previous: u64| -> u64 {
                    let delta = current.saturating_sub(previous);
                    ((delta as u128).saturating_mul(1_000_000_000) / elapsed_ns) as u64
                };
                status.read_iops = rate(read_io_count, status.previous_read_io_count);
                status.write_iops = rate(write_io_count, status.previous_write_io_count);
                status.other_iops = rate(other_io_count, status.previous_other_io_count);
                status.read_bytes_per_second =
                    rate(read_bytes_total, status.previous_read_bytes_total);
                status.write_bytes_per_second =
                    rate(write_bytes_total, status.previous_write_bytes_total);
            }
        }
        status.previous_perf_time = Some(now);
        status.previous_read_io_count = read_io_count;
        status.previous_write_io_count = write_io_count;
        status.previous_other_io_count = other_io_count;
        status.previous_read_bytes_total = read_bytes_total;
        status.previous_write_bytes_total = write_bytes_total;
    }
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id.trim())
    }
}

/// Regex classifier for backend task names read from `/proc/.../comm`.
#[derive(Debug, Default, Clone)]
pub struct ThreadNameFilter {
    match_regex: Option<Regex>,
    ignored_names: HashSet<String>,
}

impl ThreadNameFilter {
    /// Compile match patterns and collect exact names to ignore.
    pub fn new(pattern: &str, ignored_names: &[String]) -> Result<Self, InstanceError> {
        let match_regex = if pattern.is_empty() {
            None
        } else {
            let pattern = format!("(?:{pattern})");
            Some(Regex::new(&pattern)?)
        };
        Ok(Self {
            match_regex,
            ignored_names: ignored_names.iter().cloned().collect(),
        })
    }

    /// Return whether a task name belongs in CPU sampling.
    ///
    /// An empty match pattern (`None`) includes every name that is
    /// not in `ignored_names`.
    pub fn matches(&self, task_name: &str) -> bool {
        if self.ignored_names.contains(task_name) {
            return false;
        }
        self.match_regex
            .as_ref()
            .map(|regex| regex.is_match(task_name))
            .unwrap_or(true)
    }
}

/// Cgroup v2 CPU quota and cumulative throttle counters.
#[derive(Debug, Default, Clone, Copy)]
pub struct CgroupSample {
    /// `cpu.max` quota as a fraction of one core, or infinity for `max`.
    pub quota_cores: f64,
    /// Cumulative number of throttling periods from `cpu.stat`.
    pub nr_throttled: u64,
    /// Cumulative throttled CPU time in microseconds.
    pub throttled_usec: u64,
}

/// Most recent mutable state for one VM.
#[derive(Debug, Default, Clone)]
pub struct InstanceStatus {
    /// Current backend worker count.
    pub thread_count: u32,
    /// Current backend-reported vCPU count.
    pub vcpu_count: u32,
    /// Whether the last refresh succeeded.
    pub alive: bool,
    /// Whether backend ownership policy permits automatic scaling.
    /// FIXME this was removed no back again?
    pub scaling_allowed: bool,
    /// Persisted ownership classification, or `None` before the first usable
    /// snapshot of a previously unknown VM.
    pub ownership_classification: Option<bool>,
    /// Earliest time at which another ordinary scale action may be applied.
    pub cooldown_until: Option<Instant>,
    /// Process-local debug override suppressing automatic scaling.
    ///
    /// The override is deliberately cleared when the daemon restarts.
    pub manual_scaling_sticky: bool,
    /// Latest backend performance counters, [`None`] if the backend didn't
    /// provide any.
    pub perf: Option<InstancePerfSample>,
    /// Latest average CPU utilisation per backend task, from 0.0 to 1.0.
    pub per_thread_util: f64,
    /// Previous cumulative CPU sample used to compute a delta.
    pub last_cpu_sample: Option<CpuSample>,
    /// Latest comparable utilisation fraction for each sampled worker.
    pub per_worker_util: Vec<f64>,
    /// Sorted names from the latest worker sample, for roster-change logging.
    pub last_worker_names: Option<Vec<String>>,
    /// Latest cgroup v2 CPU quota and throttle counters.
    pub cgroup: Option<CgroupSample>,
    /// Increase in throttled CPU time since the preceding cgroup sample.
    pub throttled_usec_delta: u64,
    /// Bounded 1m/5m/15m I/O and CPU history.
    pub rolling: RollingMetrics,
    /// Time of the previous backend performance snapshot.
    pub previous_perf_time: Option<Instant>,
    /// Previous cumulative read count.
    pub previous_read_io_count: u64,
    /// Previous cumulative write count.
    pub previous_write_io_count: u64,
    /// Previous cumulative other-operation count.
    pub previous_other_io_count: u64,
    /// Previous cumulative read-byte count.
    pub previous_read_bytes_total: u64,
    /// Previous cumulative write-byte count.
    pub previous_write_bytes_total: u64,
    /// Latest read rate in operations per second.
    pub read_iops: u64,
    /// Latest write rate in operations per second.
    pub write_iops: u64,
    /// Latest other-operation rate per second.
    pub other_iops: u64,
    /// Latest read-latency histogram digest.
    pub read_latency_us: Option<LatencySummary>,
    /// Latest write-latency histogram digest.
    pub write_latency_us: Option<LatencySummary>,
    /// Latest read bandwidth in bytes per second.
    pub read_bytes_per_second: u64,
    /// Latest write bandwidth in bytes per second.
    pub write_bytes_per_second: u64,
}

impl InstanceStatus {
    pub fn scaling_allowed(&self) -> bool {
        self.ownership_classification.unwrap_or(false)
    }
}

/// Cumulative CPU counters sampled across one backend process.
#[derive(Debug, Clone)]
pub struct CpuSample {
    /// Sum of user and system CPU ticks across sampled tasks.
    pub cpu_ticks: u64,
    /// Monotonic time at which this sample was taken.
    pub sampled_at: Instant,
    /// Number of tasks included in the sample.
    pub thread_count: u32,
    /// Cumulative CPU counters for individual sampled tasks.
    pub per_worker: Vec<TaskCpuSample>,
}

/// One task's identity, name, and cumulative CPU counter.
#[derive(Debug, Clone)]
pub struct TaskCpuSample {
    /// Linux task identifier.
    pub tid: i32,
    /// Task name read from `/proc/.../comm`.
    pub name: String,
    /// Cumulative user and system CPU ticks.
    pub cpu_ticks: u64,
}

/// Backend-neutral latency histogram digest in microseconds.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
pub struct LatencySummary {
    /// Median latency.
    pub p50: u64,
    /// 95th-percentile latency.
    pub p95: u64,
    /// 99th-percentile latency.
    pub p99: u64,
    /// Histogram-derived arithmetic mean.
    pub avg: u64,
}

/// Backend-neutral performance counters from one snapshot.
#[derive(Debug, Default, Clone)]
pub struct InstancePerfSample {
    /// Cumulative completed reads.
    pub read_io_count: u64,
    /// Cumulative completed writes.
    pub write_io_count: u64,
    /// Cumulative completed non-read/write operations.
    pub other_io_count: u64,
    /// Cumulative bytes read.
    pub read_bytes_total: u64,
    /// Cumulative bytes written.
    pub write_bytes_total: u64,
    /// Cumulative sequential-read operations.
    pub read_seq_ops: u64,
    /// Cumulative random-read operations.
    pub read_rand_ops: u64,
    /// Cumulative sequential-write operations.
    pub write_seq_ops: u64,
    /// Cumulative random-write operations.
    pub write_rand_ops: u64,
    /// Cumulative operations smaller than the backend's small-I/O cutoff.
    pub small_ops: u64,
    /// Cumulative operations at least as large as the backend's cutoff.
    pub large_ops: u64,
    /// Read-latency digest, when the backend has read samples.
    pub read_latency_us: Option<LatencySummary>,
    /// Write-latency digest, when the backend has write samples.
    pub write_latency_us: Option<LatencySummary>,
}

impl InstancePerfSample {
    pub fn total_io_count(&self) -> u64 {
        self.read_io_count
            .saturating_add(self.write_io_count)
            .saturating_add(self.other_io_count)
    }
}

/// One backend snapshot consumed by the controller and engine.
#[derive(Debug, Clone)]
pub struct ThreadPoolSnapshot {
    /// Number of active I/O workers.
    pub thread_count: u32,
    /// Current backend-reported vCPU count.
    pub vcpu_count: u32,
    /// Performance counters captured with the current worker count.
    pub perf: Option<InstancePerfSample>,
    /// Backend-computed per-thread CPU utilisation.
    ///
    /// `None` asks the controller to sample the backend process through
    /// `/proc` using [`Instance::thread_name_filter`].
    pub per_thread_util: Option<f64>,
}

/// Store one set of per-worker utilisation samples.
fn update_worker_utilisation(status: &mut InstanceStatus, worker_util: Vec<(String, f64)>) {
    let mut names = worker_util
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    names.sort();
    if status.last_worker_names.as_ref() != Some(&names) {
        tracing::debug!(
            target: "controller",
            thread_count = names.len(),
            threads = %names.join(","),
            "worker thread set changed"
        );
        status.last_worker_names = Some(names);
    }

    status.per_worker_util = worker_util.into_iter().map(|(_, util)| util).collect();
    status.per_thread_util = if status.per_worker_util.is_empty() {
        0.0
    } else {
        status.per_worker_util.iter().sum::<f64>() / status.per_worker_util.len() as f64
    };
}

/// Match task counters by TID and convert deltas to utilisation fractions.
fn compute_per_worker_util(
    previous: &[TaskCpuSample],
    current: &[TaskCpuSample],
    wall_ticks: f64,
) -> Vec<(String, f64)> {
    let previous_by_tid: HashMap<i32, (&str, u64)> = previous
        .iter()
        .map(|task| (task.tid, (task.name.as_str(), task.cpu_ticks)))
        .collect();
    current
        .iter()
        .filter_map(|task| {
            let (_, previous_ticks) = previous_by_tid
                .get(&task.tid)
                .filter(|(name, _)| *name == task.name.as_str())?;
            let delta = task.cpu_ticks.checked_sub(*previous_ticks)?;
            // Scheduler-tick quantisation can make a short interval appear
            // fractionally above one fully occupied CPU; bound that artifact.
            let util = (delta as f64 / wall_ticks).clamp(0.0, 1.0);
            Some((task.name.clone(), util))
        })
        .collect()
}

/// Read cumulative CPU time across matching `/proc/<pid>/task/*/stat` files.
#[tracing::instrument(skip(filter), fields(pid))]
fn read_cpu_sample(
    pid: i32,
    filter: &crate::instance::ThreadNameFilter,
) -> Result<CpuSample, CpuSampleError> {
    let process = Process::new(pid)?;
    let tasks = process.tasks()?;
    let mut cpu_ticks = 0u64;
    let mut thread_count = 0u32;
    let mut per_worker = Vec::new();
    for task in tasks {
        let task = match task {
            Ok(task) => task,
            Err(error) => {
                tracing::warn!(
                    target: "controller",
                    %error,
                    "failed to enumerate backend task"
                );
                continue;
            }
        };
        let stat = match task.stat() {
            Ok(stat) => stat,
            Err(error) => {
                tracing::warn!(
                    target: "controller",
                    %error,
                    "failed to read backend task stat"
                );
                continue;
            }
        };
        if !filter.matches(&stat.comm) {
            continue;
        }
        let task_ticks = stat.utime.saturating_add(stat.stime);
        cpu_ticks = cpu_ticks.saturating_add(task_ticks);
        thread_count += 1;
        per_worker.push(TaskCpuSample {
            tid: stat.pid,
            name: stat.comm,
            cpu_ticks: task_ticks,
        });
    }
    if thread_count == 0 {
        return Err(CpuSampleError::NoCpuSamples);
    }
    Ok(CpuSample {
        cpu_ticks,
        sampled_at: Instant::now(),
        thread_count,
        per_worker,
    })
}

/// Read cgroup v2 CPU quota and throttle counters for `pid`.
fn read_cgroup_sample(pid: i32) -> Result<CgroupSample, CgroupError> {
    let process = Process::new(pid)?;
    let cgroups = process.cgroups()?;
    let path = cgroups
        .0
        .into_iter()
        .find(|entry| entry.hierarchy == 0 && entry.controllers.is_empty())
        .map(|entry| entry.pathname)
        .ok_or(CgroupError::MissingFile("cgroup path".to_string()))?;
    let base = format!("/sys/fs/cgroup{path}");

    let cpu_max = std::fs::read_to_string(format!("{base}/cpu.max"))?;
    let cpu_stat = std::fs::read_to_string(format!("{base}/cpu.stat"))?;

    let mut max_parts = cpu_max.split_whitespace();
    let quota = max_parts
        .next()
        .ok_or(CgroupError::MissingField("quota".to_string()))?;
    let period = max_parts
        .next()
        .ok_or(CgroupError::MissingField("period".to_string()))?
        .parse::<f64>()?;
    if period <= 0.0 {
        return Err(CgroupError::InvalidValue(
            "cpu.max period must be positive".to_string(),
        ));
    }
    let quota_cores = if quota == "max" {
        f64::INFINITY
    } else {
        quota.parse::<f64>()? / period
    };

    let mut sample = CgroupSample {
        quota_cores,
        ..Default::default()
    };
    for line in cpu_stat.lines() {
        let mut fields = line.split_whitespace();
        match (fields.next(), fields.next()) {
            (Some("nr_throttled"), Some(value)) => {
                sample.nr_throttled = value.parse()?;
            }
            (Some("throttled_usec"), Some(value)) => {
                sample.throttled_usec = value.parse()?;
            }
            _ => {}
        }
    }
    Ok(sample)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;

    use super::*;
    use crate::backends::BackendClientError;

    struct SnapshotClient {
        threads: u32,
    }

    #[async_trait]
    impl InstanceClient for SnapshotClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Ok(ThreadPoolSnapshot {
                thread_count: self.threads,
                vcpu_count: 2,
                perf: None,
                per_thread_util: None,
            })
        }

        async fn close(&self) {}
    }

    struct FailingClient {
        closed: Arc<AtomicBool>,
    }

    #[async_trait]
    impl InstanceClient for FailingClient {
        async fn set_thread_count(&self, _count: u32) -> Result<(), BackendClientError> {
            Ok(())
        }

        async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
            Err(BackendClientError::Disconnected("gone".into()))
        }

        async fn close(&self) {
            self.closed.store(true, Ordering::Relaxed);
        }
    }

    /// Test that a successful client snapshot updates
    /// alive/thread_count state.
    #[tokio::test]
    async fn refresh_state_applies_successful_snapshot() {
        let instance = Instance::new(
            "vm-ok".to_string(),
            Path::new(""),
            7,
            SnapshotClient { threads: 3 },
        );
        assert!(instance.refresh_state(false).await);
        let status = instance.status.read().await;
        assert!(status.alive);
        assert_eq!(status.thread_count, 3);
    }

    /// Test that client errors mark the instance dead and close the
    /// client.
    #[tokio::test]
    async fn refresh_state_marks_failed_instances_dead_and_closes() {
        let closed = Arc::new(AtomicBool::new(false));
        let instance = Instance::new(
            "vm-bad".to_string(),
            Path::new(""),
            9,
            FailingClient {
                closed: Arc::clone(&closed),
            },
        );
        assert!(!instance.refresh_state(false).await);
        let status = instance.status.read().await;
        assert!(!status.alive);
        assert!(closed.load(Ordering::Relaxed));
    }

    /// Test that `Display` for an instance prints the bare VM id.
    #[test]
    fn display_trims_instance_id() {
        let instance = Instance::new(
            "  vm-1  ".to_string(),
            Path::new(""),
            1,
            SnapshotClient { threads: 1 },
        );
        assert_eq!(instance.to_string(), "vm-1");
    }

    use super::{TaskCpuSample, compute_per_worker_util};

    use super::ThreadNameFilter;

    /// Test that an empty match list includes every task not on the
    /// ignore list.
    #[test]
    fn empty_match_list_includes_nonignored_tasks() {
        let filter = ThreadNameFilter::new("", &["helper".to_string()]).unwrap();
        assert!(filter.matches("worker"));
        assert!(!filter.matches("helper"));
    }

    /// Test that exact `IgnoreThreadNames` wins over a matching regex
    /// allowlist.
    #[test]
    fn exact_ignore_takes_precedence_over_regex_match() {
        let filter = ThreadNameFilter::new("worker[0-9]+", &["worker0".to_string()]).unwrap();
        assert!(!filter.matches("worker0"));
        assert!(filter.matches("worker1-helper"));
        assert!(!filter.matches("backend-main"));
    }

    fn task(tid: i32, name: &str, cpu_ticks: u64) -> TaskCpuSample {
        TaskCpuSample {
            tid,
            name: name.to_string(),
            cpu_ticks,
        }
    }

    /// Test that two workers sharing a name still get independent
    /// CPU-delta util samples.
    #[test]
    fn duplicate_worker_names_keep_independent_deltas() {
        let previous = vec![task(10, "worker", 100), task(11, "worker", 200)];
        let current = vec![task(11, "worker", 400), task(10, "worker", 200)];

        let util = compute_per_worker_util(&previous, &current, 500.0);
        assert_eq!(util.len(), 2);
        assert!((util[0].1 - 0.4).abs() < f64::EPSILON);
        assert!((util[1].1 - 0.2).abs() < f64::EPSILON);
    }

    /// Test that a newly appeared worker does not spike util from
    /// lifetime counters.
    #[test]
    fn new_worker_starts_without_a_lifetime_spike() {
        let previous = vec![task(10, "worker0", 100)];
        let current = vec![task(10, "worker0", 200), task(11, "worker1", 900_000)];

        let util = compute_per_worker_util(&previous, &current, 500.0);
        assert_eq!(util.len(), 1);
        assert!((util[0].1 - 0.2).abs() < f64::EPSILON);
    }
}
