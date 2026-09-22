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

use std::{fmt, sync::LazyLock, time::Instant};

use async_trait::async_trait;
use procfs::process::Process;
use regex::Regex;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    backends::BackendClientError,
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
    /// Backend-selected task names included in CPU sampling.
    pub thread_name_filter: ThreadNameFilter,
    /// Per-VM backend implementation.
    pub client: Box<dyn InstanceClient>,
    /// Latest snapshot shared by refresh, evaluation, and status output.
    ///
    /// The lock permits all VM refresh and evaluation futures to share their
    /// records safely.
    pub status: RwLock<InstanceStatus>,
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
    pub async fn refresh_state(&self) -> bool {
        match self.client.get_thread_pool_snapshot().await {
            Ok(snapshot) => self.apply_thread_pool_snapshot(snapshot).await,
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
    async fn apply_thread_pool_snapshot(&self, snapshot: ThreadPoolSnapshot) -> bool {
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
        status.perf = snapshot.perf;
        status.alive = true;
        if let Some(per_thread_util) = snapshot.per_thread_util {
            status.per_thread_util = per_thread_util.clamp(0.0, 1.0);
        } else if let (Some(prev), Some(cur)) = (status.last_cpu_sample.as_ref(), cpu.as_ref())
            && prev.thread_count == cur.thread_count
            && cur.thread_count > 0
            && let Some(d_ticks) = cur.cpu_ticks.checked_sub(prev.cpu_ticks)
        {
            let d_time_ticks = cur
                .sampled_at
                .saturating_duration_since(prev.sampled_at)
                .as_secs_f64()
                * *TICKS_PER_SECOND;
            if d_time_ticks > 0.0 {
                let measured = d_ticks as f64 / d_time_ticks / f64::from(cur.thread_count);
                // Scheduler-tick quantisation can make a short
                // interval appear fractionally above a fully
                // occupied CPU; bound that sampling artifact.
                status.per_thread_util = measured.clamp(0.0, 1.0);
            }
        }
        // A changed task count invalidates the delta. Keep the last
        // utilisation until the next like-for-like sample.
        let io_ops_total = match snapshot.perf {
            Some(perf) => perf
                .read_io_count
                .saturating_add(perf.write_io_count)
                .saturating_add(perf.other_io_count),
            None => 0,
        };
        if let Some(per_thread_util) = snapshot.per_thread_util {
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
        status.last_cpu_sample = cpu;
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
}

impl ThreadNameFilter {
    /// Compile task-name patterns. An empty list includes every task.
    pub fn new(pattern: &str) -> Result<Self, InstanceError> {
        let match_regex = if pattern.is_empty() {
            None
        } else {
            let pattern = format!("(?:{pattern})");
            Some(Regex::new(&pattern)?)
        };
        Ok(Self { match_regex })
    }

    /// Return whether a task name belongs in CPU sampling.
    pub fn matches(&self, task_name: &str) -> bool {
        self.match_regex
            .as_ref()
            .is_none_or(|regex| regex.is_match(task_name))
    }
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
#[derive(Debug, Clone, Copy)]
pub struct CpuSample {
    /// Sum of user and system CPU ticks across sampled tasks.
    pub cpu_ticks: u64,
    /// Monotonic time at which this sample was taken.
    pub sampled_at: Instant,
    /// Number of tasks included in the sample.
    pub thread_count: u32,
}

/// Backend-neutral performance counters from one snapshot.
#[derive(Debug, Default, Clone, Copy)]
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
    }
    if thread_count == 0 {
        return Err(CpuSampleError::NoCpuSamples);
    }
    Ok(CpuSample {
        cpu_ticks,
        sampled_at: Instant::now(),
        thread_count,
    })
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
                // FIXME these weren't required, looked like broken due to rebase
                perf: None,
                vcpu_count: 1,
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
        assert!(instance.refresh_state().await);
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
        assert!(!instance.refresh_state().await);
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
}
