// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! [`InstanceClient`] impl for the QEMU backend.
//!
//! This is the wire client the generic
//! [`crate::instance::Instance`] holds behind a trait object.  It
//! owns:
//!
//!   * the [`super::libvirt::LibvirtQmp`] transport (QMP multiplexed through
//!     libvirt's `virDomainQemuMonitorCommand`);
//!   * the per-VM topology cache (iothread ids + TIDs, virtio-scsi device path,
//!     virtqueue count);
//!   * the per-tick CPU sample used to compute per-IOThread utilisation.
//!
//! Everything the controller used to keep in `QemuInstance` /
//! `QemuState` now lives here, behind the generic per-VM interface.

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use procfs::process::Process;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    backends::{
        BackendClientError, IoThreadProperties, VqMapping,
        qemu::{
            helpers::{
                CpuSample, QemuError, QemuTopology, next_qemu_iothread_id, round_robin_vq_mapping,
            },
            libvirt::LibvirtQmp,
        },
    },
    instance::{InstanceClient, ThreadPoolSnapshot},
};

/// Wire client for an QEMU-managed VM.
pub struct QemuInstanceClient {
    /// libvirt vm UUID.
    pub uuid: Uuid,
    /// QMP transport shared by all operations on this VM.
    pub client: Arc<LibvirtQmp>,
    /// Maximum vCPU count reported by libvirt.
    vcpu_count: u32,
    /// Mutable topology and sampling state.
    inner: Mutex<Inner>,
}

/// Cached per-VM topology and CPU-sampling baseline.
#[derive(Debug, Default)]
struct Inner {
    /// Host process ID, or zero until discovered.
    pid: i32,

    /// Managed IOThread object identifiers.
    iothreads: Vec<String>,

    /// Mapping from IOThread identifier to host task ID.
    iothread_tids: BTreeMap<String, i32>,

    /// QOM path of the managed virtio-scsi device.
    device_path: String,

    /// Number of command virtqueues exposed by the device.
    vq_count: u16,

    /// Previous per-task CPU sample used to calculate utilisation.
    last_sample: Option<CpuSample>,
    /// Whether we've already warned that prometheus metrics are unsupported.
    warned_unsupported_metrics: bool,
}

impl QemuInstanceClient {
    /// Construct an empty per-VM client around a libvirt QMP transport.
    pub fn new(uuid: Uuid, qmp: LibvirtQmp, vcpu_count: u32) -> Self {
        Self {
            uuid,
            client: Arc::new(qmp),
            vcpu_count,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Best-effort scale-up: allocate one new IOThread and reshard
    /// the virtqueues across the new set with a round-robin
    /// mapping.  Called by `set_thread_count`.
    async fn scale_up(&self) -> Result<u32, QemuError> {
        let (iothreads, vq_count, device_path) = {
            let s = self.inner.lock().await;
            (s.iothreads.clone(), s.vq_count, s.device_path.clone())
        };
        let new_id = next_qemu_iothread_id(&iothreads);
        self.client.add_io_thread(&new_id, None).await?;
        let mut new_set = iothreads.clone();
        new_set.push(new_id.clone());
        new_set.sort();
        let mapping = round_robin_vq_mapping(&new_set, vq_count);
        if let Err(e) = self.client.set_vq_mapping(&device_path, &mapping).await {
            if let Err(del_err) = self.client.del_io_thread(&new_id).await {
                tracing::debug!(
                    target: "qemu",
                    uuid = %self.uuid,
                    iothread = %new_id,
                    "rollback object-del after set-mapping failure: {del_err}"
                );
            }
            return Err(e);
        }
        let mut s = self.inner.lock().await;
        s.iothreads = new_set;
        Ok(s.iothreads.len() as u32)
    }

    async fn scale_down(&self) -> Result<u32, QemuError> {
        let (iothreads, vq_count, device_path) = {
            let s = self.inner.lock().await;
            (s.iothreads.clone(), s.vq_count, s.device_path.clone())
        };
        if iothreads.len() <= 1 {
            return Err(QemuError::Parse(format!(
                "refusing to scale below one iothread (have {})",
                iothreads.len()
            )));
        }
        let doomed = iothreads.last().unwrap().clone();
        let remaining: Vec<String> = iothreads[..iothreads.len() - 1].to_vec();
        let mapping = round_robin_vq_mapping(&remaining, vq_count);
        self.client.set_vq_mapping(&device_path, &mapping).await?;
        if let Err(e) = self.client.del_io_thread(&doomed).await {
            tracing::warn!(
                target: "qemu",
                uuid = %self.uuid,
                iothread = %doomed,
                error = ?e,
                "object-del failed; will retry next tick"
            );
            return Err(e);
        }
        let mut s = self.inner.lock().await;
        s.iothreads = remaining;
        Ok(s.iothreads.len() as u32)
    }

    /// Build a best-effort snapshot from cached topology when this host QEMU
    /// does not implement `x-query-prometheus-metrics`.
    async fn snapshot_from_cached_topology(
        &self,
    ) -> Result<ThreadPoolSnapshot, BackendClientError> {
        let thread_count = {
            let s = self.inner.lock().await;
            s.iothreads.len() as u32
        }
        .max(1);

        if thread_count == 1 {
            tracing::debug!(
                target: "qemu",
                uuid = %self.uuid,
                "using minimal fallback snapshot without cached topology"
            );
        }

        // Keep perf counters best-effort, same policy as the normal path.
        let perf = match self.client.query_blockstats().await {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(
                    target: "qemu",
                    uuid = %self.uuid,
                    error = ?e,
                    "query-blockstats failed"
                );
                None
            }
        };

        Ok(ThreadPoolSnapshot {
            thread_count,
            perf,
            // Cannot derive per-thread util without fresh topology/TID data.
            per_thread_util: None,
            vcpu_count: self.vcpu_count,
        })
    }
}

#[async_trait]
impl InstanceClient for QemuInstanceClient {
    async fn set_thread_count(&self, target: u32) -> Result<(), BackendClientError> {
        // Call scale_up until actual thread count reaches the target (noop if
        // it's already there, so on scale down this never executes).
        loop {
            let cur = self.inner.lock().await.iothreads.len() as u32;
            if cur >= target {
                break;
            }
            let new = self.scale_up().await?;
            if new == cur {
                break;
            }
        }

        // Call scale_down until actual thread count reaches the target (noop
        // if it's already there, so on scale up this never executes).
        loop {
            let cur = self.inner.lock().await.iothreads.len() as u32;
            if cur <= target {
                break;
            }
            let new = self.scale_down().await?;
            if new == cur {
                break;
            }
        }
        Ok(())
    }

    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
        let body = match self.client.query_prometheus_metrics().await {
            Ok(body) => body,
            Err(QemuError::QmpError {
                cmd,
                class,
                desc: _,
            }) if cmd == "x-query-prometheus-metrics" && class == "CommandNotFound" => {
                let should_warn = {
                    let mut s = self.inner.lock().await;
                    if s.warned_unsupported_metrics {
                        false
                    } else {
                        s.warned_unsupported_metrics = true;
                        true
                    }
                };
                if should_warn {
                    tracing::warn!(
                        target: "qemu",
                        uuid = %self.uuid,
                        "QMP command x-query-prometheus-metrics unsupported; using fallback snapshot"
                    );
                }
                return self.snapshot_from_cached_topology().await;
            }
            Err(e) => return Err(BackendClientError::from(e)),
        };
        let topo = QemuTopology::new(&body);
        if topo.iothreads.is_empty() || topo.device_path.is_empty() {
            return Err(BackendClientError::InvalidState(format!(
                "qemu topology incomplete: iothreads={} device={}",
                topo.iothreads.len(),
                topo.device_path
            )));
        }
        let first_tid = topo.iothread_tids.values().next().copied().unwrap_or(0);
        if first_tid == 0 {
            return Err(BackendClientError::InvalidState(
                "qemu topology reported no iothread TIDs".into(),
            ));
        }
        let pid = Process::new(first_tid)?
            .status()
            .map_err(|e| BackendClientError::InvalidState(format!("pid resolution: {e}")))?
            .tgid;
        if pid <= 0 {
            return Err(BackendClientError::InvalidState(format!(
                "invalid pid {pid} from tid {first_tid}"
            )));
        }
        let sample = CpuSample::new(pid, &topo.iothread_tids)?;
        if sample.jiffies.is_empty() {
            return Err(BackendClientError::InvalidState(
                "qemu per-thread sample empty".into(),
            ));
        }

        let mut s = self.inner.lock().await;
        s.pid = pid;
        s.iothreads = topo.iothreads.clone();
        s.iothread_tids = topo.iothread_tids.clone();
        s.device_path = topo.device_path.clone();
        s.vq_count = topo.vq_count;

        let per_thread_util = if let Some(prev) = s.last_sample.take() {
            let util = &sample - &prev;
            s.last_sample = Some(sample);
            Some(util)
        } else {
            // Warm-up tick: report None so the engine leaves
            // per_thread_util at 0.0.  Next tick has a `prev`
            // sample and returns a real delta.
            s.last_sample = Some(sample);
            None
        };
        // Feed block statistics to the engine so
        // revert-on-drop validation compares apples to apples
        // across backends.  A best-effort call: an error here
        // shouldn't drop the whole snapshot, so we downgrade to
        // an empty perf sample and log at debug.
        let perf = match self.client.query_blockstats().await {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(
                    target: "qemu",
                    uuid = %self.uuid,
                    error = ?e,
                    "query-blockstats failed"
                );
                None
            }
        };
        let thread_count = topo.iothreads.len() as u32;
        Ok(ThreadPoolSnapshot {
            thread_count,
            perf,
            per_thread_util,
            vcpu_count: self.vcpu_count,
        })
    }

    async fn close(&self) {
        self.client.close().await;
    }

    async fn add_io_thread(
        &self,
        id: &str,
        props: Option<&IoThreadProperties>,
    ) -> Result<(), BackendClientError> {
        self.client
            .add_io_thread(id, props)
            .await
            .map_err(BackendClientError::from)
    }

    async fn del_io_thread(&self, id: &str) -> Result<(), BackendClientError> {
        self.client
            .del_io_thread(id)
            .await
            .map_err(BackendClientError::from)
    }

    async fn set_io_thread_vq_mapping(
        &self,
        device: &str,
        mapping: &[VqMapping],
    ) -> Result<(), BackendClientError> {
        self.client
            .set_vq_mapping(device, mapping)
            .await
            .map_err(BackendClientError::from)
    }

    async fn get_io_thread_vq_mapping(
        &self,
        device: &str,
    ) -> Result<Vec<VqMapping>, BackendClientError> {
        self.client
            .query_vq_mapping(device)
            .await
            .map_err(BackendClientError::from)
    }
}
