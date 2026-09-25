// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Shared QEMU helpers.
//!
//! Everything here is transport-neutral: prometheus-body topology
//! parsing, /proc jiffies sampling, VQ round-robin, iothread id
//! allocation, and the [`QemuError`] shape used by the QMP
//! transport in [`crate::backends::qemu::libvirt`].  The libvirt-
//! backed transport lives in `backends/qemu/libvirt.rs`; the
//! per-VM state that used to live in this module now belongs to
//! [`crate::backends::qemu::client::QemuInstanceClient`].

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    ops::Sub,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use once_cell::sync::Lazy;
use procfs::process::Process;
use regex::Regex;
use rustix::param;
use thiserror::Error;

use crate::backends::{BackendClientError, VqMapping};

/// Errors produced while querying or reconfiguring a QEMU vm.
#[derive(Debug, Error)]
pub enum QemuError {
    /// A prior transport failure made the client unusable.
    #[error("qemu client is broken")]
    Broken,
    /// Host-side I/O or libvirt transport failure.
    #[error("io: {0}")]
    Io(#[from] io::Error),
    /// Structured error returned by QMP.
    #[error("qmp {cmd} error: {class}: {desc}")]
    QmpError {
        /// QMP command that failed.
        cmd: String,
        /// QMP error class.
        class: String,
        /// Human-readable QMP error description.
        desc: String,
    },
    /// Malformed or incomplete backend data.
    #[error("parse: {0}")]
    Parse(String),
}

impl From<QemuError> for BackendClientError {
    fn from(e: QemuError) -> Self {
        match e {
            QemuError::Broken => Self::Transport("qemu client broken".into()),
            QemuError::Io(e) => Self::from_transport_io("io", &e),
            QemuError::QmpError { cmd, class, desc } => Self::QmpError { cmd, class, desc },
            QemuError::Parse(s) => Self::Protocol(s),
        }
    }
}

// ---------------------------------------------------------------------
// Topology parser
// ---------------------------------------------------------------------

static IOTHREAD_INFO_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^qemu_iothread_info\{([^}]*)\}\s+([0-9.eE+\-]+)").unwrap());
static MANAGED_IOT_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"^iot[0-9]+$").unwrap());
static VIRTIO_SCSI_NUM_QUEUES_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^qemu_virtio_scsi_num_queues\{([^}]*)\}\s+([0-9.eE+\-]+)").unwrap());

/// IOThread and virtio-scsi topology parsed from QEMU metrics.
#[derive(Debug, Default, Clone)]
pub struct QemuTopology {
    /// Sorted managed IOThread object identifiers.
    pub iothreads: Vec<String>,
    /// Host task ID for each managed IOThread.
    pub iothread_tids: BTreeMap<String, i32>,
    /// QOM path of the managed virtio-scsi device.
    pub device_path: String,
    /// Number of command virtqueues exposed by the device.
    pub vq_count: u16,
}

impl QemuTopology {
    /// Parse a Prometheus label list into unquoted key/value pairs.
    fn parse_qemu_labels(labels: &str) -> HashMap<String, String> {
        let mut out = HashMap::new();
        for kv in labels.split(',') {
            let kv = kv.trim();
            if kv.is_empty() {
                continue;
            }
            if let Some(eq) = kv.find('=') {
                let k = kv[..eq].trim().to_string();
                let v = kv[eq + 1..].trim().trim_matches('"').to_string();
                out.insert(k, v);
            }
        }
        out
    }

    /// Parse managed IOThreads and virtio-scsi queue data from QEMU metrics.
    // FIXME document body, check unit test
    pub fn new(body: &str) -> Self {
        let mut topo = Self::default();
        for line in body.lines() {
            if line.starts_with('#') {
                continue;
            }
            if let Some(m) = IOTHREAD_INFO_RE.captures(line) {
                let lbls = Self::parse_qemu_labels(m.get(1).map_or("", |x| x.as_str()));
                let id = match lbls.get("id") {
                    Some(s) => s.clone(),
                    None => continue,
                };
                if id.is_empty() || !MANAGED_IOT_ID_RE.is_match(&id) {
                    continue;
                }
                let tid_str = lbls
                    .get("tid")
                    .or_else(|| lbls.get("thread_id"))
                    .cloned()
                    .unwrap_or_default();
                let tid: i32 = match tid_str.parse() {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                topo.iothread_tids.insert(id, tid);
                continue;
            }
            if !topo.device_path.is_empty() {
                continue;
            }
            if let Some(m) = VIRTIO_SCSI_NUM_QUEUES_RE.captures(line) {
                let lbls = Self::parse_qemu_labels(m.get(1).map_or("", |x| x.as_str()));
                let path = lbls
                    .get("device")
                    .or_else(|| lbls.get("path"))
                    .cloned()
                    .unwrap_or_default();
                if path.is_empty() {
                    continue;
                }
                let v: f64 = match m.get(2).unwrap().as_str().parse() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                topo.device_path = path;
                topo.vq_count = v as u16;
            }
        }
        if !topo.iothread_tids.is_empty() {
            topo.iothreads = topo.iothread_tids.keys().cloned().collect();
            topo.iothreads.sort();
        }
        topo
    }
}

// ---------------------------------------------------------------------
// /proc helpers
// ---------------------------------------------------------------------

/// Per-IOThread cumulative CPU counters sampled at one wall-clock instant.
#[derive(Debug, Clone)]
pub struct CpuSample {
    /// Cumulative user-plus-system jiffies keyed by host task ID.
    pub jiffies: HashMap<i32, u64>,
    /// Wall-clock time at which the counters were sampled.
    // FIXME rename to sampled_at
    pub wall: Instant,
}

impl Default for CpuSample {
    fn default() -> Self {
        Self {
            jiffies: HashMap::new(),
            wall: Instant::now(),
        }
    }
}

impl CpuSample {
    /// Sample cumulative CPU jiffies for every discovered IOThread task.
    pub fn new(
        pid: i32,
        iothread_tids: &BTreeMap<String, i32>,
    ) -> Result<Self, BackendClientError> {
        let mut out = Self {
            jiffies: HashMap::new(),
            wall: Instant::now(),
        };

        let p = Process::new(pid)?;
        for &tid in iothread_tids.values() {
            let stat = p.task_from_tid(tid)?.stat()?;
            out.jiffies.insert(tid, stat.utime + stat.stime);
        }
        Ok(out)
    }

    /// Compute average per-IOThread CPU utilisation between two samples.
    // TODO add unit test for this function and the call site, and that it can be
    // folded into the Sub.
    pub fn diff_jiffies_sample(prev: &Self, cur: &Self) -> f64 {
        let Some(elapsed) = cur.wall.checked_duration_since(prev.wall) else {
            return 0.0;
        };
        let elapsed = elapsed.as_secs_f64();
        if elapsed <= 0.0 {
            return 0.0;
        }
        let hz = param::clock_ticks_per_second() as f64;
        assert!(hz > 0.0);
        let mut total = 0.0;
        let mut counted = 0u32;
        for (&tid, &after) in &cur.jiffies {
            let Some(&before) = prev.jiffies.get(&tid) else {
                continue;
            };
            if after < before {
                continue;
            }
            let delta = (after - before) as f64;
            let frac = ((delta / hz) / elapsed).clamp(0.0, 1.0);
            total += frac;
            counted += 1;
        }
        if counted == 0 {
            return 0.0;
        }
        total / counted as f64
    }
}

impl Sub<&CpuSample> for &CpuSample {
    type Output = f64;
    fn sub(self, rhs: &CpuSample) -> Self::Output {
        CpuSample::diff_jiffies_sample(rhs, self)
    }
}

// ---------------------------------------------------------------------
// VQ mapping helpers
// ---------------------------------------------------------------------

/// Assign virtqueues evenly across IOThreads in round-robin order.
pub fn round_robin_vq_mapping(iothread_ids: &[String], vq_count: u16) -> Vec<VqMapping> {
    if iothread_ids.is_empty() || vq_count == 0 {
        return Vec::new();
    }
    let mut mapping: Vec<VqMapping> = iothread_ids
        .iter()
        .map(|id| VqMapping {
            iothread: id.clone(),
            vqs: Vec::new(),
        })
        .collect();
    for vq in 0..vq_count {
        let idx = (vq as usize) % iothread_ids.len();
        mapping[idx].vqs.push(vq);
    }
    mapping
}

/// Return the lowest unused managed IOThread identifier.
pub fn next_qemu_iothread_id(existing: &[String]) -> String {
    let seen: HashSet<&str> = existing.iter().map(|s| s.as_str()).collect();
    // FIXME 0..1024 seems arbitrary
    for n in 0..1024 {
        let candidate = format!("iot{n}");
        if !seen.contains(candidate.as_str()) {
            return candidate;
        }
    }
    static EXTRA: AtomicU64 = AtomicU64::new(0);
    format!("iot-extra-{}", EXTRA.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[test]
        fn round_robin_partitions_queues(
            ids in prop::collection::vec("[a-z]{1,4}", 0..8),
            vq_count in 0u16..128,
        ) {
            let mapping = round_robin_vq_mapping(&ids, vq_count);
            if ids.is_empty() || vq_count == 0 {
                prop_assert!(mapping.is_empty());
                return Ok(());
            }

            prop_assert_eq!(mapping.len(), ids.len());
            let mut seen = vec![false; usize::from(vq_count)];
            for (idx, entry) in mapping.iter().enumerate() {
                prop_assert_eq!(&entry.iothread, &ids[idx]);
                for &vq in &entry.vqs {
                    prop_assert_eq!(usize::from(vq) % ids.len(), idx);
                    prop_assert!(!seen[usize::from(vq)]);
                    seen[usize::from(vq)] = true;
                }
            }
            prop_assert!(seen.into_iter().all(|present| present));
            let sizes: Vec<usize> = mapping.iter().map(|entry| entry.vqs.len()).collect();
            let min = sizes.iter().copied().min().unwrap();
            let max = sizes.iter().copied().max().unwrap();
            prop_assert!(max - min <= 1);
        }

        #[test]
        fn next_id_is_the_lowest_missing_iot(
            present in prop::collection::btree_set(0u32..1024, 0..32),
        ) {
            let ids: Vec<String> = present.iter().map(|n| format!("iot{n}")).collect();
            let expected = (0..1024).find(|n| !present.contains(n)).unwrap();
            prop_assert_eq!(next_qemu_iothread_id(&ids), format!("iot{expected}"));
        }
    }

    /// Test that managed `iotN` threads are kept in id order and other ids are
    /// dropped.
    #[test]
    fn topology_keeps_managed_threads_in_id_order() {
        let body = r#"
qemu_iothread_info{id="iot1",tid="124"} 1
qemu_iothread_info{id="dirtybitmap",tid="999"} 1
qemu_iothread_info{id="iot0",tid="123"} 1
"#;
        let topo = QemuTopology::new(body);
        assert_eq!(topo.iothreads, vec!["iot0", "iot1"]);
        assert_eq!(topo.iothread_tids.get("iot0"), Some(&123));
        assert_eq!(topo.iothread_tids.get("iot1"), Some(&124));
        assert_eq!(topo.iothread_tids.len(), 2);
    }

    /// Test that `thread_id` and `path` are accepted in place of `tid` and
    /// `device`.
    #[test]
    fn topology_accepts_label_aliases() {
        let body = r#"
qemu_iothread_info{id="iot0",thread_id="321"} 1
qemu_virtio_scsi_num_queues{path="/machine/peripheral/scsi0"} 4
"#;
        let topo = QemuTopology::new(body);
        assert_eq!(topo.iothreads, vec!["iot0"]);
        assert_eq!(topo.iothread_tids.get("iot0"), Some(&321));
        assert_eq!(topo.device_path, "/machine/peripheral/scsi0");
        assert_eq!(topo.vq_count, 4);
    }

    /// Test that the first device wins, noise lines are ignored, and a later
    /// thread is kept.
    #[test]
    fn topology_keeps_the_first_device_and_later_threads() {
        let body = r#"
# HELP foo
not a metric
qemu_virtio_scsi_num_queues{device=""} 3
qemu_iothread_info{id="iot7",tid="nope"} 1
qemu_virtio_scsi_num_queues{device="/machine/peripheral/scsi0"} 4
qemu_iothread_info{id="iot0",tid="123"} 1
qemu_virtio_scsi_num_queues{device="/machine/peripheral/scsi1"} 99
"#;
        let topo = QemuTopology::new(body);
        assert_eq!(topo.iothreads, vec!["iot0"]);
        assert_eq!(topo.iothread_tids.get("iot0"), Some(&123));
        assert_eq!(topo.iothread_tids.len(), 1);
        assert_eq!(topo.device_path, "/machine/peripheral/scsi0");
        assert_eq!(topo.vq_count, 4);
    }

    /// Test that a repeated IOThread id keeps the tid from the last line.
    #[test]
    fn topology_keeps_the_last_tid_for_a_repeated_id() {
        let body = r#"
qemu_iothread_info{id="iot0",tid="1"} 1
qemu_iothread_info{id="iot0",tid="2"} 1
"#;
        let topo = QemuTopology::new(body);
        assert_eq!(topo.iothreads, vec!["iot0"]);
        assert_eq!(topo.iothread_tids.get("iot0"), Some(&2));
    }
}
