// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! libvirt-backed QMP transport for the QEMU backend.
//!
//! Replaces the previous `/var/run/qemu-itc/<uuid>.sock` unix-socket
//! transport wholesale.  The `virt` crate's `qemu` feature exposes
//! `virDomainQemuMonitorCommand`, which is the same wire semantics
//! the controller used before -- send a JSON `{"execute": ...}`
//! envelope, receive a JSON `{"return": ...}` / `{"error": ...}`
//! envelope back -- but arbitrated by libvirt instead of by the
//! wrapper-opened chardev.  Consequences:
//!
//!   * Discovery: `virConnectListAllDomains` (filtered to QEMU vms with a
//!     virtio-scsi controller) replaces `readdir()` on the aux-socket
//!     directory.
//!   * No single-client chardev contention: libvirt multiplexes the QMP monitor
//!     internally, so the operator CLI and the daemon can both talk to the same
//!     vm concurrently without stepping on each other.
//!   * IOThread lifecycle: the `virt` crate does not yet expose
//!     `virDomainAddIOThread` / `virDomainDelIOThread`, so we fall back to QMP
//!     passthrough (`object-add`/`object-del`) through the same monitor
//!     command.  Effect on the live guest is identical; the libvirt XML does
//!     *not* pick up the change (see AGENT.md for the follow-up).
//!
//! Everything here is blocking under the hood -- the `virt`
//! crate is a thin FFI wrapper on the libvirt C library -- so
//! every call is wrapped in `spawn_blocking` before it hits the
//! tokio runtime.

use std::{io::Error as IoError, sync::Arc};

use serde::Deserialize;
use serde_json::{Value, json, value::RawValue};
use tokio::task;

use crate::{
    backends::qemu::helpers::QemuError,
    backends::{IoThreadProperties, VqMapping},
    instance::InstancePerfSample,
};

use uuid::Uuid;
use virt::{connect::Connect, domain::Domain, sys};

/// Shared libvirt session: one `virConnect` per daemon,
/// dial-once, thread-safe (Connect is `Send + Sync` at the
/// crate level).
#[derive(Clone)]
pub struct LibvirtConn {
    /// Thread-safe libvirt connection handle.
    inner: Arc<Connect>,
    /// URI used to open this connection.
    pub uri: String,
}

impl LibvirtConn {
    /// Dial `uri` and cache the resulting `virConnect` for
    /// later per-VM calls.  Blocking underneath, so callers
    /// must invoke this off the tokio runtime.
    pub async fn open(uri: String) -> Result<Self, QemuError> {
        let uri_c = uri.clone();
        let conn = task::spawn_blocking(move || Connect::open(Some(&uri_c)))
            .await
            .map_err(|e| QemuError::Parse(format!("spawn_blocking connect: {e}")))?
            .map_err(|e| QemuError::Io(IoError::other(format!("libvirt open({uri}): {e}"))))?;
        Ok(Self {
            inner: Arc::new(conn),
            uri,
        })
    }

    /// List every QEMU/KVM vm that is *active* and carries
    /// a `virtio-scsi` controller.  Returns the UUID string of
    /// each matching vm.  VMs whose XML we could not
    /// fetch (transient races, permission errors) are skipped
    /// silently after a `debug!` line.
    ///
    /// The XML check is intentionally loose: the wrapper
    /// (`qemu-kvm-iothread`) injects iothread declarations into
    /// QEMU's argv *after* libvirt renders the XML, so the XML
    /// itself never reflects the wrapper-managed iothreads.
    /// Downstream refresh (topology + procfs sample) discards
    /// wrongly-matched vms cleanly.
    pub async fn list_qemu_vms(&self) -> Result<Vec<Uuid>, QemuError> {
        let conn = self.inner.clone();
        task::spawn_blocking(move || -> Result<Vec<Uuid>, QemuError> {
            let vms = conn
                .list_all_domains(sys::VIR_CONNECT_LIST_DOMAINS_ACTIVE)
                .map_err(|e| QemuError::Io(IoError::other(format!("list_all_domains: {e}"))))?;
            let mut out = Vec::with_capacity(vms.len());
            for vm in vms {
                let uuid = match vm.get_uuid() {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::debug!(
                            target: "qemu",
                            error = ?e,
                            "libvirt: skip vm (uuid lookup)"
                        );
                        continue;
                    }
                };
                let xml = match vm.get_xml_desc(0) {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::debug!(
                            target: "qemu",
                            uuid = %uuid,
                            error = ?e,
                            "libvirt: skip vm (xml fetch)"
                        );
                        continue;
                    }
                };
                if xml_has_virtio_scsi(&xml) {
                    out.push(uuid);
                }
            }
            out.sort();
            Ok(out)
        })
        .await
        .map_err(|e| QemuError::Parse(format!("spawn_blocking list: {e}")))?
    }

    /// Look up the QEMU host PID for the given UUID.  Read from
    /// libvirt's per-VM pidfile (`/run/libvirt/qemu/<name>.pid`)
    /// so we do not need a QMP round-trip just to seed procfs
    /// sampling.  Returns 0 when the file is missing / the
    /// vm is gone; callers can then fall back to Tgid
    /// derivation from an iothread TID.
    /// Look up the maximum vCPU count libvirt has configured for
    /// the vm.  Used as an upper bound on I/O worker thread
    /// count: growing the pool past the guest's vCPU count is at
    /// best wasteful and at worst starves the vCPU threads that
    /// share runqueue with the workers.  Returns `Err` on any
    /// libvirt-side failure (missing / gone vm, disconnect);
    /// callers treat that as "cap unknown" and skip the check.
    pub async fn qemu_max_vcpus(&self, uuid: &Uuid) -> Result<u32, QemuError> {
        let conn = self.inner.clone();
        let uuid = uuid.to_string();
        task::spawn_blocking(move || -> Result<u32, QemuError> {
            let vm = Domain::lookup_by_uuid_string(&conn, &uuid)
                .map_err(|e| QemuError::Io(IoError::other(format!("lookup_vm({uuid}): {e}"))))?;
            vm.get_max_vcpus()
                .map(|n| n as u32)
                .map_err(|e| QemuError::Io(IoError::other(format!("get_max_vcpus({uuid}): {e}"))))
        })
        .await
        .map_err(|e| QemuError::Parse(format!("spawn_blocking max_vcpus: {e}")))?
    }

    /// Return the host process ID for a libvirt vm, or zero if unavailable.
    pub async fn qemu_host_pid(&self, uuid: &Uuid) -> Result<i32, QemuError> {
        let conn = self.inner.clone();
        let uuid = uuid.to_string();
        task::spawn_blocking(move || -> Result<i32, QemuError> {
            let vm = Domain::lookup_by_uuid_string(&conn, &uuid)
                .map_err(|e| QemuError::Io(IoError::other(format!("lookup_vm({uuid}): {e}"))))?;
            let name = vm
                .get_name()
                .map_err(|e| QemuError::Io(IoError::other(format!("vm name({uuid}): {e}"))))?;
            for path in [
                format!("/run/libvirt/qemu/{name}.pid"),
                format!("/var/run/libvirt/qemu/{name}.pid"),
            ] {
                if let Ok(s) = std::fs::read_to_string(&path)
                    && let Ok(pid) = s.trim().parse::<i32>()
                    && pid > 0
                {
                    return Ok(pid);
                }
            }
            Ok(0)
        })
        .await
        .map_err(|e| QemuError::Parse(format!("spawn_blocking pid: {e}")))?
    }
}

fn xml_has_virtio_scsi(xml: &str) -> bool {
    // Loose match: any element that mentions both a scsi
    // controller and the virtio-scsi model within the same
    // opening tag.  Libvirt canonicalises XML tag ordering, so
    // this is stable across versions.
    for line in xml.lines() {
        let t = line.trim_start();
        if !t.starts_with("<controller") {
            continue;
        }
        if (t.contains("type='scsi'") || t.contains("type=\"scsi\""))
            && (t.contains("model='virtio-scsi'") || t.contains("model=\"virtio-scsi\""))
        {
            return true;
        }
    }
    false
}

/// Per-VM QMP client over libvirt.
///
/// One instance per tracked VM; shares the parent
/// [`LibvirtConn`] via `Arc`.  Every method is a thin
/// serialise-json + libvirt-blocking-call + parse-envelope
/// pipeline, mirroring the shape of the retired unix-socket
/// [`QmpClient`].
#[derive(Clone)]
pub struct LibvirtQmp {
    /// libvirt vm UUID addressed by every command.
    pub uuid: String,
    /// Shared libvirt connection used for QMP passthrough.
    conn: LibvirtConn,
}

impl LibvirtQmp {
    /// Bind a vm UUID to an existing libvirt connection.
    pub fn new(uuid: impl Into<String>, conn: LibvirtConn) -> Self {
        Self {
            uuid: uuid.into(),
            conn,
        }
    }

    /// No-op: libvirt owns the QMP monitor, we never held our
    /// own file descriptor.  Kept for `InstanceClient::close()`
    /// symmetry.
    pub async fn close(&self) {}

    /// Serialise `{"execute": cmd [, "arguments": args]}`, dial
    /// `virDomainQemuMonitorCommand`, and unwrap the reply
    /// envelope.  Preserves the QMP error class/desc pair so
    /// callers can distinguish transport failures from guest
    /// refusals.
    pub async fn execute(
        &self,
        cmd: &str,
        args: Option<Value>,
    ) -> Result<Box<RawValue>, QemuError> {
        let req = match args {
            Some(a) => json!({"execute": cmd, "arguments": a}),
            None => json!({"execute": cmd}),
        };
        let body = serde_json::to_string(&req)
            .map_err(|e| QemuError::Parse(format!("marshal {cmd}: {e}")))?;
        let cmd_owned = cmd.to_string();
        let uuid = self.uuid.clone();
        let conn = self.conn.inner.clone();
        let raw = task::spawn_blocking(move || -> Result<String, QemuError> {
            let vm: Domain = Domain::lookup_by_uuid_string(&conn, &uuid)
                .map_err(|e| QemuError::Io(IoError::other(format!("lookup_vm({uuid}): {e}"))))?;
            vm.qemu_monitor_command(&body, sys::VIR_DOMAIN_QEMU_MONITOR_COMMAND_DEFAULT)
                .map_err(|e| {
                    QemuError::Io(IoError::other(format!(
                        "qemu_monitor_command({cmd_owned}): {e}"
                    )))
                })
        })
        .await
        .map_err(|e| QemuError::Parse(format!("spawn_blocking qmp: {e}")))??;
        parse_qmp_envelope(cmd, &raw)
    }

    /// `object-add qom-type=iothread id=<id>` with optional
    /// `poll-max-ns` (etc.) overrides.
    pub async fn add_io_thread(
        &self,
        id: &str,
        props: Option<&IoThreadProperties>,
    ) -> Result<(), QemuError> {
        if id.trim().is_empty() {
            return Err(QemuError::Parse("iothread id must be non-empty".into()));
        }
        let mut args = json!({"qom-type": "iothread", "id": id});
        if let Some(p) = props {
            let pv = serde_json::to_value(p)
                .map_err(|e| QemuError::Parse(format!("marshal props: {e}")))?;
            if let Value::Object(fields) = pv
                && let Value::Object(ref mut m) = args
            {
                for (k, v) in fields {
                    m.insert(k, v);
                }
            }
        }
        self.execute("object-add", Some(args)).await.map(|_| ())
    }

    /// `object-del id=<id>`.
    pub async fn del_io_thread(&self, id: &str) -> Result<(), QemuError> {
        if id.trim().is_empty() {
            return Err(QemuError::Parse("iothread id must be non-empty".into()));
        }
        self.execute("object-del", Some(json!({"id": id})))
            .await
            .map(|_| ())
    }

    /// `x-query-prometheus-metrics`: returns the human-readable
    /// exposition body used by
    /// [`crate::backends::qemu::helpers::parse_qemu_topology`].
    pub async fn query_prometheus_metrics(&self) -> Result<String, QemuError> {
        let raw = self.execute("x-query-prometheus-metrics", None).await?;
        #[derive(Deserialize)]
        struct Body {
            #[serde(rename = "human-readable-text")]
            human_readable: String,
        }
        let body: Body = serde_json::from_str(raw.get())
            .map_err(|e| QemuError::Parse(format!("decode x-query-prometheus-metrics: {e}")))?;
        Ok(body.human_readable)
    }

    /// `x-virtio-scsi-set-iothread-vq-mapping`.
    pub async fn set_vq_mapping(
        &self,
        device_path: &str,
        mapping: &[VqMapping],
    ) -> Result<(), QemuError> {
        if device_path.trim().is_empty() {
            return Err(QemuError::Parse("device path must be non-empty".into()));
        }
        if mapping.is_empty() {
            return Err(QemuError::Parse(
                "iothread-vq-mapping must contain at least one entry".into(),
            ));
        }
        let args = json!({
            "path": device_path,
            "iothread-vq-mapping": mapping,
        });
        self.execute("x-virtio-scsi-set-iothread-vq-mapping", Some(args))
            .await
            .map(|_| ())
    }

    /// `query-blockstats`: sum I/O op / byte counters across
    /// every block device attached to the guest.  Returned as
    /// an [`InstancePerfSample`] so the engine's revert-on-drop
    /// validation can compare pre- and post-scale IOPS.
    pub async fn query_blockstats(&self) -> Result<Option<InstancePerfSample>, QemuError> {
        let raw = self.execute("query-blockstats", None).await?;
        parse_blockstats(raw.get())
    }

    /// `qom-get path=<device>, property="iothread-vq-mapping"`.
    pub async fn query_vq_mapping(&self, device_path: &str) -> Result<Vec<VqMapping>, QemuError> {
        if device_path.trim().is_empty() {
            return Err(QemuError::Parse("device path must be non-empty".into()));
        }
        let args = json!({"path": device_path, "property": "iothread-vq-mapping"});
        let raw = self.execute("qom-get", Some(args)).await?;
        let s = raw.get();
        if s.is_empty() || s == "null" {
            return Ok(Vec::new());
        }
        let mapping: Vec<VqMapping> = serde_json::from_str(s)
            .map_err(|e| QemuError::Parse(format!("parse qom-get iothread-vq-mapping: {e}")))?;
        Ok(mapping)
    }
}

/// Fold every block device's counters from a `query-blockstats`
/// response body into a single [`InstancePerfSample`].
///
/// Kept as a free function (not a `LibvirtQmp` method) so unit
/// tests can exercise the JSON shape without a live QMP session.
pub(super) fn parse_blockstats(body: &str) -> Result<Option<InstancePerfSample>, QemuError> {
    #[derive(Deserialize)]
    struct Stats {
        #[serde(default)]
        rd_bytes: u64,
        #[serde(default)]
        wr_bytes: u64,
        #[serde(default)]
        rd_operations: u64,
        #[serde(default)]
        wr_operations: u64,
        #[serde(default)]
        flush_operations: u64,
        #[serde(default)]
        unmap_operations: u64,
    }
    #[derive(Deserialize)]
    struct Entry {
        stats: Stats,
    }
    let entries: Vec<Entry> = serde_json::from_str(body)
        .map_err(|e| QemuError::Parse(format!("decode query-blockstats: {e}")))?;
    let perf = if entries.is_empty() {
        None
    } else {
        let mut perf = InstancePerfSample::default();
        for e in entries {
            perf.read_io_count = perf.read_io_count.saturating_add(e.stats.rd_operations);
            perf.write_io_count = perf.write_io_count.saturating_add(e.stats.wr_operations);
            perf.other_io_count = perf.other_io_count.saturating_add(
                e.stats
                    .flush_operations
                    .saturating_add(e.stats.unmap_operations),
            );
            perf.read_bytes_total = perf.read_bytes_total.saturating_add(e.stats.rd_bytes);
            perf.write_bytes_total = perf.write_bytes_total.saturating_add(e.stats.wr_bytes);
        }
        Some(perf)
    };
    Ok(perf)
}

/// Split libvirt's QMP monitor reply into either the `return`
/// payload or a `QemuError::QmpError`.  Matches the semantics of
/// the retired unix-socket [`crate::backends::qemu::helpers::QmpClient`] so
/// callers see the same error variants regardless of transport.
fn parse_qmp_envelope(cmd: &str, body: &str) -> Result<Box<RawValue>, QemuError> {
    #[derive(Deserialize)]
    struct ErrBody {
        class: String,
        desc: String,
    }
    #[derive(Deserialize)]
    struct Envelope {
        #[serde(rename = "return")]
        ret: Option<Box<RawValue>>,
        error: Option<ErrBody>,
    }
    let env: Envelope = serde_json::from_str(body)
        .map_err(|e| QemuError::Parse(format!("decode {cmd} response: {e} (body={})", body)))?;
    if let Some(err) = env.error {
        return Err(QemuError::QmpError {
            cmd: cmd.to_string(),
            class: err.class,
            desc: err.desc,
        });
    }
    env.ret.ok_or_else(|| {
        QemuError::Parse(format!("qmp {cmd} response missing return/error: {}", body))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that domain XML detection accepts virtio-scsi model
    /// variants and rejects others.
    #[test]
    fn xml_matches_virtio_scsi_variants() {
        let a = r#"<controller type='scsi' index='0' model='virtio-scsi'>"#;
        let b = r#"<controller type="scsi" index="0" model="virtio-scsi"/>"#;
        let c = r#"<controller type='scsi' model='lsi'/>"#;
        let d = r#"<disk type='file'/>"#;
        assert!(xml_has_virtio_scsi(a));
        assert!(xml_has_virtio_scsi(b));
        assert!(!xml_has_virtio_scsi(c));
        assert!(!xml_has_virtio_scsi(d));
    }

    /// Test that a QMP success envelope yields the return payload.
    #[test]
    fn parse_envelope_extracts_return() {
        let body = r#"{"return":{"foo":42}}"#;
        let v = parse_qmp_envelope("query", body).unwrap();
        assert_eq!(v.get(), r#"{"foo":42}"#);
    }

    /// Test that a QMP error envelope becomes a typed error with
    /// class/desc.
    #[test]
    fn parse_envelope_surfaces_qmp_error() {
        let body = r#"{"error":{"class":"GenericError","desc":"boom"}}"#;
        let err = parse_qmp_envelope("query", body).unwrap_err();
        match err {
            QemuError::QmpError { class, desc, .. } => {
                assert_eq!(class, "GenericError");
                assert_eq!(desc, "boom");
            }
            other => panic!("unexpected err: {other:?}"),
        }
    }

    /// Test that blockstats counters are summed across disks.
    #[test]
    fn parse_blockstats_sums_across_devices() {
        let body = r#"[
            {"stats": {
                "rd_operations": 100,
                "wr_operations": 200,
                "flush_operations": 3,
                "unmap_operations": 1,
                "rd_bytes": 4096,
                "wr_bytes": 8192
            }},
            {"stats": {
                "rd_operations": 50,
                "wr_operations": 25,
                "flush_operations": 0,
                "unmap_operations": 0,
                "rd_bytes": 2048,
                "wr_bytes": 1024
            }}
        ]"#;
        let p = parse_blockstats(body).unwrap().unwrap();
        assert_eq!(p.read_io_count, 150);
        assert_eq!(p.write_io_count, 225);
        assert_eq!(p.other_io_count, 4);
        assert_eq!(p.read_bytes_total, 6144);
        assert_eq!(p.write_bytes_total, 9216);
        assert_eq!(p.total_io_count(), 150 + 225 + 4);
    }

    /// Test that empty blockstats parses as unavailable/`None`.
    #[test]
    fn parse_blockstats_empty_returns_unavailable() {
        let p = parse_blockstats("[]").unwrap();
        assert!(p.is_none());
    }
}
