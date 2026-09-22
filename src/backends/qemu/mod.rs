// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Upstream-QEMU backend.
//!
//! Discovery and command dispatch both go through libvirt (the
//! URI is per-backend config; see [`QemuConfig::libvirt_uri`]).
//! We enumerate active vms with `virConnectListAllDomains`,
//! filter to those whose XML mentions a `virtio-scsi` controller,
//! and drive per-VM QMP through `virDomainQemuMonitorCommand`.
//!
//! Per-VM state (topology cache, per-iothread CPU sample) is
//! held inside [`client::QemuInstanceClient`], which is what the
//! generic [`crate::instance::Instance`] carries as its
//! [`crate::instance::InstanceClient`] trait object.
//!
//! The libvirt connection is dialed once per daemon start-up
//! and shared across every discovered vm via
//! [`libvirt::LibvirtConn`].  If the initial connect fails
//! (libvirtd down at start-up), each tick's discovery pass
//! returns an empty vector -- no fatal error propagates -- and
//! the daemon picks the connection back up automatically once
//! libvirtd returns.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    backends::{Backend, BackendClientError, BackendRegistration},
    config::{ConfigError, load_config},
    instance::{Instance, InstanceClient},
    util::Path,
};

pub mod client;
pub mod helpers;
pub mod libvirt;

const BACKEND_NAME: &str = "qemu";

fn build_qemu_backend(dir: &Path) -> Result<Box<dyn Backend>, BackendClientError> {
    Ok(Box::new(QemuBackend::from_config_dir(dir)?))
}

#[linkme::distributed_slice(crate::backends::BACKENDS)]
static QEMU_BACKEND: BackendRegistration = BackendRegistration {
    name: BACKEND_NAME,
    build: build_qemu_backend,
};

/// QEMU-backend configuration, loaded from
/// `<backend_config_dir>/qemu.json` at backend construction.
///
/// `libvirt_uri` is the connection string dialed on the daemon's
/// libvirt handle -- discovery and per-VM QMP passthrough
/// both go through it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QemuConfig {
    /// libvirt URI used for discovery and QMP passthrough.
    #[serde(default = "default_libvirt_uri")]
    pub libvirt_uri: String,
}

fn default_libvirt_uri() -> String {
    "qemu:///system".to_string()
}

impl Default for QemuConfig {
    fn default() -> Self {
        Self {
            libvirt_uri: default_libvirt_uri(),
        }
    }
}

impl QemuConfig {
    /// Read `<dir>/qemu.json` if present, otherwise return the
    /// built-in defaults.
    pub fn from_dir(dir: &Path) -> Result<Self, ConfigError> {
        let path = Path::new(&dir.join("qemu.json"));
        if path.exists() {
            load_config(path)
        } else {
            Ok(Self::default())
        }
    }
}

/// Backend that discovers and manages QEMU vms through libvirt.
pub struct QemuBackend {
    cfg: QemuConfig,
    /// Cached libvirt handle.  `None` until the first successful
    /// `LibvirtConn::open` (lazy dial keeps daemon start-up
    /// resilient to libvirtd hiccups).  `Mutex` because
    /// `Backend::discover` is `&self`, but we need
    /// interior mutability to memoise across ticks.
    conn: Mutex<Option<libvirt::LibvirtConn>>,
}

impl QemuBackend {
    /// Construct a backend from an explicit configuration.
    pub fn new(cfg: QemuConfig) -> Self {
        Self {
            cfg,
            conn: Mutex::new(None),
        }
    }

    /// Convenience: read `<dir>/qemu.json` (or fall back to
    /// defaults) and construct.
    pub fn from_config_dir(dir: &Path) -> Result<Self, ConfigError> {
        Ok(Self::new(QemuConfig::from_dir(dir)?))
    }

    async fn get_or_open_conn(&self, uri: &str) -> Option<libvirt::LibvirtConn> {
        if let Some(c) = self.conn.lock().unwrap().clone()
            && c.uri == uri
        {
            return Some(c);
        }
        match libvirt::LibvirtConn::open(uri.to_string()).await {
            Ok(c) => {
                *self.conn.lock().unwrap() = Some(c.clone());
                Some(c)
            }
            Err(e) => {
                tracing::warn!(
                    target: "qemu",
                    uri = %uri,
                    error = ?e,
                    "libvirt connect failed"
                );
                None
            }
        }
    }
}

impl Default for QemuBackend {
    fn default() -> Self {
        Self::new(QemuConfig::default())
    }
}

#[async_trait]
impl Backend for QemuBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    /// Discover every active QEMU vm via libvirt and wrap it
    /// in a generic [`Instance`] whose wire client is a
    /// [`client::QemuInstanceClient`].  We seed
    /// [`Instance::pid`] with libvirt's per-VM pidfile so the
    /// engine's cgroup / procfs sampling kicks in immediately;
    /// the QEMU backend still supplies its own per-thread util on
    /// every snapshot so the /proc walker's thread-name filter is
    /// never consulted for this transport.
    async fn discover(&self) -> Vec<Arc<Instance>> {
        let conn = match self.get_or_open_conn(&self.cfg.libvirt_uri).await {
            Some(c) => c,
            None => return Vec::new(),
        };
        let uuids = match conn.list_qemu_vms().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    target: "qemu",
                    uri = %self.cfg.libvirt_uri,
                    error = ?e,
                    "libvirt discovery failed"
                );
                return Vec::new();
            }
        };
        let mut out = Vec::with_capacity(uuids.len());
        for uuid in uuids {
            let pid = conn.qemu_host_pid(&uuid).await.unwrap_or(0);
            let vcpu_count = match conn.qemu_max_vcpus(&uuid).await {
                Ok(count) if count > 0 => count,
                Ok(_) => {
                    tracing::warn!(
                        target: "qemu",
                        %uuid,
                        "libvirt reported zero maximum vCPUs; skipping VM"
                    );
                    continue;
                }
                Err(error) => {
                    tracing::warn!(
                        target: "qemu",
                        %uuid,
                        %error,
                        "failed to read maximum vCPU count; skipping VM"
                    );
                    continue;
                }
            };
            let libvirt_qmp = libvirt::LibvirtQmp::new(uuid, conn.clone());
            let client: Box<dyn InstanceClient> = Box::new(client::QemuInstanceClient::new(
                uuid,
                libvirt_qmp,
                vcpu_count,
            ));
            // `sock_path` is inherited from the generic
            // `Instance` model; for the libvirt-backed backend
            // it is purely informational (log context), so we
            // stuff the libvirt URI in there.
            let sock_path = Path::new(&self.cfg.libvirt_uri);
            out.push(Arc::new(Instance::new(
                uuid.to_string(),
                sock_path,
                pid,
                client,
            )));
        }
        out
    }

    fn dump_config(&self) -> serde_json::Value {
        serde_json::to_value(&self.cfg).unwrap_or(serde_json::Value::Null)
    }
}
