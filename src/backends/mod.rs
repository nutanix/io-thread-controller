// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Fleet-level backend contract and in-tree registry.
//!
//! A [`Backend`] owns backend-wide configuration and discovers zero or
//! more backend-neutral [`crate::instance::Instance`] records. Each record
//! carries an [`crate::instance::InstanceClient`] for operations on exactly
//! one VM; the record itself is deliberately not another backend trait.

//! Errors shared by per-VM backend clients.

use thiserror::Error;

/// Failure returned by a backend client operation.
#[derive(Debug, Error)]
pub enum BackendClientError {
    /// The backend deliberately does not implement this operation.
    #[error("operation not supported on this backend: {0}")]
    NotSupported(String),
    /// The transport disconnected while serving the request.
    #[error("disconnected: {0}")]
    Disconnected(String),
    /// Any other transport or framing failure.
    #[error("transport: {0}")]
    Transport(String),
    /// Protocol or framing error (payload limits, UTF-8, refusals, parse text).
    #[error("{0}")]
    Protocol(String),
    /// Incomplete or inconsistent backend or host process state.
    #[error("{0}")]
    InvalidState(String),
}

use std::{io, sync::Arc};

use async_trait::async_trait;
use linkme::distributed_slice;

use crate::{
    config::Config,
    instance::{Instance, InstanceClient},
    util::Path,
};

/// One in-tree (or out-of-tree) backend factory registered on [`BACKENDS`].
pub struct BackendRegistration {
    /// Stable backend name used in logs and diagnostics.
    pub name: &'static str,
    /// Build the backend from `Config::backend_config_dir`.
    pub build: fn(&Path) -> Result<Box<dyn Backend>, BackendClientError>,
}

/// Linked-in backend factories. Feature-gated modules append themselves here.
#[distributed_slice]
pub static BACKENDS: [BackendRegistration] = [..];

/// Fleet-level integration for one backend implementation.
#[async_trait]
pub trait Backend: Send + Sync {
    /// Stable backend name used in logs and configuration.
    fn name(&self) -> &'static str;

    /// Return the backend's complete live VM inventory.
    async fn discover(&self) -> Vec<Arc<Instance>>;

    /// Directories whose changes should trigger immediate rediscovery.
    ///
    /// Inotify identifies the changed entry and event kind, but it does not
    /// describe the backend-level inventory delta. The daemon therefore uses
    /// any event only as a prompt to run a complete discovery pass.
    fn watch_paths(&self) -> Vec<Path> {
        Vec::new()
    }
}

/// Construct the backends compiled into the daemon.
// FIXME shouldn't take the whole config, just the path
pub fn registered_backends(cfg: &Config) -> Result<Vec<Box<dyn Backend>>, BackendClientError> {
    let dir = &cfg.backend_config_dir;
    let mut backends = Vec::new();
    for registration in BACKENDS {
        backends.push((registration.build)(dir)?);
    }
    // linkme iteration order is unspecified; keep a stable order so
    // dual-backend discovery remains deterministic across runs.
    backends.sort_by_key(|backend| backend.name());
    Ok(backends)
}

/// Discover VMs laid out as `socket_dir/<id>/<socket_name>`.
///
/// The helper owns only the common directory walk and peer-PID lookup. The
/// supplied factory keeps the control protocol and concrete client
/// backend-owned.
pub async fn discover_via_control_socket<F>(
    socket_dir: impl AsRef<Path>,
    socket_name: &str,
    make_client: F,
) -> io::Result<Vec<Arc<Instance>>>
where
    F: Fn(Path) -> Box<dyn InstanceClient>,
{
    let socket_dir = socket_dir.as_ref();
    tracing::debug!("discovering in {}", socket_dir.display());
    let mut entries = match tokio::fs::read_dir(socket_dir).await {
        Ok(entries) => entries,
        Err(error) => {
            tracing::warn!(
                target: "controller",
                directory = %socket_dir.display(),
                %error,
                "backend socket directory is not readable"
            );
            return Ok(Vec::new());
        }
    };
    let mut instances = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        match entry.metadata().await {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    continue;
                }
            }
            Err(_error) => {
                // FIXME log warning
                continue;
            }
        };
        let Ok(id) = entry.file_name().into_string() else {
            continue;
        };
        let sock_path = Path::new(&socket_dir.join(entry.path().join(socket_name)));
        // FIXME add unit test for this function
        let pid = match control_socket_peer_pid(&sock_path).await {
            Ok(pid) => pid,
            Err(error) => {
                if error.kind() != io::ErrorKind::NotFound {
                    // FIXME ignore ENOENT
                    tracing::warn!(
                        target: "controller",
                        vm = %id,
                        %error,
                        "failed to identify control-socket peer"
                    );
                }
                continue;
            }
        };
        let client = make_client(sock_path.clone());
        instances.push(Arc::new(Instance::new(id, sock_path, pid, client)));
    }
    Ok(instances)
}

/// Return the process ID at the far end of a UNIX control socket.
async fn control_socket_peer_pid(sock_path: &Path) -> io::Result<i32> {
    let stream = tokio::net::UnixStream::connect(sock_path).await?;
    stream.peer_cred()?.pid().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "control-socket peer has no process ID",
        )
    })
}
