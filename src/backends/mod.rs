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

//! Types shared by per-VM backend clients.

use std::io::{Error as IoError, ErrorKind};

use procfs::ProcError;
use serde::{Deserialize, Serialize};
use serde_json;
use thiserror::Error;

use crate::{
    config::{Config, ConfigError},
    instance::{Instance, InstanceClient, InstanceError},
    util::Path,
};

/// One virtio-scsi IOThread-to-virtqueue mapping.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VqMapping {
    /// IOThread object identifier.
    pub iothread: String,
    /// Virtqueue indices assigned to this IOThread.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vqs: Vec<u16>,
}

/// Optional properties used when creating a named IOThread.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IoThreadProperties {
    /// Maximum polling window per iteration, in nanoseconds.
    #[serde(rename = "poll-max-ns", skip_serializing_if = "Option::is_none")]
    pub poll_max_ns: Option<i64>,
    /// Poll-window growth step.
    #[serde(rename = "poll-grow", skip_serializing_if = "Option::is_none")]
    pub poll_grow: Option<i64>,
    /// Poll-window shrink step.
    #[serde(rename = "poll-shrink", skip_serializing_if = "Option::is_none")]
    pub poll_shrink: Option<i64>,
    /// Maximum AIO completion batch.
    #[serde(rename = "aio-max-batch", skip_serializing_if = "Option::is_none")]
    pub aio_max_batch: Option<i64>,
    /// Minimum AIO worker-pool size.
    #[serde(rename = "thread-pool-min", skip_serializing_if = "Option::is_none")]
    pub thread_pool_min: Option<i64>,
    /// Maximum AIO worker-pool size.
    #[serde(rename = "thread-pool-max", skip_serializing_if = "Option::is_none")]
    pub thread_pool_max: Option<i64>,
}

/// Failure returned by a backend client operation.
#[derive(Debug, Error)]
pub enum BackendClientError {
    #[error(transparent)]
    Clap(#[from] clap::Error),

    #[error(transparent)]
    Config(#[from] ConfigError),

    #[error(transparent)]
    Instance(#[from] InstanceError),

    /// The backend deliberately does not implement this operation.
    #[error("operation not supported on this backend: {0}")]
    NotSupported(String),
    /// The transport disconnected while serving the request.
    #[error("disconnected: {0}")]
    Disconnected(String),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    /// Any other transport or framing failure.
    #[error("transport: {0}")]
    Transport(String),
    /// A structured QMP error response.
    #[error("qmp {cmd} error: {class}: {desc}")]
    QmpError {
        /// QMP command that failed.
        cmd: String,
        /// QMP error class.
        class: String,
        /// Human-readable error description.
        desc: String,
    },
    /// Protocol or framing error (payload limits, UTF-8, refusals, parse text).
    #[error("{0}")]
    Protocol(String),
    /// Incomplete or inconsistent backend or host process state.
    #[error("{0}")]
    InvalidState(String),
    #[error("proc error")]
    Procfs(#[from] ProcError),

    #[error(transparent)]
    Zbus(#[from] zbus::Error),
}

impl BackendClientError {
    /// Classify transport I/O while retaining operation context.
    pub fn from_transport_io(context: &str, error: &IoError) -> Self {
        let message = format!("{context}: {error}");
        if is_peer_disconnect_kind(error.kind()) {
            Self::Disconnected(message)
        } else {
            Self::Transport(message)
        }
    }
}

impl From<IoError> for BackendClientError {
    fn from(error: IoError) -> Self {
        if is_peer_disconnect_kind(error.kind()) {
            Self::Disconnected(error.to_string())
        } else {
            Self::Transport(error.to_string())
        }
    }
}

/// Return whether an I/O failure represents normal peer teardown.
fn is_peer_disconnect_kind(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::BrokenPipe
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::UnexpectedEof
            | ErrorKind::NotFound
    )
}

use std::{io, sync::Arc};

#[cfg(feature = "fake-backend")]
pub mod fake;
#[cfg(feature = "qemu-backend")]
pub mod qemu;

use async_trait::async_trait;
use linkme::distributed_slice;

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

    /// Optional backend-specific command-line interface. This allows passing
    /// arguments specific to the backend from main.
    fn cli_subcommand(&self) -> Option<clap::Command> {
        None
    }

    //  This allows passing arguments specific to the backend from main.
    async fn run_cli(&self, _matches: &clap::ArgMatches) -> Result<(), BackendClientError> {
        Err(BackendClientError::NotSupported(format!(
            "backend `{}` has no command-line interface",
            self.name()
        )))
    }

    /// Return the backend's effective configuration.
    fn dump_config(&self) -> serde_json::Value {
        serde_json::Value::Null
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
