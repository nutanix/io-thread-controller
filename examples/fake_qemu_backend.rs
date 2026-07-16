// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc. All rights reserved.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! End-to-end example: an io-thread-controller binary built by
//! *depending on the crate as a library* that plugs in a fake
//! QEMU-shaped backend for component testing.
//!
//! The point of the fake-QEMU backend is to exercise the
//! *backend-supplied ``per_thread_util``* path (the QEMU
//! backend reads per-iothread jiffies itself and hands the
//! controller a precomputed number, so the /proc walker skips)
//! without depending on a real libvirt+QEMU pair.
//!
//! Layout the example demonstrates:
//!
//!   * `FakeQemuConfig` and `FakeQemuBackend` live entirely in this file, i.e.
//!     in the consumer crate.  Nothing about them touches
//!     `io-thread-controller`'s source tree.
//!   * The consumer implements `InstanceClient` for the wire client and
//!     `Backend` for the backend handle -- exactly the trait objects the
//!     shipped controller already speaks.
//!   * `main` uses the shipped [`ControllerBuilder`] / [`run`] surface: parse
//!     config, materialise the custom backend, hand everything to the daemon.
//!
//! The companion pytest fixture spawns `fake_qemu.py` (a Python
//! process backing a UNIX socket + HTTP admin surface), then
//! runs this example against the socket.  The example connects
//! to that socket and drives the fake service's JSON-lines
//! protocol -- which is what proves both the library plug-in
//! path *and* the backend-supplied util path in the controller.

use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::Mutex,
};

use io_thread_controller::{
    backends::Backend,
    backends::BackendClientError,
    config::{Config, load_config, validate_config},
    daemon::run,
    instance::{Instance, InstanceClient, InstancePerfSample, ThreadPoolSnapshot},
    util::Path,
};

// ---------------------------------------------------------------
// Backend-specific config
// ---------------------------------------------------------------
//
// Loaded from ``<backend_config_dir>/fake_qemu.json``.  Same
// shape (``from_dir`` helper + serde defaults) as the shipped
// QEMU backend so this example mirrors the in-tree
// convention verbatim.

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FakeQemuConfig {
    /// UNIX socket the fake QEMU process listens on.  The
    /// example spins up one tracked instance per config file;
    /// discovery reports it exactly once per tick.
    #[serde(default = "default_socket")]
    socket_path: Path,
    /// Stable id used verbatim as ``Instance::id`` and every
    /// log line's ``vm=`` field.  Matches the shape of a
    /// QEMU vm UUID in real life.
    #[serde(default = "default_vm_id")]
    vm_id: String,
}

fn default_socket() -> Path {
    Path::new("/tmp/fake_qemu.sock")
}

fn default_vm_id() -> String {
    "vm-fake-qemu".to_string()
}

impl Default for FakeQemuConfig {
    fn default() -> Self {
        Self {
            socket_path: default_socket(),
            vm_id: default_vm_id(),
        }
    }
}

impl FakeQemuConfig {
    fn from_dir(dir: &Path) -> Self {
        let path = dir.join("fake_qemu.json");
        if !path.exists() {
            return Self::default();
        }
        let body = std::fs::read_to_string(&path).expect("read fake QEMU config");
        serde_json::from_str(&body).expect("deserialize fake QEMU config")
    }
}

// ---------------------------------------------------------------
// InstanceClient: wire client
// ---------------------------------------------------------------

/// Wire client for one fake-QEMU instance.  Owns the connection
/// to the UNIX socket and speaks the JSON-lines protocol
/// (``get_snapshot`` / ``set_thread_count``) with the Python
/// fake.  Lazily dials the socket on the first call so a
/// pre-fake-startup instantiation still works.
struct FakeQemuClient {
    socket_path: Path,
    stream: Mutex<Option<BufReader<UnixStream>>>,
}

impl FakeQemuClient {
    fn new(socket_path: Path) -> Self {
        Self {
            socket_path,
            stream: Mutex::new(None),
        }
    }

    /// Return an initialised buffered stream, dialing on first
    /// use and reconnecting on any transport error the caller
    /// observed on the previous round-trip.
    async fn connect(
        &self,
    ) -> Result<tokio::sync::MutexGuard<'_, Option<BufReader<UnixStream>>>, BackendClientError>
    {
        let mut guard = self.stream.lock().await;
        if guard.is_none() {
            let s = UnixStream::connect(&self.socket_path)
                .await
                .map_err(|e| BackendClientError::Transport(e.to_string()))?;
            *guard = Some(BufReader::new(s));
        }
        Ok(guard)
    }

    /// Send one JSON request line and read one JSON reply line.
    /// Drops the cached stream on any transport error so the
    /// next call reconnects fresh.
    async fn round_trip(
        &self,
        req: &serde_json::Value,
    ) -> Result<serde_json::Value, BackendClientError> {
        let mut guard = self.connect().await?;
        let stream = guard.as_mut().expect("stream just initialised");
        let mut req_line = serde_json::to_vec(req).map_err(BackendClientError::from)?;
        req_line.push(b'\n');
        if let Err(e) = stream.get_mut().write_all(&req_line).await {
            *guard = None;
            return Err(BackendClientError::Transport(e.to_string()));
        }
        let mut reply = String::new();
        match stream.read_line(&mut reply).await {
            Ok(0) => {
                *guard = None;
                Err(BackendClientError::Transport("fake_qemu closed".into()))
            }
            Ok(_) => serde_json::from_str(reply.trim()).map_err(BackendClientError::from),
            Err(e) => {
                *guard = None;
                Err(BackendClientError::Transport(e.to_string()))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct SnapshotReply {
    num_threads: u32,
    per_thread_util: f64,
    read_ios: u64,
    write_ios: u64,
}

#[async_trait]
impl InstanceClient for FakeQemuClient {
    async fn set_thread_count(&self, n: u32) -> Result<(), BackendClientError> {
        let req = serde_json::json!({
            "cmd": "set_thread_count",
            "value": n,
        });
        self.round_trip(&req).await?;
        Ok(())
    }

    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
        let req = serde_json::json!({"cmd": "get_snapshot"});
        let v = self.round_trip(&req).await?;
        let snap: SnapshotReply = serde_json::from_value(v).map_err(BackendClientError::from)?;
        Ok(ThreadPoolSnapshot {
            thread_count: snap.num_threads, // FIXME snap.num_threads should be snap.thread_count
            perf: Some(InstancePerfSample {
                read_io_count: snap.read_ios,
                write_io_count: snap.write_ios,
                ..Default::default()
            }),
            // The whole point of fake QEMU: hand the controller
            // a backend-computed per-thread util so the /proc
            // walker is skipped.  Mirrors the real QEMU
            // backend's ``client::QemuInstanceClient`` semantics.
            per_thread_util: Some(snap.per_thread_util),
            vcpu_count: 16,
        })
    }

    async fn close(&self) {
        let mut guard = self.stream.lock().await;
        *guard = None;
    }
}

// ---------------------------------------------------------------
// Backend: discovery glue
// ---------------------------------------------------------------

/// Backend.  Holds the config and materialises exactly
/// one [`Instance`] per discovery pass (the fake QEMU is a
/// singleton in every test).  We deliberately keep the
/// discovery contract identical to the shipped QEMU backend so
/// the pytest fixture can swap this example in front of the
/// real backend if we ever want an integration test against a
/// live libvirtd.
struct FakeQemuBackend {
    cfg: FakeQemuConfig,
}

impl FakeQemuBackend {
    fn from_config_dir(dir: &Path) -> Self {
        Self {
            cfg: FakeQemuConfig::from_dir(dir),
        }
    }
}

#[async_trait]
impl Backend for FakeQemuBackend {
    fn name(&self) -> &'static str {
        "fake_qemu"
    }

    async fn discover(&self) -> Vec<Arc<Instance>> {
        // The fake QEMU is always "there" from the controller's
        // point of view: a real libvirt/QEMU discovery would
        // enumerate active vms here, but for the test we
        // hand out one fixed instance.
        let client: Box<dyn InstanceClient> =
            Box::new(FakeQemuClient::new(self.cfg.socket_path.clone()));
        vec![Arc::new(Instance::new(
            self.cfg.vm_id.clone(),
            self.cfg.socket_path.clone(),
            // Fake QEMU deliberately sets pid=0 so the /proc
            // walker is skipped -- same contract as the real
            // backend, which does not know the QEMU pid until
            // after a QMP round-trip.
            0,
            client,
        ))]
    }

    fn watch_paths(&self) -> Vec<Path> {
        // Fake QEMU is a persistent singleton; no filesystem
        // churn to watch for.
        Vec::new()
    }
}

// ---------------------------------------------------------------
// Binary entrypoint
// ---------------------------------------------------------------

#[derive(Debug, Parser)]
#[command(
    name = "fake-qemu-backend",
    about = "io-thread-controller variant with a Python-driven fake QEMU \
             backend plugged in via the library API. \
             Used exclusively by tests/component/test_fake_qemu_backend.py."
)]
struct Cli {
    #[arg(long, default_value = "/etc/io-thread-controller/config.json")]
    config: Path,

    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log: String,
}

fn init_logging(filter: &str) {
    let subscriber = tracing_subscriber::fmt()
        .with_target(true)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(filter)
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_logging(&cli.log);

    let cfg: Config = load_config(&cli.config).expect("load config");
    validate_config(&cfg).expect("validate config");

    let backend = FakeQemuBackend::from_config_dir(&cfg.backend_config_dir);
    tracing::info!(
        target: "fake-qemu-backend",
        socket = %backend.cfg.socket_path.display(),
        vm = %backend.cfg.vm_id,
        "fake QEMU backend registered"
    );

    run(cfg, vec![Box::new(backend)])
        .await
        .expect("run io-thread-controller daemon");
}
