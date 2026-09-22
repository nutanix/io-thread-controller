// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! D-Bus surface for operator-facing verbs.
//!
//! Initial scope: SetThreadCount.  QEMU-specific verbs
//! (GetIOThreadVqMapping, AddIOThread, DelIOThread,
//! SetIOThreadVqMapping) land in later commits.

use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

/// D-Bus well-known bus name the daemon claims at startup.
pub const DBUS_BUS_NAME: &str = "com.nutanix.io_thread_controller1";
/// Object path served by the daemon on the system bus.
pub const DBUS_OBJECT_PATH: &str = "/com/nutanix/io_thread_controller1";
/// Interface name the operator verbs live on.
pub const DBUS_INTERFACE: &str = "com.nutanix.io_thread_controller1";
/// Upper bound on how long a D-Bus verb waits for the engine
/// task to service the request before failing the call.
pub const DBUS_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Cross-thread message shape: each variant matches one D-Bus
/// method.  The controller task pulls these off, services them
/// serially, and replies through the embedded oneshot channel.
#[derive(Debug)]
pub enum DbusRequest {
    /// Operator-driven `SetThreadCount` verb; asks the engine
    /// to walk the addressed instance to exactly `threads`.
    SetThreadCount {
        /// Instance / vm id to address.
        vm: String,
        /// Target thread count.
        threads: u32,
        /// If true, the engine remembers the manual override and
        /// suppresses automatic scaling on this instance until
        /// cleared.
        sticky: bool,
        /// One-shot channel used to complete the D-Bus method call.
        reply: oneshot::Sender<Result<(), String>>,
    },
}

/// zbus interface object -- one handle per bus connection.
#[derive(Clone)]
pub struct Service {
    /// Controller-task request channel.
    tx: mpsc::Sender<DbusRequest>,
}

impl Service {
    /// Build a service handle bound to the given engine-request
    /// channel.
    pub fn new(tx: mpsc::Sender<DbusRequest>) -> Self {
        Self { tx }
    }
}

/// Wait for a controller reply while enforcing the D-Bus request timeout.
#[cfg(debug_assertions)]
async fn await_with_timeout<T>(rx: oneshot::Receiver<T>) -> Result<T, zbus::fdo::Error> {
    match tokio::time::timeout(DBUS_REQUEST_TIMEOUT, rx).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(_canceled)) => Err(zbus::fdo::Error::Failed(
            "controller reply channel closed".into(),
        )),
        Err(_) => Err(zbus::fdo::Error::Failed(format!(
            "request timed out after {DBUS_REQUEST_TIMEOUT:?}"
        ))),
    }
}

#[zbus::interface(name = "com.nutanix.io_thread_controller1")]
impl Service {
    /// Set one VM's worker count through the debug-only control surface.
    #[cfg(debug_assertions)]
    async fn set_thread_count(
        &self,
        vm: String,
        threads: u32,
        sticky: bool,
    ) -> zbus::fdo::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(DbusRequest::SetThreadCount {
                vm,
                threads,
                sticky,
                reply: tx,
            })
            .await
            .map_err(|_| zbus::fdo::Error::Failed("controller channel closed".into()))?;
        match await_with_timeout(rx).await? {
            Ok(()) => Ok(()),
            Err(e) => Err(zbus::fdo::Error::Failed(e)),
        }
    }
}

/// Spin the D-Bus service on the well-known name / object path
/// backed by `service` and drive replies from the controller mpsc.
pub async fn serve(service: Service) -> Result<zbus::Connection, zbus::Error> {
    let conn = zbus::connection::Builder::system()?
        .name(DBUS_BUS_NAME)?
        .serve_at(DBUS_OBJECT_PATH, service)?
        .build()
        .await?;
    Ok(conn)
}
