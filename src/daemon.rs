// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Daemon event loop and backend registration.

use std::{io, sync::Arc, time::Duration};

use futures_util::{StreamExt, future::join_all};
use inotify::{Inotify, WatchMask};
use thiserror::Error;
use tokio::time::{MissedTickBehavior, interval};

pub const VERSION: &str = match option_env!("IO_THREAD_CONTROLLER_VERSION") {
    Some(v) => v,
    None => "unknown",
};

use crate::{
    backends::Backend,
    config::Config,
    controller::{Controller, ControllerError},
    dbus,
    engines::{EngineError, load_registered_engine},
    instance::Instance,
    util::Path,
};

#[derive(Error, Debug)]
pub enum DaemonError {
    #[error(transparent)]
    Controller(#[from] ControllerError),

    #[error(transparent)]
    Engine(#[from] EngineError),

    #[error("backend watch path `{0}` is not a directory")]
    InvalidWatchPath(String),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Zbus(#[from] zbus::Error),
}

/// Userspace inotify read buffer.
///
/// The kernel retains additional events in its own queue; 32 KiB simply lets
/// one read drain a large VM-lifecycle burst efficiently.
const INOTIFY_EVENT_BUF_SIZE: usize = 32 * 1024;

/// Run until SIGTERM or SIGINT.
pub async fn run(cfg: Config, backends: Vec<Box<dyn Backend>>) -> Result<(), DaemonError> {
    tracing::info!(
        target: "controller",
        version = VERSION,
        "io-thread-controller starting"
    );
    if cfg.print_status_header {
        tracing::info!(
            target: "status",
            "# vm=<id> thr=<threads> iops=<read>/<write>/<other> \
             iops_1_5_15m=<1m>/<5m>/<15m> bw_mb_s=<read>/<write> \
             cpu=<average>/<total> cpu_us_per_io_1_5_15m=<1m>/<5m>/<15m>"
        );
        tracing::info!(
            target: "status",
            "# aggregate tracked=<instances> threads=<threads> \
             iops_1_5_15m=<1m>/<5m>/<15m>"
        );
    }
    let engine = load_registered_engine(&cfg.engine_config_dir, &cfg.engine)?;
    let mut controller = Controller::new(cfg.clone(), engine)?;

    let discovered = discover_all(&backends).await;
    let (added, _) = controller.sync_instances(discovered).await?;
    tracing::info!(target: "controller", added, "initial discovery");

    let (dbus_tx, mut dbus_rx) = tokio::sync::mpsc::channel(1);
    let _dbus_connection = dbus::serve(dbus::Service::new(dbus_tx)).await?;
    let mut inotify_events = setup_inotify(&backends)?;
    let mut poll = interval(Duration::from_secs_f64(cfg.scale_poll_secs));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;

    loop {
        tokio::select! {
            _ = poll.tick() => {
                controller.tick().await?;
                controller.sync_instances(discover_all(&backends).await).await?;
            }
            Some(event) = inotify_events.next() => {
                let event = event?;
                tracing::debug!(
                    target: "controller",
                    name = ?event.name,
                    mask = ?event.mask,
                    "backend path changed; rescanning"
                );
                controller.sync_instances(discover_all(&backends).await).await?;
            }
            Some(request) = dbus_rx.recv() => {
                controller.handle_dbus_request(request).await;
            }
            _ = sigterm.recv() => break,
            _ = sigint.recv() => break,
        }
    }
    Ok(())
}

/// Discover all backend inventories concurrently.
async fn discover_all(backends: &[Box<dyn Backend>]) -> Vec<Arc<Instance>> {
    join_all(backends.iter().map(|backend| backend.discover()))
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// Watch every backend path and fail if any configured path is invalid.
///
/// Inotify identifies the changed directory entry and event kind, but it does
/// not provide the backend's resulting inventory delta. Any relevant event
/// therefore triggers a complete discovery pass, and inventory reconciliation
/// determines which VMs were added or removed.
fn setup_inotify(
    backends: &[Box<dyn Backend>],
) -> Result<inotify::EventStream<[u8; INOTIFY_EVENT_BUF_SIZE]>, DaemonError> {
    let paths: Vec<Path> = backends
        .iter()
        .flat_map(|backends| backends.watch_paths())
        .collect();
    let inotify = Inotify::init()?;
    for path in paths {
        if !path.is_dir() {
            return Err(DaemonError::InvalidWatchPath(path.display().to_string()));
        }
        inotify.watches().add(
            &path,
            WatchMask::CREATE | WatchMask::DELETE | WatchMask::MOVED_TO | WatchMask::MOVED_FROM,
        )?;
    }
    Ok(inotify.into_event_stream([0; INOTIFY_EVENT_BUF_SIZE])?)
}
