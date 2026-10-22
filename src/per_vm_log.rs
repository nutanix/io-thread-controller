// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Per-VM log files.
//!
//! [`PerVmLog`] is a [`tracing_subscriber::Layer`] that appends every event
//! carrying a `vm` field to that VM's own log file, without repeating the `vm`
//! field. Events on [`TARGET`] belong only in these files and are filtered out
//! of the main log.

use std::{
    collections::HashSet,
    fmt::{self, Write as _},
    fs::{OpenOptions, create_dir_all},
    io::Write as _,
    path::PathBuf,
    sync::{Arc, Mutex, RwLock},
    time::SystemTime,
};

use tracing::{
    Event, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{EnvFilter, Layer, layer::Context};

use crate::util::Path;

/// Default per-VM log directory.
pub const PER_VM_LOG_DIR: &str = "/var/log/io-thread-controller/vms";

/// Target for events that belong only in per-VM log files.
pub const TARGET: &str = "per_vm";

/// Build the main log's filter from `directives`, excluding [`TARGET`].
///
/// Invalid directives fall back to `info`.
pub fn main_log_filter(directives: &str) -> EnvFilter {
    EnvFilter::try_new(directives)
        .unwrap_or_else(|_| EnvFilter::new("info"))
        .add_directive(format!("{TARGET}=off").parse().expect("valid directive"))
}

/// Layer writing `vm`-tagged events to per-VM files; clones share state.
#[derive(Clone, Default)]
pub struct PerVmLog {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Destination directory; `None` disables per-VM files.
    dir: RwLock<Option<Path>>,
    /// VM IDs for which per-VM log I/O failures were already reported.
    warned: Mutex<HashSet<String>>,
}

impl PerVmLog {
    /// Write per-VM files under `dir`, or stop writing them with `None`.
    pub fn set_dir(&self, dir: Option<Path>) {
        *self.inner.dir.write().expect("lock poisoned") = dir;
    }

    /// Return the log file of `vm`, or `None` while per-VM files are disabled.
    pub fn path(&self, vm: &str) -> Option<PathBuf> {
        let name: String = vm
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                    ch
                } else {
                    '_'
                }
            })
            .collect();
        self.inner
            .dir
            .read()
            .expect("lock poisoned")
            .as_ref()
            .map(|dir| dir.join(format!("{name}.log")))
    }

    fn append(&self, vm: &str, line: &str) {
        let Some(path) = self.path(vm) else {
            return;
        };
        let result = path.parent().map_or(Ok(()), create_dir_all).and_then(|()| {
            let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
            writeln!(
                file,
                "{} {line}",
                humantime::format_rfc3339_millis(SystemTime::now())
            )
        });
        if let Err(error) = result
            && self
                .inner
                .warned
                .lock()
                .expect("lock poisoned")
                .insert(vm.to_string())
        {
            // Reporting through tracing from inside a layer would re-enter the
            // subscriber while it is dispatching this event.
            eprintln!(
                "WARN failed to write per-VM log path={} error={error}",
                path.display()
            );
        }
    }
}

impl<S: Subscriber> Layer<S> for PerVmLog {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if self.inner.dir.read().expect("lock poisoned").is_none() {
            return;
        }
        let mut record = Record::default();
        event.record(&mut record);
        if let Some(vm) = record.vm.take() {
            self.append(&vm, &record.line());
        }
    }
}

/// One event split into its VM, message, and remaining `key=value` fields.
#[derive(Default)]
struct Record {
    vm: Option<String>,
    message: Option<String>,
    fields: String,
}

impl Record {
    fn record(&mut self, field: &Field, value: String) {
        match field.name() {
            "vm" => self.vm = Some(value),
            "message" => self.message = Some(value),
            name => {
                if !self.fields.is_empty() {
                    self.fields.push(' ');
                }
                let _ = write!(self.fields, "{name}={value}");
            }
        }
    }

    /// Render the message, if any, followed by the remaining fields.
    fn line(self) -> String {
        match self.message {
            Some(message) if self.fields.is_empty() => message,
            Some(message) => format!("{message} {}", self.fields),
            None => self.fields,
        }
    }
}

impl Visit for Record {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record(field, format!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::{filter::LevelFilter, layer::SubscriberExt};

    use super::*;

    /// Test that only `vm`-tagged events reach the per-VM file, rendered
    /// as message then fields and without the `vm` field.
    #[test]
    fn test_writes_vm_tagged_events_without_vm_field() {
        let dir = tempfile::tempdir().unwrap();
        let log = PerVmLog::default();
        log.set_dir(Some(Path::new(dir.path())));
        let subscriber = tracing_subscriber::registry().with(log.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: TARGET, vm = "vm-a", "status=1 2 3");
            tracing::warn!(target: "controller", event = "scale_failed", vm = %"vm-a", target = 4, "oops");
            tracing::info!(target: "controller", event = "vm_added", vm = "vm-a");
            tracing::info!(target: "controller", "no VM here");
        });

        let contents = std::fs::read_to_string(log.path("vm-a").unwrap()).unwrap();
        let records: Vec<_> = contents
            .lines()
            .map(|line| line.split_once(' ').unwrap().1)
            .collect();
        assert_eq!(
            records,
            [
                "status=1 2 3",
                "oops event=scale_failed target=4",
                "event=vm_added"
            ]
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Test that per-VM-only events stay out of the main log, and that the
    /// main log's level does not drop them from per-VM files.
    #[test]
    fn test_per_vm_target_bypasses_main_log() {
        let dir = tempfile::tempdir().unwrap();
        let log = PerVmLog::default();
        log.set_dir(Some(Path::new(dir.path())));
        let main_log = Buffer::default();
        let writer = main_log.clone();
        let subscriber = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(move || writer.clone())
                    .with_filter(main_log_filter("warn")),
            )
            .with(log.clone().with_filter(LevelFilter::INFO));
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: TARGET, vm = "vm-a", "status=1");
            tracing::warn!(target: "controller", vm = "vm-a", "refresh failed");
        });

        let main_log = String::from_utf8(main_log.0.lock().unwrap().clone()).unwrap();
        assert!(!main_log.contains("status=1"), "{main_log}");
        assert!(main_log.contains("refresh failed"), "{main_log}");
        let contents = std::fs::read_to_string(log.path("vm-a").unwrap()).unwrap();
        assert_eq!(contents.lines().count(), 2, "{contents}");
    }

    /// Test that nothing is written while per-VM files are disabled.
    #[test]
    fn test_disabled_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let log = PerVmLog::default();
        let subscriber = tracing_subscriber::registry().with(log.clone());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: TARGET, vm = "vm-a", "status=1");
        });
        assert!(log.path("vm-a").is_none());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
