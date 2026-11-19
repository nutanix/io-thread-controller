// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

//! File-backed backend for component tests.
//!
//! This backend manages state through JSON files in `state_dir`, allowing the
//! external Python component testing harness to manipulate "VM"s as necessary.
//! All VM specific state is tracked in the corresponding file, which are read
//! from and written to as necessary by both this backend and the harness. The
//! controller actuation history is preserved in `calls.json`, which the harness
//! can use in assertions.
//!
//! In general, the harness would set up performance counters for a scenario
//! and then run the controller over the VMs. As performance metrics are
//! currently reported by the backend, these counters are directly included
//! in the VM file. The harness can also manipulate the files while the
//! controller is running, for example to simulate a gain in IOPS.
//!
//! VM JSON files consist of the following which are used to construct a
//! [`ThreadPoolSnapshot`]:
//!  * thread_count - Number of worker threads assigned to this VM
//!  * vcpu_count - Number of vCPUs
//!  * read_io_count - Cumulative read IO operations
//!  * write_io_count - Cumulative write IO operations
//!  * other_io_count - Cumulative other IO operations
//!
//! As well as some control fields:
//!  * fail_snapshot - An error should be injected when attempting to get VMs
//!    ThreadPoolSnapshot.
//!
//! `call.json` is used to give the external testing harness visibility over
//! [`InstanceClient::set_thread_count`] calls by the controller. It keeps
//! track of the following:
//!  * vm - Actuated VM
//!  * thread_count - New VM worker thread count

use std::{
    fs::{self, File, OpenOptions},
    io::{Error as IoError, Read, Seek, SeekFrom, Write},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    backends::{Backend, BackendClientError, BackendRegistration},
    config::ConfigError,
    instance::{Instance, InstanceClient, InstancePerfSample, ThreadPoolSnapshot},
    path::Path,
};

const BACKEND_NAME: &str = "fake";

/// `<backend_config_dir>/fake.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FakeConfig {
    /// Directory of `<vm-id>.json` files.
    state_dir: Path,
    /// Path to `calls.json`.
    calls_path: Path,
}

impl FakeConfig {
    /// Read `fake.json` when it exists.
    fn from_dir(dir: &Path) -> Result<Option<Self>, ConfigError> {
        let path = dir.join("fake.json");
        if !path.exists() {
            return Ok(None);
        }

        let data = fs::read_to_string(&path)?;
        Ok(Some(serde_json::from_str(&data)?))
    }
}

/// One component-test VM, as stored on disk.
///
/// Missing fields take [`VmStatus::default`]: one thread, one vCPU, and
/// `managed` so the controller may scale the VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct VmStatus {
    thread_count: u32,
    vcpu_count: u32,
    per_thread_util: f64,
    fail_snapshot: bool,
    read_io_count: u64,
    write_io_count: u64,
    other_io_count: u64,
}

impl Default for VmStatus {
    fn default() -> Self {
        Self {
            thread_count: 1,
            vcpu_count: 1,
            per_thread_util: 0.0,
            fail_snapshot: false,
            read_io_count: 0,
            write_io_count: 0,
            other_io_count: 0,
        }
    }
}

impl VmStatus {
    fn from_file(file: &File) -> Result<Self, BackendClientError> {
        let mut file = file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        let mut data = String::new();
        file.read_to_string(&mut data)?;
        Ok(serde_json::from_str(&data)?)
    }

    fn write_to(&self, file: &File) -> Result<(), BackendClientError> {
        let mut file = file.try_clone()?;
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        let data = serde_json::to_vec_pretty(self)?;
        file.write_all(&data)?;
        file.sync_all()?;
        Ok(())
    }
}

/// Exclusive `flock` held until dropped. Matches `fcntl.flock(LOCK_EX)`.
struct LockGuard {
    file: File,
}

impl LockGuard {
    /// Acquire the lock for the given VM file. Blocks until it gets the lock.
    fn acquire(vm_path: &std::path::Path) -> Result<Self, IoError> {
        if let Some(parent) = vm_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(false)
            .truncate(false)
            .read(true)
            .write(true)
            .open(vm_path)?;
        file.lock()?;
        Ok(Self { file })
    }

    /// Read the locked VM file.
    fn read(&self) -> Result<VmStatus, BackendClientError> {
        VmStatus::from_file(&self.file)
    }

    /// Write the locked VM file.
    fn write(&self, status: &VmStatus) -> Result<(), BackendClientError> {
        status.write_to(&self.file)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Handle to a VM JSON file on disk. Contains the path and provides
/// locked access to the [`VmStatus`] contents.
#[derive(Debug, Clone)]
struct VmFile {
    path: std::path::PathBuf,
}

impl VmFile {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }

    /// Lock the VM file for exclusive access.
    fn lock(&self) -> Result<LockGuard, IoError> {
        LockGuard::acquire(&self.path)
    }
}

/// Per-VM client. Paths are real filesystem paths, not `crate::path::Path`s.
struct FakeClient {
    vm: VmFile,
    calls_path: std::path::PathBuf,
}

#[async_trait]
impl InstanceClient for FakeClient {
    async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
        let guard = self.vm.lock()?;
        let mut vm = guard.read()?;
        vm.thread_count = count;
        guard.write(&vm)?;
        let mut calls = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.calls_path)?;
        let line = serde_json::json!({
            "vm": self.vm.path.file_stem().and_then(|stem| stem.to_str()).unwrap_or(""),
            "threads": count,
        });
        writeln!(calls, "{line}")?;
        Ok(())
    }

    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
        let guard = self.vm.lock()?;
        let vm = guard.read()?;
        if vm.fail_snapshot {
            return Err(BackendClientError::InvalidState(format!(
                "fake snapshot failed for {}",
                self.vm.path.display()
            )));
        }
        Ok(ThreadPoolSnapshot {
            thread_count: vm.thread_count,
            vcpu_count: vm.vcpu_count,
            perf: Some(InstancePerfSample {
                read_io_count: vm.read_io_count,
                write_io_count: vm.write_io_count,
                other_io_count: vm.other_io_count,
                read_bytes_total: 0,
                write_bytes_total: 0,
                ..Default::default()
            }),
            per_thread_util: Some(vm.per_thread_util),
        })
    }

    fn initial_pool_is_managed(&self, _thread_count: u32, _vcpu_count: u32) -> bool {
        true
    }

    async fn close(&self) {}
}

/// Load every `<vm-id>.json` file in `state_dir`.
#[tracing::instrument(skip(state_dir), fields(directory = %state_dir))]
fn discover_vms(state_dir: &Path, calls_path: &Path) -> Vec<Arc<Instance>> {
    let entries = fs::read_dir(state_dir).expect("fake backend state directory is not readable");
    let mut instances = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(id) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let client = FakeClient {
            vm: VmFile::new(path.clone()),
            calls_path: calls_path.to_path_buf(),
        };
        instances.push(Arc::new(Instance::new(
            id.to_string(),
            Path::new(id),
            0,
            client,
        )));
    }
    instances
}

/// Fleet adapter that discovers JSON VM files.
pub struct FakeBackend {
    cfg: Option<FakeConfig>,
}

impl FakeBackend {
    /// Build from `<dir>/fake.json`, or an empty inventory when it is absent.
    pub fn from_config_dir(dir: &Path) -> Result<Self, ConfigError> {
        Ok(Self {
            cfg: FakeConfig::from_dir(dir)?,
        })
    }
}

#[async_trait]
impl Backend for FakeBackend {
    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    async fn discover(&self) -> Vec<Arc<Instance>> {
        let Some(cfg) = &self.cfg else {
            return Vec::new();
        };
        discover_vms(&cfg.state_dir, &cfg.calls_path)
    }

    fn watch_paths(&self) -> Vec<Path> {
        self.cfg
            .as_ref()
            .map(|cfg| vec![cfg.state_dir.clone()])
            .unwrap_or_default()
    }
}

fn build_fake_backend(dir: &Path) -> Result<Box<dyn Backend>, BackendClientError> {
    Ok(Box::new(FakeBackend::from_config_dir(dir)?))
}

#[linkme::distributed_slice(crate::backends::BACKENDS)]
static FAKE_BACKEND: BackendRegistration = BackendRegistration {
    name: BACKEND_NAME,
    build: build_fake_backend,
};
