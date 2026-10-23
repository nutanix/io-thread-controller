// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

//! File-backed backend for component tests.
//!
//! Instead of requiring a running QEMU instance, this backend manages
//! state through JSON files in `state_dir`. This allows for the external
//! Python component testing harness to manipulate "VM"s as necessary.
//! Similarly, controller actuations are preserved in `calls.json`, which
//! the harness can use in assertions.
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
    io::{Error as IoError, Write},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{
    backends::{Backend, BackendClientError, BackendRegistration},
    config::ConfigError,
    instance::{Instance, InstanceClient, InstancePerfSample, ThreadPoolSnapshot},
    util::Path,
};

const BACKEND_NAME: &str = "fake";

fn build_fake_backend(dir: &Path) -> Result<Box<dyn Backend>, BackendClientError> {
    Ok(Box::new(FakeBackend::from_config_dir(dir)?))
}

#[linkme::distributed_slice(crate::backends::BACKENDS)]
static FAKE_BACKEND: BackendRegistration = BackendRegistration {
    name: BACKEND_NAME,
    build: build_fake_backend,
};

/// `<backend_config_dir>/fake.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FakeConfig {
    /// Directory of `<vm-id>.json` files and `calls.json`.
    state_dir: Path,
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
/// Missing fields take [`VmFile::default`]: one thread, one vCPU, and
/// `managed` so the controller may scale the VM.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
struct VmFile {
    thread_count: u32,
    vcpu_count: u32,
    /// First-snapshot ownership. `true` lets the controller scale the VM.
    managed: bool,
    fail_snapshot: bool,
    read_io_count: u64,
    write_io_count: u64,
    other_io_count: u64,
}

impl Default for VmFile {
    fn default() -> Self {
        Self {
            thread_count: 1,
            vcpu_count: 1,
            managed: true,
            fail_snapshot: false,
            read_io_count: 0,
            write_io_count: 0,
            other_io_count: 0,
        }
    }
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
        discover_vms(&cfg.state_dir)
    }

    fn watch_paths(&self) -> Vec<Path> {
        self.cfg
            .as_ref()
            .map(|cfg| vec![cfg.state_dir.clone()])
            .unwrap_or_default()
    }
}

/// Load every `<vm-id>.json` file in `state_dir`.
#[tracing::instrument(skip(state_dir), fields(directory = %state_dir))]
fn discover_vms(state_dir: &Path) -> Vec<Arc<Instance>> {
    let entries = fs::read_dir(state_dir).expect("fake backend state directory is not readable");
    let mut instances = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let Some(id) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some(vm) = read_vm(&path, id) else {
            continue;
        };
        let pid = fake_pid(id);
        let client = FakeClient {
            vm_path: path.clone(),
            calls_path: state_dir.join("calls.json"),
            managed: vm.managed,
        };
        instances.push(Arc::new(Instance::new(
            id.to_string(),
            Path::new(id),
            pid,
            client,
        )));
    }
    instances
}

/// Read one VM file, or log and return `None` when it cannot be parsed.
#[tracing::instrument(skip(path, id), fields(vm = id))]
fn read_vm(path: &std::path::Path, id: &str) -> Option<VmFile> {
    let _guard = LockGuard::acquire(&lock_path_for(path)).ok()?;
    VmFile::read(path).ok()
}

/// Per-VM client. Paths are real filesystem paths, not logical `util::Path`s.
struct FakeClient {
    vm_path: std::path::PathBuf,
    calls_path: std::path::PathBuf,
    managed: bool,
}

fn fake_pid(id: &str) -> i32 {
    let hash = id.bytes().fold(0u32, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u32::from(byte))
    });
    10_000 + (hash % 1_000_000) as i32
}

/// `<vm-id>.json` shares `<vm-id>.lock` with the pytest harness.
fn lock_path_for(vm_path: &std::path::Path) -> std::path::PathBuf {
    vm_path.with_extension("lock")
}

/// Exclusive `flock` held until dropped. Matches `fcntl.flock(LOCK_EX)`.
struct LockGuard {
    file: File,
}

impl LockGuard {
    /// Get the lock on the given file. Blocks until it gets the lock.
    fn acquire(path: &std::path::Path) -> Result<Self, IoError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok(Self { file })
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl VmFile {
    fn read(path: &std::path::Path) -> Result<Self, BackendClientError> {
        let data = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&data)?)
    }

    fn write_atomic(path: &std::path::Path, vm: &Self) -> Result<(), BackendClientError> {
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(vm)?)?;
        fs::rename(&tmp, path)?;
        Ok(())
    }
}

#[async_trait]
impl InstanceClient for FakeClient {
    async fn set_thread_count(&self, count: u32) -> Result<(), BackendClientError> {
        let _guard = LockGuard::acquire(&lock_path_for(&self.vm_path))?;
        let mut vm = VmFile::read(&self.vm_path)?;
        vm.thread_count = count;
        VmFile::write_atomic(&self.vm_path, &vm)?;
        let mut calls = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.calls_path)?;
        let line = serde_json::json!({
            "vm": self.vm_path.file_stem().and_then(|stem| stem.to_str()).unwrap_or(""),
            "threads": count,
        });
        writeln!(calls, "{line}")?;
        Ok(())
    }

    async fn get_thread_pool_snapshot(&self) -> Result<ThreadPoolSnapshot, BackendClientError> {
        let _guard = LockGuard::acquire(&lock_path_for(&self.vm_path))?;
        let vm = VmFile::read(&self.vm_path)?;
        if vm.fail_snapshot {
            return Err(BackendClientError::InvalidState(format!(
                "fake snapshot failed for {}",
                self.vm_path.display()
            )));
        }
        Ok(ThreadPoolSnapshot {
            vcpu_count: vm.vcpu_count,
            perf: Some(InstancePerfSample {
                read_io_count: vm.read_io_count,
                write_io_count: vm.write_io_count,
                other_io_count: vm.other_io_count,
                read_bytes_total: 0,
                write_bytes_total: 0,
                ..Default::default()
            }),
        })
    }

    fn initial_pool_is_managed(&self, _thread_count: u32, _vcpu_count: u32) -> bool {
        self.managed
    }

    async fn close(&self) {}
}
