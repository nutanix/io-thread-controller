// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Backend-independent VM I/O worker scaling service.

pub mod backends;
pub mod config;
pub mod controller;
pub mod daemon;
pub mod dbus;
pub mod engines;
pub mod instance;
pub mod path;
pub mod per_vm_log;
pub mod rolling;
pub mod state;
#[cfg(test)]
pub(crate) mod test;

pub(crate) trait ProcessExt {
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process>;
}

impl ProcessExt for procfs::process::Process {
    /// `/proc/<pid>` after applying [`IO_THREAD_CONTROLLER_ROOT_PATH`].
    ///
    /// `procfs::process::Process::new` always opens the real `/proc`. Callers
    /// that should honor the mock root pass this to create the process.
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process> {
        let path = crate::path::PrefixedPath::new(&format!("/proc/{pid}"));
        let path = std::path::PathBuf::from(std::path::Path::as_os_str(&path));
        Self::new_with_root(path)
    }
}
