// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>
//
//! An extension trait to [`procfs::process::Process`] which can be used in
//! tests which use the prefixed Path functionality.

use crate::path::Path;

pub(crate) trait ProcessExt {
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process>;
}

impl ProcessExt for procfs::process::Process {
    /// Create a new [`Process`] for the given pid while respecting
    /// the mock root prefix in IO_THREAD_CONTROLLER_ROOT, as
    /// `procfs::process::Process::new` always opens the real `/proc`.
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process> {
        let path = Path::new(&format!("/proc/{pid}"));
        let path = std::path::PathBuf::from(std::path::Path::as_os_str(&path));
        Self::new_with_root(path)
    }
}
