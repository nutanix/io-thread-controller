pub(crate) trait ProcessExt {
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process>;
}

impl ProcessExt for procfs::process::Process {
    /// Create a new [`Process`] for the given pid while respecting the mock
    /// root prefix, as `procfs::process::Process::new` always opens the real
    /// `/proc`.
    fn from_pid(pid: i32) -> procfs::ProcResult<procfs::process::Process> {
        let path = crate::path::Path::new(&format!("/proc/{pid}"));
        let path = std::path::PathBuf::from(std::path::Path::as_os_str(&path));
        Self::new_with_root(path)
    }
}
