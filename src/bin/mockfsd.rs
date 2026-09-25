// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

//! FUSE filesystem whose file contents come from registered callbacks.
//!
//! One process claims `com.nutanix.mockfs1` on the system bus
//! (`DBUS_SYSTEM_BUS_ADDRESS` when set) and serves every mount. Component
//! tests call `Mount` for `<root>/proc`, then register `/stat`. The daemon
//! opens that file through `util::Path`. The first line of stdout is the
//! bus name.

use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use clap::Parser;
use fuser::{
    Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags, INodeNo, ReplyAttr,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, Request,
};

const TTL: Duration = Duration::from_secs(0);

type Name = OsString;
type Content = Box<[u8]>;
type Callback = Box<dyn Fn() -> Content + Send + Sync>;

#[derive(Debug)]
struct OpenFile {
    ino: INodeNo,
    // Generated on a read at offset 0 and reused for the rest of that pass,
    // so a follow-up read observes the same bytes.
    content: Option<Content>,
}

#[derive(Debug)]
struct RegisterError {
    message: String,
}

impl RegisterError {
    fn invalid_path(path: &Path, reason: &str) -> Self {
        Self {
            message: format!("invalid path '{}': {reason}", path.display()),
        }
    }
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for RegisterError {}

struct Inner {
    children: HashMap<(INodeNo, Name), INodeNo>,
    callbacks: HashMap<INodeNo, Callback>,
    // TODO have different types for FileHandle and DirHandle
    handles: HashMap<FileHandle, OpenFile>,
    next_handle: u64,
    direntry: HashMap<FileHandle, Vec<(INodeNo, Name, FileType)>>,
    next_direntry: u64,
}

impl Inner {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            callbacks: HashMap::new(),
            handles: HashMap::new(),
            next_handle: 1,
            direntry: HashMap::new(),
            next_direntry: 1,
        }
    }

    fn register_callback(&mut self, name: &Path, callback: Callback) {
        assert!(name.is_absolute());
        let mut parent = INodeNo::ROOT;
        for part in name.components() {
            let Component::Normal(part) = part else {
                continue;
            };
            let len = u64::try_from(self.children.len()).unwrap();
            let inode = self
                .children
                .entry((parent, part.to_owned()))
                .or_insert_with(|| INodeNo(INodeNo::ROOT.0 + len + 1));
            parent = *inode;
        }
        self.callbacks.insert(parent, callback);
    }

    fn lookup(&self, parent: INodeNo, name: &OsStr) -> Option<INodeNo> {
        self.children.get(&(parent, name.to_owned())).copied()
    }

    fn is_file(&self, ino: INodeNo) -> bool {
        self.callbacks.contains_key(&ino)
    }

    fn is_dir(&self, ino: INodeNo) -> bool {
        ino == INodeNo::ROOT
            || self.children.keys().any(|(parent, _)| *parent == ino) && !self.is_file(ino)
    }

    fn file_type(&self, ino: INodeNo) -> FileType {
        if self.is_dir(ino) {
            FileType::Directory
        } else {
            FileType::RegularFile
        }
    }

    fn parent(&self, ino: INodeNo) -> Option<INodeNo> {
        self.children
            .iter()
            .find(|((_, _), child)| **child == ino)
            .map(|((parent, _), _)| *parent)
    }

    fn attr_for(&self, ino: INodeNo) -> Option<FileAttr> {
        if !self.is_file(ino) && !self.is_dir(ino) {
            return None;
        }
        let now = SystemTime::now();
        // Size 0 matches dynamic `/proc` files. A phantom size makes the
        // kernel hand one FUSE read reply to two concurrent `read` calls.
        // `FOPEN_DIRECT_IO` then drives each read from what `read` returns.
        // getattr must not run the callback: that would consume a sequence.
        let (kind, perm, nlink) = if self.is_dir(ino) {
            (FileType::Directory, 0o755, 2)
        } else {
            (FileType::RegularFile, 0o444, 1)
        };
        Some(FileAttr {
            ino,
            size: 0,
            blocks: 1,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind,
            perm,
            nlink,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }

    fn open(&mut self, ino: INodeNo) -> Option<FileHandle> {
        if !self.callbacks.contains_key(&ino) {
            return None;
        }
        let fh = FileHandle(self.next_handle);
        self.next_handle += 1;
        self.handles.insert(fh, OpenFile { ino, content: None });
        Some(fh)
    }

    fn opendir(&mut self, ino: INodeNo) -> Option<FileHandle> {
        let fh = FileHandle(self.next_direntry);
        self.next_direntry += 1;
        let mut entries = self
            .children
            .iter()
            .filter(|((parent, _), _)| *parent == ino)
            .map(|((_, name), child)| (*child, name.clone(), self.file_type(*child)))
            .collect::<Vec<_>>();
        entries.push((ino, OsString::from("."), FileType::Directory));
        if let Some(parent) = self.parent(ino) {
            entries.push((parent, OsString::from(".."), FileType::Directory));
        }
        entries.sort_by_key(|(ino, _, _)| *ino);
        self.direntry.insert(fh, entries);
        Some(fh)
    }

    fn read(&mut self, fh: FileHandle, offset: u64) -> Option<&[u8]> {
        let ino = self.handles.get(&fh)?.ino;
        if offset == 0 || self.handles.get(&fh)?.content.is_none() {
            let content = (self.callbacks.get(&ino)?)();
            self.handles.get_mut(&fh)?.content = Some(content);
        }
        self.handles.get(&fh)?.content.as_deref()
    }

    fn readdir(&self, fh: FileHandle) -> Option<&Vec<(INodeNo, Name, FileType)>> {
        self.direntry.get(&fh)
    }

    fn release(&mut self, fh: FileHandle) {
        self.handles.remove(&fh);
    }

    fn releasedir(&mut self, fh: FileHandle) {
        self.direntry.remove(&fh);
    }
}

// TODO merge MockFs and MockFsHandle into one struct
struct MockFs {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone)]
struct MockFsHandle {
    inner: Arc<Mutex<Inner>>,
}

impl MockFs {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner::new())),
        }
    }

    fn handle(&self) -> MockFsHandle {
        MockFsHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl MockFsHandle {
    fn register_callback(&self, name: &Path, callback: Callback) {
        self.inner
            .lock()
            .expect("mockfs lock")
            .register_callback(name, callback);
    }

    fn register_iterator<I, T>(&self, name: &Path, values: I)
    where
        I: IntoIterator<Item = T>,
        I::IntoIter: Send + 'static,
        T: Into<Vec<u8>>,
    {
        let values = Mutex::new(values.into_iter());
        let callback: Callback = Box::new(move || {
            values
                .lock()
                .expect("mockfs sequence lock")
                .next()
                .map(|value| value.into().into_boxed_slice())
                .unwrap_or_default()
        });
        self.register_callback(name, callback);
    }

    fn register_static_bytes(&self, name: &Path, value: Vec<u8>) -> Result<(), RegisterError> {
        validate_registration_path(name)?;
        self.register_iterator(name, vec![value].into_iter().cycle());
        Ok(())
    }

    fn register_sequence_bytes(
        &self,
        name: &Path,
        values: Vec<Vec<u8>>,
    ) -> Result<(), RegisterError> {
        validate_registration_path(name)?;
        self.register_iterator(name, values);
        Ok(())
    }
}

fn validate_registration_path(name: &Path) -> Result<(), RegisterError> {
    if !name.is_absolute() {
        return Err(RegisterError::invalid_path(name, "path must be absolute"));
    }
    for part in name.components() {
        match part {
            Component::RootDir | Component::Normal(_) => {}
            _ => {
                return Err(RegisterError::invalid_path(
                    name,
                    "path may only contain normal segments",
                ));
            }
        }
    }
    Ok(())
}

impl Filesystem for MockFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Ok(inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if !inner.is_dir(parent) {
            reply.error(Errno::ENOTDIR);
            return;
        }
        let Some(ino) = inner.lookup(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(attr) = inner.attr_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        reply.entry(&TTL, &attr, fuser::Generation(0));
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let Ok(inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        let Some(attr) = inner.attr_for(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        reply.attr(&TTL, &attr);
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: fuser::OpenFlags, reply: ReplyOpen) {
        let Ok(mut inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if !inner.is_file(ino) {
            reply.error(Errno::ENOENT);
            return;
        }
        let Some(fh) = inner.open(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        reply.opened(fh, FopenFlags::FOPEN_DIRECT_IO);
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: fuser::OpenFlags, reply: ReplyOpen) {
        let Ok(mut inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if !inner.is_dir(ino) {
            reply.error(Errno::ENOTDIR);
            return;
        }
        let Some(fh) = inner.opendir(ino) else {
            reply.error(Errno::ENOENT);
            return;
        };
        reply.opened(fh, FopenFlags::empty());
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let Ok(mut inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if !inner.is_file(ino) {
            reply.error(Errno::ENOENT);
            return;
        }
        let Some(content) = inner.read(fh, offset) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let start = content.len().min(offset as usize);
        let end = content.len().min(offset as usize + size as usize);
        reply.data(&content[start..end]);
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let Ok(inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        if !inner.is_dir(ino) {
            reply.error(Errno::ENOTDIR);
            return;
        }
        let Some(entries) = inner.readdir(fh) else {
            reply.error(Errno::ENOENT);
            return;
        };
        for (index, (ino, name, file_type)) in entries.iter().enumerate().skip(offset as usize) {
            let next = (index + 1) as u64;
            if reply.add(*ino, next, *file_type, name) {
                break;
            }
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let Ok(mut inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        inner.release(fh);
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: fuser::OpenFlags,
        reply: ReplyEmpty,
    ) {
        let Ok(mut inner) = self.inner.lock() else {
            reply.error(Errno::EIO);
            return;
        };
        inner.releasedir(fh);
        reply.ok();
    }
}

struct Mounted {
    handle: MockFsHandle,
    _session: fuser::BackgroundSession,
}

impl Mounted {
    fn new(mount_path: &Path) -> std::io::Result<Self> {
        let fs = MockFs::new();
        let handle = fs.handle();
        let session = fuser::spawn_mount(fs, mount_path, &Config::default())?;
        Ok(Self {
            handle,
            _session: session,
        })
    }

    fn handle(&self) -> MockFsHandle {
        self.handle.clone()
    }
}

/// Well-known name claimed on the system bus. One process owns it and
/// serves every mount.
const BUS_NAME: &str = "com.nutanix.mockfs1";
/// Object path served on [`BUS_NAME`].
const OBJECT_PATH: &str = "/com/nutanix/mockfs1";
/// Interface that exposes registration and liveness.
#[cfg(test)]
const INTERFACE: &str = "com.nutanix.mockfs1";

struct DbusControl {
    _conn: zbus::Connection,
}

#[derive(Clone)]
struct Control {
    mounts: Arc<Mutex<HashMap<PathBuf, Mounted>>>,
}

impl Control {
    fn new() -> Self {
        Self {
            mounts: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn mounted_handle(&self, mount_path: &str) -> zbus::fdo::Result<MockFsHandle> {
        let mount_path = normalize_mount_path(mount_path)?;
        let mounts = self.mounts.lock().expect("mockfs mounts lock");
        mounts.get(&mount_path).map(Mounted::handle).ok_or_else(|| {
            zbus::fdo::Error::InvalidArgs(format!("not mounted: {}", mount_path.display()))
        })
    }
}

fn normalize_mount_path(path: &str) -> zbus::fdo::Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute() {
        return Err(invalid_args(RegisterError::invalid_path(
            path,
            "path must be absolute",
        )));
    }
    let mut normalized = PathBuf::from("/");
    for part in path.components() {
        match part {
            Component::RootDir => {}
            Component::Normal(part) => normalized.push(part),
            _ => {
                return Err(invalid_args(RegisterError::invalid_path(
                    path,
                    "path may only contain normal segments",
                )));
            }
        }
    }
    if normalized == Path::new("/") {
        return Err(invalid_args(RegisterError::invalid_path(
            path,
            "refusing to mount the filesystem root",
        )));
    }
    Ok(normalized)
}

fn invalid_args(err: RegisterError) -> zbus::fdo::Error {
    zbus::fdo::Error::InvalidArgs(err.to_string())
}

fn io_error(err: std::io::Error) -> zbus::fdo::Error {
    zbus::fdo::Error::Failed(err.to_string())
}

#[zbus::interface(name = "com.nutanix.mockfs1")]
impl Control {
    async fn mount(&self, mount_path: String) -> zbus::fdo::Result<()> {
        let mount_path = normalize_mount_path(&mount_path)?;
        let mut mounts = self.mounts.lock().expect("mockfs mounts lock");
        if mounts.contains_key(&mount_path) {
            return Err(zbus::fdo::Error::InvalidArgs(format!(
                "already mounted: {}",
                mount_path.display()
            )));
        }
        std::fs::create_dir_all(&mount_path).map_err(io_error)?;
        let mounted = Mounted::new(&mount_path).map_err(io_error)?;
        mounts.insert(mount_path, mounted);
        Ok(())
    }

    async fn unmount(&self, mount_path: String) -> zbus::fdo::Result<()> {
        let mount_path = normalize_mount_path(&mount_path)?;
        let removed = {
            let mut mounts = self.mounts.lock().expect("mockfs mounts lock");
            mounts.remove(&mount_path)
        };
        if removed.is_none() {
            return Err(zbus::fdo::Error::InvalidArgs(format!(
                "not mounted: {}",
                mount_path.display()
            )));
        }
        Ok(())
    }

    async fn register_static(
        &self,
        mount_path: String,
        path: String,
        content: Vec<u8>,
    ) -> zbus::fdo::Result<()> {
        self.mounted_handle(&mount_path)?
            .register_static_bytes(Path::new(&path), content)
            .map_err(invalid_args)
    }

    async fn register_sequence(
        &self,
        mount_path: String,
        path: String,
        values: Vec<Vec<u8>>,
    ) -> zbus::fdo::Result<()> {
        self.mounted_handle(&mount_path)?
            .register_sequence_bytes(Path::new(&path), values)
            .map_err(invalid_args)
    }

    // TODO register callback, how to implement?

    async fn ping(&self) -> String {
        "ok".to_owned()
    }
}

async fn start_dbus_control() -> zbus::Result<DbusControl> {
    serve_control(zbus::connection::Builder::system()?).await
}

#[cfg(test)]
async fn start_dbus_control_at(address: &str) -> zbus::Result<DbusControl> {
    serve_control(zbus::connection::Builder::address(address)?).await
}

async fn serve_control(builder: zbus::connection::Builder<'_>) -> zbus::Result<DbusControl> {
    let conn = builder
        .name(BUS_NAME)?
        .serve_at(OBJECT_PATH, Control::new())?
        .build()
        .await?;
    Ok(DbusControl { _conn: conn })
}

#[derive(Debug, Parser)]
#[command(
    name = "mockfsd",
    about = "Mount callback filesystems from a D-Bus control pane"
)]
struct Args {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Args::parse();
    let _dbus = start_dbus_control().await?;
    // Pytest reads this single line. Keep later diagnostics on stderr.
    println!("{BUS_NAME}");
    let _ = std::io::stdout().flush();
    tokio::signal::ctrl_c().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error,
        fs::{self, File},
        io::{self, ErrorKind, Read, Seek, SeekFrom},
        path::{Component, Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering::SeqCst},
        thread,
    };

    use super::{BUS_NAME, INTERFACE, MockFsHandle, Mounted, OBJECT_PATH, start_dbus_control_at};

    /// Tempdir mount used by the filesystem tests.
    struct MountedMockFs {
        handle: MockFsHandle,
        dir: tempfile::TempDir,
        _mounted: Mounted,
    }

    impl MountedMockFs {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let mounted = Mounted::new(dir.path()).unwrap();
            let handle = mounted.handle();
            Self {
                handle,
                dir,
                _mounted: mounted,
            }
        }

        fn mount_path(&self) -> &Path {
            self.dir.path()
        }

        fn register_fn<F, O>(&self, name: impl AsRef<Path>, generate: F) -> PathBuf
        where
            F: Fn() -> O + Send + Sync + 'static,
            O: Into<Vec<u8>>,
        {
            let name = name.as_ref();
            self.handle
                .register_callback(name, Box::new(move || generate().into().into_boxed_slice()));
            self.path_for(name)
        }

        fn register_static(
            &self,
            name: impl AsRef<Path>,
            value: impl Into<Vec<u8>> + Clone + Send + 'static,
        ) -> PathBuf {
            let name = name.as_ref();
            self.handle
                .register_iterator(name, vec![value.into()].into_iter().cycle());
            self.path_for(name)
        }

        fn register_iterator<I, T>(&self, name: impl AsRef<Path>, values: I) -> PathBuf
        where
            I: IntoIterator<Item = T>,
            I::IntoIter: Send + 'static,
            T: Into<Vec<u8>>,
        {
            let name = name.as_ref();
            self.handle.register_iterator(name, values);
            self.path_for(name)
        }

        fn path_for(&self, name: &Path) -> PathBuf {
            let mut relative = PathBuf::new();
            for part in name.components() {
                if let Component::Normal(part) = part {
                    relative.push(part);
                }
            }
            self.mount_path().join(relative)
        }
    }

    /// Private session bus so control-plane tests do not touch the system bus.
    struct PrivateBus {
        address: String,
        child: std::process::Child,
        _dir: tempfile::TempDir,
    }

    impl PrivateBus {
        fn start() -> std::io::Result<Self> {
            let dir = tempfile::tempdir()?;
            let socket_path = dir.path().join("bus");
            let config_path = dir.path().join("bus.conf");
            std::fs::write(
                &config_path,
                format!(
                    r#"<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-BUS Bus Configuration 1.0//EN"
 "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig>
  <type>session</type>
  <listen>unix:path={}</listen>
  <auth>EXTERNAL</auth>
  <policy context="default">
    <allow own="*"/>
    <allow send_destination="*"/>
    <allow receive_sender="*"/>
  </policy>
</busconfig>
"#,
                    socket_path.display()
                ),
            )?;
            let mut child = std::process::Command::new("dbus-daemon")
                .arg(format!("--config-file={}", config_path.display()))
                .arg("--nofork")
                .arg("--nopidfile")
                .arg("--print-address=1")
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()?;
            let mut address = String::new();
            if let Some(stdout) = child.stdout.take() {
                use std::io::BufRead;
                std::io::BufReader::new(stdout).read_line(&mut address)?;
            }
            let address = address.trim().to_owned();
            if address.is_empty() {
                let mut stderr = String::new();
                if let Some(mut err) = child.stderr.take() {
                    use std::io::Read;
                    let _ = err.read_to_string(&mut stderr);
                }
                let _ = child.kill();
                let _ = child.wait();
                return Err(std::io::Error::other(format!(
                    "dbus-daemon did not print an address: {stderr}"
                )));
            }
            Ok(Self {
                address,
                child,
                _dir: dir,
            })
        }
    }

    impl Drop for PrivateBus {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    struct DbusControlRuntime {
        bus: PrivateBus,
        _control: super::DbusControl,
    }

    impl DbusControlRuntime {
        async fn new() -> Result<Self, Box<dyn Error>> {
            let bus = PrivateBus::start()?;
            let control = start_dbus_control_at(&bus.address).await?;
            Ok(Self {
                bus,
                _control: control,
            })
        }

        async fn proxy(&self) -> Result<zbus::Proxy<'static>, Box<dyn Error>> {
            let conn = zbus::connection::Builder::address(&*self.bus.address)?
                .build()
                .await?;
            let proxy = zbus::Proxy::new_owned(conn, BUS_NAME, OBJECT_PATH, INTERFACE).await?;
            Ok(proxy)
        }
    }

    #[test]
    fn test_mockfs_register_creates_file() {
        let mmfs = MountedMockFs::new();
        let mount_path = mmfs.mount_path();

        assert!(!mount_path.join("test").exists());
        let path = mmfs.register_static("/test", "hello");
        assert_eq!(mount_path.join("test"), path);
        assert!(path.exists());
    }

    #[test]
    fn test_mockfs_register_static_sets_content() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_static("/test", "hello");

        let out = fs::read(&path)?;
        assert_eq!(&out, &b"hello");

        let out = fs::read(&path)?;
        assert_eq!(&out, &b"hello");

        Ok(())
    }

    #[test]
    fn test_mockfs_register_iterator_advances_then_ends() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_iterator("/test", vec!["a", "b"]);

        let a = fs::read(&path)?;
        let b = fs::read(&path)?;
        let c = fs::read(&path)?;

        assert_eq!(&a, &b"a");
        assert_eq!(&b, &b"b");
        assert_eq!(&c, &b"");

        Ok(())
    }

    #[test]
    fn test_mockfs_directories_created_and_detected() {
        let mmfs = MountedMockFs::new();
        mmfs.register_static("/a/b/c", "hello");

        let a = mmfs.mount_path().join("a");
        let b = a.join("b");
        let c = b.join("c");

        assert!(a.exists());
        assert!(a.is_dir());

        assert!(b.exists());
        assert!(b.is_dir());

        assert!(c.exists());
        assert!(c.is_file());
    }

    #[test]
    fn test_mockfs_read_buflen_respected() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_static("/test", "0123456789");

        let mut buf = [0u8; 3];
        let mut file = File::open(&path)?;

        let read = file.read(&mut buf)?;

        assert_eq!(buf.len(), read);
        assert_eq!(&buf, b"012");

        Ok(())
    }

    #[test]
    fn test_mockfs_read_offset_respected() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_static("/test", "0123456789");

        let mut buf = [0u8; 3];
        let mut file = File::open(&path)?;
        let start = file.seek(SeekFrom::Start(2))?;
        assert_eq!(start, 2);

        let read = file.read(&mut buf)?;

        assert_eq!(buf.len(), read);
        assert_eq!(&buf, b"234");

        Ok(())
    }

    #[test]
    fn test_mockfs_read_handles_eof() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_static("/test", "0123456789");

        let mut buf = [0u8; 100];
        let mut file = File::open(&path)?;

        let read = file.read(&mut buf)?;
        let buf = &buf[..read];

        assert_eq!(buf.len(), 10);
        assert_eq!(buf, b"0123456789");

        Ok(())
    }

    #[test]
    fn test_mockfs_read_unstable() -> Result<(), Box<dyn Error>> {
        // A read starting at offset 0 observes freshly generated content.
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_iterator("/test", vec!["a", "b"]);

        let mut buf = [0u8; 1];
        let mut file = File::open(&path)?;

        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"a");

        file.seek(SeekFrom::Start(0))?;
        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"b");

        Ok(())
    }

    #[test]
    fn test_mockfs_read_unstable_with_multiple_handles() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_iterator("/test", vec!["a", "b"]);

        let mut buf = [0u8; 1];
        let mut file_a = File::open(&path)?;
        let mut file_b = File::open(&path)?;

        let read = file_a.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"a");

        let read = file_b.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"b");

        file_a.seek(SeekFrom::Start(0))?;
        let read = file_a.read(&mut buf)?;
        assert_eq!(read, 0);

        Ok(())
    }

    #[test]
    fn test_mockfs_read_chunked() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_iterator("/test", vec!["ab", "cd"]);

        let mut buf = [0u8; 1];
        let mut file = File::open(&path)?;

        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"a");

        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"b");

        let read = file.read(&mut buf)?;
        assert_eq!(read, 0);

        assert_eq!(file.seek(SeekFrom::Start(0))?, 0);

        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"c");

        let read = file.read(&mut buf)?;
        assert_eq!(read, 1);
        assert_eq!(&buf, b"d");

        Ok(())
    }

    #[test]
    fn test_mockfs_read_to_end_is_consistent() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path = mmfs.register_static("/test", "ab");

        let mut file = File::open(&path)?;

        let mut contents = Vec::new();
        let read = file.read_to_end(&mut contents)?;
        assert_eq!(read, 2);
        assert_eq!(&contents, b"ab");

        Ok(())
    }

    #[test]
    fn test_mockfs_file_not_found_doesnt_crash() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let real_path = mmfs.register_static("/test", "hello");
        let fake_path = mmfs.mount_path().join("fake");
        assert!(!fake_path.exists());

        let error = File::open(&fake_path).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound);

        assert_eq!(fs::read(&real_path)?, b"hello");

        Ok(())
    }

    #[test]
    fn test_mockfs_register_conflicting_paths_file_first() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path_a = mmfs.register_static("/a", "hello from a");
        let path_b = mmfs.register_static("/a/b", "hello from b");

        assert_eq!(fs::read(&path_a)?, b"hello from a");
        assert_eq!(
            fs::read(&path_b).unwrap_err().kind(),
            ErrorKind::NotADirectory
        );

        assert!(path_a.is_file());
        assert!(!path_a.is_dir());

        Ok(())
    }

    #[test]
    fn test_mockfs_register_conflicting_paths_dir_first() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let path_b = mmfs.register_static("/a/b", "hello from b");
        let path_a = mmfs.register_static("/a", "hello from a");

        assert_eq!(fs::read(&path_a)?, b"hello from a");
        assert_eq!(
            fs::read(&path_b).unwrap_err().kind(),
            ErrorKind::NotADirectory
        );

        assert!(path_a.is_file());
        assert!(!path_a.is_dir());

        Ok(())
    }

    #[test]
    fn test_mockfs_register_conflicting_paths_early_open_file_first() -> Result<(), Box<dyn Error>>
    {
        let mmfs = MountedMockFs::new();

        let path_a = mmfs.register_static("/a", "hello from a");
        let mut file_a = File::open(&path_a)?;
        let path_b = mmfs.register_static("/a/b", "hello from b");

        let mut buf = [0u8; 20];
        let read = file_a.read(&mut buf)?;
        let buf = &buf[..read];
        assert_eq!(buf, b"hello from a");

        assert_eq!(
            fs::read(&path_b).unwrap_err().kind(),
            ErrorKind::NotADirectory
        );

        Ok(())
    }

    #[test]
    fn test_mockfs_register_conflicting_paths_early_open_dir_first() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();

        let path_b = mmfs.register_static("/a/b", "hello from b");
        let mut file_b = File::open(&path_b)?;

        let path_a = mmfs.register_static("/a", "hello from a");

        let mut buf = [0u8; 20];
        assert_eq!(fs::read(&path_a)?, b"hello from a");
        let read = file_b.read(&mut buf)?;
        let buf = &buf[..read];
        assert_eq!(buf, b"hello from b");

        Ok(())
    }

    #[test]
    fn test_mockfs_register_twice() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let _ = mmfs.register_static("/test", "first");
        let path = mmfs.register_static("/test", "second");

        assert_eq!(fs::read(&path)?, b"second");

        Ok(())
    }

    #[test]
    fn test_mockfs_stress_test_reads_should_be_independent() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();
        let ctr = AtomicU64::new(1);
        let path = mmfs.register_fn("/test", move || ctr.fetch_add(1, SeqCst).to_le_bytes());

        for i in 0..10_000 {
            let bytes = fs::read(&path)?;
            assert_eq!(bytes.len(), 8);
            let buf: [u8; 8] = bytes.try_into().unwrap();
            let val = u64::from_le_bytes(buf);
            assert_eq!(val, i + 1);
        }

        Ok(())
    }

    #[test]
    fn test_mockfs_concurrent_access_is_ok() -> Result<(), Box<dyn Error>> {
        let mmfs = MountedMockFs::new();

        let ctr = AtomicU64::new(1);
        let path = mmfs.register_fn("/test", move || ctr.fetch_add(1, SeqCst).to_le_bytes());

        let handles = (0..10)
            .map(|thread_id| {
                let path = path.clone();
                thread::spawn(move || {
                    let mut prev = 0;
                    for iter in 0..1_000 {
                        let bytes = fs::read(&path)?;
                        if bytes.len() != 8 {
                            return Err(io::Error::other(format!(
                                "thread={thread_id} iter={iter}: expected 8 bytes, got {}: {bytes:?}",
                                bytes.len(),
                            )));
                        }
                        let buf: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
                            io::Error::other(format!(
                                "thread={thread_id} iter={iter}: failed [u8; 8] conversion, len={}, bytes={bytes:?}",
                                bytes.len(),
                            ))
                        })?;

                        let val = u64::from_le_bytes(buf);
                        if val <= prev {
                            return Err(io::Error::other(format!(
                                "thread={thread_id} iter={iter}: non-monotonic value prev={prev} val={val} raw={buf:?}"
                            )));
                        }
                        prev = val;
                    }
                    Ok(())
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap()?;
        }

        Ok(())
    }

    #[test]
    #[should_panic]
    fn test_mockfs_panic_on_relative_path() {
        let mmfs = MountedMockFs::new();
        let _ = mmfs.register_static("test", "hello");
    }

    #[test]
    #[should_panic]
    fn test_mockfs_panic_on_local_dir() {
        let mmfs = MountedMockFs::new();
        let _ = mmfs.register_static("./test", "hello");
    }

    #[test]
    #[should_panic]
    fn test_mockfs_panic_on_parent_dir() {
        let mmfs = MountedMockFs::new();
        let _ = mmfs.register_static("../test", "hello");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_register_static_over_dbus() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;
        let mount_dir = tempfile::tempdir()?;
        let mount_path = mount_dir.path().to_str().unwrap();

        proxy.call::<_, _, ()>("Mount", &(mount_path,)).await?;
        proxy
            .call::<_, _, ()>("RegisterStatic", &(mount_path, "/test", b"hello".to_vec()))
            .await?;

        let out = fs::read(mount_dir.path().join("test"))?;
        assert_eq!(out, b"hello");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_ping_is_available() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;

        let reply: String = proxy.call("Ping", &()).await?;
        assert_eq!(reply, "ok");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_register_sequence_over_dbus() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;
        let mount_dir = tempfile::tempdir()?;
        let mount_path = mount_dir.path().to_str().unwrap();

        proxy.call::<_, _, ()>("Mount", &(mount_path,)).await?;
        proxy
            .call::<_, _, ()>(
                "RegisterSequence",
                &(mount_path, "/test", vec![b"a".to_vec(), b"b".to_vec()]),
            )
            .await?;

        let path = mount_dir.path().join("test");
        assert_eq!(fs::read(&path)?, b"a");
        assert_eq!(fs::read(&path)?, b"b");
        assert_eq!(fs::read(&path)?, b"");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_invalid_path_returns_invalid_args() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;
        let mount_dir = tempfile::tempdir()?;
        let mount_path = mount_dir.path().to_str().unwrap();

        proxy.call::<_, _, ()>("Mount", &(mount_path,)).await?;
        let err = proxy
            .call::<_, _, ()>(
                "RegisterStatic",
                &(mount_path, "relative/path", b"oops".to_vec()),
            )
            .await
            .expect_err("relative path must be rejected");
        assert!(
            err.to_string().contains("invalid path"),
            "unexpected error: {err}"
        );
        assert!(!mount_dir.path().join("relative").exists());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_register_twice_updates_content() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;
        let mount_dir = tempfile::tempdir()?;
        let mount_path = mount_dir.path().to_str().unwrap();

        proxy.call::<_, _, ()>("Mount", &(mount_path,)).await?;
        proxy
            .call::<_, _, ()>("RegisterStatic", &(mount_path, "/test", b"first".to_vec()))
            .await?;
        proxy
            .call::<_, _, ()>("RegisterStatic", &(mount_path, "/test", b"second".to_vec()))
            .await?;

        let out = fs::read(mount_dir.path().join("test"))?;
        assert_eq!(out, b"second");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_mockfs_concurrent_mounts_are_independent() -> Result<(), Box<dyn Error>> {
        let runtime = DbusControlRuntime::new().await?;
        let proxy = runtime.proxy().await?;
        let mount_a = tempfile::tempdir()?;
        let mount_b = tempfile::tempdir()?;
        let path_a = mount_a.path().to_str().unwrap();
        let path_b = mount_b.path().to_str().unwrap();

        proxy.call::<_, _, ()>("Mount", &(path_a,)).await?;
        proxy.call::<_, _, ()>("Mount", &(path_b,)).await?;
        proxy
            .call::<_, _, ()>("RegisterStatic", &(path_a, "/stat", b"from-a".to_vec()))
            .await?;
        proxy
            .call::<_, _, ()>("RegisterStatic", &(path_b, "/stat", b"from-b".to_vec()))
            .await?;

        assert_eq!(fs::read(mount_a.path().join("stat"))?, b"from-a");
        assert_eq!(fs::read(mount_b.path().join("stat"))?, b"from-b");

        let err = proxy
            .call::<_, _, ()>("Mount", &(path_a,))
            .await
            .expect_err("a second mount of the same path must fail");
        assert!(
            err.to_string().contains("already mounted"),
            "unexpected error: {err}"
        );

        proxy.call::<_, _, ()>("Unmount", &(path_a,)).await?;
        assert!(!mount_a.path().join("stat").exists());
        assert_eq!(fs::read(mount_b.path().join("stat"))?, b"from-b");
        Ok(())
    }
}
