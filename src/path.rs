// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! A Path-like struct that prefixes absolute paths with a prefix path.
//! Useful for testing services with a mock root directory.

use std::{
    convert::Infallible,
    env,
    ffi::OsStr,
    fmt::{self, Debug},
    ops::Deref,
    str::FromStr,
    sync::LazyLock,
};

use glob;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    path: std::path::PathBuf,
}

impl Serialize for Path {
    /// Strips the effective root so deserializing doesn't prefix twice.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let path = match self.path.strip_prefix(&*ROOT_PATH) {
            Ok(rel) if self.path.is_absolute() => std::path::Path::new("/").join(rel),
            _ => self.path.clone(),
        };
        serializer.serialize_str(&path.to_string_lossy())
    }
}

impl<'de> Deserialize<'de> for Path {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(&s))
    }
}

impl Deref for Path {
    type Target = std::path::Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<Self> for Path {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl AsRef<std::path::Path> for Path {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt(f)
    }
}

static ROOT_PATH: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
    std::path::PathBuf::from(
        env::var("IO_THREAD_CONTROLLER_ROOT_PATH").unwrap_or(String::from("/")),
    )
});

/// Returns the prefix that will be applied to new `crate::path::Path`s. In
/// non-test configurations, this will be either `/` or the contents of the
///`IO_THREAD_CONTROLLER_ROOT_PATH` env var.
///
/// In test configurations, this may differ per thread to support concurrently
/// running unit tests.
pub(crate) fn effective_root() -> std::path::PathBuf {
    #[cfg(test)]
    if let Some(root) = crate::test::mockdir::ROOT_OVERRIDE.with(|slot| slot.borrow().clone()) {
        return root;
    }

    ROOT_PATH.clone()
}

impl Path {
    pub fn new<S>(s: &S) -> Self
    where
        S: AsRef<OsStr> + ?Sized,
    {
        let mut pb = std::path::PathBuf::from(s);
        if pb.is_absolute() {
            pb = effective_root().join(pb.strip_prefix("/").expect("absolute path"))
        }
        Self { path: pb }
    }

    pub fn glob(&self) -> glob::Paths {
        glob::glob(&self.path.to_string_lossy()).expect("valid glob pattern")
    }

    pub fn join(&self, other: impl AsRef<std::path::Path>) -> Self {
        Self {
            path: self.path.join(other),
        }
    }
}

impl<T> From<T> for Path
where
    T: AsRef<OsStr>,
{
    fn from(value: T) -> Self {
        Self::new(&value)
    }
}

impl FromStr for Path {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that relative `Path` values are stored unchanged.
    #[test]
    fn relative_paths_are_unchanged() {
        let path = Path::new("relative/sock");
        assert_eq!(path.as_os_str(), OsStr::new("relative/sock"));
    }

    /// Test that absolute `Path` values stay absolute under the default
    /// root.
    #[test]
    fn absolute_paths_keep_absolute_form_under_default_root() {
        let path = Path::new("/var/run/example.sock");
        assert_eq!(path.as_os_str(), OsStr::new("/var/run/example.sock"));
    }

    /// Test that `FromStr` builds a `Path` from a string.
    #[test]
    fn from_str_builds_path() {
        let path: Path = "tmp/example".parse().unwrap();
        assert_eq!(path.as_os_str(), OsStr::new("tmp/example"));
    }
}
