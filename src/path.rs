// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! A Path-like struct that prefixes absolute paths with a prefix path.
//! Useful for testing services with a mock root directory.

use std::{
    convert::Infallible,
    env,
    ffi::{OsStr, OsString},
    fmt::{self, Debug},
    ops::Deref,
    str::FromStr,
    sync::LazyLock,
};

use glob;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct PrefixedPath {
    path: std::path::PathBuf,
}

impl Serialize for PrefixedPath {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.path.to_string_lossy())
    }
}

impl<'de> Deserialize<'de> for PrefixedPath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::new(&s))
    }
}

impl Deref for PrefixedPath {
    type Target = std::path::Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

impl AsRef<Self> for PrefixedPath {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl AsRef<std::path::Path> for PrefixedPath {
    fn as_ref(&self) -> &std::path::Path {
        &self.path
    }
}

impl fmt::Display for PrefixedPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.path.fmt(f)
    }
}

static ROOT_PATH: LazyLock<std::path::PathBuf> = LazyLock::new(|| {
    std::path::PathBuf::from(
        env::var("IO_THREAD_CONTROLLER_ROOT_PATH").unwrap_or(String::from("/")),
    )
});

/// Returns the path new `util::Path`s will have prefixed. In non-test
/// configurations, this will be either `/` or the contents of the
/// `IO_THREAD_CONTROLLER_ROOT_PATH` env var.
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

impl PrefixedPath {
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

impl From<&str> for PrefixedPath {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for PrefixedPath {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl From<&OsStr> for PrefixedPath {
    fn from(s: &OsStr) -> Self {
        Self::new(s)
    }
}

impl From<OsString> for PrefixedPath {
    fn from(s: OsString) -> Self {
        Self::new(&s)
    }
}

impl FromStr for PrefixedPath {
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
    fn test_relative_paths_are_unchanged() {
        let path = PrefixedPath::new("relative/sock");
        assert_eq!(path.as_os_str(), OsStr::new("relative/sock"));
    }

    /// Test that paths are prefixed with the contents of the env var.
    #[test]
    fn test_absolute_paths_keep_absolute_form_under_default_root() {
        let env = env::var("IO_THREAD_CONTROLLER_ROOT_PATH").unwrap_or(String::from(""));
        let path = PrefixedPath::new("/var/run/example.sock");
        assert_eq!(
            path.as_os_str(),
            OsStr::new(&format!("{env}/var/run/example.sock"))
        );
    }

    /// Test that `FromStr` builds a `Path` from a string.
    #[test]
    fn test_from_str_builds_path() {
        let path: PrefixedPath = "tmp/example".parse().unwrap();
        assert_eq!(path.as_os_str(), OsStr::new("tmp/example"));
    }
}
