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
};

use glob;
use lazy_static::lazy_static;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct Path {
    path: std::path::PathBuf,
}

impl Serialize for Path {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.path.to_string_lossy())
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

lazy_static! {
    static ref IO_THREAD_CONTROLLER_ROOT_PATH: std::path::PathBuf = std::path::PathBuf::from(
        env::var("IO_THREAD_CONTROLLER_ROOT_PATH").unwrap_or(String::from("/"))
    );
}

impl Path {
    pub fn new<S>(s: &S) -> Self
    where
        S: AsRef<OsStr> + ?Sized,
    {
        let mut pb = std::path::PathBuf::from(s);
        if pb.is_absolute() {
            pb = IO_THREAD_CONTROLLER_ROOT_PATH.join(pb.strip_prefix("/").expect("absolute path"))
        }
        Self { path: pb }
    }

    pub fn glob(&self) -> glob::Paths {
        glob::glob(&self.path.to_string_lossy()).expect("valid glob pattern")
    }
}

impl From<&str> for Path {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for Path {
    fn from(s: String) -> Self {
        Self::new(&s)
    }
}

impl From<&OsStr> for Path {
    fn from(s: &OsStr) -> Self {
        Self::new(s)
    }
}

impl From<OsString> for Path {
    fn from(s: OsString) -> Self {
        Self::new(&s)
    }
}

impl FromStr for Path {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::new(s))
    }
}

#[macro_export]
macro_rules! path {
    ( $x: ident, $p: expr) => {
        lazy_static! {
            pub static ref $x: $crate::conf::Path = $crate::conf::Path::new($p);
        }
        impl AsRef<std::path::Path> for $x {
            fn as_ref(&self) -> &std::path::Path {
                &self
            }
        }
        impl AsRef<std::ffi::OsStr> for $x {
            fn as_ref(&self) -> &std::ffi::OsStr {
                self.as_os_str()
            }
        }
    };
}
