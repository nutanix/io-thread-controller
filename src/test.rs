// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>
//
//! Shared infrastructure for crate unit tests.

use std::{cell::RefCell, path::PathBuf};

use rstest::fixture;

thread_local! {
    pub static ROOT_OVERRIDE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

/// Scoped root override for tests that access absolute paths through
/// `util::Path`.
pub(crate) struct RootOverride {
    previous: Option<PathBuf>,
}

impl Drop for RootOverride {
    fn drop(&mut self) {
        ROOT_OVERRIDE.with(|root| {
            root.replace(self.previous.take());
        });
    }
}

/// Override the root used by `util::Path` on the current test thread.
pub(crate) fn override_root(root: impl Into<PathBuf>) -> RootOverride {
    let previous = ROOT_OVERRIDE.with(|slot| slot.replace(Some(root.into())));
    RootOverride { previous }
}

/// Temporary mock root installed as the current test thread's `util::Path`
/// prefix.
pub(crate) struct MockDir {
    _dir: tempfile::TempDir,
    _root_override: RootOverride,
    // path: PathBuf,
}

impl MockDir {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();

        Self {
            _root_override: override_root(dir.path()),
            // path: dir.path().into(),
            _dir: dir,
        }
    }

    // pub(crate) fn join(&self, path: impl AsRef<Path>) -> PathBuf {
    // self.path.join(path.as_ref())
    // }
}

#[fixture]
pub(crate) fn mock_dir() -> MockDir {
    MockDir::new()
}
