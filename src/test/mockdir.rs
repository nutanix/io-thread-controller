// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Leonardo Forchini <leonardo.forchini@nutanix.com>
//
//! `tempfile::TempDir` wrapper which controls the prefix of `util::Path`s.

use std::cell::RefCell;

use rstest::fixture;

thread_local! {
    pub static ROOT_OVERRIDE: RefCell<Option<std::path::PathBuf>> = const { RefCell::new(None) };
}

/// Scoped root override for tests that access absolute paths through
/// `util::Path`.
pub(crate) struct RootOverride {
    previous: Option<std::path::PathBuf>,
}

impl Drop for RootOverride {
    fn drop(&mut self) {
        ROOT_OVERRIDE.with(|root| {
            root.replace(self.previous.take());
        });
    }
}

/// Override the root used by `util::Path` on the current test thread.
pub(crate) fn override_root(root: impl Into<std::path::PathBuf>) -> RootOverride {
    let previous = ROOT_OVERRIDE.with(|slot| slot.replace(Some(root.into())));
    RootOverride { previous }
}

/// Temporary mock root installed as the current test thread's `util::Path`
/// prefix.
pub(crate) struct MockDir {
    path: std::path::PathBuf,
    _root_override: RootOverride,
    _dir: tempfile::TempDir,
}

impl MockDir {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();

        Self {
            path: path.to_path_buf(),
            _root_override: override_root(path),
            _dir: dir,
        }
    }
}

/// Create a new `MockDir` as an `rstest` fixture
#[fixture]
pub(crate) fn mockdir() -> MockDir {
    MockDir::new()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::util;
    use crate::util::effective_root;

    /// Ensure that a new `MockDir` actually changes the effective root.
    #[test]
    fn test_mockdir_changes_effective_root() {
        let old_root = effective_root();

        let _mockdir = MockDir::new();
        assert_ne!(old_root, effective_root());
    }

    /// Ensure new `util::Paths`s use a given `MockDir`'s prefix.
    #[rstest]
    fn test_mockdir_modifies_util_paths(mockdir: MockDir) {
        let util_path = util::Path::from("/etc/passwd");
        assert_ne!(mockdir.path, std::path::Path::new("/"));
        assert_eq!(util_path.to_path_buf(), mockdir.path.join("etc/passwd"));
    }

    /// Ensure paths created before a new `MockDir` are not prefixed with its
    /// dir.
    // TODO is this the behaviour we want? MockDirs will mostly be used as
    // fixtures so I don't think it should come up too often.
    #[test]
    fn test_mockdir_preserves_existing_paths() {
        let util_path = util::Path::from("/etc/passwd");

        let mockdir = MockDir::new();
        assert_ne!(util_path.to_path_buf(), mockdir.path.join("etc/passwd"));
    }

    /// Ensure once a `MockDir` goes out of scope the effective root is
    /// restored.
    #[test]
    fn test_mockdir_reverts_on_drop() {
        let _outer = MockDir::new();
        let outer_path = effective_root();

        let inner_path = {
            let _inner = MockDir::new();
            effective_root()
        };

        let post_drop_path = effective_root();

        assert_ne!(outer_path, inner_path);
        assert_eq!(outer_path, post_drop_path);
    }
}
