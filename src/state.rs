// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2026 Nutanix, Inc.
//
// Author: Thanos Makatos <thanos.makatos@nutanix.com>

//! Restart-surviving VM ownership classification.
//!
//! One JSON file below `/run` records which VMs automatic scaling may and may
//! not manage. `/run` survives daemon restarts but is cleared on host reboot,
//! which is the desired lifetime for backend-specific startup classification.

use std::{
    collections::BTreeSet,
    io::{self, ErrorKind},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::util::Path;

#[derive(Debug, Error)]
pub enum StateError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error("VM error: {0}")]
    VmError(String),
}

/// VM classifications that survive an io-thread-controller process restart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VmOwnership {
    /// VMs whose first usable snapshot matched backend management policy.
    #[serde(default)]
    pub managed_vms: BTreeSet<String>,
    /// VMs whose first usable snapshot must remain operator-managed.
    #[serde(default)]
    pub unmanaged_vms: BTreeSet<String>,
}

impl VmOwnership {
    /// Return a previously recorded classification.
    pub fn classification(&self, vm_id: &str) -> Option<bool> {
        if self.managed_vms.contains(vm_id) {
            Some(true)
        } else if self.unmanaged_vms.contains(vm_id) {
            Some(false)
        } else {
            None
        }
    }

    /// Record the immutable classification for one VM.
    pub fn record(&mut self, vm_id: &str, managed: bool) -> Result<(), StateError> {
        if self.classification(vm_id).is_some() {
            return Err(StateError::VmError(format!(
                "VM {vm_id} already has an ownership classification"
            )));
        }
        if managed {
            self.managed_vms.insert(vm_id.to_string());
        } else {
            self.unmanaged_vms.insert(vm_id.to_string());
        }
        Ok(())
    }

    /// Reject a file that classifies the same VM both ways.
    fn validate(&self) -> Result<(), StateError> {
        if !(self.managed_vms.is_disjoint(&self.unmanaged_vms)) {
            return Err(StateError::VmError(
                "managed_vms and unmanaged_vms overlap".to_string(),
            ));
        }
        Ok(())
    }
}

/// Atomic JSON-file store for [`VmOwnership`].
#[derive(Debug, Clone)]
pub struct VmStateStore {
    path: Path,
}

impl VmStateStore {
    /// Address the ownership registry at `path`.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().clone(),
        }
    }

    /// Load the registry, returning an empty classification when absent.
    pub fn load(&self) -> Result<VmOwnership, StateError> {
        let data = match std::fs::read(&self.path) {
            Ok(data) => data,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                return Ok(VmOwnership::default());
            }
            Err(error) => {
                return Err(error)?;
            }
        };
        let state: VmOwnership = serde_json::from_slice(&data)?;
        state.validate()?;
        Ok(state)
    }

    /// Atomically replace the complete ownership registry.
    pub fn save(&self, state: &VmOwnership) -> Result<(), StateError> {
        state.validate()?;
        let path = Path::new(".");
        let parent = self.path.parent().unwrap_or_else(|| &path);
        std::fs::create_dir_all(parent)?;
        let tmp = self
            .path
            .with_extension(format!("json.tmp-{}", std::process::id()));
        let data = serde_json::to_vec_pretty(state)?;
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that managed/unmanaged sets save and reload from a single
    /// JSON file.
    #[test]
    fn ownership_round_trips_in_one_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = VmStateStore::new(Path::new(&dir.path().join("ownership.json")));
        let mut expected = VmOwnership::default();
        expected.record("managed", true).unwrap();
        expected.record("../unmanaged", false).unwrap();

        store.save(&expected).unwrap();

        assert_eq!(store.load().unwrap(), expected);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// Test that a missing ownership file loads as empty/default state.
    #[test]
    fn missing_state_uses_defaults() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            VmStateStore::new(Path::new(&dir.path().join("missing.json")))
                .load()
                .unwrap(),
            VmOwnership::default()
        );
    }

    /// Test that a VM listed as both managed and unmanaged is rejected
    /// on load.
    #[test]
    fn overlapping_classifications_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ownership.json");
        std::fs::write(
            &path,
            r#"{"managed_vms":["same"],"unmanaged_vms":["same"]}"#,
        )
        .unwrap();
        assert!(VmStateStore::new(Path::new(&path)).load().is_err());
    }
}
