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
pub mod rolling;
pub mod state;
pub mod util;
