# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Fixtures for the component testing harness."""

import pytest
from harness.common import POLL_S, built_binary
from harness.controller import Controller
from harness.dbus import DBusServer
from harness.json_backend import JsonBackend
from harness.mockfs import MockProc


@pytest.fixture
def json_backend(tmp_path):
    """Create a new JsonBackend running at ``tmp_path``."""
    return JsonBackend(tmp_path)


@pytest.fixture
def dbus_server(tmp_path):
    """Create a new isolated DBus bus."""
    server = DBusServer(tmp_path)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def mock_proc(tmp_path, dbus_server):
    """Mount a mock proc at tmp_path controlled over the given bus."""
    proc = MockProc(tmp_path, built_binary("mockfsd"), dbus_server)
    yield proc
    proc.stop()


@pytest.fixture
def controller(tmp_path, json_backend, dbus_server):
    """Start up a new `io-thread-controller`."""
    del json_backend
    launched = Controller(tmp_path, built_binary("io-thread-controller"), dbus_server)
    yield launched
    launched.stop()


@pytest.fixture
def fast_threshold():
    """Decisions land on the next eligible poll, with validation disabled."""
    return {
        "scale_up_threshold_percent": 60,
        "scale_down_sustain_secs": 1,
        "max_scale_down_step": 1,
        "scale_up_min_gain_percent": 0,
        "scale_down_revert_drop_percent": 0,
        "scale_validation_delay_secs": 0,
        "scale_validation_delay_per_thread_secs": 0,
        "post_revert_up_grace_secs": 0,
        "post_revert_up_recovery_secs": 0,
        "post_revert_up_backoff_max_secs": 0,
        "scale_down_backoff_max_secs": 0,
    }


@pytest.fixture
def run_controller(controller):
    """Start the daemon on the threshold engine."""

    def start(engine_config, **overrides):
        settings = {
            "scale_poll_secs": POLL_S,
            "min_thread_count": 1,
            "max_thread_count": 8,
            "host_cpu_scale_up_ceiling_percent": 0,
            "cooldown_secs": 0,
        }
        settings.update(overrides)
        controller(
            engine="threshold",
            engine_config=engine_config,
            controller_overrides=settings,
        )

    return start
