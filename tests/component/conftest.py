# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Fixtures for the local component-test harness."""

import pytest

from harness.common import POLL_S, built_binary
from harness.controller import Controller
from harness.dbus import DBusServer
from harness.fake_backend import FakeBackend
from harness.mockfs import MockProc


@pytest.fixture
def fake_backend(tmp_path):
    return FakeBackend(tmp_path)


@pytest.fixture
def dbus_server(tmp_path):
    server = DBusServer(tmp_path)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def mock_proc(tmp_path, dbus_server):
    proc = MockProc(tmp_path, built_binary("mockfsd"), dbus_server)
    yield proc
    proc.stop()


@pytest.fixture
def controller(tmp_path, fake_backend, dbus_server):
    del fake_backend  # created first so the state directory exists
    launched = Controller(tmp_path, built_binary("io-thread-controller"), dbus_server)
    yield launched
    launched.stop()


@pytest.fixture
def fast_threshold():
    """Decisions land on the next eligible poll, with validation disabled."""
    return {
        "scale_up_threshold_percent": 60,
        "scale_down_sustain_polls": 1,
        "max_scale_down_step": 1,
        "scale_up_min_gain_percent": 0,
        "scale_down_revert_drop_percent": 0,
        "scale_validation_sample_polls": 0,
    }


@pytest.fixture
def run_threshold(controller):
    """Start the daemon on the threshold engine with open controller bounds."""

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
