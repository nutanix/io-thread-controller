# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Fixtures for the component testing harness."""

import pytest
from harness.common import built_binary
from harness.controller import Controller
from harness.dbus import DBusServer
from harness.json_backend import JsonBackend
from harness.mockfs import MockProc


@pytest.fixture
def json_backend(tmp_path, mock_proc):
    """Create a new JsonBackend running at ``tmp_path``."""
    backend = JsonBackend(tmp_path, mock_proc)
    yield backend
    backend.stop()


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
