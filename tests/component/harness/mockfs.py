# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import Any

import dbus
import dbus.bus

from harness.common import wait_for
from harness.dbus import DBusServer

MOCKFS_BUS_NAME = "com.nutanix.mockfs1"
MOCKFS_OBJECT_PATH = "/com/nutanix/mockfs1"
MOCKFS_INTERFACE = "com.nutanix.mockfs1"


class MockProc:
    """mockfsd mounted at ``<root>/proc`` and controlled over D-Bus.

    The process claims ``com.nutanix.mockfs1`` on the harness system bus.
    ``Mount`` exposes the FUSE tree; ``RegisterSequence`` feeds ``/stat``.
    """

    def __init__(self, root: Path, binary: Path, dbus_server: DBusServer) -> None:
        """Create a new mock `/proc` controlled over DBus."""
        self.mount = root / "proc"
        self.mount.mkdir(parents=True, exist_ok=True)
        self._proc: subprocess.Popen[str] | None = None
        self._bus: dbus.bus.BusConnection | None = None
        self._iface: Any | None = None
        self._lock = threading.Lock()
        self._stderr_path = root / "mockfsd.stderr"
        self._stderr = open(self._stderr_path, "w")  # noqa: SIM115
        try:
            env = os.environ.copy()
            assert dbus_server.address is not None
            env["DBUS_SYSTEM_BUS_ADDRESS"] = dbus_server.address
            self._proc = subprocess.Popen(
                [str(binary)],
                stdout=subprocess.PIPE,
                stderr=self._stderr,
                text=True,
                env=env,
            )
            self._bus = dbus.bus.BusConnection(dbus_server.address)
            wait_for(self._name_claimed, timeout=5, description="mockfsd bus name")
            self._iface = dbus.Interface(
                self._bus.get_object(MOCKFS_BUS_NAME, MOCKFS_OBJECT_PATH),
                MOCKFS_INTERFACE,
            )
            wait_for(self._healthy, timeout=5, description="mockfsd ping")
            self._iface.Mount(str(self.mount))
        except Exception:
            self.stop()
            raise

    def _stderr_text(self) -> str:
        """Return mockfsd's stderr so far, for error messages."""
        self._stderr.flush()
        return self._stderr_path.read_text(errors="replace").strip()

    def _name_claimed(self) -> bool:
        """Return whether mockfsd owns its bus name; raise if it has exited."""
        assert self._proc is not None
        assert self._bus is not None

        if self._proc.poll() is not None:
            raise RuntimeError(f"mockfsd exited during startup: {self._stderr_text()}")
        return self._bus.name_has_owner(MOCKFS_BUS_NAME)

    def _healthy(self) -> bool:
        """Return whether mockfsd answers ``Ping``; raise if it has exited."""
        assert self._proc is not None
        assert self._iface is not None

        if self._proc.poll() is not None:
            raise RuntimeError(f"mockfsd exited during startup: {self._stderr_text()}")
        try:
            return self._iface.Ping() == "ok"
        except dbus.exceptions.DBusException:
            return False

    def register_sequence(
        self,
        path: str | Path,
        values: list[bytes],
    ) -> None:
        """Register successive file bodies for ``path`` on this mount."""
        assert self._iface is not None

        payload = dbus.Array(
            [dbus.ByteArray(bytes(value)) for value in values],
            signature="ay",
        )
        with self._lock:
            self._iface.RegisterSequence(str(self.mount), str(path), payload)

    def register_static(self, path: str | Path, value: bytes) -> None:
        """Register a static file body for ``path`` on this mount."""
        assert self._iface is not None

        with self._lock:
            self._iface.RegisterStatic(str(self.mount), str(path), list(value))

    def unregister(self, path: str | Path) -> None:
        """Remove ``path`` and any descendants from this mount."""
        assert self._iface is not None

        with self._lock:
            self._iface.Unregister(str(self.mount), str(path))

    def stop(self) -> None:
        """Unmount if possible, stop mockfsd, and close its stderr log."""
        if (
            self._iface is not None
            and self._proc is not None
            and self._proc.poll() is None
        ):
            try:
                self._iface.Unmount(str(self.mount))
            except dbus.exceptions.DBusException:
                pass
        if self._proc is not None and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)
        if self._stderr is not None and not self._stderr.closed:
            self._stderr.close()
