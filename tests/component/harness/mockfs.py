# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import ctypes
import os
import signal
import subprocess
import threading
from collections.abc import Callable
from pathlib import Path

import dbus
import dbus.bus
import dbus.service

from harness.common import wait_for

MOCKFS_BUS_NAME = "com.nutanix.mockfs1"
MOCKFS_OBJECT_PATH = "/com/nutanix/mockfs1"
MOCKFS_INTERFACE = "com.nutanix.mockfs1"

CALLBACK_BUS_NAME = "com.nutanix.mockfs.callback1"
CALLBACK_OBJECT_PATH = "/com/nutanix/mockfs/callback1"
CALLBACK_INTERFACE = "com.nutanix.mockfs.callback1"

_GLIB_LOCK = threading.Lock()
_GLIB_LOOP_PTR = None
_GLIB_LOOP_THREAD = None
_GLIB_INITIALIZED = False


def _ensure_glib_loop() -> None:
    """Install DBusGMainLoop and start a GLib MainLoop via ctypes.

    Required for inbound D-Bus calls (mockfsd -> harness). Uses ctypes to
    avoid a PyGObject dependency; the GLib loop is shared process-wide.
    """
    global _GLIB_LOOP_PTR, _GLIB_LOOP_THREAD, _GLIB_INITIALIZED
    with _GLIB_LOCK:
        if _GLIB_INITIALIZED:
            return
        from dbus.mainloop.glib import DBusGMainLoop, threads_init

        threads_init()
        DBusGMainLoop(set_as_default=True)

        lib = ctypes.CDLL("libglib-2.0.so.0")
        lib.g_main_loop_new.argtypes = [ctypes.c_void_p, ctypes.c_int]
        lib.g_main_loop_new.restype = ctypes.c_void_p
        lib.g_main_loop_run.argtypes = [ctypes.c_void_p]
        lib.g_main_loop_quit.argtypes = [ctypes.c_void_p]

        # Keep a reference to avoid GC
        _ensure_glib_loop._lib = lib  # type: ignore[attr-defined]

        _GLIB_LOOP_PTR = lib.g_main_loop_new(None, 0)
        _GLIB_LOOP_THREAD = threading.Thread(
            target=lib.g_main_loop_run,
            args=(_GLIB_LOOP_PTR,),
            daemon=True,
            name="mockfs-glib-loop",
        )
        _GLIB_LOOP_THREAD.start()
        _GLIB_INITIALIZED = True


class _CallbackAgent(dbus.service.Object):
    """D-Bus object serving GetFileContent for mockfsd callbacks."""

    def __init__(
        self,
        bus: dbus.bus.BusConnection,
        registry: dict[tuple[str, str], Callable[[], bytes | str]],
        registry_lock: threading.Lock,
    ) -> None:
        super().__init__(bus, CALLBACK_OBJECT_PATH)
        self._registry = registry
        self._registry_lock = registry_lock

    @dbus.service.method(CALLBACK_INTERFACE, in_signature="ss", out_signature="ay")
    def GetFileContent(self, mount_path: str, path: str):  # type: ignore[no-untyped-def]
        with self._registry_lock:
            callback = self._registry.get((mount_path, path))
        if callback is None:
            return dbus.ByteArray(b"")
        try:
            value = callback()
        except Exception:  # noqa: BLE001
            return dbus.ByteArray(b"")
        if value is None:
            return dbus.ByteArray(b"")
        if isinstance(value, str):
            value = value.encode()
        return dbus.ByteArray(bytes(value))


class _CallbackService:
    """One service per D-Bus address; owns BusName + agent + registry."""

    def __init__(self, address: str) -> None:
        _ensure_glib_loop()
        self.address = address
        self._bus = dbus.bus.BusConnection(address)
        self._registry: dict[tuple[str, str], Callable[[], bytes | str]] = {}
        self._registry_lock = threading.Lock()
        self._bus_name = dbus.service.BusName(CALLBACK_BUS_NAME, self._bus)
        self._agent = _CallbackAgent(self._bus, self._registry, self._registry_lock)

    def register(self, mount: str, path: str, fn: Callable[[], bytes | str]) -> None:
        with self._registry_lock:
            self._registry[(mount, path)] = fn

    def unregister(self, mount: str, path: str) -> None:
        with self._registry_lock:
            self._registry.pop((mount, path), None)

    def clear_mount(self, mount: str) -> None:
        with self._registry_lock:
            for key in [k for k in self._registry if k[0] == mount]:
                self._registry.pop(key, None)


_CALLBACK_SERVICES: dict[str, _CallbackService] = {}
_CALLBACK_SERVICES_LOCK = threading.Lock()


def _get_callback_service(address: str) -> _CallbackService:
    with _CALLBACK_SERVICES_LOCK:
        svc = _CALLBACK_SERVICES.get(address)
        if svc is None:
            svc = _CallbackService(address)
            _CALLBACK_SERVICES[address] = svc
        return svc


class MockProc:
    """mockfsd mounted at ``<root>/proc`` and controlled over D-Bus.

    The process claims ``com.nutanix.mockfs1`` on the harness system bus.
    ``Mount`` exposes the FUSE tree; ``RegisterSequence`` feeds ``/stat``.
    ``RegisterCallback`` registers a Python callable whose return value is
    served on every read via the callback agent.
    """

    def __init__(self, root: Path, binary: Path, dbus_server: dbus.Bus) -> None:
        """Create a new mock `/proc` controlled over DBus."""
        self.mount = root / "proc"
        self.mount.mkdir(parents=True, exist_ok=True)
        self._proc = None
        self._bus = None
        self._iface = None
        self._lock = threading.Lock()
        self._stderr_path = root / "mockfsd.stderr"
        self._stderr = open(self._stderr_path, "w")  # noqa: SIM115
        self._dbus_address: str | None = dbus_server.address
        self._callback_service: _CallbackService | None = None
        try:
            env = os.environ.copy()
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

    def _ensure_callback_service(self) -> _CallbackService:
        """Lazily create the callback agent for this bus."""
        if self._callback_service is None:
            assert self._dbus_address is not None
            self._callback_service = _get_callback_service(self._dbus_address)
        return self._callback_service

    def register_static(
        self,
        path: Path | str,
        content: bytes | str,
    ) -> None:
        """Register a single static file body for ``path`` on this mount."""
        assert self._iface is not None
        if isinstance(content, str):
            content = content.encode()
        payload = dbus.ByteArray(bytes(content))
        with self._lock:
            self._iface.RegisterStatic(str(self.mount), str(path), payload)

    def register_sequence(
        self,
        path: Path | str,
        values: list[bytes | str],
    ) -> None:
        """Register successive file bodies for ``path`` on this mount."""
        assert self._iface is not None

        normalized: list[bytes] = []
        for v in values:
            if isinstance(v, str):
                v = v.encode()
            normalized.append(bytes(v))
        payload = dbus.Array(
            [dbus.ByteArray(v) for v in normalized],
            signature="ay",
        )
        with self._lock:
            self._iface.RegisterSequence(str(self.mount), str(path), payload)

    def register_callback(
        self,
        path: Path | str,
        callback: Callable[[], bytes | str],
    ) -> None:
        """Register a live callback for ``path``; called on every read.

        ``callback`` is invoked on the GLib thread for each FUSE read at
        offset 0. It must be fast (<5s) and thread-safe; exceptions yield
        empty content.
        """
        assert self._iface is not None
        svc = self._ensure_callback_service()
        svc.register(str(self.mount), str(path), callback)
        with self._lock:
            self._iface.RegisterCallback(str(self.mount), str(path))

    def unregister(self, path: Path | str) -> None:
        """Remove ``path`` and any descendants from this mount."""
        assert self._iface is not None

        if self._callback_service is not None:
            self._callback_service.unregister(str(self.mount), str(path))
        with self._lock:
            self._iface.Unregister(str(self.mount), str(path))

    def stop(self) -> None:
        """Unmount if possible, stop mockfsd, and close its stderr log."""
        if self._callback_service is not None and self._dbus_address is not None:
            self._callback_service.clear_mount(str(self.mount))
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
