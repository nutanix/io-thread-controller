# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Local component-test harness."""

import fcntl
import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

import dbus
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_VM = "vm-a"
CONFIG_FILE = "/etc/io-thread-controller.json"
ENGINE_DIR = "/etc/io-thread-controller.d/engines"
BACKEND_DIR = "/etc/io-thread-controller.d/backends"
STATE_DIR = "/var/fake"
OWNERSHIP_FILE = "/run/io-thread-controller/vm-ownership.json"
MOCKFS_BUS_NAME = "com.nutanix.mockfs1"
MOCKFS_OBJECT_PATH = "/com/nutanix/mockfs1"
MOCKFS_INTERFACE = "com.nutanix.mockfs1"
CONTROLLER_BUS_NAME = "com.nutanix.io_thread_controller1"
CONTROLLER_OBJECT_PATH = "/com/nutanix/io_thread_controller1"
CONTROLLER_INTERFACE = "com.nutanix.io_thread_controller1"
POLL_S = 0.2


def wait_for(predicate, timeout, description):
    """Poll ``predicate`` until it is true or ``timeout`` seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for %s" % (description,))


def _physical(root, logical):
    return root / logical.lstrip("/")


def _target_dir():
    raw = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--offline"],
        cwd=REPO_ROOT,
        text=True,
    )
    return Path(json.loads(raw)["target_directory"])


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def _task_stat(tid, name, ticks):
    fields = ["0"] * 49
    fields[10] = str(ticks)  # field 14: utime
    fields[16] = "1"  # field 20: num_threads
    fields[18] = "1"  # field 22: starttime
    return ("%d (%s) S %s\n" % (tid, name, " ".join(fields))).encode()


class FakeBackend:
    """JSON VM directory the ``fake`` backend discovers."""

    def __init__(self, root, mock_proc):
        self.root = root
        self.mock_proc = mock_proc
        _physical(root, STATE_DIR).mkdir(parents=True, exist_ok=True)
        self.state_dir = _physical(root, STATE_DIR)
        self.default_vm = DEFAULT_VM
        self._util = {}
        self._published = {}
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self.add_vm(DEFAULT_VM)
        self._monitor = threading.Thread(target=self._monitor_procfs, daemon=True)
        self._monitor.start()

    def add_vm(self, vm_id, **fields):
        util = fields.pop("per_thread_util", 0.0)
        defaults = {
            "thread_count": 1,
            "vcpu_count": 1,
            "managed": True,
            "fail_snapshot": False,
            "read_io_count": 0,
            "write_io_count": 0,
            "other_io_count": 0,
        }
        defaults.update(fields)
        self._update(vm_id, **defaults)
        self.set_util(util, vm_id)
        return vm_id

    def set_util(self, util, vm=None):
        vm_id = vm or self.default_vm
        with self._state_lock:
            self._util[vm_id] = util
        self._sync_procfs(vm_id)

    def set_threads(self, count, vm=None):
        self._update(vm or self.default_vm, thread_count=count)

    def set_vcpu_count(self, count, vm=None):
        self._update(vm or self.default_vm, vcpu_count=count)

    def fail_snapshot(self, failed=True, vm=None):
        self._update(vm or self.default_vm, fail_snapshot=failed)

    def set_managed(self, managed, vm=None):
        self._update(vm or self.default_vm, managed=managed)

    def set_io_counts(self, read=0, write=0, other=0, vm=None):
        """Set cumulative read, write, and other I/O counts."""
        self._update(
            vm or self.default_vm,
            read_io_count=read,
            write_io_count=write,
            other_io_count=other,
        )

    def thread_count(self, vm=None):
        return self._read(vm or self.default_vm)["thread_count"]

    def pid(self, vm=None):
        """Return the deterministic process ID used by the fake backend."""
        value = 0
        for byte in (vm or self.default_vm).encode():
            value = ((value * 31) + byte) & 0xFFFFFFFF
        return 10_000 + (value % 1_000_000)

    def stop(self):
        self._stop.set()
        self._monitor.join(timeout=5)

    def clear_calls(self):
        path = self.state_dir / "calls.json"
        if path.exists():
            path.unlink()

    def calls(self, vm=None):
        """Thread-count targets ``set_thread_count`` recorded, in order."""
        vm_id = vm or self.default_vm
        path = self.state_dir / "calls.json"
        if not path.exists():
            return []
        recorded = []
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get("vm") == vm_id:
                recorded.append(item["threads"])
        return recorded

    def _vm_path(self, vm_id):
        return self.state_dir / ("%s.json" % vm_id)

    def _read(self, vm_id):
        return json.loads(self._vm_path(vm_id).read_text())

    def _update(self, vm_id, **fields):
        path = self._vm_path(vm_id)
        lock_path = path.with_suffix(".lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, "a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            data = {}
            if path.exists():
                data = json.loads(path.read_text())
            data.update(fields)
            temporary = path.with_suffix(".json.tmp-pytest")
            temporary.write_text(json.dumps(data))
            os.replace(temporary, path)
            fcntl.flock(lock, fcntl.LOCK_UN)

    def _monitor_procfs(self):
        while not self._stop.wait(0.02):
            for path in self.state_dir.glob("*.json"):
                if path.name == "calls.json":
                    continue
                self._sync_procfs(path.stem)

    def _sync_procfs(self, vm_id):
        path = self._vm_path(vm_id)
        if not path.exists():
            return
        data = self._read(vm_id)
        thread_count = data["thread_count"]
        with self._state_lock:
            util = self._util.get(vm_id, 0.0)
            previous = self._published.get(vm_id)
            signature = (thread_count, util)
            if previous == signature:
                return
            self._published[vm_id] = signature

        pid = self.pid(vm_id)
        previous_count = previous[0] if previous is not None else 0
        for index in range(thread_count, previous_count):
            self.mock_proc.unregister("/%d/task/%d" % (pid, pid + index + 1))

        ticks_per_poll = round(
            POLL_S * os.sysconf("SC_CLK_TCK") * max(0.0, min(1.0, util))
        )
        for index in range(thread_count):
            tid = pid + index + 1
            values = [
                _task_stat(tid, "fake-worker-%d" % index, sample * ticks_per_poll)
                for sample in range(1000)
            ]
            self.mock_proc.register_sequence(
                "/%d/task/%d/stat" % (pid, tid),
                values,
            )


def _prepare_healthy_vm(fake_backend, threads, util, io=0, vcpu=16):
    """Set the default VM's threads, utilisation, vCPU cap, and read I/O."""
    fake_backend.set_threads(threads)
    fake_backend.set_vcpu_count(vcpu)
    fake_backend.set_util(util)
    fake_backend.set_io_counts(io, 0, 0)


def _setup_fake_backend(root):
    """Write ``fake.json`` so the daemon reads this tree's VM directory."""
    _write_json(
        _physical(root, BACKEND_DIR) / "fake.json",
        {"state_dir": STATE_DIR},
    )


class DBusServer:
    def __init__(self, root):
        self._socket = root / "dbus.sock"
        self._config = root / "dbus.xml"
        self._config.write_text(
            """<!DOCTYPE busconfig PUBLIC "-//freedesktop//DTD D-BUS Bus Configuration 1.0//EN"
 "http://www.freedesktop.org/standards/dbus/1.0/busconfig.dtd">
<busconfig>
  <type>system</type>
  <listen>unix:path=%s</listen>
  <auth>EXTERNAL</auth>
  <policy context="default">
    <allow own="*"/>
    <allow send_destination="*"/>
    <allow receive_sender="*"/>
  </policy>
</busconfig>
"""
            % self._socket
        )
        self._proc = None
        self.address = None

    def start(self):
        self._proc = subprocess.Popen(
            [
                "dbus-daemon",
                "--nofork",
                "--config-file=%s" % self._config,
                "--print-address",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert self._proc.stdout is not None
        self.address = self._proc.stdout.readline().strip()
        if not self.address:
            raise RuntimeError("dbus-daemon did not print an address")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self._socket.exists():
                return
            if self._proc.poll() is not None:
                raise RuntimeError("dbus-daemon exited before listening")
            time.sleep(0.01)
        raise RuntimeError("dbus socket %s did not appear" % self._socket)

    def stop(self):
        if self._proc is None or self._proc.poll() is not None:
            return
        self._proc.send_signal(signal.SIGTERM)
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)


class MockProc:
    """mockfsd mounted at ``<root>/proc`` and controlled over D-Bus.

    The process claims ``com.nutanix.mockfs1`` on the harness system bus.
    ``Mount`` exposes the FUSE tree; ``RegisterSequence`` feeds ``/stat``.
    """

    def __init__(self, root, binary, dbus_server):
        self.mount = root / "proc"
        self.mount.mkdir(parents=True, exist_ok=True)
        self._proc = None
        self._bus = None
        self._iface = None
        self._lock = threading.Lock()
        self._stderr_path = root / "mockfsd.stderr"
        self._stderr = open(self._stderr_path, "w")
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
            assert self._proc.stdout is not None
            name = self._proc.stdout.readline().strip()
            if self._proc.poll() is not None or name != MOCKFS_BUS_NAME:
                raise RuntimeError(
                    "mockfsd did not claim %s (printed %r): %s"
                    % (MOCKFS_BUS_NAME, name, self._stderr_text())
                )
            self._bus = dbus.bus.BusConnection(dbus_server.address)
            self._iface = dbus.Interface(
                self._bus.get_object(MOCKFS_BUS_NAME, MOCKFS_OBJECT_PATH),
                MOCKFS_INTERFACE,
            )
            wait_for(self._healthy, timeout=5, description="mockfsd ping")
            self._iface.Mount(str(self.mount))
        except Exception:
            self.stop()
            raise

    def _stderr_text(self):
        self._stderr.flush()
        return self._stderr_path.read_text(errors="replace").strip()

    def _healthy(self):
        if self._proc.poll() is not None:
            raise RuntimeError("mockfsd exited during startup: %s" % self._stderr_text())
        try:
            return self._iface.Ping() == "ok"
        except dbus.exceptions.DBusException:
            return False

    def register_sequence(self, path, values):
        """Register successive file bodies for ``path`` on this mount."""
        payload = dbus.Array(
            [dbus.ByteArray(bytes(value)) for value in values],
            signature="ay",
        )
        with self._lock:
            self._iface.RegisterSequence(str(self.mount), path, payload)

    def unregister(self, path):
        """Remove ``path`` and any descendants from this mount."""
        with self._lock:
            self._iface.Unregister(str(self.mount), path)

    def stop(self):
        if self._iface is not None and self._proc is not None and self._proc.poll() is None:
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


class Controller:
    """Starts ``io-thread-controller`` against the fake backend."""

    def __init__(self, root, binary, dbus_server):
        self.root = root
        self._binary = binary
        self._dbus = dbus_server
        self._proc = None
        self._log_path = root / "daemon.log"
        self.ownership_path = _physical(root, OWNERSHIP_FILE)

    def __call__(self, engine, engine_config, controller_overrides):
        self.stop()
        config = {
            "scale_poll_secs": 0.2,
            "enable_per_vm_status_line": True,
            "enable_aggregate_status_line": True,
            "min_thread_count": 1,
            "max_thread_count": 8,
            "host_cpu_scale_up_ceiling_percent": 0,
            "cooldown_secs": 0.0,
            "engine": engine,
            "engine_config_dir": ENGINE_DIR,
            "backend_config_dir": BACKEND_DIR,
            "vm_state_path": OWNERSHIP_FILE,
            "dry_run": False,
            "print_status_header": False,
        }
        config.update(controller_overrides)
        _write_json(_physical(self.root, CONFIG_FILE), config)
        _write_json(
            _physical(self.root, ENGINE_DIR) / ("%s.json" % engine),
            engine_config,
        )
        _write_json(
            _physical(self.root, BACKEND_DIR) / "fake.json",
            {"state_dir": STATE_DIR},
        )
        self.start()

    def start(self):
        log_fd = os.open(
            self._log_path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644
        )
        self._proc = subprocess.Popen(
            [
                str(self._binary),
                "--config",
                CONFIG_FILE,
                "--log-style",
                "human",
                "--log-level",
                "info",
            ],
            cwd=REPO_ROOT,
            env={
                **os.environ,
                "IO_THREAD_CONTROLLER_ROOT_PATH": str(self.root),
                "DBUS_SYSTEM_BUS_ADDRESS": self._dbus.address,
                "IO_THREAD_CONTROLLER_DBUS_ADDRESS": self._dbus.address,
                "RUST_LOG": "info",
            },
            stdout=log_fd,
            stderr=log_fd,
        )
        os.close(log_fd)
        wait_for(
            self._dbus_ready,
            timeout=5,
            description="controller D-Bus service",
        )

    def logs(self):
        if not self._log_path.exists():
            return ""
        return self._log_path.read_text(errors="replace")

    def _dbus_ready(self):
        try:
            bus = dbus.bus.BusConnection(self._dbus.address)
            return bus.name_has_owner(CONTROLLER_BUS_NAME)
        except dbus.exceptions.DBusException:
            return False

    def stats(self):
        """Return structured controller status from the private D-Bus service."""
        try:
            bus = dbus.bus.BusConnection(self._dbus.address)
            stats = bus.call_blocking(
                CONTROLLER_BUS_NAME,
                CONTROLLER_OBJECT_PATH,
                CONTROLLER_INTERFACE,
                "GetStats",
                "",
                (),
            )
            return json.loads(stats)
        except dbus.exceptions.DBusException:
            return None

    def tracked(self):
        """Return the number of VMs with a successful current refresh."""
        stats = self.stats()
        return (
            None
            if stats is None
            else sum(vm["alive"] for vm in stats["vms"])
        )

    def thread_count(self, vm_id):
        """Return one tracked VM's current controller thread count."""
        stats = self.stats()
        if stats is None:
            return None
        for vm in stats["vms"]:
            if vm["vm"] == vm_id:
                return vm["thread_count"]
        return None

    def stop(self):
        if self._proc is None or self._proc.poll() is not None:
            self._proc = None
            return
        self._proc.send_signal(signal.SIGINT)
        try:
            self._proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)
        self._proc = None


@pytest.fixture
def dbus_server(tmp_path):
    server = DBusServer(tmp_path)
    server.start()
    yield server
    server.stop()


@pytest.fixture
def mock_proc(tmp_path, dbus_server):
    binary = _target_dir() / "debug" / "mockfsd"
    if not binary.exists():
        raise RuntimeError("mockfsd is not built at %s" % binary)
    proc = MockProc(tmp_path, binary, dbus_server)
    yield proc
    proc.stop()


@pytest.fixture
def fake_backend(tmp_path, mock_proc):
    backend = FakeBackend(tmp_path, mock_proc)
    yield backend
    backend.stop()


@pytest.fixture
def controller(tmp_path, fake_backend, dbus_server):
    del fake_backend  # created first so the state directory exists
    binary = _target_dir() / "debug" / "io-thread-controller"
    if not binary.exists():
        raise RuntimeError("io-thread-controller is not built at %s" % binary)
    launched = Controller(tmp_path, binary, dbus_server)
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
