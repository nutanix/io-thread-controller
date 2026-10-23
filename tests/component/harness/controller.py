# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import json
import os
import signal
import subprocess

import dbus

from harness.common import POLL_S, REPO_ROOT, physical, wait_for, write_json
from harness.fake_backend import OWNERSHIP_FILE, STATE_DIR

CONFIG_FILE = "/etc/io-thread-controller.json"
ENGINE_DIR = "/etc/io-thread-controller.d/engines"
BACKEND_DIR = "/etc/io-thread-controller.d/backends"
CONTROLLER_BUS_NAME = "com.nutanix.io_thread_controller1"
CONTROLLER_OBJECT_PATH = "/com/nutanix/io_thread_controller1"
CONTROLLER_INTERFACE = "com.nutanix.io_thread_controller1"


class Controller:
    """Starts ``io-thread-controller`` against the fake backend."""

    def __init__(self, root, binary, dbus_server):
        self.root = root
        self._binary = binary
        self._dbus = dbus_server
        self._proc = None
        self._log_path = root / "daemon.log"
        self.ownership_path = physical(root, OWNERSHIP_FILE)

    def __call__(self, engine, engine_config, controller_overrides):
        """(Re)start the daemon with ``engine`` and freshly written config files."""
        self.stop()
        config = {
            "scale_poll_secs": POLL_S,
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
        write_json(physical(self.root, CONFIG_FILE), config)
        write_json(
            physical(self.root, ENGINE_DIR) / ("%s.json" % engine),
            engine_config,
        )
        write_json(
            physical(self.root, BACKEND_DIR) / "fake.json",
            {"state_dir": STATE_DIR},
        )
        self.start()

    def start(self):
        """Launch the daemon and wait until it owns its D-Bus name."""
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
        """Return the daemon's combined stdout/stderr, or "" before first start."""
        if not self._log_path.exists():
            return ""
        return self._log_path.read_text(errors="replace")

    def _dbus_ready(self):
        """Return whether the controller has claimed its bus name."""
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
        """SIGINT the daemon, escalating to SIGKILL after 5 seconds."""
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
