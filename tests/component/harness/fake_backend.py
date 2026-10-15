# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import fcntl
import json
import os
import threading

from harness.common import POLL_S, physical

DEFAULT_VM = "vm-a"
STATE_DIR = "/var/fake"
OWNERSHIP_FILE = "/run/io-thread-controller/vm-ownership.json"


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
        physical(root, STATE_DIR).mkdir(parents=True, exist_ok=True)
        self.state_dir = physical(root, STATE_DIR)
        self.default_vm = DEFAULT_VM
        self._util = {}
        self._published = {}
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self.add_vm(DEFAULT_VM)
        self._monitor = threading.Thread(target=self._monitor_procfs, daemon=True)
        self._monitor.start()

    def add_vm(self, vm_id, **fields):
        """Create ``vm_id`` with idle defaults overridden by ``fields``."""
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

    def prepare_healthy_vm(self, threads, util, io=0, vcpu=16):
        """Set the default VM's threads, utilisation, vCPU cap, and read I/O."""
        self.set_threads(threads)
        self.set_vcpu_count(vcpu)
        self.set_util(util)
        self.set_io_counts(io, 0, 0)

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
        """Forget recorded ``set_thread_count`` calls for every VM."""
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
        """Update the contents of ``vm_id`` with ``fields`` atomically."""
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
