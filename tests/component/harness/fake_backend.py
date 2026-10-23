# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import fcntl
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

from harness.common import physical
from harness.mockfs import MockProc

DEFAULT_VM = "vm-a"
STATE_DIR = "/var/fake"
OWNERSHIP_FILE = "/run/io-thread-controller/vm-ownership.json"


def _task_stat(tid: int, name: str, ticks: int) -> bytes:
    fields = ["0"] * 49
    fields[10] = str(ticks)  # field 14: utime
    fields[16] = "1"  # field 20: num_threads
    fields[18] = "1"  # field 22: starttime
    return f"{tid} ({name}) S {' '.join(fields)}\n".encode()


class FakeBackend:
    """JSON VM directory the ``fake`` backend discovers."""

    def __init__(self, root: Path, mock_proc: MockProc) -> None:
        """Create a new FakeBackend interface at the given path."""
        self.root = root
        self.mock_proc = mock_proc
        physical(root, STATE_DIR).mkdir(parents=True, exist_ok=True)
        self.state_dir = physical(root, STATE_DIR)
        self.default_vm = DEFAULT_VM
        self._util: dict[str, float] = {}
        self._thread_counts: dict[str, int] = {}
        self._cumulative_ticks: dict[str, dict[int, int]] = {}
        self._last_time: dict[str, float] = {}
        self._state_lock = threading.Lock()
        self._stop = threading.Event()
        self.add_vm(DEFAULT_VM)
        self._monitor = threading.Thread(target=self._advance_ticks, daemon=True)
        self._monitor.start()

    def add_vm(self, vm_id: str, **fields: Any) -> None:
        """Create ``vm_id`` with idle defaults overridden by ``fields``."""
        util = fields.pop("per_thread_util", 0.0)
        thread_count = fields.pop("thread_count", 1)
        defaults = {
            "vcpu_count": 1,
            "managed": True,
            "fail_snapshot": False,
            "read_io_count": 0,
            "write_io_count": 0,
            "other_io_count": 0,
        }
        defaults.update(fields)
        self._update(vm_id, **defaults)
        with self._state_lock:
            self._thread_counts[vm_id] = thread_count
            self._util[vm_id] = util
            self._cumulative_ticks[vm_id] = {}
            self._last_time[vm_id] = time.monotonic()
            # initialise procfs for this VM
            self._sync_procfs_locked(vm_id)

    def set_util(self, util: float, vm: str | None = None) -> None:
        """Set utilisation for a given VM."""
        vm_id = vm or self.default_vm
        with self._state_lock:
            self._util[vm_id] = util

    def set_threads(self, count: int, vm: str | None = None) -> None:
        """Set thread count for a given VM."""
        vm_id = vm or self.default_vm
        with self._state_lock:
            old = self._thread_counts.get(vm_id, 0)
            self._thread_counts[vm_id] = count
            pid = self.pid(vm_id)
            # handle removed threads
            for index in range(count, old):
                tid = pid + index + 1
                self.mock_proc.unregister(f"/{pid}/task/{tid}")
                self._cumulative_ticks.get(vm_id, {}).pop(tid, None)
            # handle new threads
            for index in range(old, count):
                tid = pid + index + 1
                # initialise ticks for new thread to current average or 0
                ticks = 0
                if vm_id in self._cumulative_ticks and self._cumulative_ticks[vm_id]:
                    # use average of existing ticks as starting point to avoid large jump
                    vals = list(self._cumulative_ticks[vm_id].values())
                    ticks = sum(vals) // len(vals) if vals else 0
                if vm_id not in self._cumulative_ticks:
                    self._cumulative_ticks[vm_id] = {}
                self._cumulative_ticks[vm_id][tid] = ticks
                self.mock_proc.register_static(
                    f"/{pid}/task/{tid}/stat",
                    _task_stat(tid, f"fake-worker-{index}", ticks),
                )

    def set_vcpu_count(self, count: int, vm: str | None = None) -> None:
        """Set vCPU cound for a given VM."""
        self._update(vm or self.default_vm, vcpu_count=count)

    def fail_snapshot(self, failed: bool = True, vm: str | None = None) -> None:
        """Set whether the next snapshot should fail for the given VM."""
        self._update(vm or self.default_vm, fail_snapshot=failed)

    def set_managed(self, managed: bool, vm: str | None = None) -> None:
        """Set whether the given VM is managed."""
        self._update(vm or self.default_vm, managed=managed)

    def set_io_counts(
        self, read: int = 0, write: int = 0, other: int = 0, vm: str | None = None
    ) -> None:
        """Set cumulative read, write, and other I/O counts."""
        self._update(
            vm or self.default_vm,
            read_io_count=read,
            write_io_count=write,
            other_io_count=other,
        )

    def prepare_healthy_vm(
        self, threads: int, util: float, io: int = 0, vcpu: int = 16
    ) -> None:
        """Set the default VM's threads, utilisation, vCPU cap, and read I/O."""
        self.set_threads(threads)
        self.set_vcpu_count(vcpu)
        self.set_util(util)
        self.set_io_counts(io, 0, 0)

    def thread_count(self, vm: str | None = None) -> int:
        """Get the number of threads assigned to the given VM."""
        vm_id = vm or self.default_vm
        # count procfs entries
        pid = self.pid(vm_id)
        task_dir = physical(self.root, f"/proc/{pid}/task")
        if not task_dir.exists():
            # fallback to in-memory count
            with self._state_lock:
                return self._thread_counts.get(vm_id, 0)
        try:
            return len([p for p in task_dir.iterdir() if p.is_dir()])
        except OSError:
            with self._state_lock:
                return self._thread_counts.get(vm_id, 0)

    def pid(self, vm: str | None = None) -> int:
        """Return the deterministic process ID used by the fake backend."""
        value = 0
        for byte in (vm or self.default_vm).encode():
            value = ((value * 31) + byte) & 0xFFFFFFFF
        return 10_000 + (value % 1_000_000)

    def stop(self) -> None:
        self._stop.set()
        self._monitor.join(timeout=5)

    def clear_calls(self) -> None:
        """Forget recorded ``set_thread_count`` calls for every VM."""
        path = self.state_dir / "calls.json"
        if path.exists():
            path.unlink()

    def calls(self, vm: str | None = None) -> list[int]:
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

    def _vm_path(self, vm_id: str) -> Path:
        """Get the path to the JSON file of the given VM."""
        return self.state_dir / f"{vm_id}.json"

    def _read(self, vm_id: str) -> dict[str, Any]:
        """Get the contents of the given VM's JSON file."""
        return json.loads(self._vm_path(vm_id).read_text())

    def _update(self, vm_id: str, **fields: Any) -> None:
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

    def _advance_ticks(self) -> None:
        """Periodically advance CPU ticks for all VMs based on util."""
        while not self._stop.wait(0.02):
            now = time.monotonic()
            with self._state_lock:
                for vm_id, util in list(self._util.items()):
                    last = self._last_time.get(vm_id, now)
                    elapsed = now - last
                    self._last_time[vm_id] = now
                    ticks_per_sec = os.sysconf("SC_CLK_TCK") * max(0.0, min(1.0, util))
                    delta_ticks = round(elapsed * ticks_per_sec)
                    if delta_ticks == 0:
                        continue
                    pid = self.pid(vm_id)
                    # count procfs entries to handle Rust-initiated changes
                    task_dir = physical(self.root, f"/proc/{pid}/task")
                    if not task_dir.exists():
                        continue
                    try:
                        tids = [
                            int(p.name)
                            for p in task_dir.iterdir()
                            if p.is_dir() and p.name.isdigit()
                        ]
                    except OSError:
                        continue
                    if not tids:
                        continue
                    if vm_id not in self._cumulative_ticks:
                        self._cumulative_ticks[vm_id] = {}
                    for tid in tids:
                        index = tid - pid - 1
                        if index < 0:
                            continue
                        prev = self._cumulative_ticks[vm_id].get(tid, 0)
                        new_ticks = prev + delta_ticks
                        self._cumulative_ticks[vm_id][tid] = new_ticks
                        self.mock_proc.register_static(
                            f"/{pid}/task/{tid}/stat",
                            _task_stat(tid, f"fake-worker-{index}", new_ticks),
                        )

    def _sync_procfs_locked(self, vm_id: str) -> None:
        """Initialise procfs for vm_id with current thread count and util (caller holds lock)."""
        thread_count = self._thread_counts.get(vm_id, 0)
        pid = self.pid(vm_id)
        # ensure cumulative dict exists
        if vm_id not in self._cumulative_ticks:
            self._cumulative_ticks[vm_id] = {}
        for index in range(thread_count):
            tid = pid + index + 1
            ticks = self._cumulative_ticks[vm_id].get(tid, 0)
            self.mock_proc.register_static(
                f"/{pid}/task/{tid}/stat",
                _task_stat(tid, f"fake-worker-{index}", ticks),
            )

