# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import fcntl
import json
import os
from pathlib import Path
from typing import Any

from harness.common import physical

DEFAULT_VM = "vm-a"
STATE_DIR = "/var/fake"
OWNERSHIP_FILE = "/run/io-thread-controller/vm-ownership.json"


class FakeBackend:
    """JSON VM directory the ``fake`` backend discovers."""

    def __init__(self, root: Path):
        """Create a new FakeBackend interface at the given path."""
        self.root = root
        physical(root, STATE_DIR).mkdir(parents=True, exist_ok=True)
        self.state_dir = physical(root, STATE_DIR)
        self.default_vm = DEFAULT_VM
        self.add_vm(DEFAULT_VM)

    def add_vm(self, vm_id: str, **fields: Any) -> None:
        """Create ``vm_id`` with idle defaults overridden by ``fields``."""
        defaults = {
            "thread_count": 1,
            "vcpu_count": 1,
            "per_thread_util": 0.0,
            "managed": True,
            "fail_snapshot": False,
            "read_io_count": 0,
            "write_io_count": 0,
            "other_io_count": 0,
        }
        defaults.update(fields)
        self._update(vm_id, **defaults)

    def set_util(self, util: float, vm: str | None = None) -> None:
        """Set utilisation for a given VM."""
        self._update(vm or self.default_vm, per_thread_util=util)

    def set_threads(self, count: int, vm: str | None = None) -> None:
        """Set thread count for a given VM."""
        self._update(vm or self.default_vm, thread_count=count)

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
        return self._read(vm or self.default_vm)["thread_count"]

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

    def _read(self, vm_id: str) -> Any:
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
