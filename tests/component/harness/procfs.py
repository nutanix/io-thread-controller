# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Helpers for /proc stat file generation and parsing."""

import os
from pathlib import Path

from harness.common import POLL_S


def task_stat(tid: int, name: str, ticks: int) -> bytes:
    """Render ``/proc/<pid>/task/<tid>/stat`` with ``ticks`` in field 14."""
    fields = ["0"] * 49
    fields[10] = str(ticks)  # utime
    fields[16] = "1"
    fields[18] = "1"
    return f"{tid} ({name}) S {' '.join(fields)}\n".encode()


def host_stat(user: int, idle: int) -> bytes:
    """Render ``/proc/stat`` with the given user/idle jiffies."""
    body = f"cpu  {user} 0 0 {idle} 0 0 0 0 0 0\n"
    body += f"cpu0 {user} 0 0 {idle} 0 0 0 0 0 0\n"
    body += "ctxt 1\nbtime 1\nprocesses 1\n"
    return body.encode("ascii")


def ticks_per_poll(util: float) -> int:
    """Ticks contributed per poll at ``util`` (0.0–1.0)."""
    return round(POLL_S * os.sysconf("SC_CLK_TCK") * max(0.0, min(1.0, util)))


def parse_utime(text: str) -> int:
    """Parse utime (field 14) from a task stat string."""
    try:
        after = text.split(") S ", 1)[1]
    except IndexError:
        return 0
    fields = after.split()
    return int(fields[10]) if len(fields) > 10 else 0


def read_utime(path: Path) -> int:
    """Read utime from a mock proc stat file at ``path``."""
    return parse_utime(path.read_text())
