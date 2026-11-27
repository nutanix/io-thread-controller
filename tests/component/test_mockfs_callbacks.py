# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Ported from a9f130c to show callback ergonomics with fixtures.

Original at ``a9f130c:tests/component/test_controller.py`` used::

    def _high_host_sequence():
        samples = [_stat(0, 1000)]
        for _ in range(40):
            user += 10000
            samples.append(_stat(user, idle))
        return samples
    mock_proc.register_sequence(Path("/stat"), _high_host_sequence())

This file replaces that with inline callbacks and ``procfs`` helpers,
using ``mock_proc`` fixtures from ``tests/component/conftest.py``.
"""

from pathlib import Path

from harness.mockfs import MockProc
from harness.procfs import host_stat, read_utime, task_stat, ticks_per_poll


def test_task_stat_callback_advances_with_live_util(mock_proc: MockProc):
    """Each read advances ticks; util changes apply without re-register."""
    util = [0.5]
    ticks: dict[int, int] = {}
    pid, tid = 10000, 10001

    def task_cb() -> bytes:
        tpp = ticks_per_poll(util[0])
        ticks[tid] = ticks.get(tid, 0) + tpp
        return task_stat(tid, "fake-worker-0", ticks[tid])

    mock_proc.register_callback(f"/{pid}/task/{tid}/stat", task_cb)
    path = mock_proc.mount / str(pid) / "task" / str(tid) / "stat"

    tpp = ticks_per_poll(0.5)
    assert read_utime(path) == tpp
    assert read_utime(path) == tpp * 2
    assert read_utime(path) == tpp * 3

    util[0] = 1.0
    assert read_utime(path) == tpp * 3 + ticks_per_poll(1.0)


def test_host_stat_callback_never_exhausts(mock_proc: MockProc):
    """Replaces 40-element ``_high_host_sequence`` with unbounded callback."""
    state = {"user": 0, "idle": 1000, "first": True}

    def host_cb() -> bytes:
        if state["first"]:
            state["first"] = False
            out = host_stat(0, state["idle"])
            state["user"] = 10000
            return out
        out = host_stat(state["user"], state["idle"])
        state["user"] += 10000
        return out

    mock_proc.register_callback(Path("/stat"), host_cb)

    stat = mock_proc.mount / "stat"
    assert "cpu  0 0 0 1000" in stat.read_text()
    for _ in range(50):
        text = stat.read_text()
        assert text != ""  # sequence would return "" after 40
        assert "cpu  " in text


def test_threads_scale_via_callbacks(mock_proc: MockProc):
    """Adding/removing threads via callbacks (mirrors FakeBackend._sync_procfs)."""
    pid = 10000
    for i in range(2):
        tid = pid + i + 1
        mock_proc.register_callback(
            f"/{pid}/task/{tid}/stat",
            lambda t=tid: host_stat(t, 0),  # trivial content
        )
    assert (mock_proc.mount / str(pid) / "task" / str(pid + 1) / "stat").exists()
    assert (mock_proc.mount / str(pid) / "task" / str(pid + 2) / "stat").exists()

    mock_proc.unregister(f"/{pid}/task/{pid + 2}")
    assert not (mock_proc.mount / str(pid) / "task" / str(pid + 2) / "stat").exists()

    mock_proc.register_callback(
        f"/{pid}/task/{pid + 2}/stat", lambda: host_stat(999, 0)
    )
    assert (mock_proc.mount / str(pid) / "task" / str(pid + 2) / "stat").exists()
