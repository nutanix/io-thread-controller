# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Controller policy observed through a running daemon and the fake backend."""

import json
import re
import time

from conftest import POLL_S, _prepare, wait_for

HEALTHY_VM = "vm-a"
FAILED_VM = "vm-bad"

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_VM_RE = re.compile(r'\bvm="?([^"\s]+)"?')
_THR_RE = re.compile(r"\bthr=(\d+)\b")
_TRACKED_RE = re.compile(r"\btracked=(\d+)")
_ACTION_RE = re.compile(r'\baction="?([A-Za-z]+)"?')


def _plain(logs):
    """Drop SGR color codes so field matchers see ``tracked=1`` and ``status:``."""
    return _ANSI_RE.sub("", logs)


def _stat(user, idle):
    body = "cpu  %d 0 0 %d 0 0 0 0 0 0\n" % (user, idle)
    body += "cpu0 %d 0 0 %d 0 0 0 0 0 0\n" % (user, idle)
    body += "ctxt 1\nbtime 1\nprocesses 1\n"
    return body.encode("ascii")


def _high_host_sequence():
    """Baseline sample, then busy deltas that pin host utilisation near 1."""
    samples = [_stat(0, 1000)]
    idle = 1000
    user = 0
    for _ in range(40):
        user += 10000
        samples.append(_stat(user, idle))
    return samples


def _status_threads(logs, vm_id):
    found = None
    for line in _plain(logs).splitlines():
        if " status:" not in line or "aggregate" in line:
            continue
        vm = _VM_RE.search(line)
        thr = _THR_RE.search(line)
        if vm and thr and vm.group(1) == vm_id:
            found = int(thr.group(1))
    return found


def _tracked(logs):
    found = None
    for line in _plain(logs).splitlines():
        if "aggregate" not in line:
            continue
        match = _TRACKED_RE.search(line)
        if match:
            found = int(match.group(1))
    return found


def _engine_actions(logs):
    actions = []
    for line in _plain(logs).splitlines():
        if " engine:" not in line:
            continue
        vm = _VM_RE.search(line)
        action = _ACTION_RE.search(line)
        if vm and action:
            actions.append((vm.group(1), action.group(1)))
    return actions


def test_actuation_enforces_vcpu_cap(controller, fake_backend, run_threshold, fast_threshold):
    """A managed VM's pool stays within its vCPU count, and an unmanaged VM does not move."""
    _prepare(fake_backend, threads=3, util=0.95, vcpu=4)
    run_threshold(fast_threshold)

    wait_for(
        lambda: 4 in fake_backend.calls(),
        timeout=10,
        description="scale up to the vCPU count",
    )
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == [4], controller.logs()
    assert fake_backend.thread_count() == 4

    fake_backend.set_util(0.0)
    wait_for(
        lambda: fake_backend.calls()[-1:] == [3],
        timeout=10,
        description="scale down to 3",
    )
    assert fake_backend.thread_count() == 3

    controller.stop()
    controller.ownership_path.parent.mkdir(parents=True, exist_ok=True)
    controller.ownership_path.write_text(
        json.dumps({"managed_vms": [], "unmanaged_vms": [HEALTHY_VM]})
    )
    before = list(fake_backend.calls())
    controller.start()
    fake_backend.set_util(0.95)
    time.sleep(POLL_S * 4)
    fake_backend.set_util(0.0)
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == before, controller.logs()
    assert fake_backend.thread_count() == 3


def test_actuation_enforces_controller_bounds_and_host_ceiling(
    controller, fake_backend, mock_proc, run_threshold, fast_threshold
):
    """Controller min, max, host CPU ceiling, and cooldown gate ordinary scales."""
    _prepare(fake_backend, threads=2, util=0.95, vcpu=8)
    run_threshold(
        fast_threshold,
        min_thread_count=2,
        max_thread_count=3,
        cooldown_secs=30,
    )
    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale up to the controller maximum",
    )
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == [3], controller.logs()
    assert fake_backend.thread_count() == 3

    fake_backend.set_util(0.0)
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == [3], controller.logs()
    assert fake_backend.thread_count() == 3

    controller.stop()
    fake_backend.clear_calls()
    fake_backend.set_threads(4)
    fake_backend.set_util(0.0)
    controller(
        engine="threshold",
        engine_config=_fast_engine(),
        controller_overrides={
            "scale_poll_secs": POLL_S,
            "min_thread_count": 2,
            "max_thread_count": 8,
            "host_cpu_scale_up_ceiling_percent": 0,
            "cooldown_secs": 0,
        },
    )
    wait_for(
        lambda: fake_backend.calls() == [3, 2],
        timeout=10,
        description="scale down to the controller minimum",
    )
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == [3, 2], controller.logs()
    assert fake_backend.thread_count() == 2

    controller.stop()
    mock_proc.register_sequence("/stat", _high_host_sequence())
    fake_backend.clear_calls()
    _prepare(fake_backend, threads=1, util=0.95, vcpu=8)
    run_threshold(fast_threshold, host_cpu_scale_up_ceiling_percent=50)
    wait_for(
        lambda: fake_backend.calls() == [2],
        timeout=10,
        description="scale up before the host ceiling is known",
    )
    time.sleep(POLL_S * 4)
    assert fake_backend.calls() == [2], controller.logs()
    assert fake_backend.thread_count() == 2


def test_tick_refreshes_evaluates_and_drops_failed_instances(
    controller, fake_backend, run_threshold, fast_threshold
):
    """One poll refreshes a healthy VM and drops a VM whose snapshot fails."""
    _prepare(fake_backend, threads=4, util=0.95, vcpu=1)
    fake_backend.add_vm(
        FAILED_VM,
        thread_count=2,
        vcpu_count=4,
        per_thread_util=0.95,
        fail_snapshot=True,
    )
    run_threshold(fast_threshold)

    wait_for(
        lambda: _tracked(controller.logs()) == 1
        and _status_threads(controller.logs(), HEALTHY_VM) == fake_backend.thread_count(),
        timeout=10,
        description="healthy VM status after the failed VM is dropped",
    )
    assert fake_backend.calls(FAILED_VM) == [], controller.logs()
    actions = _engine_actions(controller.logs())
    assert actions, controller.logs()
    assert {vm for vm, _action in actions} == {HEALTHY_VM}, controller.logs()
