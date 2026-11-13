# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Controller policy observed through a running daemon and the fake backend."""

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from harness.common import POLL_S, wait_for
from harness.controller import Controller
from harness.fake_backend import FakeBackend
from harness.mockfs import MockProc

HEALTHY_VM = "vm-a"
FAILED_VM = "vm-bad"


def _stat(user: int, idle: int) -> bytes:
    """Render a minimal ``/proc/stat`` with the given user and idle jiffies."""
    body = f"cpu  {user} 0 0 {idle} 0 0 0 0 0 0\n"
    body += f"cpu0 {user} 0 0 {idle} 0 0 0 0 0 0\n"
    body += "ctxt 1\nbtime 1\nprocesses 1\n"
    return body.encode("ascii")


def _high_host_sequence() -> list[bytes]:
    """Baseline sample, then busy deltas that pin host utilisation near 1."""
    samples = [_stat(0, 1000)]
    idle = 1000
    user = 0
    for _ in range(40):
        user += 10000
        samples.append(_stat(user, idle))
    return samples


def test_actuation_enforces_vcpu_cap(
    controller: Controller,
    fake_backend: FakeBackend,
    run_controller: Callable,
    fast_threshold: Any,
):
    """Test that a managed VM's pool stays within its vCPU count."""
    fake_backend.prepare_healthy_vm(threads=3, util=0.95, vcpu=4)
    run_controller(fast_threshold)

    wait_for(
        lambda: fake_backend.calls() == [4],
        timeout=10,
        description="scale up to the vCPU count",
    )
    assert fake_backend.calls() == [4], controller.logs()
    assert fake_backend.thread_count() == 4

    # Keep util at 0.95 and ensure it doesn't scale to 5.
    #
    # TODO make a fake engine which this test can use to see if the controller
    # really does enforce this. Currently hard to distinguish between engine and
    # controller ensuring not 5.
    time.sleep(POLL_S * 5)
    assert 5 not in fake_backend.calls()


def test_actuation_enforces_thread_maximum(
    controller: Controller,
    fake_backend: FakeBackend,
    run_controller: Callable,
    fast_threshold: Any,
):
    """Test that the controller configured maximum prevents further scale-up."""
    fake_backend.prepare_healthy_vm(threads=2, util=0.95, vcpu=8)
    run_controller(
        fast_threshold,
        min_thread_count=2,
        max_thread_count=3,
        cooldown_secs=0,
    )

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale up to the controller maximum",
    )
    assert fake_backend.thread_count() == 3

    fake_backend.clear_calls()
    time.sleep(POLL_S * 5)
    assert fake_backend.calls() == [], controller.logs()
    assert fake_backend.thread_count() == 3


def test_actuation_enforces_cooldown(
    controller: Controller,
    fake_backend: FakeBackend,
    run_controller: Callable,
    fast_threshold: Any,
):
    """
    Test that controller's cooldown prevents scale-down immediately after
    a scale-up.
    """
    fake_backend.prepare_healthy_vm(threads=2, util=0.95, vcpu=8)
    run_controller(
        fast_threshold,
        min_thread_count=2,
        max_thread_count=3,
        cooldown_secs=30,
    )
    wait_for(
        lambda: len(fake_backend.calls()) > 0,
        timeout=10,
        description="scale action",
    )

    # Drop below the threshold and ensure there are no scale actions during
    # the cooldown.
    fake_backend.clear_calls()
    fake_backend.set_util(0.0)
    time.sleep(POLL_S * 5)
    assert fake_backend.calls() == [], controller.logs()
    assert fake_backend.thread_count() == 3


def test_actuation_enforces_thread_minimum(
    controller: Controller,
    fake_backend: FakeBackend,
    run_controller: Callable,
    fast_threshold: Any,
):
    """
    Test that the controller prevents scaling below the configured minimum.
    """
    fake_backend.prepare_healthy_vm(threads=4, util=0.00, vcpu=8)
    run_controller(
        fast_threshold,
        min_thread_count=2,
        max_thread_count=3,
        cooldown_secs=0,
    )
    wait_for(
        lambda: fake_backend.calls() == [3, 2],
        timeout=10,
        description="scale down to the controller minimum",
    )
    assert fake_backend.thread_count() == 2

    # Stay below the threshold and ensure there isn't another scale action
    fake_backend.clear_calls()
    time.sleep(POLL_S * 5)
    assert fake_backend.calls() == [], controller.logs()
    assert fake_backend.thread_count() == 2


def test_actuation_enforces_host_cpu_scale_up_ceiling(
    controller: Controller,
    fake_backend: FakeBackend,
    mock_proc: MockProc,
    run_controller: Callable,
    fast_threshold: Any,
):
    """
    Test that high host CPU usage prevents another threshold-engine scale-up.
    """
    mock_proc.register_sequence(Path("/stat"), _high_host_sequence())
    fake_backend.prepare_healthy_vm(threads=1, util=0.95, vcpu=8)
    run_controller(
        fast_threshold, host_cpu_scale_up_ceiling_percent=50, cooldown_secs=0
    )
    wait_for(
        lambda: fake_backend.calls() == [2],
        timeout=10,
        description="first scale-up before the host ceiling is known",
    )
    fake_backend.clear_calls()
    time.sleep(POLL_S * 5)
    assert fake_backend.calls() == [], controller.logs()
    assert fake_backend.thread_count() == 2


def test_tick_refreshes_evaluates_and_drops_failed_instances(
    controller: Controller,
    fake_backend: FakeBackend,
    run_controller: Callable,
    fast_threshold: Any,
):
    """
    Test that one poll refreshes a healthy VM and drops a VM whose snapshot
    fails.
    """
    fake_backend.prepare_healthy_vm(threads=4, util=0.95, vcpu=1)
    fake_backend.add_vm(
        FAILED_VM,
        thread_count=2,
        vcpu_count=4,
        per_thread_util=0.95,
        fail_snapshot=True,
    )
    run_controller(fast_threshold)

    wait_for(
        lambda: controller.thread_count(HEALTHY_VM) == fake_backend.thread_count(),
        timeout=10,
        description="healthy VM actuated while the failed VM is dropped",
    )

    assert fake_backend.calls(FAILED_VM) == [], controller.logs()
