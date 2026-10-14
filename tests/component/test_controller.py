# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Controller policy observed through a running daemon and the fake backend."""

import time

from conftest import POLL_S, _prepare_healthy_vm, wait_for

HEALTHY_VM = "vm-a"
FAILED_VM = "vm-bad"

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


def test_actuation_enforces_vcpu_cap(controller, fake_backend, run_threshold, fast_threshold):
    """A managed VM's pool stays within its vCPU count."""
    _prepare_healthy_vm(fake_backend, threads=3, util=0.95, vcpu=4)
    run_threshold(fast_threshold)

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
    controller, fake_backend, run_threshold, fast_threshold
):
    """Controller maximum prevents further scale-up."""
    _prepare_healthy_vm(fake_backend, threads=2, util=0.95, vcpu=8)
    run_threshold(
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
    controller, fake_backend, run_threshold, fast_threshold
):
    """Cooldown prevents scale-down immediately after a scale-up."""
    _prepare_healthy_vm(fake_backend, threads=2, util=0.95, vcpu=8)
    run_threshold(
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
    controller, fake_backend, run_threshold, fast_threshold
):
    """Controller minimum prevents scaling actions."""
    _prepare_healthy_vm(fake_backend, threads=4, util=0.00, vcpu=8)
    run_threshold(
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
    controller, fake_backend, mock_proc, run_threshold, fast_threshold
):
    """High host CPU prevents another threshold-engine scale-up."""
    mock_proc.register_sequence("/stat", _high_host_sequence())
    _prepare_healthy_vm(fake_backend, threads=1, util=0.95, vcpu=8)
    run_threshold(
        fast_threshold,
        host_cpu_scale_up_ceiling_percent=50,
        cooldown_secs=0
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
    controller, fake_backend, run_threshold, fast_threshold
):
    """One poll refreshes a healthy VM and drops a VM whose snapshot fails."""
    _prepare_healthy_vm(fake_backend, threads=4, util=0.95, vcpu=1)
    fake_backend.add_vm(
        FAILED_VM,
        thread_count=2,
        vcpu_count=4,
        per_thread_util=0.95,
        fail_snapshot=True,
    )
    run_threshold(fast_threshold)

    wait_for(
        lambda: controller.tracked() == 1
        and controller.thread_count(HEALTHY_VM) == fake_backend.thread_count(),
        timeout=10,
        description="healthy VM status after the failed VM is dropped",
    )

    stats = controller.stats()
    assert stats is not None
    vms = {vm["vm"]: vm for vm in stats["vms"]}
    assert vms[HEALTHY_VM]["alive"] is True
    assert vms[HEALTHY_VM]["thread_count"] == fake_backend.thread_count()
    assert vms[FAILED_VM]["alive"] is False
    assert fake_backend.calls(FAILED_VM) == [], controller.logs()
