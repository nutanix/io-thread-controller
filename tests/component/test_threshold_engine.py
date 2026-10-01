# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Threshold engine decisions observed through a running daemon."""

import time

import pytest

from conftest import POLL_S, _prepare_healthy_vm, wait_for

BASELINE_IO = 10_000


@pytest.fixture
def validating_threshold():
    """Same thresholds, with a hold long enough to change I/O before the sample."""
    return {
        "scale_up_threshold_percent": 60,
        "scale_down_sustain_polls": 1,
        "max_scale_down_step": 1,
        "scale_up_min_gain_percent": 10,
        "scale_down_revert_drop_percent": 10,
        "scale_validation_sample_polls": 3,
    }


def _ticked(controller):
    """True once the first poll reports the healthy VM over D-Bus."""
    return controller.tracked() >= 1


def test_hold_below_scale_up_threshold(controller, fake_backend, run_threshold, fast_threshold):
    """Utilisation under the up threshold, and not low enough to shrink, holds."""
    _prepare_healthy_vm(fake_backend, threads=2, util=0.5)
    run_threshold(fast_threshold)

    wait_for(
        lambda: _ticked(controller),
        timeout=10,
        description="first poll classified the held VM",
    )
    assert fake_backend.calls() == [], controller.logs()
    assert fake_backend.thread_count() == 2


def test_scale_up_one_thread(fake_backend, run_threshold, fast_threshold):
    """A saturated worker grows by one and stops at the controller maximum."""
    _prepare_healthy_vm(fake_backend, threads=1, util=0.95)
    run_threshold(fast_threshold, max_thread_count=2)

    wait_for(
        lambda: fake_backend.calls() == [2],
        timeout=10,
        description="scale up by one thread",
    )
    assert fake_backend.thread_count() == 2


def test_scale_down_one_thread(fake_backend, run_threshold, fast_threshold):
    """Low utilisation must last for the sustain window, then drops by one step."""
    _prepare_healthy_vm(fake_backend, threads=4, util=0.0)
    run_threshold(fast_threshold)

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale down by one thread",
    )
    assert fake_backend.thread_count() == 3


def test_scale_down_step_limit(controller, fake_backend, run_threshold, fast_threshold):
    """An idle pool sheds one thread per successful scale, not the whole surplus."""
    _prepare_healthy_vm(fake_backend, threads=6, util=0.0)
    fast_threshold['max_scale_down_step'] = 1
    run_threshold(fast_threshold)

    wait_for(
        lambda: fake_backend.calls() == [5, 4, 3, 2, 1],
        timeout=10,
        description="step down to the controller minimum",
    )
    assert fake_backend.thread_count() == 1, controller.logs()


def test_scale_up_reverts_without_io_gain(
     fake_backend, run_threshold, validating_threshold
):
    """A scale-up that does not gain the required I/O is restored and then held."""
    _prepare_healthy_vm(fake_backend, threads=2, util=0.95, io=BASELINE_IO)
    run_threshold(validating_threshold)

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale up before validation",
    )

    # Keep util high at 0.95; any thread count drops are reverts.
    wait_for(
        lambda: fake_backend.calls() == [3, 2],
        timeout=10,
        description="revert the scale-up",
    )

    assert fake_backend.thread_count() == 2


def test_scale_up_kept_when_io_gains(
    fake_backend, run_threshold, validating_threshold
):
    """Enough extra I/O keeps the new thread and allows the next scale-up."""
    _prepare_healthy_vm(fake_backend, threads=2, util=0.95, io=BASELINE_IO)
    run_threshold(validating_threshold)

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale up before validation",
    )

    fake_backend.set_io_counts(BASELINE_IO * 2, 0, 0)
    wait_for(
        lambda: len(fake_backend.calls()) == 2,
        timeout=10,
        description="next scale-up after validation",
    )
    assert fake_backend.calls() == [3, 4]

def test_scale_down_kept_when_io_keeps_rising(
    controller, fake_backend, run_threshold, validating_threshold
):
    """A scale-down stays when cumulative I/O keeps increasing.

    QEMU ``query-blockstats`` counters only grow. A lower total is not a
    regression this backend can report, so validation must leave the new
    count in place while the counter climbs.
    """
    _prepare_healthy_vm(fake_backend, threads=4, util=0.0, io=BASELINE_IO)
    run_threshold(validating_threshold)

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale down before validation",
    )
    fake_backend.set_util(0.5)
    fake_backend.set_io_counts(BASELINE_IO * 2, 0, 0)
    time.sleep(POLL_S * 5)
    assert fake_backend.calls() == [3], controller.logs()
    assert fake_backend.thread_count() == 3

@pytest.mark.xfail(reason="at some point, prev_io_count_total becomes baseline_iops", strict=True)
def test_scale_up_continues_with_increasing_iops(
    controller, fake_backend, run_threshold, validating_threshold
):
    """
    As long as threads give a constant amount of IOPS, i.e. not saturating,
    there is no need to revert.
    """
    io = 100_000
    iops = 500
    max_seen = 0
    _prepare_healthy_vm(fake_backend, threads=1, util=0.95, io=io, vcpu=1000)
    run_threshold(validating_threshold, max_thread_count=1000)

    for _ in range(1,100):
        wait_for(
            lambda: len(fake_backend.calls()) > 0,
            timeout=10,
            description="scale up\n" + controller.logs()
        )
        for x in fake_backend.calls():
            assert x > max_seen, controller.logs()
            max_seen = x
        max_seen = fake_backend.calls()[-1]
        iops *= 1.05 * validating_threshold['scale_up_min_gain_percent']/100
        io += iops
        fake_backend.set_io_counts(io, 0, 0)
        fake_backend.clear_calls()


@pytest.mark.xfail(reason="validation compares the lifetime counter instead of the IOPS rate", strict=True)
def test_scale_down_reverts_when_iops_rate_falls(
    controller, fake_backend, run_threshold, validating_threshold
):
    """A collapsed IOPS rate reverts a scale-down even though the counter rises.

    QEMU blockstats are lifetime totals. They do not fall when the guest
    slows down; the loss shows up as a smaller delta between polls. This
    pool's counter keeps climbing, but the climb slows from about 100k IOPS
    to about 1k IOPS, well past the 10% allowance. The previous thread count
    should be restored.

    The engine multiplies the lifetime total by that percentage instead of
    comparing the two rates, so a still-growing counter looks like a gain
    and the scale-down is kept.
    """
    # 20_000 ops before each 0.2s poll is 100_000 IOPS. 200 ops/poll is 1_000.
    lifetime = 10_000_000
    fast_step = 20_000
    slow_step = 200
    config = dict(validating_threshold)
    config["scale_down_sustain_polls"] = 5
    io = lifetime
    _prepare_healthy_vm(fake_backend, threads=4, util=0.0, io=io)
    run_threshold(config)

    deadline = time.monotonic() + 10
    while not fake_backend.calls() and time.monotonic() < deadline:
        io += fast_step
        fake_backend.set_io_counts(io, 0, 0)
        time.sleep(POLL_S)
    assert fake_backend.calls() == [3], controller.logs()
    count_at_scale = io

    fake_backend.set_util(0.5)
    settled = time.monotonic() + POLL_S * 8
    while time.monotonic() < settled:
        io += slow_step
        fake_backend.set_io_counts(io, 0, 0)
        time.sleep(POLL_S)
    assert io > count_at_scale
    assert fake_backend.calls() == [3, 4], controller.logs()
    assert fake_backend.thread_count() == 4


@pytest.mark.xfail(reason="validation compares the lifetime counter instead of the IOPS rate", strict=True)
def test_scale_up_kept_when_iops_rate_rises(
    controller, fake_backend, run_threshold, validating_threshold
):
    """A doubled IOPS rate keeps a scale-up even though the lifetime counter barely moves.

    The counter starts at 10_000_000. After the pool grows, completed I/O
    rises from about 100k IOPS to about 200k IOPS, twice the 10% allowance.
    That extra work is only a few hundred thousand operations, far under 10%
    of the lifetime total. The engine treats the scale-up as a failure and
    restores the old count.
    """
    # 20_000 ops before each 0.2s poll is 100_000 IOPS. 40_000 ops/poll is 200_000.
    lifetime = 10_000_000
    baseline_step = 20_000
    gained_step = 40_000
    io = lifetime
    _prepare_healthy_vm(fake_backend, threads=2, util=0.5, io=io)
    run_threshold(validating_threshold)

    wait_for(
        lambda: _ticked(controller),
        timeout=10,
        description="first poll before the scale-up",
    )
    baseline_until = time.monotonic() + POLL_S * 4
    while time.monotonic() < baseline_until:
        io += baseline_step
        fake_backend.set_io_counts(io, 0, 0)
        time.sleep(POLL_S)
    assert fake_backend.calls() == [], controller.logs()

    fake_backend.set_util(0.95)
    deadline = time.monotonic() + 10
    while fake_backend.calls() != [3] and time.monotonic() < deadline:
        io += baseline_step
        fake_backend.set_io_counts(io, 0, 0)
        time.sleep(POLL_S)
    assert fake_backend.calls() == [3], controller.logs()
    count_at_scale = io

    settled = time.monotonic() + POLL_S * 8
    while time.monotonic() < settled:
        io += gained_step
        fake_backend.set_io_counts(io, 0, 0)
        time.sleep(POLL_S)
    assert count_at_scale < io < count_at_scale + count_at_scale // 10
    calls = fake_backend.calls()
    assert calls[0] == 3 and calls[1] == 4, controller.logs()
    assert 2 not in calls, controller.logs()


def test_scale_down_kept_when_io_holds(
    controller, fake_backend, run_threshold, validating_threshold
):
    """A scale-down whose I/O stays inside the drop allowance is left in place."""
    _prepare_healthy_vm(fake_backend, threads=4, util=0.0, io=BASELINE_IO)
    run_threshold(validating_threshold)

    wait_for(
        lambda: fake_backend.calls() == [3],
        timeout=10,
        description="scale down before validation",
    )
    fake_backend.set_util(0.5)
    assert fake_backend.calls() == [3], controller.logs()
    assert fake_backend.thread_count() == 3
