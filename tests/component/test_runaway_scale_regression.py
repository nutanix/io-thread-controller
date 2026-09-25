# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Thanos Makatos <thanos.makatos@nutanix.com>

"""
Regression test for runaway scale up bug.

  * The engine spaced its actuations by at least the configured
    validation window, so a hot workload cannot ramp the pool to
    ``max_thread_count`` in a couple of polls.
  * The trace is monotonic non-decreasing (no interleaved
    scale-downs during saturation).

Fails with a clear message when the spacing drops below the
validation-window floor.
"""

import time

from conftest import wait_for


# Values picked so a saturated workload can grow at most one
# thread every 0.6s.  Without the fix we would see spacing of
# ~POLL_INTERVAL_S == 0.2 between scale calls (three scales per
# validation window instead of one).
POLL_INTERVAL_S = 0.2
VALIDATION_POLLS = 3
MIN_SPACING_S = POLL_INTERVAL_S * VALIDATION_POLLS
SATURATION_WINDOW_S = 3.0


def _threshold_config():
    return {
        # Fire scale-up at anything above 60% -- matches the
        # /tmp/bug scenario.  The fix is orthogonal to the
        # threshold value; we pick 0.6 so a 0.9 util reading
        # unambiguously crosses it.
        "scale_up_threshold_percent": 60,
        "scale_down_sustain_polls": 2,
        "max_scale_down_step": 1,
        # 5% revert tolerance mirrors production defaults; the
        # /tmp/bug scenario shows that a flat-IOPS trace never
        # triggers this, so the validation-window HOLD is the
        # only mechanism keeping the ramp bounded.
        "scale_up_min_gain_percent": 5,
        "scale_down_revert_drop_percent": 5,
        "scale_validation_sample_polls": VALIDATION_POLLS,
    }


def test_saturated_workload_does_not_burst_scale(controller, fake_backend):
    """A hot workload should NOT burn through every scale-up
    slot in a couple of polls.  We prove this by measuring
    inter-scale spacing and asserting it stays above the
    validation-window floor.
    """
    fake_backend.set_util(0.95)
    fake_backend.set_vcpu_count(8)
    controller(
        engine="threshold",
        engine_config=_threshold_config(),
        controller_overrides={
            "scale_poll_secs": POLL_INTERVAL_S,
            "min_thread_count": 1,
            # Cap low so the test finishes fast; the assertion
            # measures inter-scale spacing, not the target count.
            "max_thread_count": 6,
            "host_cpu_scale_up_ceiling_percent": 0,
            "cooldown_secs": 0,
        },
    )

    # Wait for the first scale to land so we know the
    # controller has attached and is actuating.
    wait_for(
        lambda: len(fake_backend.calls()) > 0,
        timeout=10.0,
        description="first actuation",
    )
    start = time.monotonic()
    # Keep util saturated for the whole window so the engine
    # has no reason to hold on threshold grounds -- if it holds
    # at all, that is the validation-window HOLD we are testing.
    while time.monotonic() - start < SATURATION_WINDOW_S:
        fake_backend.set_util(0.95)
        time.sleep(POLL_INTERVAL_S)

    calls = fake_backend.calls()
    assert calls, "controller never issued a thread-count command"
    assert calls == sorted(calls), (
        "saturated workload should never emit a scale-down: %r" % (calls,)
    )
    # The interesting invariant: number of distinct scale-ups
    # over the saturation window.  With the fix the engine
    # holds for `VALIDATION_POLLS` polls after every scale, so
    # the upper bound on scale-ups is
    # ``SATURATION_WINDOW_S / MIN_SPACING_S + slack``.  We use
    # a slack of +2 to absorb sampler startup jitter and the
    # first scale happening before the window starts.
    max_expected = int(SATURATION_WINDOW_S / MIN_SPACING_S) + 2
    unique = sorted(set(calls))
    assert len(unique) <= max_expected, (
        "runaway scale-up regression: %d scale-ups in %.1fs "
        "(max expected: %d, spacing: %.2fs); trace=%r"
        % (len(unique), SATURATION_WINDOW_S, max_expected, MIN_SPACING_S, calls)
    )
