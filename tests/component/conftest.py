# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Fixtures for the component testing harness."""

import pytest
from harness.fake_backend import FakeBackend


@pytest.fixture
def fake_backend(tmp_path):
    """Create a new FakeBackend running at ``tmp_path``."""
    return FakeBackend(tmp_path)
