# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

"""Fixtures for the component testing harness."""

import pytest
from harness.json_backend import JsonBackend


@pytest.fixture
def json_backend(tmp_path):
    """Create a new JsonBackend running at ``tmp_path``."""
    return JsonBackend(tmp_path)
