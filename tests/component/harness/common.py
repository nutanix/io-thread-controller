# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any


def physical(root: Path, logical: str) -> Path:
    """Prefix an absolute path with the test-specific root."""
    return root / logical.lstrip("/")


def write_json(path: Path, payload: Any) -> None:
    """Write ``payload`` as JSON to ``path``, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def wait_for(predicate: Callable[[], bool], timeout: int, description: str) -> None:
    """Poll ``predicate`` until it is true or ``timeout`` seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")
