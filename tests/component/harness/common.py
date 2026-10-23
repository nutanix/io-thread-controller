# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import json
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
POLL_S = 1


def wait_for(predicate: Callable[[], bool], timeout: int, description: str) -> None:
    """Poll ``predicate`` until it is true or ``timeout`` seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {description}")


def physical(root: Path, logical: str) -> Path:
    """Prefix an absolute path with the test-specific root."""
    return root / logical.lstrip("/")


def write_json(path: Path, payload: Any) -> None:
    """Write ``payload`` as JSON to ``path``, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def target_dir() -> Path:
    """Return cargo's target directory for this workspace."""
    raw = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--offline"],
        cwd=REPO_ROOT,
        text=True,
    )
    return Path(json.loads(raw)["target_directory"])


def built_binary(name: str) -> Path:
    """Return the path of a debug binary, failing if it has not been built."""
    binary = target_dir() / "debug" / name
    if not binary.exists():
        raise RuntimeError(f"{name} is not built at {binary}")
    return binary
