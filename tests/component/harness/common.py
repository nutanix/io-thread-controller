# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import json
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
POLL_S = 0.2


def wait_for(predicate, timeout, description):
    """Poll ``predicate`` until it is true or ``timeout`` seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for %s" % (description,))


def physical(root, logical):
    """Prefix an absolute path with the test-specific root."""
    return root / logical.lstrip("/")


def write_json(path, payload):
    """Write ``payload`` as JSON to ``path``, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def target_dir():
    """Return cargo's target directory for this workspace."""
    raw = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps", "--offline"],
        cwd=REPO_ROOT,
        text=True,
    )
    return Path(json.loads(raw)["target_directory"])


def built_binary(name):
    """Return the path of a debug binary, failing if it has not been built."""
    binary = target_dir() / "debug" / name
    if not binary.exists():
        raise RuntimeError("%s is not built at %s" % (name, binary))
    return binary
