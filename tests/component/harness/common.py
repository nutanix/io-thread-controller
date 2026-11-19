# SPDX-License-Identifier: BSD-3-Clause
# Copyright (c) 2026 Nutanix, Inc. All rights reserved.
#
# Author: Leonardo Forchini <leonardo.forchini@nutanix.com>

import json
from pathlib import Path
from typing import Any


def physical(root: Path, logical: str) -> Path:
    """Prefix an absolute path with the test-specific root."""
    return root / logical.lstrip("/")


def write_json(path: Path, payload: Any) -> None:
    """Write ``payload`` as JSON to ``path``, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
