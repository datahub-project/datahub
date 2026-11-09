"""Opt-in loader: ``pytest -p batch_plan_dump`` from the smoke-test directory.

``-p utilities.batch_plan_dump`` is not importable at pytest preparse
(the smoke-test directory is not on ``sys.path`` yet). This module is.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utilities.batch_plan_dump import (  # noqa: E402
    pytest_addoption,
    pytest_collection_modifyitems,
)

__all__ = ["pytest_addoption", "pytest_collection_modifyitems"]
