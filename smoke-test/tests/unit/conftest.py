"""CPU-only pytest tree. Do not import live GMS fixtures here."""

from __future__ import annotations

import pytest
from _pytest.nodes import Item


def pytest_collection_modifyitems(items: list[Item]) -> None:
    bad = [item.nodeid for item in items if not item.nodeid.startswith("tests/unit/")]
    if bad:
        raise pytest.UsageError(
            "tests/unit collection escaped the unit tree: " + ", ".join(bad[:5])
        )
