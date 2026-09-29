"""
Placeholder for future Docker-backed pgQueue integration tests.

PgQueue DDL requires ``pg_partman`` (see ``datahub-upgrade/.../sqlsetup/pgqueue``).
Unit coverage lives under ``tests/unit/pgqueue/``.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skip(
    reason=(
        "Automated pgQueue IT requires Postgres with pg_partman + SqlSetup DDL; "
        "not wired in CI yet."
    )
)
def test_pgqueue_live_database_placeholder() -> None:
    raise AssertionError("unreachable")
