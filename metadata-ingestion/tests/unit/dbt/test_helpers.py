from datetime import datetime, timezone
from unittest import mock

from datahub.ingestion.source.dbt.dbt_tests import (
    DBTFreshnessCriteria,
    DBTFreshnessInfo,
)


def create_mock_dbt_node(table_name: str) -> mock.Mock:
    """Helper to create a mock DBTNode with a name attribute."""
    node = mock.Mock()
    node.name = table_name
    return node


def make_freshness_info() -> DBTFreshnessInfo:
    return DBTFreshnessInfo(
        invocation_id="test-123",
        status="pass",
        max_loaded_at=datetime(2026, 1, 13, 10, 0, 0, tzinfo=timezone.utc),
        snapshotted_at=datetime(2026, 1, 13, 12, 0, 0, tzinfo=timezone.utc),
        max_loaded_at_time_ago_in_s=7200.0,
        warn_after=DBTFreshnessCriteria(count=12, period="hour"),
        error_after=None,
    )
