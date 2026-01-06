"""Tests for the _SmartSqlAssertionInput class."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.sdk.assertion_input.smart_sql_assertion_input import (
    _SmartSqlAssertionInput,
)
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, CorpUserUrn, DatasetUrn


@pytest.fixture
def mock_entity_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def dataset_urn() -> DatasetUrn:
    return DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
    )


@pytest.fixture
def corp_user_urn() -> CorpUserUrn:
    return CorpUserUrn.from_string("urn:li:corpuser:test_user")


@pytest.fixture
def fixed_datetime() -> datetime:
    return datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def test_smart_sql_assertion_input_creates_monitor_info_with_adjustment_settings(
    mock_entity_client: MagicMock,
    dataset_urn: DatasetUrn,
    corp_user_urn: CorpUserUrn,
    fixed_datetime: datetime,
) -> None:
    """Test that monitor info includes adjustment settings for AI inference."""
    input_obj = _SmartSqlAssertionInput(
        dataset_urn=dataset_urn,
        entity_client=mock_entity_client,
        statement="SELECT COUNT(*) FROM test_table",
        sensitivity="high",
        training_data_lookback_days=90,
        created_by=corp_user_urn,
        created_at=fixed_datetime,
        updated_by=corp_user_urn,
        updated_at=fixed_datetime,
    )

    assertion_urn = AssertionUrn("urn:li:assertion:test")
    status = models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE)
    schedule = models.CronScheduleClass(cron="0 */6 * * *", timezone="UTC")

    monitor_info = input_obj._create_monitor_info(assertion_urn, status, schedule)

    assert monitor_info.assertionMonitor is not None
    assert monitor_info.assertionMonitor.settings is not None
    assert monitor_info.assertionMonitor.settings.adjustmentSettings is not None

    adjustment_settings = monitor_info.assertionMonitor.settings.adjustmentSettings
    assert adjustment_settings.trainingDataLookbackWindowDays == 90
    assert adjustment_settings.sensitivity is not None
    assert adjustment_settings.sensitivity.level == 10  # HIGH = 10
