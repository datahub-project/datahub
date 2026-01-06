"""Tests for the SmartSqlAssertionClient."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.smart_sql_assertion import SmartSqlAssertion
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    InferenceSensitivity,
)
from acryl_datahub_cloud.sdk.assertions_client import AssertionsClient
from acryl_datahub_cloud.sdk.errors import SDKUsageError
from datahub.metadata.urns import AssertionUrn, DatasetUrn
from tests.sdk.assertions.conftest import (
    DEFAULT_EXISTING_DATASET_URNS,
    StubDataHubClient,
)

FROZEN_TIME = "2021-01-15 00:00:00"


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_creates_new_assertion(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that sync_smart_sql_assertion creates a new assertion when urn is None."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        display_name="Test Smart SQL Assertion",
        sensitivity="medium",
        enabled=True,
    )

    # Verify upsert was called twice (assertion + monitor)
    assert mock_upsert.call_count == 2
    assert isinstance(result, SmartSqlAssertion)
    assert result.statement == "SELECT COUNT(*) FROM test_table"
    assert result.display_name == "Test Smart SQL Assertion"


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_requires_statement_for_creation(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that statement is required when creating a new assertion."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub

    with pytest.raises(SDKUsageError, match="statement"):
        client.sync_smart_sql_assertion(
            dataset_urn=any_dataset_urn,
            statement=None,  # No statement provided
            display_name="Test Smart SQL Assertion",
        )


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_sensitivity_levels(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that different sensitivity levels work correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    # Test with string sensitivity
    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        sensitivity="high",
    )
    assert result.sensitivity == InferenceSensitivity.HIGH

    # Reset mock
    mock_upsert.reset_mock()

    # Test with enum sensitivity
    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        sensitivity=InferenceSensitivity.LOW,
    )
    assert result.sensitivity == InferenceSensitivity.LOW


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_schedule(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that custom schedule is applied correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        schedule="0 */2 * * *",  # Every 2 hours
    )

    assert result.schedule.cron == "0 */2 * * *"


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_exclusion_windows(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that exclusion windows are applied correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    exclusion_window = {
        "start": datetime(2025, 1, 1, 0, 0, 0),
        "end": datetime(2025, 1, 2, 0, 0, 0),
    }

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        exclusion_windows=[exclusion_window],
    )

    assert len(result.exclusion_windows) == 1


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_tags(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that tags are applied correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        tags=["automated", "smart_sql"],
    )

    tag_strs = [str(tag) for tag in result.tags]
    assert "urn:li:tag:automated" in tag_strs
    assert "urn:li:tag:smart_sql" in tag_strs


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_update_existing_assertion(
    smart_sql_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that updating an existing assertion works correctly."""
    client = AssertionsClient(smart_sql_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    smart_sql_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        display_name="Updated Smart SQL Assertion",
        # Statement not provided - should use existing
    )

    assert mock_upsert.call_count == 2
    assert isinstance(result, SmartSqlAssertion)
    # Should use existing statement
    assert "SELECT COUNT(*)" in result.statement


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_preserves_existing_values_when_not_provided(
    smart_sql_stub_datahub_client: StubDataHubClient,
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that existing values are preserved when not provided in update."""
    client = AssertionsClient(smart_sql_stub_datahub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    smart_sql_stub_datahub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        # Only update enabled status, preserve other values
        enabled=False,
    )

    # Existing values should be preserved
    assert result.statement == "SELECT COUNT(*) FROM test_table WHERE status = 'active'"


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_raises_error_on_dataset_urn_mismatch(
    smart_sql_stub_datahub_client: StubDataHubClient,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that dataset URN mismatch raises an error."""
    client = AssertionsClient(smart_sql_stub_datahub_client)  # type: ignore[arg-type]  # Stub

    with pytest.raises(SDKUsageError, match="Dataset URN mismatch"):
        client.sync_smart_sql_assertion(
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:test,different_dataset,PROD)",
            urn=any_assertion_urn,
        )


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_training_data_lookback_days(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that training data lookback days is applied correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        training_data_lookback_days=90,
    )

    assert result.training_data_lookback_days == 90


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_with_incident_behavior(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that incident behavior is applied correctly."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        incident_behavior=["raise_on_fail", "resolve_on_pass"],
    )

    assert AssertionIncidentBehavior.RAISE_ON_FAIL in result.incident_behavior
    assert AssertionIncidentBehavior.RESOLVE_ON_PASS in result.incident_behavior


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_disabled(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that enabled=False creates an inactive assertion."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        enabled=False,
    )

    assert result.mode == AssertionMode.INACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_defaults_to_enabled(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that assertion is enabled by default."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
    )

    assert result.mode == AssertionMode.ACTIVE


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_creates_assertion_with_urn_when_not_found(
    any_dataset_urn: DatasetUrn,
    any_assertion_urn: AssertionUrn,
) -> None:
    """Test that a new assertion is created when the specified URN doesn't exist."""
    stub_client = StubDataHubClient(existing_urns=DEFAULT_EXISTING_DATASET_URNS)
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        urn=any_assertion_urn,
        statement="SELECT COUNT(*) FROM test_table",
    )

    assert mock_upsert.call_count == 2
    assert isinstance(result, SmartSqlAssertion)


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_skip_dataset_exists_check(
    any_dataset_urn: DatasetUrn,
) -> None:
    """Test that skip_dataset_exists_check allows creating assertions for non-existent datasets."""
    # Create a client with NO existing dataset URNs
    stub_client = StubDataHubClient(existing_urns=set())
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub
    mock_upsert = MagicMock()
    stub_client.entities.upsert = mock_upsert  # type: ignore[method-assign]

    # This should succeed with skip_dataset_exists_check=True
    result = client.sync_smart_sql_assertion(
        dataset_urn=any_dataset_urn,
        statement="SELECT COUNT(*) FROM test_table",
        skip_dataset_exists_check=True,
    )

    assert isinstance(result, SmartSqlAssertion)


@freeze_time(FROZEN_TIME)
def test_sync_smart_sql_assertion_fails_dataset_exists_check() -> None:
    """Test that assertion creation fails when dataset doesn't exist and check is enabled."""
    # Use a dataset URN that is NOT in DEFAULT_EXISTING_DATASET_URNS
    nonexistent_dataset_urn = DatasetUrn.from_string(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,nonexistent_table,PROD)"
    )
    stub_client = StubDataHubClient(existing_urns=set())
    client = AssertionsClient(stub_client)  # type: ignore[arg-type]  # Stub

    with pytest.raises(SDKUsageError, match="does not exist"):
        client.sync_smart_sql_assertion(
            dataset_urn=nonexistent_dataset_urn,
            statement="SELECT COUNT(*) FROM test_table",
            skip_dataset_exists_check=False,
        )
