from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    SnowflakeOpenflowSource,
)

MINIMAL_CONNECTION: Dict[str, Any] = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}

# Patched where the class is defined -- get_connection is a method on the
# class itself, so this affects every caller regardless of which module holds
# a reference to SnowflakeConnectionConfig.
GET_CONNECTION = (
    "datahub.ingestion.source.snowflake.snowflake_connection."
    "SnowflakeConnectionConfig.get_connection"
)


def _mock_connection(
    rows: Optional[List[Dict[str, Any]]] = None,
    query_error: Optional[Exception] = None,
) -> MagicMock:
    connection = MagicMock()
    if query_error is not None:
        connection.query.side_effect = query_error
    else:
        connection.query.return_value = rows if rows is not None else []
    return connection


@patch(GET_CONNECTION)
def test_connect_failure_is_reported_not_raised(mock_get_connection: MagicMock) -> None:
    mock_get_connection.side_effect = Exception("insufficient privileges")

    report = SnowflakeOpenflowSource.test_connection(MINIMAL_CONNECTION)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is False
    assert report.basic_connectivity.failure_reason == "insufficient privileges"
    # Never reached the probe: nothing to report a capability for.
    assert report.capability_report is None


@patch(GET_CONNECTION)
def test_visible_deployments_report_connectivity_and_capability_success(
    mock_get_connection: MagicMock,
) -> None:
    connection = _mock_connection(rows=[{"name": "dep1"}])
    mock_get_connection.return_value = connection

    report = SnowflakeOpenflowSource.test_connection(MINIMAL_CONNECTION)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    containers = report.capability_report[SourceCapability.CONTAINERS]
    assert containers.capable is True
    connection.close.assert_called_once()


@patch(GET_CONNECTION)
def test_zero_visible_deployments_is_a_capability_failure_naming_monitor(
    mock_get_connection: MagicMock,
) -> None:
    # The privilege-filtering case this method exists to catch: the connection
    # succeeds, but the role holds no MONITOR grant on any Openflow object, so
    # SHOW OPENFLOW DEPLOYMENTS returns exit-0 with zero rows.
    connection = _mock_connection(rows=[])
    mock_get_connection.return_value = connection

    report = SnowflakeOpenflowSource.test_connection(MINIMAL_CONNECTION)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    containers = report.capability_report[SourceCapability.CONTAINERS]
    assert containers.capable is False
    assert containers.failure_reason is not None
    assert "MONITOR" in containers.failure_reason
    connection.close.assert_called_once()


@patch(GET_CONNECTION)
def test_probe_error_is_reported_not_raised(mock_get_connection: MagicMock) -> None:
    # Distinct from the zero-rows case: the probe itself failed (a transient
    # error, a different permission gap), not merely returning nothing.
    connection = _mock_connection(query_error=Exception("session expired"))
    mock_get_connection.return_value = connection

    report = SnowflakeOpenflowSource.test_connection(MINIMAL_CONNECTION)

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable is True
    assert report.capability_report is not None
    containers = report.capability_report[SourceCapability.CONTAINERS]
    assert containers.capable is False
    assert containers.failure_reason == "session expired"
    connection.close.assert_called_once()
