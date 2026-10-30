"""How the connector reaches Databricks: the test-connection capability report
over a mocked WorkspaceClient, and the auth the hive_metastore SQLAlchemy
connection is built with."""

from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.catalog import CatalogInfo

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.unity.connection_test import UnityCatalogConnectionTest
from datahub.ingestion.source.unity.source import UnityCatalogSource
from tests.unit.unity.conftest import WAREHOUSE_ID, unity_config

# --- UnityCatalogConnectionTest --------------------------------------------

ANALYZE_PROFILING: Dict[str, Any] = {
    "method": "analyze",
    "enabled": True,
    "warehouse_id": WAREHOUSE_ID,
}


def _query_history(query_text: str) -> Dict[str, Any]:
    return {
        "res": [
            {
                "query_id": "q1",
                "query_text": query_text,
                "query_start_time_ms": 1700000000000,
                "query_end_time_ms": 1700000001000,
            }
        ]
    }


def _reachable(ws: MagicMock, query_text: Optional[str] = "SELECT 1") -> MagicMock:
    """One catalog, and one query in history (none when query_text is None)."""
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.api_client.do.return_value = (
        _query_history(query_text) if query_text is not None else {"res": []}
    )
    return ws


def _connection_test(ws: MagicMock, **extra: Any) -> UnityCatalogConnectionTest:
    config = unity_config(**extra)
    ws.config.warehouse_id = config.warehouse_id
    with patch(
        "datahub.ingestion.source.unity.connection_test.create_workspace_client",
        return_value=ws,
    ):
        return UnityCatalogConnectionTest(config)


def _basic(report: TestConnectionReport) -> CapabilityReport:
    assert report.basic_connectivity is not None
    return report.basic_connectivity


def test_every_capability_is_reported_when_everything_is_reachable(
    workspace_client: MagicMock,
) -> None:
    report = _connection_test(
        _reachable(workspace_client), profiling=ANALYZE_PROFILING
    ).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.USAGE_STATS].capable
    assert report.capability_report[SourceCapability.DATA_PROFILING].capable


def test_disabled_features_are_left_out_of_the_capability_report(
    workspace_client: MagicMock,
) -> None:
    report = _connection_test(
        _reachable(workspace_client), include_usage_statistics=False
    ).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report == {}


def test_catalog_listing_failure_is_a_basic_connectivity_failure(
    workspace_client: MagicMock,
) -> None:
    ws = _reachable(workspace_client)
    ws.catalogs.list.side_effect = PermissionDenied("token lacks workspace access")

    report = _connection_test(ws, include_usage_statistics=False).get_connection_test()

    assert not _basic(report).capable
    assert _basic(report).failure_reason


def test_empty_query_history_is_a_usage_failure(workspace_client: MagicMock) -> None:
    report = _connection_test(
        _reachable(workspace_client, query_text=None)
    ).get_connection_test()

    assert report.capability_report is not None
    usage = report.capability_report[SourceCapability.USAGE_STATS]
    assert not usage.capable
    assert usage.failure_reason


@pytest.mark.parametrize(
    "extra, expect_capable",
    [
        # System-tables path: table-level usage survives redaction.
        ({"warehouse_id": WAREHOUSE_ID}, True),
        # Column usage needs SQL text, so redaction degrades usage completely.
        ({"warehouse_id": WAREHOUSE_ID, "include_column_usage_stats": True}, False),
        # REST API path has no table_lineage fallback.
        ({}, False),
    ],
)
def test_redacted_query_text_is_only_tolerated_on_the_system_tables_path(
    workspace_client: MagicMock, extra: Dict[str, Any], expect_capable: bool
) -> None:
    """Databricks redacts query text for some workspaces; usage stays possible
    only where system.access.table_lineage can stand in for the SQL."""
    report = _connection_test(
        _reachable(workspace_client, query_text="<REDACTED>"), **extra
    ).get_connection_test()

    assert report.capability_report is not None
    usage = report.capability_report[SourceCapability.USAGE_STATS]
    assert usage.capable == expect_capable
    assert bool(usage.mitigation_message) == expect_capable
    assert bool(usage.failure_reason) != expect_capable


def test_missing_warehouse_is_a_profiling_failure_only(
    workspace_client: MagicMock,
) -> None:
    ws = _reachable(workspace_client)
    ws.warehouses.get.side_effect = NotFound("warehouse does not exist")

    report = _connection_test(
        ws, include_usage_statistics=False, profiling=ANALYZE_PROFILING
    ).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report is not None
    profiling = report.capability_report[SourceCapability.DATA_PROFILING]
    assert not profiling.capable
    assert profiling.failure_reason


# --- hive_metastore SQLAlchemy connection ----------------------------------


def _hive_connect(options: Dict[str, Any]) -> MagicMock:
    """Build the source with the Databricks DBAPI patched; return the patched connect."""
    config = unity_config(
        token="my-pat",
        warehouse_id=WAREHOUSE_ID,
        include_hive_metastore=True,
        options=options,
    )
    with patch("databricks.sql.connect") as connect:
        UnityCatalogSource(PipelineContext(run_id="hive-auth-test"), config)
    return connect


def test_hive_metastore_connection_uses_recipe_auth() -> None:
    """The warehouse http_path and a bearer-token credentials provider come from
    the recipe, so the SQLAlchemy URL never has to carry the token."""
    connect = _hive_connect({})

    kwargs = connect.call_args.kwargs
    assert kwargs["http_path"] == f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    headers = kwargs["credentials_provider"]()()
    assert headers["Authorization"] == "Bearer my-pat"


def test_hive_metastore_user_connect_args_take_precedence() -> None:
    connect = _hive_connect(
        {"connect_args": {"http_path": "/sql/1.0/warehouses/user_warehouse"}}
    )

    kwargs = connect.call_args.kwargs
    assert kwargs["http_path"] == "/sql/1.0/warehouses/user_warehouse"
    assert "credentials_provider" in kwargs
