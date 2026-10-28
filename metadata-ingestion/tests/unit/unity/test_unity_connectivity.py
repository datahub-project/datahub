import os
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.catalog import CatalogInfo
from urllib3 import HTTPSConnectionPool

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection_test import UnityCatalogConnectionTest
from datahub.ingestion.source.unity.proxy_patch import (
    _create_patched_open_method,
    mask_proxy_credentials,
)

# --- UnityCatalogConnectionTest --------------------------------------------

ANALYZE_PROFILING: Dict[str, Any] = {
    "method": "analyze",
    "enabled": True,
    "warehouse_id": "wh1",
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


def _ws(query_text: Optional[str] = "SELECT 1") -> MagicMock:
    ws = MagicMock()
    ws.catalogs.list.return_value = [CatalogInfo(name="main")]
    ws.api_client.do.return_value = (
        _query_history(query_text) if query_text is not None else {"res": []}
    )
    return ws


def _connection_test(ws: MagicMock, **extra: Any) -> UnityCatalogConnectionTest:
    config = UnityCatalogSourceConfig.model_validate(
        {"workspace_url": "https://example.cloud.databricks.com", "token": "t", **extra}
    )
    ws.config.warehouse_id = config.warehouse_id
    with patch(
        "datahub.ingestion.source.unity.connection_test.create_workspace_client",
        return_value=ws,
    ):
        return UnityCatalogConnectionTest(config)


def _basic(report: TestConnectionReport) -> CapabilityReport:
    assert report.basic_connectivity is not None
    return report.basic_connectivity


def test_every_capability_is_reported_when_everything_is_reachable() -> None:
    report = _connection_test(_ws(), profiling=ANALYZE_PROFILING).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.USAGE_STATS].capable
    assert report.capability_report[SourceCapability.DATA_PROFILING].capable


def test_disabled_features_are_left_out_of_the_capability_report() -> None:
    report = _connection_test(
        _ws(), include_usage_statistics=False
    ).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report == {}


def test_catalog_listing_failure_is_a_basic_connectivity_failure() -> None:
    ws = _ws()
    ws.catalogs.list.side_effect = PermissionDenied("token lacks workspace access")

    report = _connection_test(ws, include_usage_statistics=False).get_connection_test()

    assert not _basic(report).capable
    assert _basic(report).failure_reason


def test_empty_query_history_is_a_usage_failure() -> None:
    report = _connection_test(_ws(query_text=None)).get_connection_test()

    assert report.capability_report is not None
    usage = report.capability_report[SourceCapability.USAGE_STATS]
    assert not usage.capable
    assert usage.failure_reason


@pytest.mark.parametrize(
    "extra, expect_capable",
    [
        # System-tables path: table-level usage survives redaction.
        ({"warehouse_id": "wh1"}, True),
        # Column usage needs SQL text, so redaction degrades usage completely.
        ({"warehouse_id": "wh1", "include_column_usage_stats": True}, False),
        # REST API path has no table_lineage fallback.
        ({}, False),
    ],
)
def test_redacted_query_text_is_only_tolerated_on_the_system_tables_path(
    extra: Dict[str, Any], expect_capable: bool
) -> None:
    report = _connection_test(
        _ws(query_text="<REDACTED>"), **extra
    ).get_connection_test()

    assert report.capability_report is not None
    usage = report.capability_report[SourceCapability.USAGE_STATS]
    assert usage.capable == expect_capable
    assert bool(usage.mitigation_message) == expect_capable
    assert bool(usage.failure_reason) != expect_capable


def test_missing_warehouse_is_a_profiling_failure_only() -> None:
    ws = _ws()
    ws.warehouses.get.side_effect = NotFound("warehouse wh1 does not exist")

    report = _connection_test(
        ws, include_usage_statistics=False, profiling=ANALYZE_PROFILING
    ).get_connection_test()

    assert _basic(report).capable
    assert report.capability_report is not None
    profiling = report.capability_report[SourceCapability.DATA_PROFILING]
    assert not profiling.capable
    assert profiling.failure_reason


# --- proxy_patch -----------------------------------------------------------


def test_mask_proxy_credentials_hides_the_password_only() -> None:
    masked = mask_proxy_credentials("http://alice:s3cret@proxy.example.com:3128/p")

    assert masked == "http://alice:***@proxy.example.com:3128/p"
    assert mask_proxy_credentials("http://proxy.example.com:3128") == (
        "http://proxy.example.com:3128"
    )


class _FakeThriftClient:
    """The attributes of databricks-sql's THttpClient that the patch reads."""

    scheme = "https"
    host = "example.cloud.databricks.com"
    port = 443
    max_connections = 5
    realhost = "example.cloud.databricks.com"
    realport = 443
    proxy_uri: Optional[str] = None

    def __init__(self, proxy_uri: Optional[str] = None) -> None:
        self.proxy_uri = proxy_uri

    def using_proxy(self) -> bool:
        return self.proxy_uri is not None


def _pool(client: _FakeThriftClient) -> Any:
    return vars(client)["_THttpClient__pool"]


def test_patched_open_builds_a_direct_pool_without_a_proxy() -> None:
    original = MagicMock()
    client = _FakeThriftClient()

    _create_patched_open_method(original)(client)

    pool = _pool(client)
    assert isinstance(pool, HTTPSConnectionPool)
    assert (pool.host, pool.port) == (client.host, client.port)
    original.assert_not_called()


def test_patched_open_sends_proxy_auth_headers_from_the_environment() -> None:
    original = MagicMock()
    proxy_url = "http://alice:s3cret@proxy.example.com:3128"
    client = _FakeThriftClient(proxy_uri=proxy_url)

    with (
        patch.dict(os.environ, {"HTTPS_PROXY": proxy_url}, clear=True),
        patch("urllib3.poolmanager.ProxyManager") as proxy_manager,
    ):
        _create_patched_open_method(original)(client)

    kwargs = proxy_manager.call_args.kwargs
    assert proxy_manager.call_args.args == (proxy_url,)
    assert "proxy-authorization" in kwargs["proxy_headers"]
    connection = proxy_manager.return_value.connection_from_host
    connection.assert_called_once()
    assert connection.call_args.kwargs["host"] == client.realhost
    assert connection.call_args.kwargs["port"] == client.realport
    assert _pool(client) is connection.return_value
    original.assert_not_called()


def test_patched_open_falls_back_to_the_original_when_it_cannot_handle_the_client() -> (
    None
):
    original = MagicMock()
    patched = _create_patched_open_method(original)

    unknown_scheme = _FakeThriftClient()
    unknown_scheme.scheme = "ftp"
    patched(unknown_scheme)

    broken = _FakeThriftClient()
    broken.using_proxy = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
    patched(broken)

    assert [c.args for c in original.call_args_list] == [
        (unknown_scheme,),
        (broken,),
    ]
