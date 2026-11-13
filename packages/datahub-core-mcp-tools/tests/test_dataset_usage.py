"""get_dataset_usage_history: dated query-count points from a dataset's usageStats."""

from typing import Any, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.openapi_client import RawVersionedBatch, VersionedAspectPair
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.dataset_usage import get_dataset_usage_history

DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.orders,PROD)"
)
# 2026-09-08 and 2026-09-09, midnight UTC.
SEP_8, SEP_9 = 1788825600000, 1788912000000


class FakeBackend:
    """A ReadBackend serving one dataset's usage; records the ranges asked for."""

    def __init__(self, dataset: Optional[dict], exists: bool = True) -> None:
        self.dataset = dataset
        self._exists = exists
        self.ranges: List[str] = []

    def exists(self, urn: str) -> bool:
        return self._exists

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        self.ranges.append(time_range)
        return cast(Optional[JsonObject], self.dataset)

    def aspect_versions(self, pairs: list[VersionedAspectPair]) -> RawVersionedBatch:
        return RawVersionedBatch(aspects={}, errors={}, http_calls=0)

    def glossary_term_version(self, urn: str) -> JsonObject | None:
        return None

    def version_set_history(
        self, urn: str, start: int, count: int
    ) -> JsonObject | None:
        return None

    def glossary_terms(self, urns: list[str]) -> list[JsonObject | None]:
        return []

    def tags_named(self, name: str, count: int) -> list[JsonObject]:
        return []

    def description_sources(self, urn: str) -> dict | None:
        return None

    def ownership_types_named(self, name: str, count: int) -> list[JsonObject]:
        return []

    def domains_named(
        self, name: str, parent_domain: str | None, count: int
    ) -> list[JsonObject]:
        return []

    def list_queries(self, variables: JsonObject) -> JsonObject:
        return {}

    def lineage_paths(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        return {}

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> JsonObject:
        return {}

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return {}

    def search_documents(self, variables: JsonObject, *, semantic: bool) -> JsonObject:
        return {}

    def document_contents(self, urns: list[str]) -> list[JsonObject | None]:
        return []


def _run(backend: FakeBackend, **kwargs: Any) -> dict:
    context = ToolContext([ReadBackendBinding(backend)])
    with with_datahub_client(MagicMock(), tool_context=context):
        return get_dataset_usage_history(**kwargs)


def test_points_are_dated_in_utc_and_sorted() -> None:
    backend = FakeBackend(
        {
            "usageStats": {
                "aggregations": {"totalSqlQueries": 27},
                "buckets": [
                    {"bucket": SEP_9, "metrics": {"totalSqlQueries": 15}},
                    {"bucket": SEP_8, "metrics": {"totalSqlQueries": 12}},
                    # Incomplete buckets are left out rather than plotted as 0.
                    {"bucket": SEP_9 + 86400000, "metrics": {}},
                    {"bucket": None, "metrics": {"totalSqlQueries": 3}},
                ],
            }
        }
    )

    result = _run(backend, urn=DATASET_URN)

    assert result == {
        "urn": DATASET_URN,
        "timeRange": "MONTH",
        "totalQueryCount": 27,
        "points": [
            {"date": "2026-09-08", "value": 12},
            {"date": "2026-09-09", "value": 15},
        ],
    }
    assert backend.ranges == ["MONTH"]


def test_a_dataset_never_queried_has_no_points() -> None:
    result = _run(
        FakeBackend({"usageStats": {"aggregations": {}, "buckets": []}}),
        urn=DATASET_URN,
        time_range="WEEK",
    )

    assert result["totalQueryCount"] == 0
    assert result["points"] == []


@pytest.mark.parametrize(
    ("backend", "kwargs", "message"),
    [
        (FakeBackend({"usageStats": None}), {}, "unavailable"),
        (FakeBackend(None), {}, "not found"),
        (FakeBackend({"usageStats": {}}, exists=False), {}, "not found"),
        (
            FakeBackend({"usageStats": {}}),
            {"urn": "urn:li:dashboard:(looker,dashboards.1)"},
            "datasets only",
        ),
        (FakeBackend({"usageStats": {}}), {"urn": "orders"}, "search"),
    ],
    ids=["no-usage", "no-dataset", "missing", "not-a-dataset", "a-name"],
)
def test_errors_are_tool_errors(
    backend: FakeBackend, kwargs: dict, message: str
) -> None:
    with pytest.raises(ToolError, match=message):
        _run(backend, **{"urn": DATASET_URN, **kwargs})


def test_graphql_backend_sends_the_usage_query() -> None:
    client = MagicMock()
    client._graph.exists.return_value = True

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value={"dataset": {"usageStats": {"buckets": []}}},
    ) as execute:
        with with_datahub_client(client):
            get_dataset_usage_history(urn=DATASET_URN, time_range="QUARTER")

    assert execute.call_args.kwargs["operation_name"] == "getDatasetUsageHistory"
    assert execute.call_args.kwargs["variables"] == {
        "urn": DATASET_URN,
        "timeRange": "QUARTER",
    }


async def test_registered_read_only_with_a_closed_range() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tool = {t.name: t for t in await client.list_tools()}[
            "get_dataset_usage_history"
        ]

    assert tool.annotations is not None
    assert tool.annotations.read_only_hint is True
    assert tool.input_schema["properties"]["time_range"]["enum"] == [
        "DAY",
        "WEEK",
        "MONTH",
        "QUARTER",
    ]
