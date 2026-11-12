"""The search tool: what it sends to DataHub and what it returns."""

from typing import Any, Callable, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.search_flags import SearchFlagsOverride
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.search import (
    _rewrite_plus_joined_wildcards,
    drop_short_words,
    search,
    search_description,
)
from datahub_core_mcp_tools.view_preference import CustomView, NoView

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.orders,PROD)"
DOCUMENT_URN = "urn:li:document:runbook"
# 2026-05-20T13:45:00Z
MAY_20_MS = 1779284700000


class FakeBackend:
    """A ReadBackend whose search returns ``total_for(variables)`` matches."""

    def __init__(
        self,
        results: Optional[list[dict]] = None,
        total_for: Optional[Callable[[dict], int]] = None,
    ) -> None:
        self.results = results or []
        self.total_for = total_for or (lambda variables: len(self.results))
        # Typed loosely so tests can index into what was sent.
        self.sent: list[dict[str, Any]] = []

    def exists(self, urn: str) -> bool:
        return True

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def entity(self, urn: str, *, include_contract: bool = False) -> None:
        return None

    def related_documents(self, urn: str, start: int, count: int) -> dict:
        return {}

    def search_entities(self, variables: JsonObject) -> JsonObject:
        self.sent.append(dict(variables))
        total = self.total_for(variables)
        return cast(
            JsonObject,
            {
                "start": 0,
                "count": total,
                "total": total,
                "searchResults": [{"entity": e} for e in self.results][:total],
                "facets": [],
            },
        )

    @property
    def queries(self) -> list[str]:
        return [variables["query"] for variables in self.sent]


def _run(backend: FakeBackend, *items: object, **kwargs: object) -> dict:
    context = ToolContext([NoView(), ReadBackendBinding(backend), *items])
    with with_datahub_client(MagicMock(), tool_context=context):
        return search(**kwargs)  # type: ignore[arg-type]


def test_search_reads_through_the_bound_backend() -> None:
    backend = FakeBackend(
        results=[
            {"urn": DATASET_URN, "properties": {"name": "orders"}, "tags": []},
            {"urn": DOCUMENT_URN, "info": {"title": "Runbook"}},
        ]
    )

    result = _run(backend, query="/q orders", filter="platform = snowflake")

    assert [r["entity"]["urn"] for r in result["searchResults"]] == [
        DATASET_URN,
        DOCUMENT_URN,
    ]
    assert "tags" not in result["searchResults"][0]["entity"]
    (sent,) = backend.sent
    assert sent["query"] == "/q orders"
    assert sent["types"] is None
    assert sent["searchFlags"] == {"skipHighlighting": True, "maxAggValues": 5}
    rules = [rule for clause in sent["orFilters"] for rule in clause["and"]]
    assert {
        "field": "platform.keyword",
        "condition": "EQUAL",
        "values": ["urn:li:dataPlatform:snowflake"],
    } in rules
    # Configuration documents never reach an agent.
    assert any(rule["field"] == "urn" and rule["negated"] for rule in rules)


def test_entity_type_filter_becomes_types() -> None:
    backend = FakeBackend()
    _run(backend, query="*", filter="entity_type IN (dataset, dashboard)")
    assert backend.sent[0]["types"] == ["DATASET", "DASHBOARD"]


def test_a_bad_filter_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="Empty IN list"):
        _run(FakeBackend(), query="*", filter="platform IN ()")
    with pytest.raises(ToolError, match="Invalid filter"):
        _run(FakeBackend(), query="*", filter="deprecated = maybe")


def test_paging_sorting_and_the_result_cap() -> None:
    backend = FakeBackend()
    _run(
        backend,
        query="*",
        num_results=500,
        offset=20,
        sort_by="lastModifiedAt",
        sort_order="asc",
    )
    (sent,) = backend.sent
    assert (sent["count"], sent["start"]) == (50, 20)
    assert sent["sortInput"] == {
        "sortCriteria": [{"field": "lastModifiedAt", "sortOrder": "ASCENDING"}]
    }


def test_sorting_by_relevance_keeps_the_default_ranking() -> None:
    # No index can sort on "relevance"; GMS would fail the whole search.
    backend = FakeBackend()
    _run(backend, query="/q orders", sort_by=" Relevance ")
    assert "sortInput" not in backend.sent[0]


def test_zero_results_returns_only_facets() -> None:
    backend = FakeBackend(results=[{"urn": DATASET_URN}])
    result = _run(backend, query="*", num_results=0)
    assert backend.sent[0]["count"] == 1
    assert "searchResults" not in result and "count" not in result
    assert result["total"] == 1


def test_a_host_override_merges_over_the_search_flags() -> None:
    backend = FakeBackend()
    _run(
        backend,
        SearchFlagsOverride({"skipCache": True, "maxAggValues": 20}),
        query="*",
    )
    assert backend.sent[0]["searchFlags"] == {
        "skipHighlighting": True,
        "maxAggValues": 20,
        "skipCache": True,
    }


def test_the_bound_view_is_applied() -> None:
    backend = FakeBackend()
    context = ToolContext(
        [CustomView(urn="urn:li:dataHubView:mine"), ReadBackendBinding(backend)]
    )
    with with_datahub_client(MagicMock(), tool_context=context):
        search(query="*")
    assert backend.sent[0]["viewUrn"] == "urn:li:dataHubView:mine"


def test_results_carry_dates_and_no_redundant_dataset_name() -> None:
    backend = FakeBackend(
        results=[
            {
                "urn": DATASET_URN,
                "name": "analytics.orders",
                "properties": {"name": "orders", "lastModified": {"time": MAY_20_MS}},
            },
            {"urn": DOCUMENT_URN, "info": {"lastModified": {"time": MAY_20_MS}}},
            {
                "urn": "urn:li:dataset:(urn:li:dataPlatform:dbt,model,PROD)",
                "name": "model",
                "properties": {"lastModified": {"time": 0}},
            },
        ]
    )

    dataset, document, unnamed = (
        r["entity"] for r in _run(backend, query="*")["searchResults"]
    )

    assert dataset == {
        "urn": DATASET_URN,
        "properties": {"name": "orders", "lastModified": "2026-05-20"},
    }
    assert document["info"] == {"lastModified": "2026-05-20"}
    # The key name stays when it is the only name; an unknown date goes.
    assert unnamed["name"] == "model"
    assert "lastModified" not in unnamed["properties"]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("/q pet+profile*", "/q (pet AND profile*)"),
        ("/q point+sale+app*", "/q (point AND sale AND app*)"),
        ("/q ai+usage OR ai+feature*", "/q ai+usage OR (ai AND feature*)"),
        ("/q -pet+profile*", "/q -(pet AND profile*)"),
        ("/q customer+churn", "/q customer+churn"),
        ('/q "online payment*"', '/q "online payment*"'),
        ("/q name:orders+daily*", "/q name:orders+daily*"),
        ("pet+profile*", "pet+profile*"),
    ],
)
def test_plus_joined_wildcards_become_and_groups(query: str, expected: str) -> None:
    # A wildcard term skips the analyzer that splits "customer+churn" into words.
    assert _rewrite_plus_joined_wildcards(query) == expected


@pytest.mark.parametrize(
    ("query", "expected", "removed"),
    [
        ("/q os+version", "/q version", ["os"]),
        ("/q registration ip", "/q registration", ["ip"]),
        ("/q top+10+customers", "/q top+customers", ["10"]),
        # Through a dropped word, "+" survives only between two "+" separators.
        ("/q device os+version", "/q device version", ["os"]),
        ("/q sales.report+id", "/q sales.report", ["id"]),
    ],
)
def test_short_words_are_dropped_from_simple_queries(
    query: str, expected: str, removed: list[str]
) -> None:
    result = drop_short_words(query)
    assert (result.query, result.removed) == (expected, removed)


@pytest.mark.parametrize(
    "query",
    [
        "/q os",  # alone it still matches a value that is exactly "os"
        "/q id+os",  # nothing longer would be left
        "/q os AND version",
        "/q os+version OR ip",
        "/q (os version)",
        "/q os -version",
        '/q "os version"',
        "/q os+version*",
        "/q name:os+version",
        "/q os++version",
        "registration ip",  # not /q syntax
    ],
)
def test_other_queries_are_never_rewritten(query: str) -> None:
    result = drop_short_words(query)
    assert result.query == query
    assert result.note(query) is None


def test_an_empty_short_word_search_is_retried_without_the_word() -> None:
    backend = FakeBackend(
        results=[{"urn": DATASET_URN}],
        total_for=lambda variables: 1 if variables["query"] == "/q version" else 0,
    )

    result = _run(backend, query="/q os+version")

    assert backend.queries == ["/q os+version", "/q version"]
    assert result["total"] == 1
    (note,) = result["search_notes"]
    assert note.startswith("No results for /q os+version.")
    assert note.endswith("Searched for: /q version")


def test_a_short_word_search_with_results_is_not_retried() -> None:
    backend = FakeBackend(results=[{"urn": DATASET_URN}])
    result = _run(backend, query="/q registration+ip")
    assert backend.queries == ["/q registration+ip"]
    assert "search_notes" not in result


def test_a_failing_retry_keeps_the_empty_result() -> None:
    backend = FakeBackend(total_for=lambda variables: 0)
    original = backend.search_entities

    def flaky(variables: JsonObject) -> JsonObject:
        if backend.sent:
            raise RuntimeError("Failed to execute search")
        return original(variables)

    backend.search_entities = flaky  # type: ignore[method-assign]

    result = _run(backend, query="/q os+version")

    assert result["total"] == 0
    (note,) = result["search_notes"]
    assert note.startswith(
        "No results for /q os+version. Couldn't search again without 'os':"
    )


@patch("datahub_core_mcp_tools.graphql_helpers.execute_graphql")
def test_graphql_backend_sends_the_search_document(execute: MagicMock) -> None:
    execute.return_value = {
        "searchAcrossEntities": {
            "total": 1,
            "searchResults": [{"entity": {"urn": DATASET_URN}}],
        }
    }
    with with_datahub_client(MagicMock(), tool_context=ToolContext([NoView()])):
        result = search(query="/q orders", num_results=5)

    assert result["searchResults"] == [{"entity": {"urn": DATASET_URN}}]
    call = execute.call_args.kwargs
    assert call["operation_name"] == "search"
    assert "searchFlags: $searchFlags" in call["query"]
    assert call["variables"]["count"] == 5


async def test_agents_may_use_common_parameter_names() -> None:
    backend = FakeBackend()
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)
    context = ToolContext([NoView(), ReadBackendBinding(backend)])
    with with_datahub_client(MagicMock(), tool_context=context):
        async with Client(mcp) as client:
            await client.call_tool(
                "search",
                {
                    "query": "*",
                    "filters": "platform = snowflake",
                    "limit": 3,
                    "start": 6,
                },
            )
    (sent,) = backend.sent
    assert (sent["count"], sent["start"]) == (3, 6)
    assert "platform.keyword" in str(sent["orFilters"])


def test_descriptions_list_the_filters_and_the_deployments_sort_fields() -> None:
    oss = search_description(is_oss=True)
    cloud = search_description(is_oss=False)
    assert "$" not in oss and "WITHIN" in oss
    assert "lastModifiedAt" in oss and "queryCountLast30DaysFeature" not in oss
    assert "queryCountLast30DaysFeature" in cloud
