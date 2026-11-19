"""get_lineage: the existence guard, server-side paging, fields and search flags.

The guard keeps a nonexistent URN (an error) distinguishable from a real entity
with no lineage (an empty result). The paging tests cover the contract the tool
advertises: `offset` reaches the backend, `hasMore`/`nextOffset` describe the whole
result set rather than the page, and following `nextOffset` cannot skip rows.
"""

import copy
from typing import Any, Dict, List, Optional, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp.exceptions import ToolError
from fastmcp.tools import Tool

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.search_flags import SearchFlagsOverride
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.lineage import get_lineage

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
SCHEMA_FIELD_URN = f"urn:li:schemaField:({DATASET_URN},customer_id)"
DEFAULT_FLAGS = {"skipHighlighting": True, "maxAggValues": 3}

# Facets are computed over the whole lineage graph, so the backend returns them
# fully populated even on a page with no rows.
FACETS = [
    {
        "field": "platform",
        "displayName": "Platform",
        "aggregations": [{"value": "snowflake", "count": 41}],
    }
]

# "The backend did not send this field at all", as opposed to sending zero.
OMITTED = object()


def lineage_page(returned: int, total: Any, *, facets: bool = True) -> Dict[str, Any]:
    """A searchAcrossLineage result with `returned` rows out of `total` matches."""
    page: Dict[str, Any] = {
        "searchResults": [
            {
                "entity": {
                    "urn": f"urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.t{i},PROD)",
                    "type": "DATASET",
                    "properties": {"name": f"t{i}", "description": "A table"},
                    "ownership": {
                        "owners": [{"owner": {"urn": "urn:li:corpuser:jdoe"}}]
                    },
                },
                "degree": 1,
            }
            for i in range(returned)
        ]
    }
    if total is not OMITTED:
        page["total"] = total
    if facets:
        page["facets"] = FACETS
    return page


class FakeBackend:
    """A ReadBackend serving one fixed lineage page; records what was asked."""

    def __init__(self, page: Optional[Dict[str, Any]] = None, exists: bool = True):
        self.page = page if page is not None else lineage_page(0, 0, facets=False)
        self._exists = exists
        self.exists_calls: List[str] = []
        self.sent: List[Dict[str, Any]] = []

    def exists(self, urn: str) -> bool:
        self.exists_calls.append(urn)
        return self._exists

    def search_lineage(self, variables: JsonObject) -> JsonObject:
        self.sent.append(dict(variables))
        return cast(JsonObject, copy.deepcopy(self.page))

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


def _run(backend: FakeBackend, *items: object, **kwargs: Any) -> dict:
    context = ToolContext([ReadBackendBinding(backend), *items])
    with with_datahub_client(MagicMock(), tool_context=context):
        return get_lineage(**kwargs)


def test_nonexistent_urn_raises_before_any_lineage_query() -> None:
    backend = FakeBackend(exists=False)

    with pytest.raises(ToolError, match="not found"):
        _run(backend, urn=DATASET_URN)

    assert backend.sent == []


def test_existing_urn_with_no_lineage_returns_empty() -> None:
    result = _run(FakeBackend(), urn=DATASET_URN)

    assert result["upstreams"]["searchResults"] == []
    assert result["upstreams"]["hasMore"] is False


def test_bare_name_is_rejected_with_how_to_get_a_urn() -> None:
    with pytest.raises(ToolError, match="search"):
        _run(FakeBackend(), urn="orders")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"urn": DATASET_URN, "column": "customer_id"},
        {"urn": SCHEMA_FIELD_URN},
    ],
    ids=["column", "schema_field_urn"],
)
def test_column_lineage_checks_the_parent_dataset(kwargs: Dict[str, Any]) -> None:
    # A schemaField has no key aspect of its own: exists() would be false for it.
    backend = FakeBackend()

    _run(backend, **kwargs)

    assert backend.exists_calls == [DATASET_URN]
    assert backend.sent[0]["urn"] == SCHEMA_FIELD_URN


def test_column_on_a_non_dataset_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="dataset"):
        _run(
            FakeBackend(),
            urn="urn:li:dashboard:(looker,dashboards.1)",
            column="revenue",
        )


def test_column_with_dashboard_filter_is_rejected() -> None:
    # Column lineage only resolves to datasets, so this filter always returns 0.
    backend = FakeBackend()

    with pytest.raises(ToolError, match="without an entity_type filter"):
        _run(
            backend,
            urn=DATASET_URN,
            column="order_status",
            upstream=False,
            filter="entity_type IN (dashboard, chart)",
        )

    assert backend.sent == []


def test_column_with_dataset_filter_runs() -> None:
    result = _run(
        FakeBackend(),
        urn=DATASET_URN,
        column="order_status",
        upstream=False,
        filter="entity_type = dataset",
    )

    assert result["metadata"]["queryType"] == "column-level-lineage"


def test_invalid_filter_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="Invalid filter"):
        _run(FakeBackend(), urn=DATASET_URN, filter="platform = ")


async def _run_through_mcp(backend: FakeBackend, arguments: Dict[str, Any]) -> Any:
    # Aliases and the direction coercion are applied by pydantic, as FastMCP runs it.
    context = ToolContext([ReadBackendBinding(backend)])
    with with_datahub_client(MagicMock(), tool_context=context):
        return await Tool.from_function(get_lineage).run(
            {"urn": DATASET_URN, **arguments}
        )


async def test_direction_string_selects_downstream() -> None:
    backend = FakeBackend()

    result = await _run_through_mcp(backend, {"direction": "downstream", "depth": 2})

    assert backend.sent[0]["direction"] == "DOWNSTREAM"
    assert "downstreams" in result.structured_content


async def test_direction_both_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="twice"):
        await _run_through_mcp(FakeBackend(), {"direction": "both"})


@pytest.mark.parametrize(
    ("max_hops", "degrees"),
    [(1, ["1"]), (2, ["1", "2"]), (3, ["1", "2", "3+"]), (7, ["1", "2", "3+"])],
)
def test_max_hops_becomes_a_degree_filter(max_hops: int, degrees: List[str]) -> None:
    backend = FakeBackend()

    _run(backend, urn=DATASET_URN, max_hops=max_hops)

    (clause,) = backend.sent[0]["orFilters"]
    assert {"field": "degree", "values": degrees} in [
        {"field": rule["field"], "values": rule["values"]} for rule in clause["and"]
    ]


def test_offset_is_sent_as_backend_start() -> None:
    # Slicing a fetched page by offset only discards rows the backend already picked,
    # so any offset >= max_results came back empty while `start` was always 0.
    backend = FakeBackend(lineage_page(returned=5, total=56))

    _run(backend, urn=DATASET_URN, max_results=30, offset=30)

    assert backend.sent[0]["start"] == 30
    assert backend.sent[0]["count"] == 30


def test_has_more_is_measured_against_total_not_page() -> None:
    result = _run(
        FakeBackend(lineage_page(returned=30, total=56)),
        urn=DATASET_URN,
        max_results=30,
    )

    upstreams = result["upstreams"]
    assert upstreams["returned"] == 30
    assert upstreams["hasMore"] is True
    assert upstreams["nextOffset"] == 30


def test_last_page_reports_no_more() -> None:
    result = _run(
        FakeBackend(lineage_page(returned=26, total=56)),
        urn=DATASET_URN,
        max_results=30,
        offset=30,
    )

    upstreams = result["upstreams"]
    assert upstreams["hasMore"] is False
    assert "nextOffset" not in upstreams


def test_missing_total_is_derived_from_the_page() -> None:
    # Defaulting a missing total to 0 gave "returned 10 of 0" and hasMore=False.
    result = _run(
        FakeBackend(lineage_page(returned=10, total=OMITTED)),
        urn=DATASET_URN,
        max_results=30,
        offset=20,
    )

    upstreams = result["upstreams"]
    assert upstreams["returned"] == 10
    assert upstreams["total"] == 30
    assert upstreams["hasMore"] is False


def test_budget_truncation_without_total_still_pages() -> None:
    # Rows the budget dropped must stay reachable through nextOffset.
    with patch("datahub_core_mcp_tools.graphql_helpers.TOOL_RESPONSE_TOKEN_LIMIT", 120):
        result = _run(
            FakeBackend(lineage_page(returned=10, total=OMITTED)),
            urn=DATASET_URN,
            max_results=30,
        )

    upstreams = result["upstreams"]
    assert upstreams["truncatedDueToTokenBudget"] is True
    assert 0 < upstreams["returned"] < 10
    assert upstreams["hasMore"] is True
    assert upstreams["nextOffset"] == upstreams["returned"]


def test_facets_only_on_the_first_non_empty_page() -> None:
    first = _run(FakeBackend(lineage_page(returned=5, total=56)), urn=DATASET_URN)[
        "upstreams"
    ]
    continuation = _run(
        FakeBackend(lineage_page(returned=26, total=56)), urn=DATASET_URN, offset=30
    )["upstreams"]
    past_end = _run(
        FakeBackend(lineage_page(returned=0, total=56)), urn=DATASET_URN, offset=90
    )["upstreams"]

    assert first["facets"] == FACETS
    assert "facets" not in continuation
    assert continuation["returned"] == 26
    assert "facets" not in past_end
    assert past_end["searchResults"] == []
    assert past_end["hasMore"] is False


def test_fields_is_an_allowlist_per_node() -> None:
    page = lineage_page(returned=1, total=1)

    default = _run(FakeBackend(page), urn=DATASET_URN)["upstreams"]["searchResults"]
    owners = _run(FakeBackend(page), urn=DATASET_URN, fields=["ownership"])[
        "upstreams"
    ]["searchResults"]

    assert "ownership" not in default[0]["entity"]
    assert "ownership" in default[0]["entity"]["omittedFields"]
    assert "ownership" in owners[0]["entity"]
    assert default[0]["degree"] == 1


def test_downstream_metric_and_semantic_model_are_named() -> None:
    # Their names live under `info`; the projection lifts them to the top.
    backend = FakeBackend(
        {
            "total": 2,
            "searchResults": [
                {
                    "entity": {
                        "urn": "urn:li:metric:(urn:li:dataPlatform:snowflake,analytics,net_revenue)",
                        "type": "METRIC",
                        "info": {
                            "name": "Net Revenue",
                            "description": "Recognized revenue net of returns",
                        },
                    },
                    "degree": 1,
                },
                {
                    "entity": {
                        "urn": "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,revenue)",
                        "type": "SEMANTIC_MODEL",
                        "info": {
                            "name": "Revenue",
                            "description": "Orders joined to customers",
                        },
                    },
                    "degree": 1,
                },
            ],
        }
    )

    result = _run(backend, urn=DATASET_URN, upstream=False)

    entities = [item["entity"] for item in result["downstreams"]["searchResults"]]
    assert [entity["name"] for entity in entities] == ["Net Revenue", "Revenue"]
    assert all(
        any(entry.startswith("description (") for entry in entity["omittedFields"])
        for entity in entities
    )


def test_column_lineage_returns_lineage_columns() -> None:
    target = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
    backend = FakeBackend(
        {
            "total": 1,
            "searchResults": [
                {
                    "entity": {"urn": target, "type": "DATASET"},
                    "paths": [
                        {
                            "path": [
                                {"type": "SCHEMA_FIELD", "fieldPath": "customer_id"},
                                {"type": "SCHEMA_FIELD", "fieldPath": "id"},
                            ]
                        }
                    ],
                    "degree": 1,
                }
            ],
        }
    )

    result = _run(backend, urn=DATASET_URN, column="customer_id", upstream=False)

    (node,) = result["downstreams"]["searchResults"]
    assert node["lineageColumns"] == ["id"]
    assert "paths" not in node
    assert result["metadata"]["groupedBy"] == "dataset"


def test_search_flags_default_and_override() -> None:
    plain, overridden = FakeBackend(), FakeBackend()

    _run(plain, urn=DATASET_URN)
    _run(
        overridden,
        SearchFlagsOverride({"skipCache": True, "maxAggValues": 10}),
        urn=DATASET_URN,
    )

    assert plain.sent[0]["searchFlags"] == DEFAULT_FLAGS
    assert overridden.sent[0]["searchFlags"] == {
        "skipHighlighting": True,
        "maxAggValues": 10,
        "skipCache": True,
    }


def test_graphql_backend_sends_get_entity_lineage() -> None:
    client = MagicMock()
    client._graph.exists.return_value = True

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value={"searchAcrossLineage": lineage_page(returned=2, total=2)},
    ) as execute:
        with with_datahub_client(client):
            result = get_lineage(urn=DATASET_URN, upstream=False)

    assert execute.call_args.kwargs["operation_name"] == "GetEntityLineage"
    assert "query GetEntityLineage" in execute.call_args.kwargs["query"]
    sent = execute.call_args.kwargs["variables"]["input"]
    assert sent["direction"] == "DOWNSTREAM"
    assert sent["searchFlags"] == DEFAULT_FLAGS
    assert result["downstreams"]["returned"] == 2
