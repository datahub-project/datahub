"""get_dataset_queries: the listQueries request it builds and the caps on what returns."""

import copy
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock, patch

import pytest
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import (
    CUSTOM_PROPERTY_VALUE_HARD_LIMIT,
    QUERY_LENGTH_HARD_LIMIT,
    with_datahub_client,
)
from datahub_core_mcp_tools.openapi_client import RawVersionedBatch, VersionedAspectPair
from datahub_core_mcp_tools.read_backend import JsonObject, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.dataset_queries import get_dataset_queries

ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
CUSTOMERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"


def _query(urn: str, sql: str, **properties: Any) -> Dict[str, Any]:
    return {
        "urn": urn,
        "properties": {
            "source": "MANUAL",
            "statement": {"value": sql, "language": "SQL"},
            **properties,
        },
        "platform": {"name": "snowflake"},
        "subjects": [
            {"dataset": {"urn": ORDERS}},
            {
                "dataset": {"urn": ORDERS},
                "schemaField": {"urn": f"urn:li:schemaField:({ORDERS},id)"},
            },
            {"dataset": {"urn": CUSTOMERS}},
            {"schemaField": {"urn": f"urn:li:schemaField:({CUSTOMERS},id)"}},
        ],
    }


def _page(*queries: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "start": 0,
        "count": len(queries),
        "total": len(queries),
        "queries": list(queries),
    }


class FakeBackend:
    """A ReadBackend serving one listQueries page; records what was asked."""

    def __init__(self, page: Dict[str, Any], exists: bool = True) -> None:
        self.page = page
        self._exists = exists
        self.sent: List[Dict[str, Any]] = []

    def exists(self, urn: str) -> bool:
        return self._exists

    def dataset_usage(self, urn: str, time_range: str) -> JsonObject | None:
        return None

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

    def ownership_types_named(self, name: str, count: int) -> list[JsonObject]:
        return []

    def domains_named(
        self, name: str, parent_domain: str | None, count: int
    ) -> list[JsonObject]:
        return []

    def list_queries(self, variables: JsonObject) -> JsonObject:
        self.sent.append(dict(variables))
        return cast(JsonObject, copy.deepcopy(self.page))

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
        return get_dataset_queries(**kwargs)


def _entities_filter(variables: Dict[str, Any]) -> List[str]:
    (clause,) = variables["orFilters"]
    (rule,) = [rule for rule in clause["and"] if rule["field"] == "entities"]
    return rule["values"]


def test_dataset_queries_filter_by_dataset_and_page() -> None:
    backend = FakeBackend(_page(_query("urn:li:query:q1", "SELECT 1")))

    _run(backend, urn=ORDERS, start=10, count=5)

    (sent,) = backend.sent
    assert _entities_filter(sent) == [ORDERS]
    assert (sent["start"], sent["count"]) == (10, 5)
    assert "source" not in sent


def test_column_and_source_narrow_the_request() -> None:
    backend = FakeBackend(_page())

    _run(backend, urn=ORDERS, column="customer_id", source="MANUAL")

    (sent,) = backend.sent
    assert _entities_filter(sent) == [f"urn:li:schemaField:({ORDERS},customer_id)"]
    assert sent["source"] == "MANUAL"


def test_subjects_become_distinct_dataset_urns() -> None:
    result = _run(FakeBackend(_page(_query("urn:li:query:q1", "SELECT 1"))), urn=ORDERS)

    assert result["queries"][0]["subjects"] == [ORDERS, CUSTOMERS]


def test_long_sql_and_custom_property_values_are_capped() -> None:
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(5000))
    long_value = "x" * (CUSTOM_PROPERTY_VALUE_HARD_LIMIT + 1000)
    backend = FakeBackend(
        _page(
            _query(
                "urn:li:query:q1",
                long_sql,
                customProperties=[
                    {"key": "config", "value": long_value},
                    {"key": "owner_team", "value": "analytics"},
                ],
            )
        )
    )

    (query,) = _run(backend, urn=ORDERS)["queries"]

    statement = query["properties"]["statement"]["value"]
    assert len(statement) <= QUERY_LENGTH_HARD_LIMIT
    assert statement.endswith("[truncated]")
    props = {e["key"]: e["value"] for e in query["properties"]["customProperties"]}
    assert len(props["config"]) < len(long_value)
    assert props["owner_team"] == "analytics"


def test_no_queries_is_an_empty_list() -> None:
    result = _run(FakeBackend(_page()), urn=ORDERS)

    assert result["total"] == 0
    assert result["queries"] == []


def test_missing_dataset_and_bad_arguments_are_tool_errors() -> None:
    with pytest.raises(ToolError, match="not found"):
        _run(FakeBackend(_page(), exists=False), urn=ORDERS)
    with pytest.raises(ToolError, match="search"):
        _run(FakeBackend(_page()), urn="orders")
    with pytest.raises(ToolError, match="dataset"):
        _run(
            FakeBackend(_page()),
            urn="urn:li:dashboard:(looker,dashboards.1)",
            column="revenue",
        )


def test_graphql_backend_sends_list_queries() -> None:
    client = MagicMock()
    client._graph.exists.return_value = True

    with patch(
        "datahub_core_mcp_tools.graphql_helpers.execute_graphql",
        return_value={"listQueries": _page(_query("urn:li:query:q1", "SELECT 1"))},
    ) as execute:
        with with_datahub_client(client):
            result = get_dataset_queries(urn=ORDERS, source="SYSTEM")

    assert execute.call_args.kwargs["operation_name"] == "listQueries"
    assert execute.call_args.kwargs["variables"]["input"]["source"] == "SYSTEM"
    assert result["queries"][0]["urn"] == "urn:li:query:q1"
