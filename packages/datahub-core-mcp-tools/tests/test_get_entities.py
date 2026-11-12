"""get_entities: what it reads, through which backend, and what it returns."""

import copy
from typing import Any, Optional, Sequence, cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools import graphql_helpers
from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import (
    GraphQLReadBackend,
    JsonObject,
    ReadBackendBinding,
)
from datahub_core_mcp_tools.result_policy import ResultPolicyBinding
from datahub_core_mcp_tools.schema_matching import FieldText
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.entities import get_entities, get_entities_description

ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.orders,PROD)"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.users,PROD)"
MISSING = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.missing,PROD)"
QUERY = "urn:li:query:weekly-revenue"
ASSERTION = "urn:li:assertion:orders-volume"
CONTRACT = "urn:li:dataContract:orders"
DOCUMENT = "urn:li:document:runbook"


def _dataset(urn: str, columns: Sequence[str] = ("id",), **extra: Any) -> dict:
    return {
        "urn": urn,
        "properties": {"name": urn.split(".")[-1].split(",")[0]},
        "schemaMetadata": {
            "fields": [{"fieldPath": c, "nativeDataType": "VARCHAR"} for c in columns]
        },
        **extra,
    }


class FakeBackend:
    """A ReadBackend serving fixed entities; records what was read."""

    def __init__(self, *entities: dict, related: Optional[dict] = None) -> None:
        self.entities = {e["urn"]: e for e in entities}
        self.related = related
        self.reads: list[tuple[str, bool]] = []
        self.related_reads: list[str] = []

    def exists(self, urn: str) -> bool:
        return urn in self.entities

    def entity(self, urn: str, *, include_contract: bool = False) -> JsonObject | None:
        self.reads.append((urn, include_contract))
        return cast(JsonObject, copy.deepcopy(self.entities[urn]))

    def related_documents(self, urn: str, start: int, count: int) -> JsonObject:
        self.related_reads.append(urn)
        return cast(JsonObject, {"entity": {"relatedDocuments": self.related}})

    def aspects(self, urn: str) -> dict[str, JsonObject] | None:
        return None

    def search_entities(self, variables: JsonObject) -> JsonObject:
        return {}


def _run(backend: FakeBackend, *items: object, **kwargs: Any) -> Any:
    with with_datahub_client(
        MagicMock(), tool_context=ToolContext([ReadBackendBinding(backend), *items])
    ):
        return get_entities(**kwargs)


def test_a_single_urn_returns_one_entity() -> None:
    result = _run(FakeBackend(_dataset(ORDERS)), urns=ORDERS)

    assert isinstance(result, dict)
    assert result["urn"] == ORDERS
    assert result["name"] == "orders"


def test_a_batch_keeps_the_callers_order_and_reports_each_failure_in_place() -> None:
    backend = FakeBackend(_dataset(ORDERS), _dataset(USERS))

    result = _run(backend, urns=[USERS, MISSING, "orders", ORDERS])

    assert [item["urn"] for item in result] == [USERS, MISSING, "orders", ORDERS]
    assert result[1]["error"] == f"Entity {MISSING} not found"
    # A name where a URN belongs gets told how to turn it into one.
    assert "search(" in result[2]["error"]
    assert "error" not in result[0] and "error" not in result[3]
    assert [urn for urn, _ in backend.reads] == [USERS, ORDERS]


def test_a_single_missing_urn_raises() -> None:
    with pytest.raises(ToolError, match="not found"):
        _run(FakeBackend(), urns=MISSING)


def test_a_batch_with_no_urn_at_all_raises_naming_each_value() -> None:
    with pytest.raises(ToolError) as error:
        _run(FakeBackend(), urns=["orders", ""])

    assert "orders" in str(error.value)
    assert "urns[1]" in str(error.value)


def test_a_json_stringified_list_is_a_batch() -> None:
    backend = FakeBackend(_dataset(ORDERS), _dataset(USERS))

    result = _run(backend, urns=f'["{ORDERS}", "{USERS}"]')

    assert isinstance(result, list)
    assert [item["urn"] for item in result] == [ORDERS, USERS]


def test_the_contract_is_read_only_when_named() -> None:
    backend = FakeBackend(_dataset(ORDERS))

    _run(backend, urns=ORDERS)
    _run(backend, urns=ORDERS, fields=["contract"])

    assert backend.reads == [(ORDERS, False), (ORDERS, True)]


def test_related_documents_cost_a_read_only_when_named() -> None:
    related = {"total": 1, "documents": [{"urn": DOCUMENT, "__typename": "Document"}]}
    backend = FakeBackend(_dataset(ORDERS), related=related)

    plain = _run(backend, urns=ORDERS)
    named = _run(backend, urns=ORDERS, fields=["relatedDocuments"])

    assert "relatedDocuments" not in plain
    assert named["relatedDocuments"] == {"total": 1, "documents": [{"urn": DOCUMENT}]}
    assert backend.related_reads == [ORDERS]


def test_a_named_description_is_capped_and_a_withheld_one_measured_whole(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(graphql_helpers, "REQUESTED_DESCRIPTION_LENGTH_LIMIT", 60)
    text = "word " * 30
    backend = FakeBackend(
        _dataset(ORDERS, properties={"name": "orders", "description": text})
    )

    named = _run(backend, urns=ORDERS, fields=["description"])
    withheld = _run(backend, urns=ORDERS)

    assert named["description"].startswith("word word")
    assert named["description"].endswith("chars]")
    assert len(named["description"]) > 40
    # The manifest reports the real length, not the capped one.
    assert f"description ({len(text.strip())} chars)" in withheld["omittedFields"]


def test_named_columns_split_the_schema_budget_between_urns() -> None:
    wide = [f"col_{i:03d}" for i in range(200)]
    backend = FakeBackend(_dataset(ORDERS, wide), _dataset(USERS, wide))

    with patch.object(graphql_helpers, "ENTITY_SCHEMA_TOKEN_BUDGET", 400):
        single = _run(backend, urns=ORDERS, schema_columns=["col"])
        both = _run(backend, urns=[ORDERS, USERS], schema_columns=["col"])

    single_fields = single["schemaMetadata"]["fields"]
    both_fields = both[0]["schemaMetadata"]["fields"]
    assert 0 < len(both_fields) < len(single_fields) < 200
    assert single["schemaMetadata"]["matchedColumns"] == 200


def test_an_exact_column_name_is_kept_ahead_of_broad_matches() -> None:
    columns = [f"user_{i:03d}" for i in range(200)] + ["USER"]
    backend = FakeBackend(_dataset(ORDERS, columns))

    with patch.object(graphql_helpers, "ENTITY_SCHEMA_TOKEN_BUDGET", 50):
        result = _run(backend, urns=ORDERS, schema_columns="user")

    assert result["schemaMetadata"]["fields"][0]["fieldPath"] == "USER"


def test_assertion_lists_are_capped_and_contract_members_ranked() -> None:
    runs = [
        {
            "result": {
                "type": "ERROR",
                "nativeResults": [
                    {"key": f"k{i}", "value": "x" * 500} for i in range(15)
                ],
                "error": {
                    "type": "UNKNOWN",
                    "properties": [{"key": "message", "value": "boom"}],
                },
            }
        }
    ]
    assertion = {"urn": ASSERTION, "runEvents": {"total": 1, "runEvents": runs}}

    def member(urn: str, result_type: Optional[str]) -> dict:
        events = [{"result": {"type": result_type}}] if result_type else []
        return {"assertion": {"urn": urn, "runEvents": {"runEvents": events}}}

    members = [member(f"urn:li:assertion:p{i}", "SUCCESS") for i in range(30)]
    members.append(member("urn:li:assertion:failing", "FAILURE"))
    contract = {
        "urn": CONTRACT,
        "status": {"state": "ACTIVE"},
        "properties": {"entityUrn": ORDERS, "dataQuality": members},
    }
    backend = FakeBackend(assertion, contract)

    run = _run(backend, urns=ASSERTION, fields=["runEvents"])["runEvents"]["runEvents"][
        0
    ]
    verdict = _run(backend, urns=CONTRACT)["properties"]

    assert len(run["result"]["nativeResults"]) == 10
    assert run["result"]["nativeResultsTotal"] == 15
    assert run["result"]["nativeResults"][0]["value"].endswith("[+300 chars]")
    assert run["result"]["error"]["properties"] == [{"key": "message", "value": "boom"}]
    assert len(verdict["dataQuality"]) == 25
    assert verdict["dataQualityTotal"] == 31
    assert verdict["dataQuality"][0]["assertion"]["urn"] == "urn:li:assertion:failing"


def test_trust_labels_and_configuration_documents() -> None:
    hidden = "urn:li:document:retrieval-policy-default"

    class Decisions:
        def annotation(self, urn: str, *, explain: bool = False) -> dict:
            return {"grade": "certified", **({"reason": "rule"} if explain else {})}

        def header(self, urns: Sequence[str]) -> None:
            return None

        def is_configuration(self, urn: str) -> bool:
            return False

    class Policy:
        def decide(self, urns: Sequence[str]) -> Decisions:
            return Decisions()

    backend = FakeBackend(_dataset(ORDERS), {"urn": hidden})

    result = _run(
        backend,
        ResultPolicyBinding(Policy()),
        urns=[ORDERS, hidden],
        explain_trust=True,
    )

    assert result[0]["trust"] == {"grade": "certified", "reason": "rule"}
    assert "Retrieval policy" in result[1]["error"]
    assert [urn for urn, _ in backend.reads] == [ORDERS]


def _graph_backend(entity: dict) -> tuple[GraphQLReadBackend, list[Optional[str]]]:
    sent: list[Optional[str]] = []

    def execute(graph: Any, *, operation_name: Optional[str] = None, **_: Any) -> dict:
        sent.append(operation_name)
        return {"entity": entity}

    patcher = patch("datahub_core_mcp_tools.graphql_helpers.execute_graphql", execute)
    patcher.start()
    return GraphQLReadBackend(Mock()), sent


@pytest.mark.parametrize(
    "urn, operation",
    [
        (QUERY, "GetQueryEntity"),
        (ASSERTION, "GetAssertionEntity"),
        (ORDERS, "GetEntity"),
    ],
)
def test_each_entity_kind_is_read_with_its_own_query(urn: str, operation: str) -> None:
    backend, sent = _graph_backend({"urn": urn})
    try:
        assert backend.entity(urn) == {"urn": urn}
    finally:
        patch.stopall()

    assert sent == [operation]


class _LastColumnFirst:
    def scores(
        self, keywords: Sequence[str], fields: Sequence[FieldText]
    ) -> list[float]:
        return [float(i) for i in range(len(fields))]


@pytest.mark.anyio
async def test_a_host_matcher_ranks_named_columns_on_its_own_server() -> None:
    backend = FakeBackend(_dataset(ORDERS, ["a", "b", "c"]))
    mcp, default = FastMCP("custom"), FastMCP("default")
    register_tools(mcp, is_oss=True, schema_field_matcher=_LastColumnFirst())
    register_tools(default, is_oss=True)

    async def columns(server: FastMCP) -> list[str]:
        with with_datahub_client(
            MagicMock(), tool_context=ToolContext([ReadBackendBinding(backend)])
        ):
            async with Client(server) as client:
                result = await client.call_tool(
                    "get_entities", {"urns": ORDERS, "schema_columns": ["zz"]}
                )
        fields = cast(dict, result.data)["schemaMetadata"]["fields"]
        return [f["fieldPath"] for f in fields]

    assert await columns(mcp) == ["c", "b"]
    assert await columns(default) == []


def test_description_mentions_documentation_only_on_cloud() -> None:
    assert "`documentation`" not in get_entities_description(is_oss=True)
    assert "`documentation`" in get_entities_description(is_oss=False)
