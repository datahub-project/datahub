"""add_structured_properties and remove_structured_properties."""

from typing import Iterator, cast
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)

RETENTION = "urn:li:structuredProperty:privacy.retentionDays"
CLASSES = "urn:li:structuredProperty:privacy.classes"
OWNER_TEAM = "urn:li:structuredProperty:org.ownerTeam"
REVIEWED = "urn:li:structuredProperty:gov.reviewedOn"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"

DEFINITIONS = {
    RETENTION: ("datahub.number", "SINGLE"),
    CLASSES: ("datahub.string", "MULTIPLE"),
    OWNER_TEAM: ("datahub.urn", "SINGLE"),
    REVIEWED: ("datahub.date", "SINGLE"),
}
UPSERTED: dict = {"upsertStructuredProperties": {"properties": []}}
REMOVED: dict = {"removeStructuredProperties": {"properties": []}}


def _graphql(writes: list) -> object:
    """Answer definition reads from DEFINITIONS and writes from ``writes`` in order."""
    pending = list(writes)

    def answer(**kwargs: object) -> object:
        if kwargs["operation_name"] == "getStructuredPropertyDefinition":
            urn = cast(dict, kwargs["variables"])["urn"]
            if urn not in DEFINITIONS:
                # What GMS answers for a property that does not exist.
                return {
                    "entity": {
                        "urn": urn,
                        "type": "STRUCTURED_PROPERTY",
                        "exists": False,
                        "definition": {
                            "qualifiedName": "",
                            "cardinality": "SINGLE",
                            "valueType": {
                                "urn": "urn:li:dataType:datahub.string",
                                "info": {"qualifiedName": "datahub.string"},
                            },
                        },
                    }
                }
            value_type, cardinality = DEFINITIONS[urn]
            return {
                "entity": {
                    "urn": urn,
                    "type": "STRUCTURED_PROPERTY",
                    "exists": True,
                    "definition": {
                        "qualifiedName": urn.split(":")[-1],
                        "cardinality": cardinality,
                        "valueType": {
                            "urn": "x",
                            "info": {"qualifiedName": value_type},
                        },
                    },
                }
            }
        result = pending.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    return answer


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    with with_datahub_client(client):
        yield client._graph


def _writes(graph: MagicMock) -> list[dict]:
    return [
        c.kwargs["variables"]["input"]
        for c in graph.execute_graphql.call_args_list
        if c.kwargs["operation_name"]
        in ("upsertStructuredProperties", "removeStructuredProperties")
    ]


def test_add_typed_values_to_each_asset(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = _graphql([UPSERTED, UPSERTED])

    result = add_structured_properties(
        property_values={
            RETENTION: ["90"],
            CLASSES: ["PII", "SENSITIVE"],
            OWNER_TEAM: ["urn:li:corpGroup:privacy"],
            REVIEWED: ["2024-12-22"],
        },
        entity_urns=[USERS, ORDERS],
    )

    assert result["entities_updated"] == 2
    assert "added 4 structured propert(ies) to 2 entit(ies)" in result["message"]
    params = [
        {"structuredPropertyUrn": RETENTION, "values": [{"numberValue": 90.0}]},
        {
            "structuredPropertyUrn": CLASSES,
            "values": [{"stringValue": "PII"}, {"stringValue": "SENSITIVE"}],
        },
        {
            "structuredPropertyUrn": OWNER_TEAM,
            "values": [{"stringValue": "urn:li:corpGroup:privacy"}],
        },
        {"structuredPropertyUrn": REVIEWED, "values": [{"stringValue": "2024-12-22"}]},
    ]
    assert _writes(graph) == [
        {"assetUrn": USERS, "structuredPropertyInputParams": params},
        {"assetUrn": ORDERS, "structuredPropertyInputParams": params},
    ]


@pytest.mark.parametrize(
    ("property_values", "error"),
    [
        ({}, "at least one property"),
        ({"retention": ["90"]}, "must be a structured property URN"),
        ({"urn:li:structuredProperty:gone": ["x"]}, "does not exist"),
        ({CLASSES: []}, "at least one value"),
        ({RETENTION: ["ninety"]}, "takes numbers"),
        ({RETENTION: ["90", "30"]}, "takes a single value"),
        ({OWNER_TEAM: ["privacy-team"]}, "takes DataHub URNs"),
        ({REVIEWED: ["22/12/2024"]}, "takes dates as YYYY-MM-DD"),
        ({REVIEWED: ["2024-12-22T00:00:00Z"]}, "takes dates as YYYY-MM-DD"),
    ],
)
def test_invalid_values_write_nothing(
    graph: MagicMock, property_values: dict, error: str
) -> None:
    graph.execute_graphql.side_effect = _graphql([])

    with pytest.raises(ToolError, match=error):
        add_structured_properties(property_values=property_values, entity_urns=[USERS])

    assert _writes(graph) == []


def test_a_bad_asset_urn_writes_nothing(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = _graphql([])

    with pytest.raises(ToolError, match=r"entity_urns\[1\]"):
        add_structured_properties(
            property_values={CLASSES: ["PII"]}, entity_urns=[USERS, "orders"]
        )

    graph.execute_graphql.assert_not_called()


def test_a_partial_failure_names_what_was_applied(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = _graphql(
        [
            UPSERTED,
            GraphError([{"message": "Unauthorized to perform this action."}]),
            requests.ReadTimeout("read timed out"),
        ]
    )
    third = "urn:li:dashboard:(looker,sales)"

    with pytest.raises(ToolError) as raised:
        add_structured_properties(
            property_values={CLASSES: ["PII"]}, entity_urns=[USERS, ORDERS, third]
        )

    message = str(raised.value)
    assert f"applied to 1 of 3 entit(ies) ({USERS})" in message
    assert (
        f"{ORDERS}: DataHub rejected upsertStructuredProperties: Unauthorized"
        in message
    )
    assert f"{third}: upsertStructuredProperties got no reply" in message
    assert len(_writes(graph)) == 3


def test_remove_properties_from_each_asset(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = _graphql([REMOVED, REMOVED])

    result = remove_structured_properties(
        property_urns=[RETENTION, CLASSES, RETENTION], entity_urns=[USERS, ORDERS]
    )

    assert "removed 2 structured propert(ies) from 2 entit(ies)" in result["message"]
    assert _writes(graph) == [
        {"assetUrn": USERS, "structuredPropertyUrns": [RETENTION, CLASSES]},
        {"assetUrn": ORDERS, "structuredPropertyUrns": [RETENTION, CLASSES]},
    ]
    # Removing a reference to a deleted property is how it gets cleaned up.
    assert not [
        c
        for c in graph.execute_graphql.call_args_list
        if c.kwargs["operation_name"] == "getStructuredPropertyDefinition"
    ]


def test_an_empty_result_is_a_failure(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = _graphql([{"removeStructuredProperties": None}])

    with pytest.raises(ToolError, match="applied to 0 of 1 .* no result"):
        remove_structured_properties(property_urns=[RETENTION], entity_urns=[USERS])


def test_definitions_are_read_through_the_bound_backend() -> None:
    client = MagicMock()
    backend = MagicMock(spec=["structured_property"])
    backend.structured_property.return_value = None

    with with_datahub_client(
        client,
        tool_context=ToolContext([ReadBackendBinding(cast(ReadBackend, backend))]),
    ):
        with pytest.raises(ToolError, match="does not exist"):
            add_structured_properties(
                property_values={RETENTION: ["90"]}, entity_urns=[USERS]
            )

    backend.structured_property.assert_called_once_with(RETENTION)
    client._graph.execute_graphql.assert_not_called()


async def test_values_are_published_as_one_type_and_numbers_still_work() -> None:
    datahub = MagicMock()
    datahub._graph.execute_graphql.side_effect = _graphql([UPSERTED])
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(datahub):
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
            await client.call_tool(
                "add_structured_properties",
                {"property_values": {RETENTION: [30]}, "entity_urns": USERS},
            )

    schema = tools["add_structured_properties"].input_schema["properties"]
    # Gemini accepts only a single type; a multi-type anyOf fails the whole tool
    # list on several OpenAI-compatible endpoints.
    assert schema["property_values"]["additionalProperties"]["items"] == {
        "type": "string"
    }
    assert _writes(datahub._graph)[0]["structuredPropertyInputParams"][0]["values"] == [
        {"numberValue": 30.0}
    ]
    for name in ("add_structured_properties", "remove_structured_properties"):
        tool = tools[name]
        assert tool.annotations is not None
        assert tool.annotations.destructive_hint is True
        assert tool.annotations.idempotent_hint is True
        assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
