"""add_owners and remove_owners."""

from typing import Iterator
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.owners import add_owners, remove_owners

JOHN = "urn:li:corpuser:john.doe"
ENGINEERING = "urn:li:corpGroup:data-engineering"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
DASHBOARD = "urn:li:dashboard:(looker,sales)"
TECHNICAL = "urn:li:ownershipType:__system__technical_owner"
PRODUCER = "urn:li:ownershipType:4a1b2c"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


def _write(graph: MagicMock) -> dict:
    write = graph.execute_graphql.call_args_list[-1].kwargs
    assert write["operation_name"] in ("batchAddOwners", "batchRemoveOwners")
    return write["variables"]["input"]


def _types_listed(*types: tuple[str, str]) -> dict:
    return {
        "listOwnershipTypes": {
            "ownershipTypes": [
                {"urn": urn, "info": {"name": name}} for urn, name in types
            ]
        }
    }


def test_add_a_user_and_a_group_to_assets(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchAddOwners": True}

    result = add_owners(
        owner_urns=[JOHN, ENGINEERING, JOHN],
        entity_urns=[USERS, DASHBOARD],
        ownership_type="TECHNICAL_OWNER",
    )

    assert result["entities_updated"] == 2
    assert "Successfully added 2 owner(s) to 2 entit(ies)" in result["message"]
    assert "total in scope" in result["message"]
    assert _write(graph) == {
        "owners": [
            {
                "ownerUrn": JOHN,
                "ownerEntityType": "CORP_USER",
                "ownershipTypeUrn": TECHNICAL,
            },
            {
                "ownerUrn": ENGINEERING,
                "ownerEntityType": "CORP_GROUP",
                "ownershipTypeUrn": TECHNICAL,
            },
        ],
        "ownershipTypeUrn": TECHNICAL,
        "resources": [{"resourceUrn": USERS}, {"resourceUrn": DASHBOARD}],
    }
    assert {c.args[0] for c in graph.exists.call_args_list} == {JOHN, ENGINEERING}


@pytest.mark.parametrize(
    "ownership_type",
    [
        "BUSINESS_OWNER",
        "business owner",
        "__system__business_owner",
        "urn:li:ownershipType:__system__business_owner",
    ],
)
def test_built_in_types_resolve_without_a_lookup(
    graph: MagicMock, ownership_type: str
) -> None:
    graph.execute_graphql.return_value = {"batchAddOwners": True}

    add_owners(owner_urns=[JOHN], entity_urns=[USERS], ownership_type=ownership_type)

    assert graph.execute_graphql.call_count == 1
    assert _write(graph)["ownershipTypeUrn"] == (
        "urn:li:ownershipType:__system__business_owner"
    )


def test_a_custom_type_is_found_by_its_exact_name(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = [
        _types_listed(
            ("urn:li:ownershipType:9z", "Producer Lead"), (PRODUCER, "Producer")
        ),
        {"batchAddOwners": True},
    ]

    add_owners(owner_urns=[JOHN], entity_urns=[USERS], ownership_type="producer")

    assert _write(graph)["ownershipTypeUrn"] == PRODUCER


@pytest.mark.parametrize(
    ("listed", "error"),
    [
        (_types_listed(("urn:li:ownershipType:9z", "Producer Lead")), "was not found"),
        (
            _types_listed(
                (PRODUCER, "Producer"), ("urn:li:ownershipType:9z", "producer")
            ),
            "matches several types",
        ),
    ],
)
def test_an_unknown_or_ambiguous_custom_type_writes_nothing(
    graph: MagicMock, listed: dict, error: str
) -> None:
    graph.execute_graphql.return_value = listed

    with pytest.raises(ToolError, match=error):
        add_owners(owner_urns=[JOHN], entity_urns=[USERS], ownership_type="Producer")

    assert graph.execute_graphql.call_count == 1


def test_remove_owners_of_any_type(graph: MagicMock) -> None:
    # Removing a deleted user's ownership is how it gets cleaned up.
    graph.exists.return_value = False
    graph.execute_graphql.return_value = {"batchRemoveOwners": True}

    result = remove_owners(owner_urns=[JOHN], entity_urns=[USERS])

    assert "Successfully removed 1 owner(s) from 1 entit(ies)" in result["message"]
    assert _write(graph) == {"ownerUrns": [JOHN], "resources": [{"resourceUrn": USERS}]}


@pytest.mark.parametrize("ownership_type", ["", "  "])
def test_a_blank_type_on_remove_means_any_type(
    graph: MagicMock, ownership_type: str
) -> None:
    graph.execute_graphql.return_value = {"batchRemoveOwners": True}

    remove_owners(owner_urns=[JOHN], entity_urns=[USERS], ownership_type=ownership_type)

    assert _write(graph) == {"ownerUrns": [JOHN], "resources": [{"resourceUrn": USERS}]}


def test_remove_one_ownership_type(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchRemoveOwners": True}

    remove_owners(owner_urns=[JOHN], entity_urns=[USERS], ownership_type="DATA_STEWARD")

    assert _write(graph)["ownershipTypeUrn"] == (
        "urn:li:ownershipType:__system__data_steward"
    )


@pytest.mark.parametrize(
    ("owners", "entities", "ownership_type", "error"),
    [
        ([], [USERS], "TECHNICAL_OWNER", "at least one user or group"),
        (["john.doe"], [USERS], "TECHNICAL_OWNER", "must be user"),
        ([USERS], [USERS], "TECHNICAL_OWNER", "must be user"),
        (["urn:li:corpuser:gone"], [USERS], "TECHNICAL_OWNER", "do not exist"),
        ([JOHN], [], "TECHNICAL_OWNER", "at least one asset"),
        ([JOHN], ["users"], "TECHNICAL_OWNER", r"entity_urns\[0\]"),
        ([JOHN], [USERS], "  ", "must name an ownership type"),
    ],
)
def test_invalid_arguments_write_nothing(
    graph: MagicMock,
    owners: list[str],
    entities: list[str],
    ownership_type: str,
    error: str,
) -> None:
    graph.exists.side_effect = lambda urn: urn != "urn:li:corpuser:gone"

    with pytest.raises(ToolError, match=error):
        add_owners(
            owner_urns=owners, entity_urns=entities, ownership_type=ownership_type
        )

    graph.execute_graphql.assert_not_called()


def test_a_write_gms_did_not_apply_is_an_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchAddOwners": False}

    with pytest.raises(ToolError, match="did not apply"):
        add_owners(
            owner_urns=[JOHN], entity_urns=[USERS], ownership_type="DATA_STEWARD"
        )


def test_a_refused_write_reports_gms_reason(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "Unauthorized to perform this action.", "extensions": {}}]
    )

    with pytest.raises(ToolError, match="rejected batchRemoveOwners: Unauthorized"):
        remove_owners(owner_urns=[JOHN], entity_urns=[USERS])

    assert graph.execute_graphql.call_count == 1


def test_a_write_without_a_reply_has_an_unknown_outcome(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError) as raised:
        add_owners(
            owner_urns=[JOHN], entity_urns=[USERS], ownership_type="DATA_STEWARD"
        )

    assert "may or may not have been applied" in str(raised.value)
    assert graph.execute_graphql.call_count == 1


async def test_published_schema_and_annotations() -> None:
    datahub = MagicMock()
    datahub._graph.exists.return_value = True
    datahub._graph.execute_graphql.return_value = {"batchAddOwners": True}
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(datahub):
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
            result = await client.call_tool(
                "add_owners",
                {
                    "owner_urns": JOHN,
                    "entity_urns": f'["{USERS}"]',
                    "ownership_type": "TECHNICAL_OWNER",
                },
            )

    assert result.data["entities_updated"] == 1
    # One JSON type, not anyOf [enum, string]: strict function-calling APIs
    # (Gemini, several OpenAI-compatible endpoints) reject multi-type unions.
    add_schema = tools["add_owners"].input_schema["properties"]["ownership_type"]
    assert add_schema["type"] == "string"
    assert "anyOf" not in add_schema
    remove_schema = tools["remove_owners"].input_schema["properties"]["ownership_type"]
    assert {b.get("type") for b in remove_schema["anyOf"]} == {"string", "null"}
    for name, destructive in (("add_owners", False), ("remove_owners", True)):
        tool = tools[name]
        assert tool.annotations is not None
        assert tool.annotations.destructive_hint is destructive
        assert tool.annotations.idempotent_hint is True
        assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
