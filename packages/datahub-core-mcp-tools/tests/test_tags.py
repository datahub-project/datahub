"""add_tags and remove_tags.

The tools run against a mocked graph, so they exercise the default
GraphQLReadBackend (its existence check) as well as the write.
"""

from typing import Dict, Iterator, List, cast
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
from datahub_core_mcp_tools.tools.tags import add_tags, remove_tags

PII = "urn:li:tag:PII"
SENSITIVE = "urn:li:tag:Sensitive"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
ORDERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.orders,PROD)"
DASHBOARD = "urn:li:dashboard:(looker,sales)"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


def _write(graph: MagicMock) -> dict:
    """The variables of the one mutation the tool sent."""
    assert graph.execute_graphql.call_count == 1
    return graph.execute_graphql.call_args.kwargs["variables"]["input"]


def _serve_schemas(graph: MagicMock, schemas: Dict[str, List[str]]) -> None:
    """Serve each dataset's raw schemaMetadata aspect with these field paths."""
    graph.exists.side_effect = lambda urn: (
        not urn.startswith("urn:li:dataset") or (urn in schemas)
    )
    graph.get_entity_semityped.side_effect = lambda urn: {
        "schemaMetadata": {"fields": [{"fieldPath": path} for path in schemas[urn]]}
    }


def _schema_reads(graph: MagicMock) -> List[str]:
    return [c.args[0] for c in graph.get_entity_semityped.call_args_list]


def test_add_tags_to_assets_and_columns(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchAddTags": True}
    _serve_schemas(graph, {USERS: ["id", "email"]})

    result = add_tags(
        tag_urns=[PII, SENSITIVE],
        entity_urns=[USERS, USERS, ORDERS],
        column_paths=["email", None, ""],
    )

    assert result["success"] is True
    assert result["entities_updated"] == 3
    assert "Successfully added 2 tag(s) to 3 entit(ies)" in result["message"]
    assert "total in scope" in result["message"]
    assert graph.execute_graphql.call_args.kwargs["operation_name"] == "batchAddTags"
    assert _write(graph) == {
        "tagUrns": [PII, SENSITIVE],
        "resources": [
            {
                "resourceUrn": USERS,
                "subResource": "email",
                "subResourceType": "DATASET_FIELD",
            },
            {"resourceUrn": USERS},
            {"resourceUrn": ORDERS},
        ],
    }
    assert {PII, SENSITIVE} <= {c.args[0] for c in graph.exists.call_args_list}
    # Only the dataset with a column is read, and only once.
    assert _schema_reads(graph) == [USERS]


V2_EMAIL = "[version=2.0].[type=string].email"
V2_CITY = "[version=2.0].[type=struct].address.[type=string].city"


@pytest.mark.parametrize(
    ("paths", "column", "expected"),
    [
        # A v1 schema stores the plain name.
        (["id", "email"], "email", "email"),
        # A v2 schema: the name and the exact path both reach the v2 path.
        (["[version=2.0].[type=long].id", V2_EMAIL], "email", V2_EMAIL),
        (["[version=2.0].[type=long].id", V2_EMAIL], V2_EMAIL, V2_EMAIL),
        # A nested field by its dotted name.
        (["[version=2.0].[type=struct].address", V2_CITY], "address.city", V2_CITY),
        # Case is ignored only when no column matches exactly.
        (["EMAIL"], "email", "EMAIL"),
        (["email", "EMAIL"], "EMAIL", "EMAIL"),
    ],
)
def test_a_column_is_written_under_its_schema_field_path(
    graph: MagicMock, paths: List[str], column: str, expected: str
) -> None:
    graph.execute_graphql.return_value = {"batchAddTags": True}
    _serve_schemas(graph, {USERS: paths})

    add_tags(tag_urns=[PII], entity_urns=[USERS], column_paths=[column])

    assert _write(graph)["resources"][0]["subResource"] == expected


@pytest.mark.parametrize(
    ("schemas", "column", "error"),
    [
        # Two branches of a v2 union simplify to the same name.
        (
            {
                USERS: [
                    "[version=2.0].[type=union].[type=A].value",
                    "[version=2.0].[type=union].[type=B].value",
                ]
            },
            "value",
            r"matches 2 columns.*\[type=A\].value.*\[type=B\].value",
        ),
        ({USERS: ["Email", "EMAIL"]}, "email", "matches 2 columns"),
        # A leaf name alone does not pick a nested column, but names it.
        ({USERS: [V2_CITY]}, "city", r"not in the schema.*address.*city"),
        ({USERS: ["id"]}, "email", "not in the schema.*list_schema_fields"),
        ({USERS: []}, "email", "has no schema fields"),
        ({}, "email", "does not exist"),
    ],
)
def test_a_column_that_is_not_one_schema_field_writes_nothing(
    graph: MagicMock, schemas: Dict[str, List[str]], column: str, error: str
) -> None:
    _serve_schemas(graph, schemas)

    with pytest.raises(ToolError, match=error):
        remove_tags(tag_urns=[PII], entity_urns=[USERS], column_paths=[column])

    graph.execute_graphql.assert_not_called()


def test_a_failed_schema_read_writes_nothing(graph: MagicMock) -> None:
    graph.get_entity_semityped.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError, match="could not read the columns"):
        add_tags(tag_urns=[PII], entity_urns=[USERS], column_paths=["email"])

    graph.execute_graphql.assert_not_called()


def test_remove_tags_does_not_require_the_tag_to_exist(graph: MagicMock) -> None:
    # Removing a reference to a deleted tag is how it gets cleaned up.
    graph.execute_graphql.return_value = {"batchRemoveTags": True}
    _serve_schemas(graph, {USERS: ["email"]})
    graph.exists.side_effect = lambda urn: urn == USERS

    result = remove_tags(tag_urns=[PII], entity_urns=[USERS], column_paths=["email"])

    assert "Successfully removed 1 tag(s) from 1 entit(ies)" in result["message"]
    assert graph.execute_graphql.call_args.kwargs["operation_name"] == (
        "batchRemoveTags"
    )
    assert _write(graph)["resources"][0]["subResource"] == "email"


def test_add_tags_refuses_a_tag_that_does_not_exist(graph: MagicMock) -> None:
    graph.exists.side_effect = lambda urn: urn != SENSITIVE

    with pytest.raises(ToolError, match="do not exist.*urn:li:tag:Sensitive"):
        add_tags(tag_urns=[PII, SENSITIVE], entity_urns=[USERS])

    graph.execute_graphql.assert_not_called()


@pytest.mark.parametrize(
    ("tag_urns", "entity_urns", "column_paths", "error"),
    [
        (["PII"], [USERS], None, "must be a tag URN"),
        (["urn:li:glossaryTerm:PII"], [USERS], None, "must be a tag URN"),
        ([], [USERS], None, "at least one tag"),
        ([PII], [], None, "at least one asset"),
        ([PII], [USERS, "users"], None, r"entity_urns\[1\]"),
        ([PII], [USERS, ORDERS], ["email"], "pair one-for-one"),
        ([PII], [DASHBOARD], ["email"], "not a dataset"),
    ],
)
def test_invalid_arguments_write_nothing(
    graph: MagicMock,
    tag_urns: list,
    entity_urns: list,
    column_paths: list | None,
    error: str,
) -> None:
    with pytest.raises(ToolError, match=error):
        add_tags(tag_urns=tag_urns, entity_urns=entity_urns, column_paths=column_paths)

    graph.execute_graphql.assert_not_called()


def test_a_write_gms_did_not_apply_is_an_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchAddTags": False}

    with pytest.raises(ToolError, match="did not apply"):
        add_tags(tag_urns=[PII], entity_urns=[USERS])


def test_a_refused_write_reports_gms_reason_and_is_not_retried(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [
            {
                "message": "Unauthorized to perform this action. Please contact "
                "your DataHub administrator.",
                "extensions": {"code": 403, "type": "UNAUTHORIZED"},
            }
        ]
    )

    with pytest.raises(ToolError, match="rejected batchAddTags: Unauthorized"):
        add_tags(tag_urns=[PII], entity_urns=[USERS])

    assert graph.execute_graphql.call_count == 1


def test_a_write_without_a_reply_has_an_unknown_outcome(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError) as raised:
        remove_tags(tag_urns=[PII], entity_urns=[USERS])

    assert "may or may not have been applied" in str(raised.value)
    assert "get_entities" in str(raised.value)
    assert graph.execute_graphql.call_count == 1


def test_existence_is_read_through_the_bound_backend() -> None:
    client = MagicMock()
    backend = MagicMock(spec=["exists"])
    backend.exists.return_value = False
    bound = ReadBackendBinding(cast(ReadBackend, backend))

    with with_datahub_client(client, tool_context=ToolContext([bound])):
        with pytest.raises(ToolError, match="do not exist"):
            add_tags(tag_urns=[PII], entity_urns=[USERS])

    backend.exists.assert_called_once_with(PII)
    client._graph.exists.assert_not_called()
    client._graph.execute_graphql.assert_not_called()


async def test_list_arguments_sent_as_strings() -> None:
    datahub = MagicMock()
    datahub._graph.exists.return_value = True
    datahub._graph.execute_graphql.return_value = {"batchAddTags": True}
    _serve_schemas(datahub._graph, {USERS: ["email"]})
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(datahub):
        async with Client(mcp) as client:
            tool = {t.name: t for t in await client.list_tools()}["add_tags"]
            result = await client.call_tool(
                "add_tags",
                {
                    "tag_urns": f'["{PII}"]',
                    "entity_urns": USERS,
                    "column_paths": '["email"]',
                },
            )

    for param in ("tag_urns", "entity_urns", "column_paths"):
        schema = tool.input_schema["properties"][param]
        types = {v.get("type") for v in schema.get("anyOf", [schema])}
        assert "array" in types and "string" not in types
    assert result.data["success"] is True
    assert _write(datahub._graph)["resources"] == [
        {
            "resourceUrn": USERS,
            "subResource": "email",
            "subResourceType": "DATASET_FIELD",
        }
    ]


async def test_registered_as_write_tools() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}

    for name, destructive in (("add_tags", False), ("remove_tags", True)):
        tool = tools[name]
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is False
        assert tool.annotations.destructive_hint is destructive
        assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
