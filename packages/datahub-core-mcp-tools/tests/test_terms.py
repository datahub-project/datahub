"""add_terms and remove_terms.

They share their validation and write with add_tags and remove_tags
(label_writes), which test_tags covers in depth; these tests pin what differs
for glossary terms.
"""

from typing import Iterator
from unittest.mock import MagicMock

import pytest
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.terms import add_glossary_terms, remove_glossary_terms

REVENUE = "urn:li:glossaryTerm:Revenue"
EMAIL = "urn:li:glossaryTerm:EmailAddress"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


def test_add_terms_to_an_asset_and_a_column(graph: MagicMock) -> None:
    v2_email = "[version=2.0].[type=string].email"
    graph.get_entity_semityped.return_value = {
        "schemaMetadata": {"fields": [{"fieldPath": v2_email}]}
    }
    graph.execute_graphql.return_value = {"batchAddTerms": True}

    result = add_glossary_terms(
        term_urns=[REVENUE, EMAIL],
        entity_urns=[USERS, USERS],
        column_paths=[None, "email"],
    )

    assert "Successfully added 2 glossary term(s) to 2 entit(ies)" in result["message"]
    graph.get_entity_semityped.assert_called_once_with(USERS)
    call = graph.execute_graphql.call_args.kwargs
    assert call["operation_name"] == "batchAddTerms"
    assert call["variables"]["input"] == {
        "termUrns": [REVENUE, EMAIL],
        "resources": [
            {"resourceUrn": USERS},
            {
                "resourceUrn": USERS,
                "subResource": v2_email,
                "subResourceType": "DATASET_FIELD",
            },
        ],
    }


def test_remove_terms(graph: MagicMock) -> None:
    graph.exists.return_value = False
    graph.execute_graphql.return_value = {"batchRemoveTerms": True}

    result = remove_glossary_terms(term_urns=[REVENUE], entity_urns=[USERS])

    assert (
        "Successfully removed 1 glossary term(s) from 1 entit(ies)" in result["message"]
    )
    assert graph.execute_graphql.call_args.kwargs["operation_name"] == (
        "batchRemoveTerms"
    )


def test_add_terms_refuses_a_term_that_does_not_exist(graph: MagicMock) -> None:
    graph.exists.return_value = False

    with pytest.raises(ToolError, match="glossary terms do not exist.*Revenue"):
        add_glossary_terms(term_urns=[REVENUE], entity_urns=[USERS])

    graph.execute_graphql.assert_not_called()


@pytest.mark.parametrize("term", ["Revenue", "urn:li:tag:Revenue"])
def test_a_term_must_be_a_glossary_term_urn(graph: MagicMock, term: str) -> None:
    with pytest.raises(ToolError, match="must be a glossary term URN.*glossaryTerm"):
        add_glossary_terms(term_urns=[term], entity_urns=[USERS])

    graph.execute_graphql.assert_not_called()


async def test_registered_as_add_terms_and_remove_terms() -> None:
    client = MagicMock()
    client._graph.exists.return_value = True
    client._graph.execute_graphql.return_value = {"batchAddTerms": True}
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(client):
        async with Client(mcp) as mcp_client:
            tools = {t.name: t for t in await mcp_client.list_tools()}
            # Stringified lists, as some clients send them.
            await mcp_client.call_tool(
                "add_terms", {"term_urns": REVENUE, "entity_urns": f'["{USERS}"]'}
            )

    assert client._graph.execute_graphql.call_args.kwargs["variables"]["input"] == {
        "termUrns": [REVENUE],
        "resources": [{"resourceUrn": USERS}],
    }
    for name, destructive in (("add_terms", False), ("remove_terms", True)):
        annotations = tools[name].annotations
        assert annotations is not None
        assert annotations.read_only_hint is False
        assert annotations.destructive_hint is destructive
        assert annotations.idempotent_hint is True
        assert groups_for_tags(tools[name].meta["fastmcp"]["tags"]) == [
            "write_metadata"
        ]
