"""create_glossary_term and add_related_terms."""

import uuid
from typing import Iterator
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tools.glossary import (
    add_related_terms,
    create_glossary_term,
)

NODE = "urn:li:glossaryNode:FinancialKPIs"
REVENUE = "urn:li:glossaryTerm:Revenue"
MRR = "urn:li:glossaryTerm:MRR"
ARR = "urn:li:glossaryTerm:ARR"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


def _sent(graph: MagicMock) -> dict:
    assert graph.execute_graphql.call_count == 1
    return graph.execute_graphql.call_args.kwargs


# ---------------------------------------------------------------------------
# create_glossary_term
# ---------------------------------------------------------------------------


def test_create_a_term_under_a_node(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"createGlossaryTerm": REVENUE}

    result = create_glossary_term(
        name="Revenue",
        description="Closed-won deals.",
        parent_node_urn=NODE,
        id="Revenue",
    )

    assert result == {"success": True, "term_urn": REVENUE}
    assert _sent(graph)["variables"] == {
        "input": {
            "id": "Revenue",
            "name": "Revenue",
            "description": "Closed-won deals.",
            "parentNode": NODE,
        }
    }
    graph.exists.assert_called_once_with(NODE)


def test_without_an_id_the_term_still_has_a_known_urn(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError) as raised:
        create_glossary_term(name="Revenue", description="d")

    term_id = _sent(graph)["variables"]["input"]["id"]
    uuid.UUID(term_id)
    assert "may or may not have been applied" in str(raised.value)
    assert f"urn:li:glossaryTerm:{term_id}" in str(raised.value)


@pytest.mark.parametrize(
    ("parent", "exists"),
    [(NODE, False), ("urn:li:tag:FinancialKPIs", True), ("FinancialKPIs", True)],
)
def test_a_parent_that_is_not_an_existing_node_is_refused(
    graph: MagicMock, parent: str, exists: bool
) -> None:
    graph.exists.return_value = exists

    with pytest.raises(ToolError, match="parent_node_urn"):
        create_glossary_term(name="Revenue", description="d", parent_node_urn=parent)

    graph.execute_graphql.assert_not_called()


@pytest.mark.parametrize(("name", "term_id"), [("Revenue", "a,b"), ("  ", None)])
def test_unusable_names_and_ids_are_refused(
    graph: MagicMock, name: str, term_id: str | None
) -> None:
    with pytest.raises(ToolError, match="create_glossary_term"):
        create_glossary_term(name=name, description="d", id=term_id)

    graph.execute_graphql.assert_not_called()


def test_a_duplicate_name_reports_gms_reason(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [
            {
                "message": "Glossary Term with this name already exists at this "
                "level of the Business Glossary"
            }
        ]
    )

    with pytest.raises(ToolError, match="already exists at this level"):
        create_glossary_term(name="Revenue", description="d")

    assert graph.execute_graphql.call_count == 1


# ---------------------------------------------------------------------------
# add_related_terms
# ---------------------------------------------------------------------------


def test_add_is_a_relationships(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"addRelatedTerms": True}

    result = add_related_terms(
        term_urn=MRR, related_term_urns=[REVENUE, ARR, REVENUE], relationship_type="isA"
    )

    assert result == {
        "success": True,
        "term_urn": MRR,
        "related_term_urns": [REVENUE, ARR],
        "relationship_type": "isA",
    }
    assert _sent(graph)["variables"] == {
        "input": {"urn": MRR, "termUrns": [REVENUE, ARR], "relationshipType": "isA"}
    }


@pytest.mark.parametrize(
    ("term", "related", "error"),
    [
        ("MRR", [REVENUE], "Expected the form urn:li:glossaryTerm:"),
        ("urn:li:tag:MRR", [REVENUE], "must be a glossary term URN"),
        (MRR, ["urn:li:tag:Revenue"], r"related_term_urns\[0\]"),
        (MRR, [], "at least one term"),
        (MRR, [MRR], "related to itself"),
    ],
)
def test_invalid_relationships_write_nothing(
    graph: MagicMock, term: str, related: list[str], error: str
) -> None:
    with pytest.raises(ToolError, match=error):
        add_related_terms(
            term_urn=term, related_term_urns=related, relationship_type="isA"
        )

    graph.execute_graphql.assert_not_called()


def test_a_relationship_gms_did_not_add_is_an_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"addRelatedTerms": False}

    with pytest.raises(ToolError, match="did not add"):
        add_related_terms(
            term_urn=MRR, related_term_urns=[REVENUE], relationship_type="hasA"
        )


async def test_relationship_types_offered_on_core() -> None:
    client = MagicMock()
    client._graph.execute_graphql.return_value = {"addRelatedTerms": True}
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(client):
        async with Client(mcp) as mcp_client:
            tools = {t.name: t for t in await mcp_client.list_tools()}
            await mcp_client.call_tool(
                "add_related_terms",
                {
                    "term_urn": MRR,
                    "related_term_urns": REVENUE,
                    "relationship_type": "Inherits",
                },
            )
            cloud_only = await mcp_client.call_tool(
                "add_related_terms",
                {
                    "term_urn": MRR,
                    "related_term_urns": [REVENUE],
                    "relationship_type": "synonymOf",
                },
                raise_on_error=False,
            )

    schema = tools["add_related_terms"].input_schema["properties"]["relationship_type"]
    assert schema["enum"] == ["isA", "hasA"]
    sent = client._graph.execute_graphql.call_args_list
    assert len(sent) == 1
    assert sent[0].kwargs["variables"]["input"]["relationshipType"] == "isA"
    assert cloud_only.is_error
    assert "not available on DataHub Core" in str(cloud_only.content)

    hints = {
        name: (
            tools[name].annotations.destructive_hint,
            tools[name].annotations.idempotent_hint,
        )
        for name in ("create_glossary_term", "add_related_terms")
    }
    assert hints == {
        "create_glossary_term": (False, False),
        "add_related_terms": (False, True),
    }


async def test_relationship_types_offered_on_cloud() -> None:
    client = MagicMock()
    client._graph.execute_graphql.return_value = {"addRelatedTerms": True}
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=False)

    with with_datahub_client(client):
        async with Client(mcp) as mcp_client:
            tools = {t.name: t for t in await mcp_client.list_tools()}
            for kind in ("synonymOf", "Inherits"):
                await mcp_client.call_tool(
                    "add_related_terms",
                    {
                        "term_urn": MRR,
                        "related_term_urns": [REVENUE],
                        "relationship_type": kind,
                    },
                )

    tool = tools["add_related_terms"]
    assert set(tool.input_schema["properties"]["relationship_type"]["enum"]) == {
        "synonymOf",
        "antonymOf",
        "translatesTo",
        "hasValue",
        "isRelatedTo",
        "isA",
        "hasA",
    }
    assert "translatesTo" in (tool.description or "")
    assert [
        call.kwargs["variables"]["input"]["relationshipType"]
        for call in client._graph.execute_graphql.call_args_list
    ] == ["synonymOf", "isA"]
    assert tool.annotations.idempotent_hint is True
