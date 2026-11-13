"""set_domains and remove_domains."""

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
from datahub_core_mcp_tools.tools.domains import remove_domains, set_domains

MARKETING = "urn:li:domain:marketing"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
DASHBOARD = "urn:li:dashboard:(looker,sales)"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


def _write(graph: MagicMock) -> dict:
    assert graph.execute_graphql.call_count == 1
    assert graph.execute_graphql.call_args.kwargs["operation_name"] == "batchSetDomain"
    return graph.execute_graphql.call_args.kwargs["variables"]["input"]


def test_set_a_domain_on_assets(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchSetDomain": True}

    result = set_domains(domain_urn=f" {MARKETING} ", entity_urns=[USERS, DASHBOARD])

    assert result["domain_urn"] == MARKETING
    assert result["entities_updated"] == 2
    assert "total in scope" in result["message"]
    assert _write(graph) == {
        "domainUrn": MARKETING,
        "resources": [{"resourceUrn": USERS}, {"resourceUrn": DASHBOARD}],
    }
    graph.exists.assert_called_once_with(MARKETING)


def test_remove_domains_unsets_the_domain(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchSetDomain": True}

    result = remove_domains(entity_urns=[USERS])

    assert result["entities_updated"] == 1
    assert _write(graph) == {"domainUrn": None, "resources": [{"resourceUrn": USERS}]}
    graph.exists.assert_not_called()


@pytest.mark.parametrize(
    ("domain_urn", "entity_urns", "error"),
    [
        ("marketing", [USERS], "must be a DataHub URN"),
        ("urn:li:tag:marketing", [USERS], "must be a domain URN"),
        ("urn:li:domain:gone", [USERS], "domain urn:li:domain:gone does not exist"),
        (MARKETING, [], "at least one asset"),
        (MARKETING, [USERS, "users"], r"entity_urns\[1\]"),
    ],
)
def test_invalid_arguments_write_nothing(
    graph: MagicMock, domain_urn: str, entity_urns: list[str], error: str
) -> None:
    graph.exists.side_effect = lambda urn: urn != "urn:li:domain:gone"

    with pytest.raises(ToolError, match=error):
        set_domains(domain_urn=domain_urn, entity_urns=entity_urns)

    graph.execute_graphql.assert_not_called()


def test_a_write_gms_did_not_apply_is_an_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"batchSetDomain": False}

    with pytest.raises(ToolError, match="did not apply"):
        remove_domains(entity_urns=[USERS])


def test_a_refused_write_reports_gms_reason(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "Unauthorized to perform this action.", "extensions": {}}]
    )

    with pytest.raises(ToolError, match="rejected batchSetDomain: Unauthorized"):
        set_domains(domain_urn=MARKETING, entity_urns=[USERS])

    assert graph.execute_graphql.call_count == 1


def test_a_write_without_a_reply_has_an_unknown_outcome(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError) as raised:
        set_domains(domain_urn=MARKETING, entity_urns=[USERS])

    assert "may or may not have been applied" in str(raised.value)
    assert graph.execute_graphql.call_count == 1


def test_the_domain_is_checked_through_the_bound_backend() -> None:
    client = MagicMock()
    backend = MagicMock(spec=["exists"])
    backend.exists.return_value = False

    with with_datahub_client(
        client,
        tool_context=ToolContext([ReadBackendBinding(cast(ReadBackend, backend))]),
    ):
        with pytest.raises(ToolError, match="does not exist"):
            set_domains(domain_urn=MARKETING, entity_urns=[USERS])

    backend.exists.assert_called_once_with(MARKETING)
    client._graph.execute_graphql.assert_not_called()


async def test_registered_as_write_tools_taking_stringified_lists() -> None:
    datahub = MagicMock()
    datahub._graph.exists.return_value = True
    datahub._graph.execute_graphql.return_value = {"batchSetDomain": True}
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    with with_datahub_client(datahub):
        async with Client(mcp) as client:
            tools = {t.name: t for t in await client.list_tools()}
            result = await client.call_tool(
                "set_domains", {"domain_urn": MARKETING, "entity_urns": USERS}
            )

    assert result.data["entities_updated"] == 1
    for name in ("set_domains", "remove_domains"):
        tool = tools[name]
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is False
        assert tool.annotations.destructive_hint is True
        assert tool.annotations.idempotent_hint is True
        assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
        assert tool.input_schema["properties"]["entity_urns"]["type"] == "array"
