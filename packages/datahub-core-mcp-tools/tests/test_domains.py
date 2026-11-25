"""create_domain, set_domains and remove_domains."""

from typing import Iterator, cast
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from datahub.metadata.schema_classes import DomainPropertiesClass
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.domains import (
    create_domain,
    remove_domains,
    set_domains,
)

MARKETING = "urn:li:domain:marketing"
USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
DASHBOARD = "urn:li:dashboard:(looker,sales)"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    # No domain at the id create_domain derives (only the key aspect).
    client._graph.get_entity_semityped.return_value = {}
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


def test_an_unknown_remove_says_why_a_retry_is_safe(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError) as raised:
        remove_domains(entity_urns=[USERS])

    assert "already unset changes nothing" in str(raised.value)
    assert "sets the same domain" not in str(raised.value)


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


# ---------------------------------------------------------------------------
# create_domain
# ---------------------------------------------------------------------------

CONFLICT = GraphError(
    [
        {
            "message": '"Campaigns" already exists in this domain. Please pick a '
            "unique name.",
            "extensions": {"code": 409, "type": "CONFLICT"},
        }
    ]
)


def _listed(*domains: tuple[str, str]) -> dict:
    return {
        "listDomains": {
            "domains": [{"urn": urn, "properties": {"name": n}} for urn, n in domains]
        }
    }


def test_create_a_domain_under_a_parent(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = lambda **kw: {
        "createDomain": "urn:li:domain:" + kw["variables"]["input"]["id"]
    }

    result = create_domain(
        name=" Campaigns ", description="Campaign data", parent_domain=MARKETING
    )

    sent = graph.execute_graphql.call_args.kwargs["variables"]["input"]
    assert result == {"urn": f"urn:li:domain:{sent['id']}", "created": True}
    assert sent["name"] == "Campaigns"
    assert sent["description"] == "Campaign data"
    assert sent["parentDomain"] == MARKETING
    graph.exists.assert_any_call(MARKETING)


def test_a_repeated_create_finds_the_domain_by_its_id(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = lambda **kw: {
        "createDomain": "urn:li:domain:" + kw["variables"]["input"]["id"]
    }
    first = create_domain(name="Campaigns", parent_domain=MARKETING)
    # GMS's name check runs on a search index that lags writes, so the second
    # call must find the domain by a direct read instead.
    graph.get_entity_semityped.return_value = {
        "domainProperties": DomainPropertiesClass(
            name="Campaigns", parentDomain=MARKETING
        )
    }

    second = create_domain(name="Campaigns", parent_domain=MARKETING)

    assert second == {"urn": first["urn"], "created": False}
    assert graph.execute_graphql.call_count == 1
    graph.get_entity_semityped.assert_called_with(first["urn"])


def _domains_at(graph: MagicMock, held: dict[str, tuple[str, str | None]]) -> None:
    """Domains at these URNs (name, parent); every other id is free."""
    graph.get_entity_semityped.side_effect = lambda urn: (
        {
            "domainProperties": DomainPropertiesClass(
                name=held[urn][0], parentDomain=held[urn][1]
            )
        }
        if urn in held
        else {}
    )


def _creates(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = lambda **kw: {
        "createDomain": "urn:li:domain:" + kw["variables"]["input"]["id"]
    }


def test_a_domain_moved_away_from_its_id_does_not_block_a_new_one(
    graph: MagicMock,
) -> None:
    _creates(graph)
    first = create_domain(name="Campaigns", parent_domain=MARKETING)
    _domains_at(graph, {first["urn"]: ("Campaigns", "urn:li:domain:finance")})

    second = create_domain(name="Campaigns", parent_domain=MARKETING)

    assert second["created"] is True
    assert second["urn"] != first["urn"]


def test_a_retry_after_a_moved_domain_returns_the_domain_it_made(
    graph: MagicMock,
) -> None:
    # The first id holds a domain since moved, so the create goes to the next
    # id. A retry after a lost reply must land on that same id, not a new one.
    _creates(graph)
    moved = create_domain(name="Campaigns", parent_domain=MARKETING)["urn"]
    _domains_at(graph, {moved: ("Campaigns", "urn:li:domain:finance")})
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")
    with pytest.raises(ToolError) as raised:
        create_domain(name="Campaigns", parent_domain=MARKETING)
    sent = graph.execute_graphql.call_args.kwargs["variables"]["input"]["id"]
    made = f"urn:li:domain:{sent}"
    assert made != moved
    assert made in str(raised.value)
    _domains_at(
        graph,
        {
            moved: ("Campaigns", "urn:li:domain:finance"),
            made: ("Campaigns", MARKETING),
        },
    )
    calls = graph.execute_graphql.call_count

    retry = create_domain(name="Campaigns", parent_domain=MARKETING)

    assert retry == {"urn": made, "created": False}
    assert graph.execute_graphql.call_count == calls


def test_every_derived_id_taken_is_an_error(graph: MagicMock) -> None:
    graph.get_entity_semityped.side_effect = lambda urn: {
        "domainProperties": DomainPropertiesClass(name="Renamed")
    }

    with pytest.raises(ToolError, match="renamed or moved"):
        create_domain(name="Campaigns")

    graph.execute_graphql.assert_not_called()


ID_TAKEN = GraphError(
    [{"message": "This Domain already exists!", "extensions": {"code": 400}}]
)


def test_a_refused_id_holding_this_domain_returns_it(graph: MagicMock) -> None:
    # An overlapping call made the domain between the read and the write.
    sent: list[str] = []

    def refuse(**kw: object) -> object:
        urn = "urn:li:domain:" + cast(dict, kw["variables"])["input"]["id"]
        sent.append(urn)
        _domains_at(graph, {urn: ("Campaigns", MARKETING)})
        raise ID_TAKEN

    graph.execute_graphql.side_effect = refuse

    result = create_domain(name="Campaigns", parent_domain=MARKETING)

    assert result == {"urn": sent[0], "created": False}


def test_a_refused_id_holding_another_domain_is_an_error(graph: MagicMock) -> None:
    def refuse(**kw: object) -> object:
        urn = "urn:li:domain:" + cast(dict, kw["variables"])["input"]["id"]
        _domains_at(graph, {urn: ("Other", MARKETING)})
        raise ID_TAKEN

    graph.execute_graphql.side_effect = refuse

    with pytest.raises(ToolError, match="already exists"):
        create_domain(name="Campaigns", parent_domain=MARKETING)


def test_a_duplicate_under_the_same_parent_returns_the_existing_domain(
    graph: MagicMock,
) -> None:
    existing = "urn:li:domain:campaigns"
    graph.execute_graphql.side_effect = [
        CONFLICT,
        _listed(
            ("urn:li:domain:campaigns-eu", "Campaigns EU"), (existing, "Campaigns")
        ),
    ]

    result = create_domain(
        name="Campaigns", description="ignored", parent_domain=MARKETING
    )

    assert result["urn"] == existing
    assert result["created"] is False
    assert "description was not applied" in result["note"]
    lookup = graph.execute_graphql.call_args_list[1].kwargs
    assert lookup["operation_name"] == "findDomainsByName"
    assert lookup["variables"]["parentDomain"] == MARKETING
    assert graph.execute_graphql.call_count == 2


def test_a_top_level_duplicate_is_looked_up_among_top_level_domains(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.side_effect = [CONFLICT, _listed((MARKETING, "Marketing"))]

    result = create_domain(name="Marketing")

    assert result == {"urn": MARKETING, "created": False}
    lookup = graph.execute_graphql.call_args_list[1].kwargs["variables"]
    assert lookup["parentDomain"] is None


def test_the_same_name_under_another_parent_is_a_new_domain(graph: MagicMock) -> None:
    # GMS checks names among siblings only, so this create succeeds.
    graph.execute_graphql.return_value = {"createDomain": "urn:li:domain:new"}

    result = create_domain(name="Campaigns", parent_domain="urn:li:domain:finance")

    assert result == {"urn": "urn:li:domain:new", "created": True}
    assert graph.execute_graphql.call_count == 1


def test_a_conflict_whose_domain_is_not_found_is_an_error(graph: MagicMock) -> None:
    # Search matches loosely: a similar name is not the conflicting domain.
    graph.execute_graphql.side_effect = [CONFLICT, _listed((MARKETING, "campaigns"))]

    with pytest.raises(ToolError, match="already exists under .* not found"):
        create_domain(name="Campaigns", parent_domain=MARKETING)


@pytest.mark.parametrize(
    ("name", "parent", "error"),
    [
        ("  ", None, "`name` is required"),
        ("Campaigns", "urn:li:domain:gone", "does not exist"),
        ("Campaigns", "urn:li:tag:Marketing", "must be a domain URN"),
    ],
)
def test_invalid_arguments_create_nothing(
    graph: MagicMock, name: str, parent: str | None, error: str
) -> None:
    graph.exists.return_value = False

    with pytest.raises(ToolError, match=error):
        create_domain(name=name, parent_domain=parent)

    graph.execute_graphql.assert_not_called()


def test_a_create_without_a_reply_names_the_domain_to_check(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError) as raised:
        create_domain(name="Campaigns")

    sent = graph.execute_graphql.call_args.kwargs["variables"]["input"]
    assert "may or may not have been applied" in str(raised.value)
    assert f"urn:li:domain:{sent['id']}" in str(raised.value)
    assert graph.execute_graphql.call_count == 1


def test_other_refusals_reach_the_caller(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "Unauthorized to perform this action.", "extensions": {}}]
    )

    with pytest.raises(ToolError, match="rejected createDomain: Unauthorized"):
        create_domain(name="Campaigns")

    assert graph.execute_graphql.call_count == 1


async def test_create_domain_is_a_safe_write() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tools = {t.name: t for t in await client.list_tools()}

    tool = tools["create_domain"]
    assert tool.annotations is not None
    assert tool.annotations.destructive_hint is False
    assert tool.annotations.idempotent_hint is True
    assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
    assert "create_domain" in (tools["set_domains"].description or "")
