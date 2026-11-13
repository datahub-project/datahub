"""create_glossary_term_version, get_glossary_term_versions and
compare_glossary_term_versions.

Most tests serve GraphQL through the caller's graph, so they exercise the default
GraphQLReadBackend as well as the tools.
"""

import os
from typing import Callable, Iterator, List, Optional, cast
from unittest import mock
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from datahub.metadata.schema_classes import VersionSetKeyClass
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import (
    JsonObject,
    ReadBackend,
    ReadBackendBinding,
)
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.glossary_versioning import (
    compare_glossary_term_versions,
    create_glossary_term_version,
    get_glossary_term_versions,
)
from datahub_core_mcp_tools.version_requirements import get_version_requirement

VERSION_SET_URN = "urn:li:versionSet:(kpi-revenue,glossaryTerm)"
TERM_A_URN = "urn:li:glossaryTerm:Revenue"
TERM_B_URN = "urn:li:glossaryTerm:Revenue_v2"
READ_TOOLS = {"get_glossary_term_versions", "compare_glossary_term_versions"}
VERSIONING_TOOLS = {*READ_TOOLS, "create_glossary_term_version"}


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    client._graph.exists.return_value = True
    with with_datahub_client(client):
        yield client._graph


# ---------------------------------------------------------------------------
# get_glossary_term_versions
# ---------------------------------------------------------------------------

VERSIONS_SEARCH_RESPONSE = {
    "versionSet": {
        "urn": VERSION_SET_URN,
        "versionsSearch": {
            "total": 2,
            "searchResults": [
                {
                    "entity": {
                        "urn": TERM_B_URN,
                        "hierarchicalName": "Revenue v2",
                        "properties": {
                            "name": "Revenue v2",
                            "description": "Excluding internal accounts.",
                            "termSource": "INTERNAL",
                        },
                        "versionProperties": {
                            "version": {"versionTag": "v2"},
                            "isLatest": True,
                            "comment": "Q2 policy update.",
                            "created": {"time": 1700000002000},
                        },
                    }
                },
                {
                    "entity": {
                        "urn": TERM_A_URN,
                        "hierarchicalName": "Revenue",
                        "properties": {
                            "name": "Revenue",
                            "description": "Sum of all closed-won deals.",
                            "termSource": "INTERNAL",
                        },
                        "versionProperties": {
                            "version": {"versionTag": "v1"},
                            "isLatest": False,
                            "comment": None,
                            "created": {"time": 1700000001000},
                        },
                    }
                },
            ],
        },
    }
}


def test_versions_by_version_set_urn(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = VERSIONS_SEARCH_RESPONSE

    result = get_glossary_term_versions(VERSION_SET_URN)

    assert result["version_set_urn"] == VERSION_SET_URN
    assert result["total"] == 2
    latest, old = result["versions"]
    assert (latest["urn"], latest["version_tag"], latest["is_latest"]) == (
        TERM_B_URN,
        "v2",
        True,
    )
    assert latest["created_timestamp_ms"] == 1700000002000
    assert (old["version_tag"], old["is_latest"]) == ("v1", False)
    # No term lookup is needed.
    assert graph.execute_graphql.call_count == 1
    assert graph.execute_graphql.call_args.kwargs["operation_name"] == (
        "getVersionSetHistory"
    )


def test_versions_by_term_urn_look_up_its_version_set(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": TERM_B_URN,
                "versionProperties": {
                    "versionSet": {"urn": VERSION_SET_URN},
                    "version": {"versionTag": "v2"},
                    "isLatest": True,
                    "comment": None,
                },
            }
        },
        VERSIONS_SEARCH_RESPONSE,
    ]

    result = get_glossary_term_versions(TERM_B_URN)

    assert result["version_set_urn"] == VERSION_SET_URN
    assert result["total"] == 2
    lookup, history = graph.execute_graphql.call_args_list
    assert lookup.kwargs["operation_name"] == "getTermVersionSetUrn"
    assert lookup.kwargs["variables"] == {"urn": TERM_B_URN}
    assert history.kwargs["variables"]["urn"] == VERSION_SET_URN


def test_an_unversioned_term_is_a_tool_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"entity": {"urn": TERM_A_URN}}

    with pytest.raises(ToolError, match="not versioned"):
        get_glossary_term_versions(TERM_A_URN)


def test_a_missing_term_is_not_found_rather_than_unversioned(graph: MagicMock) -> None:
    graph.exists.return_value = False

    with pytest.raises(ToolError, match="not found"):
        get_glossary_term_versions(TERM_A_URN)
    graph.execute_graphql.assert_not_called()


def test_a_version_set_is_checked_by_its_own_key_aspect(graph: MagicMock) -> None:
    graph.exists.return_value = False
    graph.get_aspect.return_value = None

    with pytest.raises(ToolError, match="not found"):
        get_glossary_term_versions(VERSION_SET_URN)
    assert graph.get_aspect.call_args.args == (VERSION_SET_URN, VersionSetKeyClass)


@pytest.mark.parametrize(
    ("urn", "message"),
    [
        ("urn:li:dataset:(urn:li:dataPlatform:hive,db.t,PROD)", "unsupported URN"),
        ("Revenue", "must be a DataHub URN"),
    ],
)
def test_other_urns_are_tool_errors(graph: MagicMock, urn: str, message: str) -> None:
    with pytest.raises(ToolError, match=message):
        get_glossary_term_versions(urn)
    graph.execute_graphql.assert_not_called()


# ---------------------------------------------------------------------------
# compare_glossary_term_versions
# ---------------------------------------------------------------------------


def _term(urn: str, name: str, description: str, tag: str) -> dict:
    return {
        "urn": urn,
        "hierarchicalName": name,
        "properties": {
            "name": name,
            "description": description,
            "termSource": "INTERNAL",
            "customProperties": [{"key": "unit", "value": "USD"}],
        },
        "ownership": {
            "owners": [
                {
                    "owner": {"urn": "urn:li:corpuser:finance"},
                    "type": "TECHNICAL_OWNER",
                }
            ]
        },
        "tags": {"tags": [{"tag": {"urn": tag}}]},
        "deprecation": None,
        "versionProperties": {
            "versionSet": {"urn": VERSION_SET_URN},
            "version": {"versionTag": "v1" if urn == TERM_A_URN else "v2"},
            "isLatest": urn == TERM_B_URN,
            "comment": None,
        },
    }


TERM_A = _term(TERM_A_URN, "Revenue", "Sum of all closed-won deals.", "urn:li:tag:kpi")
TERM_B = _term(
    TERM_B_URN, "Revenue v2", "Sum excluding internal accounts.", "urn:li:tag:kpi"
)


def test_compare_lists_the_fields_that_differ(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"entities": [TERM_A, TERM_B]}

    result = compare_glossary_term_versions(TERM_A_URN, TERM_B_URN)

    assert result["term_a"]["urn"] == TERM_A_URN
    assert result["term_a"]["version_tag"] == "v1"
    assert result["term_b"]["is_latest"] is True
    assert result["term_b"]["owners"] == [
        {"urn": "urn:li:corpuser:finance", "type": "TECHNICAL_OWNER"}
    ]
    assert result["changes"] == [
        {"field": "name", "value_a": "Revenue", "value_b": "Revenue v2"},
        {
            "field": "definition",
            "value_a": "Sum of all closed-won deals.",
            "value_b": "Sum excluding internal accounts.",
        },
    ]


def test_compare_identical_terms_has_no_changes(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {
        "entities": [TERM_A, {**TERM_A, "urn": TERM_B_URN}]
    }

    assert compare_glossary_term_versions(TERM_A_URN, TERM_B_URN)["changes"] == []


@pytest.mark.parametrize(
    "entities",
    [
        [],
        # entities(urns:) answers a URN GMS has never seen with a bare stub.
        [TERM_A, {"urn": TERM_B_URN}],
    ],
)
def test_compare_a_missing_term_is_a_tool_error(
    graph: MagicMock, entities: list
) -> None:
    graph.execute_graphql.return_value = {"entities": entities}

    with pytest.raises(ToolError, match="was not found"):
        compare_glossary_term_versions(TERM_A_URN, TERM_B_URN)


def test_compare_needs_glossary_term_urns(graph: MagicMock) -> None:
    with pytest.raises(ToolError, match="term_urn_b"):
        compare_glossary_term_versions(TERM_A_URN, VERSION_SET_URN)
    graph.execute_graphql.assert_not_called()


class _TermsBackend:
    """A ReadBackend serving fixed terms, as a host-bound backend would."""

    def __init__(self, terms: List[Optional[JsonObject]]) -> None:
        self.terms = terms

    def glossary_terms(self, urns: list[str]) -> List[Optional[JsonObject]]:
        return self.terms


def test_compare_reads_through_a_bound_read_backend() -> None:
    backend = _TermsBackend([cast(JsonObject, TERM_B), cast(JsonObject, TERM_A)])
    client = MagicMock()
    bound = ReadBackendBinding(cast(ReadBackend, backend))
    with with_datahub_client(client, tool_context=ToolContext([bound])):
        result = compare_glossary_term_versions(TERM_A_URN, TERM_B_URN)

    assert [change["field"] for change in result["changes"]] == ["name", "definition"]
    client._graph.execute_graphql.assert_not_called()


# ---------------------------------------------------------------------------
# create_glossary_term_version
# ---------------------------------------------------------------------------

CREATED = {"createGlossaryTermVersion": {"urn": VERSION_SET_URN}}


def _write_input(graph: MagicMock) -> dict:
    call = graph.execute_graphql.call_args
    assert call.kwargs["operation_name"] == "createGlossaryTermVersion"
    return call.kwargs["variables"]["input"]


def test_create_a_version_in_a_version_set(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = CREATED

    result = create_glossary_term_version(
        name="Revenue v3",
        description="Net of refunds.",
        version="v3",
        version_set_urn=VERSION_SET_URN,
        comment="Refunds policy",
        id="Revenue_v3",
    )

    assert result == {
        "success": True,
        "version_set_urn": VERSION_SET_URN,
        "term_urn": "urn:li:glossaryTerm:Revenue_v3",
    }
    assert _write_input(graph) == {
        "id": "Revenue_v3",
        "name": "Revenue v3",
        "description": "Net of refunds.",
        "version": "v3",
        "versionSet": VERSION_SET_URN,
        "comment": "Refunds policy",
    }
    # The version set is checked by its own key aspect, not DataHubGraph.exists.
    graph.exists.assert_not_called()


def test_create_a_version_after_a_term_names_the_new_term(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = CREATED

    result = create_glossary_term_version(
        name="Revenue v2", description="d", version="v2", previous_term_urn=TERM_A_URN
    )

    sent = _write_input(graph)
    assert sent["previousTermUrn"] == TERM_A_URN
    # An id is always sent, so the new term's URN is known.
    assert result["term_urn"] == f"urn:li:glossaryTerm:{sent['id']}"
    graph.exists.assert_called_once_with(TERM_A_URN)


@pytest.mark.parametrize(
    ("arguments", "error"),
    [
        (
            {"version_set_urn": VERSION_SET_URN, "previous_term_urn": TERM_A_URN},
            "not both",
        ),
        ({"previous_term_urn": "urn:li:tag:Revenue"}, "previous_term_urn"),
        ({"version_set_urn": TERM_A_URN}, "VersionSet URN"),
        ({"version": " "}, "`version` is required"),
        ({"id": "a,b"}, "`id` may not contain"),
    ],
)
def test_invalid_arguments_write_nothing(
    graph: MagicMock, arguments: dict, error: str
) -> None:
    call = {"name": "Revenue v2", "description": "d", "version": "v2", **arguments}

    with pytest.raises(ToolError, match=error):
        create_glossary_term_version(**call)

    graph.execute_graphql.assert_not_called()


def test_a_missing_previous_term_is_not_found(graph: MagicMock) -> None:
    graph.exists.return_value = False

    with pytest.raises(ToolError, match="not found"):
        create_glossary_term_version(
            name="Revenue v2",
            description="d",
            version="v2",
            previous_term_urn=TERM_A_URN,
        )

    graph.execute_graphql.assert_not_called()


def test_a_refused_version_reports_gms_reason(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "The version set already holds an unpublished version."}]
    )

    with pytest.raises(ToolError, match="unpublished version"):
        create_glossary_term_version(
            name="Revenue v2",
            description="d",
            version="v2",
            version_set_urn=VERSION_SET_URN,
        )

    assert graph.execute_graphql.call_count == 1


def test_a_version_without_a_reply_names_the_term_to_check(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ConnectionError("connection reset")

    with pytest.raises(ToolError) as raised:
        create_glossary_term_version(
            name="Revenue v2", description="d", version="v2", id="Revenue_v2"
        )

    assert "may or may not have been applied" in str(raised.value)
    assert "urn:li:glossaryTerm:Revenue_v2" in str(raised.value)


# ---------------------------------------------------------------------------
# Version gate and registration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tool",
    [
        get_glossary_term_versions,
        compare_glossary_term_versions,
        create_glossary_term_version,
    ],
)
def test_needs_a_gms_with_glossary_term_versioning(tool: Callable[..., dict]) -> None:
    requirement = get_version_requirement(tool)
    assert requirement is not None
    assert requirement.oss_min == (1, 10, 0, 0)
    assert requirement.cloud_min == (0, 3, 16, 0)


@pytest.mark.parametrize("value", ["true", "false", None])
async def test_registration_is_gated_by_env_flag(value: Optional[str]) -> None:
    instance = FastMCP("test-versioning")
    env = {"ENTITY_VERSIONING_ENABLED": value} if value is not None else {}
    with mock.patch.dict(os.environ, env):
        if value is None:
            os.environ.pop("ENTITY_VERSIONING_ENABLED", None)
        register_tools(instance, is_oss=True)

    async with Client(instance) as client:
        tools = {t.name: t for t in await client.list_tools()}
    registered = VERSIONING_TOOLS & set(tools)
    assert registered == (VERSIONING_TOOLS if value == "true" else set())
    for name in registered:
        annotations = tools[name].annotations
        assert annotations is not None
        assert annotations.read_only_hint is (name in READ_TOOLS)
        assert annotations.destructive_hint is False
    if value == "true":
        assert (
            "mutation" in tools["create_glossary_term_version"].meta["fastmcp"]["tags"]
        )
