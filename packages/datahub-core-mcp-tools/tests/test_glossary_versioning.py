"""get_glossary_term_versions and compare_glossary_term_versions.

Most tests serve GraphQL through the caller's graph, so they exercise the default
GraphQLReadBackend as well as the tools.
"""

import os
from typing import Callable, Iterator, List, Optional, cast
from unittest import mock
from unittest.mock import MagicMock

import pytest
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
    get_glossary_term_versions,
)
from datahub_core_mcp_tools.version_requirements import get_version_requirement

VERSION_SET_URN = "urn:li:versionSet:(kpi-revenue,glossaryTerm)"
TERM_A_URN = "urn:li:glossaryTerm:Revenue"
TERM_B_URN = "urn:li:glossaryTerm:Revenue_v2"
VERSIONING_TOOLS = {"get_glossary_term_versions", "compare_glossary_term_versions"}


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
# Version gate and registration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tool", [get_glossary_term_versions, compare_glossary_term_versions]
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
        assert annotations is not None and annotations.read_only_hint is True
