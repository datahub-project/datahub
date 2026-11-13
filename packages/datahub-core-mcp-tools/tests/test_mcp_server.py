import asyncio
import json
import pathlib
import threading
from typing import Any, Iterator
from unittest.mock import MagicMock

import pytest
from fastmcp import Client, FastMCP

import datahub_core_mcp_tools
from datahub_core_mcp_tools.graphql_helpers import (
    get_datahub_client,
    with_datahub_client,
)
from datahub_core_mcp_tools.mcp_server import (
    async_background,
    disable_list_changed_capabilities,
    register_tools,
)


async def _tools(mcp: FastMCP) -> dict[str, Any]:
    async with Client(mcp) as client:
        return {tool.name: tool for tool in await client.list_tools()}


async def test_registration_is_per_instance() -> None:
    registered, untouched = FastMCP("a"), FastMCP("b")

    register_tools(registered, is_oss=True)

    assert set(await _tools(registered)) == {
        "get_me",
        "search",
        "get_lineage",
        "get_dataset_queries",
        "get_dataset_usage_history",
        "get_aspect_history",
        "get_entities",
        "list_schema_fields",
        "get_lineage_paths_between",
        "search_documents",
        "grep_documents",
        "grep_entities",
        "create_tag",
        "add_tags",
        "remove_tags",
        "add_terms",
        "remove_terms",
        "create_glossary_term",
        "add_related_terms",
        "create_domain",
        "set_domains",
        "remove_domains",
        "add_owners",
        "remove_owners",
        "update_description",
        "add_structured_properties",
        "remove_structured_properties",
    }
    assert await _tools(untouched) == {}


async def test_registering_twice_on_separate_instances_is_independent() -> None:
    first, second = FastMCP("a"), FastMCP("b")
    register_tools(first, is_oss=True)
    register_tools(second, is_oss=False)

    assert (
        set(await _tools(first))
        == set(await _tools(second))
        == {
            "get_me",
            "search",
            "get_lineage",
            "get_dataset_queries",
            "get_dataset_usage_history",
            "get_aspect_history",
            "get_entities",
            "list_schema_fields",
            "get_lineage_paths_between",
            "search_documents",
            "grep_documents",
            "grep_entities",
            "create_tag",
            "add_tags",
            "remove_tags",
            "add_terms",
            "remove_terms",
            "create_glossary_term",
            "add_related_terms",
            "create_domain",
            "set_domains",
            "remove_domains",
            "add_owners",
            "remove_owners",
            "update_description",
            "add_structured_properties",
            "remove_structured_properties",
        }
    )


async def test_tools_carry_tags_and_explicit_annotations() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    get_me = (await _tools(mcp))["get_me"]

    assert get_me.annotations.read_only_hint is True
    assert get_me.annotations.destructive_hint is False
    assert get_me.annotations.open_world_hint is False
    assert get_me.meta["fastmcp"]["tags"] == ["user"]

    search = (await _tools(mcp))["search"]
    assert search.annotations.read_only_hint is True
    assert search.annotations.destructive_hint is False
    assert set(search.meta["fastmcp"]["tags"]) == {"search", "trust_labeled"}


@pytest.mark.parametrize("value", [None, "true"])
async def test_write_tools_are_registered_by_default(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    if value is None:
        monkeypatch.delenv("TOOLS_IS_MUTATION_ENABLED", raising=False)
    else:
        monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", value)
    mcp = FastMCP("t")

    registered = register_tools(mcp, is_oss=True)

    writes = {
        name
        for name, tool in (await _tools(mcp)).items()
        if "mutation" in tool.meta["fastmcp"]["tags"]
    }
    assert {
        "create_tag",
        "add_tags",
        "remove_tags",
        "add_terms",
        "remove_terms",
        "create_glossary_term",
        "add_related_terms",
        "create_domain",
        "set_domains",
        "remove_domains",
        "add_owners",
        "remove_owners",
        "update_description",
        "add_structured_properties",
        "remove_structured_properties",
    } <= writes
    assert writes <= set(registered)


async def test_writes_switch_off_registers_no_write_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TOOLS_IS_MUTATION_ENABLED", "false")
    # Every gated tool on, so the switch is shown to cover them too.
    monkeypatch.setenv("ENTITY_VERSIONING_ENABLED", "true")
    mcp = FastMCP("t")

    registered = register_tools(mcp, is_oss=True)

    tools = await _tools(mcp)
    assert set(registered) == set(tools)
    assert not [n for n, t in tools.items() if "mutation" in t.meta["fastmcp"]["tags"]]
    assert all(t.annotations.read_only_hint for t in tools.values())
    assert {"search", "get_entities", "get_glossary_term_versions"} <= set(tools)


def _multi_type_unions(node: Any, path: str) -> Iterator[str]:
    """The path of every anyOf/oneOf whose non-null branches differ in type, and
    of every `type` list with more than one non-null type."""
    if isinstance(node, dict):
        declared = node.get("type")
        if isinstance(declared, list) and len([t for t in declared if t != "null"]) > 1:
            yield f"{path}.type: {json.dumps(declared)}"
        for key in ("anyOf", "oneOf"):
            branches = node.get(key)
            if isinstance(branches, list):
                types = {
                    json.dumps(branch.get("type"))
                    for branch in branches
                    if isinstance(branch, dict) and branch.get("type") != "null"
                }
                if len(types) > 1:
                    yield f"{path}.{key}: {json.dumps(branches)}"
        for key, value in node.items():
            yield from _multi_type_unions(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _multi_type_unions(value, f"{path}[{index}]")


async def test_no_tool_parameter_is_a_multi_type_union(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Several OpenAI-compatible endpoints and Gemini reject the whole tool list
    # when one parameter is e.g. anyOf [array, string], and Gemini also rejects
    # {"type": ["string", "number"]}. A nullable anyOf [T, null] is accepted.
    monkeypatch.setenv("ENTITY_VERSIONING_ENABLED", "true")
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    offenders = [
        offender
        for name, tool in (await _tools(mcp)).items()
        for offender in _multi_type_unions(tool.input_schema, name)
    ]

    assert offenders == []


async def test_no_proposal_tools_or_modules() -> None:
    # Proposals have no OSS backend; they live in DataHub Cloud's own package.
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    assert not [name for name in await _tools(mcp) if name.startswith("propose_")]
    package_dir = pathlib.Path(datahub_core_mcp_tools.__file__).parent
    assert not [p for p in package_dir.rglob("*.py") if "propos" in p.name]


@pytest.mark.parametrize("mode", ["legacy", "auto"])
async def test_list_changed_is_not_advertised(mode: str) -> None:
    mcp = FastMCP("t")
    disable_list_changed_capabilities(mcp)

    async with Client(mcp, mode=mode) as client:
        capabilities = client.server_capabilities

    assert capabilities is not None
    assert capabilities.tools is not None
    assert not capabilities.tools.list_changed


async def test_background_tools_see_their_own_callers_client() -> None:
    # Two callers run at once; each tool body must see the client its caller set,
    # even though both run in worker threads from the same pool.
    both_started = threading.Barrier(2, timeout=5)

    def whoami() -> Any:
        both_started.wait()
        return get_datahub_client()

    tool = async_background(whoami)
    alice, bob = MagicMock(), MagicMock()

    async def call_as(client: Any) -> Any:
        with with_datahub_client(client):
            return await tool()

    seen = await asyncio.gather(call_as(alice), call_as(bob))

    assert seen[0] is alice
    assert seen[1] is bob
