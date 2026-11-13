import dataclasses
from typing import Iterable, Optional

import pytest
from fastmcp import Client, FastMCP

from datahub_core_mcp_tools.mcp_instructions import (
    INTRO,
    InstructionSize,
    build_instructions,
)
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.tool_groups import (
    TOOL_REF_TYPE_GROUP,
    TOOL_REF_TYPE_TOOL,
    ToolRef,
    groups_for_tags,
    is_tool_selected,
    resolve_tool_refs,
)


@dataclasses.dataclass
class _Tool:
    name: str
    tags: Optional[Iterable[str]]


AVAILABLE = [
    _Tool("search", {"search"}),
    _Tool("get_lineage", {"search"}),
    _Tool("add_tags", {"mutation"}),
    _Tool("remove_tags", {"mutation"}),
    _Tool("get_me", {"user"}),
    _Tool("validate_recipe", {"ingestion"}),
]


def group(group_id: str, enabled: bool = True) -> ToolRef:
    return ToolRef(TOOL_REF_TYPE_GROUP, group_id, enabled)


def tool(name: str, enabled: bool = True) -> ToolRef:
    return ToolRef(TOOL_REF_TYPE_TOOL, name, enabled)


def test_groups_for_tags() -> None:
    assert groups_for_tags({"search"}) == ["read_metadata"]
    assert groups_for_tags({"read_only"}) == ["read_metadata"]
    assert groups_for_tags({"mutation"}) == ["write_metadata"]
    assert groups_for_tags({"ingestion"}) == ["ingestion"]
    assert groups_for_tags({"user"}) == []


@pytest.mark.parametrize(
    ("refs", "expected"),
    [
        (None, None),
        ([], None),
        ([group("read_metadata")], {"search", "get_lineage"}),
        ([group("read_metadata"), tool("get_lineage", False)], {"search"}),
        (
            [group("write_metadata"), tool("search")],
            {"add_tags", "remove_tags", "search"},
        ),
        ([group("read_metadata"), group("read_metadata", False)], set()),
        ([group("no_such_group")], set()),
    ],
)
def test_resolve_tool_refs(
    refs: Optional[list[ToolRef]], expected: Optional[set]
) -> None:
    resolved = resolve_tool_refs(refs, AVAILABLE)

    assert resolved == (None if expected is None else frozenset(expected))


@pytest.mark.parametrize(
    ("refs", "served"),
    [
        # "All tools" never includes an opt-in group.
        (None, {"search", "get_lineage", "add_tags", "remove_tags", "get_me"}),
        ([], {"search", "get_lineage", "add_tags", "remove_tags", "get_me"}),
        (
            [group("read_metadata"), group("write_metadata")],
            {"search", "get_lineage", "add_tags", "remove_tags", "get_me"},
        ),
        ([group("ingestion")], {"validate_recipe", "get_me"}),
        ([tool("validate_recipe")], {"validate_recipe", "get_me"}),
        ([group("ingestion"), tool("validate_recipe", False)], {"get_me"}),
    ],
)
def test_opt_in_groups_are_served_only_when_selected(
    refs: Optional[list[ToolRef]], served: set[str]
) -> None:
    assert {t.name for t in AVAILABLE if is_tool_selected(t, refs)} == served


READ_TOOLS = {
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
}
WRITE_TOOLS = {
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
    "save_document",
    "update_document",
}
INGESTION_TOOLS = {"list_connector_types", "get_recipe_schema", "validate_recipe"}


@pytest.mark.parametrize(
    ("groups", "expected"),
    [
        (None, {"get_me", *READ_TOOLS, *WRITE_TOOLS}),
        ([], {"get_me"}),
        (["write_metadata"], {"get_me", *WRITE_TOOLS}),
        (["read_metadata"], {"get_me", *READ_TOOLS}),
        (["ingestion"], {"get_me", *INGESTION_TOOLS}),
    ],
)
async def test_register_tools_by_group_always_keeps_system_tools(
    groups: Optional[list[str]], expected: set[str]
) -> None:
    mcp = FastMCP("t")

    registered = register_tools(mcp, is_oss=True, groups=groups)

    async with Client(mcp) as client:
        assert (
            {t.name for t in await client.list_tools()} == set(registered) == expected
        )


def test_instruction_sizes() -> None:
    served = ["search", "get_entities", "get_me"]

    assert build_instructions(InstructionSize.NONE, served) is None
    assert build_instructions(InstructionSize.SMALL, served) == INTRO

    large = build_instructions(InstructionSize.LARGE, served)
    assert large is not None and large.startswith(INTRO)
    assert "`search`, then `get_entities`" in large
    assert "empty search" in large
    assert len(large) < 2048


def test_alternative_tools_are_offered_as_choices() -> None:
    large = build_instructions(
        InstructionSize.LARGE, ["update_description", "add_tags", "add_owners"]
    )

    assert large is not None
    assert "`update_description`, `add_tags`, or `add_owners`" in large
    assert "then `add_tags`" not in large


def test_large_instructions_name_only_served_tools() -> None:
    large = build_instructions(InstructionSize.LARGE, ["get_me"])

    assert large is not None
    assert "`get_me`" in large
    for absent in ("search", "get_lineage", "add_tags", "empty search"):
        assert absent not in large


def test_custom_instructions_come_last() -> None:
    assert build_instructions(InstructionSize.NONE, [], "  Be brief.  ") == "Be brief."
    small = build_instructions(InstructionSize.SMALL, [], "Be brief.")
    assert small == f"{INTRO}\n\nBe brief."


async def test_ingestion_kill_switch_beats_the_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGESTION_TOOLS_ENABLED", "false")
    mcp = FastMCP("t")

    registered = register_tools(mcp, is_oss=True, groups=["ingestion"])

    assert registered == ["get_me"]
