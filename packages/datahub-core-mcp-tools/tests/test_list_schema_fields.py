"""list_schema_fields: ranking, paging, notes and per-column structured properties."""

from collections.abc import Sequence
from copy import deepcopy
from typing import Any, Optional
from unittest.mock import Mock

import pytest
from fastmcp import FastMCP
from fastmcp.exceptions import ToolError, ValidationError
from fastmcp.tools import Tool

from datahub_core_mcp_tools import entity_cleaning, mcp_server
from datahub_core_mcp_tools.schema_matching import FieldText, SubstringMatcher
from datahub_core_mcp_tools.tools import entities

pytestmark = pytest.mark.anyio
URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)"
STEWARD = "urn:li:corpuser:steward"


def _property(name: str, *values: Optional[dict], entities: Any = None) -> dict:
    return {
        "structuredProperty": {
            "urn": f"urn:li:structuredProperty:{name}",
            "definition": {"qualifiedName": name},
        },
        "values": list(values),
        "valueEntities": entities,
    }


@pytest.fixture
def fields() -> list[dict]:
    return [
        {
            "fieldPath": "z_email",
            "description": "email email",
            "label": "Email",
            "schemaFieldEntity": {
                "documentation": {"documentations": [{"documentation": "email"}]}
            },
            "tags": {"tags": [{"tag": {"properties": {"name": "email"}}}]},
            "glossaryTerms": {"terms": [{"term": {"properties": {"name": "email"}}}]},
        },
        {"fieldPath": "a_contact", "description": "email", "label": "person"},
        {"fieldPath": "b_user", "description": "user email"},
        {"fieldPath": "c_other", "description": None},
    ]


@pytest.fixture
def editable_fields() -> list[dict]:
    return []


@pytest.fixture
def backend(fields: list[dict], editable_fields: list[dict]) -> Mock:
    backend = Mock()
    backend.exists.side_effect = lambda urn: urn == URN
    backend.entity.side_effect = lambda urn: {
        "urn": urn,
        "schemaMetadata": {"fields": deepcopy(fields)},
        "editableSchemaMetadata": {
            "editableSchemaFieldInfo": deepcopy(editable_fields)
        },
    }
    return backend


@pytest.fixture
def schema_tool(monkeypatch: pytest.MonkeyPatch, backend: Mock) -> Tool:
    monkeypatch.setattr(entities, "get_read_backend", lambda: backend)
    return Tool.from_function(entities.list_schema_fields)


async def _run(tool: Tool, **args: Any) -> dict:
    return dict((await tool.run({"urn": URN, **args})).structured_content or {})


@pytest.mark.parametrize(
    "keywords,expected,matching",
    [
        (["email"], ["z_email", "a_contact", "b_user", "c_other"], 3),
        (["USER", "email"], ["z_email", "b_user", "a_contact", "c_other"], 3),
        (["user"], ["b_user", "a_contact", "c_other", "z_email"], 1),
    ],
)
async def test_keywords_rank_matches_first_without_filtering(
    schema_tool: Tool, keywords: list[str], expected: list[str], matching: int
) -> None:
    data = await _run(schema_tool, keywords=keywords)

    assert [field["fieldPath"] for field in data["fields"]] == expected
    assert data["matchingCount"] == matching
    assert "note" not in data


def test_substring_scores_count_every_matching_text() -> None:
    fields = [FieldText("z_email", ("email email", "Email", "email"))]

    assert SubstringMatcher().scores(["EMAIL"], fields) == [4.0]
    assert SubstringMatcher().scores([], fields) == [0.0]


async def test_a_host_matcher_drives_ranking_count_and_full_text(
    fields: list[dict], backend: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entities, "get_read_backend", lambda: backend)
    fields[3]["description"] = "Long definition. " * 40

    class ReverseMatcher:
        def scores(
            self, keywords: Sequence[str], fields: Sequence[FieldText]
        ) -> list[float]:
            return [0.0, 0.0, 1.0, 2.0]

    default_server, custom_server = FastMCP("default"), FastMCP("custom")
    mcp_server.register_tools(default_server, is_oss=True)
    mcp_server.register_tools(
        custom_server, is_oss=True, schema_field_matcher=ReverseMatcher()
    )
    default_tool = await default_server.get_tool("list_schema_fields")
    custom_tool = await custom_server.get_tool("list_schema_fields")
    assert default_tool is not None and custom_tool is not None
    assert custom_tool.parameters == default_tool.parameters

    args = {"urn": URN, "keywords": ["accountId"]}
    default = (await default_tool.run(args)).structured_content or {}
    custom = (await custom_tool.run(args)).structured_content or {}

    assert default["matchingCount"] == 0
    assert [f["fieldPath"] for f in custom["fields"]] == [
        "c_other",
        "b_user",
        "a_contact",
        "z_email",
    ]
    assert custom["matchingCount"] == 2
    # A matched column gets its full definition, not the preview.
    assert custom["fields"][0]["description"] == fields[3]["description"].strip()


@pytest.mark.parametrize("budget", [False, True])
async def test_more_matches_name_the_continuation_offset(
    schema_tool: Tool, monkeypatch: pytest.MonkeyPatch, budget: bool
) -> None:
    if budget:
        monkeypatch.setattr(entity_cleaning, "ENTITY_SCHEMA_TOKEN_BUDGET", 1)

    data = await _run(
        schema_tool, keywords=["email"], offset=1, limit=100 if budget else 1
    )

    assert data["returned"] == 1
    assert data["remainingCount"] == 2
    assert data["note"] == "1 more columns matched; call again with offset=2"


async def test_an_offset_past_the_end_is_an_empty_page(schema_tool: Tool) -> None:
    data = await _run(schema_tool, offset=10)

    assert data["fields"] == []
    assert data["remainingCount"] == 0


async def test_no_match_keeps_unfiltered_columns_and_says_ui_edits_are_unsearched(
    schema_tool: Tool, editable_fields: list[dict]
) -> None:
    editable_fields.append(
        {"fieldPath": "c_other", "description": "Gross revenue in USD"}
    )

    data = await _run(schema_tool, keywords=["revenue"])

    assert data["matchingCount"] == 0
    assert data["returned"] == 4
    assert "UI-edited metadata is not searched" in data["note"]


async def test_an_empty_schema_has_its_own_note(
    schema_tool: Tool, fields: list[dict]
) -> None:
    fields.clear()

    data = await _run(schema_tool, keywords=["email"])

    assert data["fields"] == []
    assert data["matchingCount"] is None
    assert data["note"] == "the dataset has no schema fields"


@pytest.mark.parametrize(
    "keywords", ["email,user", ["email,user"], '["email", "user"]']
)
async def test_keyword_coercion_at_the_tool_boundary(
    schema_tool: Tool, keywords: list[str] | str
) -> None:
    data = await _run(schema_tool, keywords=keywords)

    assert data["matchingCount"] == 3


async def test_a_non_string_keyword_is_rejected(schema_tool: Tool) -> None:
    with pytest.raises(ValidationError):
        await schema_tool.run({"urn": URN, "keywords": [123]})


async def test_a_name_or_missing_dataset_is_an_error(schema_tool: Tool) -> None:
    with pytest.raises(ToolError, match="search"):
        await schema_tool.run({"urn": "events"})
    with pytest.raises(ToolError, match="not found"):
        await schema_tool.run({"urn": URN.replace("events", "missing")})


async def test_structured_properties_are_returned_bounded_and_matched_whole(
    schema_tool: Tool, fields: list[dict], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entity_cleaning, "FIELD_STRUCTURED_PROPERTY_CHAR_LIMIT", 30)
    monkeypatch.setattr(entity_cleaning, "FIELD_STRUCTURED_PROPERTY_MAX_VALUES", 2)
    guidance = "Join on account id. " + "padding " * 10 + "never sum refunds"
    fields[3]["schemaFieldEntity"] = {
        "structuredProperties": {
            "properties": [
                _property("ai_context.instructions", {"stringValue": guidance}, None),
                _property("rank", {"numberValue": 0}),
                _property("tiers", *({"stringValue": t} for t in "abc")),
                # A URN value arrives in both lists; it is reported once.
                _property(
                    "steward", {"stringValue": STEWARD}, entities=[{"urn": STEWARD}]
                ),
                _property(
                    "owner_team", entities=[None, {"urn": "urn:li:corpGroup:data"}]
                ),
            ]
        }
    }

    data = await _run(schema_tool, keywords=["refunds"])

    column = data["fields"][0]
    assert column["fieldPath"] == "c_other"
    assert data["matchingCount"] == 1
    properties = {p["name"]: p["values"] for p in column["structuredProperties"]}
    assert properties["ai_context.instructions"][0].startswith("Join on account id.")
    assert properties["ai_context.instructions"][0].endswith("chars]")
    assert properties["rank"] == [0]
    assert properties["tiers"] == ["a", "b"]
    assert properties["steward"] == [STEWARD]
    assert properties["owner_team"] == ["urn:li:corpGroup:data"]


async def test_the_dataset_siblings_are_named(schema_tool: Tool, backend: Mock) -> None:
    dbt = "urn:li:dataset:(urn:li:dataPlatform:dbt,my_db.my_schema.events,PROD)"
    entity = backend.entity.side_effect
    backend.entity.side_effect = lambda urn: {
        **entity(urn),
        "siblings": {"isPrimary": False},
        "siblingsSearch": {
            "searchResults": [
                {
                    "entity": {
                        "urn": dbt,
                        "siblings": {"isPrimary": True},
                        "schemaMetadata": {
                            "fields": [
                                {
                                    "fieldPath": "C_OTHER",
                                    "description": "Android events.",
                                }
                            ]
                        },
                    }
                }
            ]
        },
    }

    data = await _run(schema_tool, keywords=["android"])

    assert data["siblings"] == [dbt]
    assert data["fields"][0] == {
        "fieldPath": "c_other",
        "description": "Android events.",
    }


def test_listing_can_be_switched_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LIST_SCHEMA_FIELDS_ENABLED", "false")

    assert "list_schema_fields" not in mcp_server.register_tools(
        FastMCP("x"), is_oss=True
    )
