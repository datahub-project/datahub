"""grep_entities: regex search over one entity's string metadata, in bounded pages."""

import importlib
import json
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TagAssociationClass,
)
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError, ValidationError
from fastmcp.tools import Tool

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import async_background, register_tools
from datahub_core_mcp_tools.tools.grep_entities import grep_entities

pytestmark = pytest.mark.anyio


def _schema_aspect(*fields: tuple[str, str]) -> dict:
    return SchemaMetadataClass(
        schemaName="test",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=MySqlDDLClass(tableSchema=""),
        fields=[
            SchemaFieldClass(
                fieldPath=name,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
                description=desc,
            )
            for name, desc in fields
        ],
    ).to_obj()


class _FakeReadBackend:
    def __init__(self, *, exists: bool = True, aspects: dict | None = None):
        self._exists = exists
        self._aspects = aspects
        self.aspects_called = False

    def exists(self, urn: str) -> bool:
        return self._exists

    def aspects(self, urn: str) -> dict | None:
        self.aspects_called = True
        return self._aspects


DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.wide_table,PROD)"


class TestGrepEntities:
    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_finds_column_in_schema(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "schemaMetadata": _schema_aspect(
                    ("zip_code", "Property ZIP code"),
                    ("tenant_name", "Primary tenant"),
                    ("created_at", "Row creation timestamp"),
                ),
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="(?i)zip|tenant"
        )
        snippets = " ".join(m["snippet"] for m in result["matches"])
        assert "zip_code" in snippets
        assert "tenant_name" in snippets
        assert result["matchCount"] >= 2

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_finds_tag_by_name(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "globalTags": GlobalTagsClass(
                    tags=[TagAssociationClass(tag="urn:li:tag:pii")],
                ).to_obj(),
            },
        )
        result = await async_background(grep_entities)(urn=DATASET_URN, pattern="pii")
        assert result["matchCount"] >= 1
        assert "pii" in result["matches"][0]["snippet"]

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_no_matches_returns_empty(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "datasetProperties": DatasetPropertiesClass(
                    description="Quarterly revenue data",
                ).to_obj()
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="nonexistent_xyzzy"
        )
        assert result["matches"] == []
        assert result["matchCount"] == 0

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_empty_pattern_returns_early(self, mock_get_backend):
        backend = _FakeReadBackend(exists=True)
        mock_get_backend.return_value = backend
        result = await async_background(grep_entities)(urn=DATASET_URN, pattern="   ")
        assert result["matches"] == []
        assert not backend.aspects_called

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_case_sensitive_by_default(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "datasetProperties": DatasetPropertiesClass(
                    description="Contains PII Data Elements",
                ).to_obj()
            },
        )
        assert (await async_background(grep_entities)(urn=DATASET_URN, pattern="pii"))[
            "matchCount"
        ] == 0
        assert (
            await async_background(grep_entities)(urn=DATASET_URN, pattern="(?i)pii")
        )["matchCount"] >= 1

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    @pytest.mark.parametrize("pattern", ["(unclosed", "a{2000}("])
    async def test_invalid_regex_is_mcp_tool_error(
        self, mock_get_backend: MagicMock, pattern: str
    ) -> None:
        server = FastMCP("grep-test")
        server.add_tool(Tool.from_function(grep_entities))
        async with Client(server) as client:
            result = await client.call_tool(
                "grep_entities",
                {"urn": DATASET_URN, "pattern": pattern},
                raise_on_error=False,
            )
        assert result.is_error
        assert result.content
        mock_get_backend.assert_not_called()

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_fetch_failure_raises_tool_error(
        self, mock_get_backend: MagicMock
    ) -> None:
        failure = TimeoutError("backend unavailable")
        mock_get_backend.return_value.exists.return_value = True
        mock_get_backend.return_value.aspects.side_effect = failure
        with pytest.raises(ToolError) as exc:
            await async_background(grep_entities)(urn=DATASET_URN, pattern="needle")
        assert exc.value.__cause__ is failure

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_nonexistent_entity_raises(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(exists=False)
        with pytest.raises(ToolError, match="not found"):
            await async_background(grep_entities)(urn=DATASET_URN, pattern="x")

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_budget_truncates_and_caps_size(self, mock_get_backend, monkeypatch):
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 500)
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={"a": {"text": "match " * 500}},
        )
        result = await async_background(grep_entities)(urn=DATASET_URN, pattern="match")
        assert "next_offset" in result
        assert len(json.dumps(result)) <= 500

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_budget_caps_single_long_value(self, mock_get_backend, monkeypatch):
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 1000)
        long_body = "x" * 400_000 + "FINDME" + "y" * 400_000
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={"documentInfo": {"contents": {"text": long_body}}},
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="FINDME"
        )
        assert result["matchCount"] >= 1
        assert len(result["matches"][0]["snippet"]) <= 1200
        assert "FINDME" in result["matches"][0]["snippet"]

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_pagination_with_start_offset(self, mock_get_backend, monkeypatch):
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 500)
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "aspect_a": {"text": "match " * 200},
                "aspect_b": {"text": "match here too"},
            },
        )
        first = await async_background(grep_entities)(urn=DATASET_URN, pattern="match")
        assert "next_offset" in first

        second = await async_background(grep_entities)(
            urn=DATASET_URN,
            pattern="match",
            start_offset=first["next_offset"],
        )
        assert second["matchCount"] >= 1
        assert second["matches"][0]["snippet_start"] >= first["next_offset"]

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    @pytest.mark.parametrize(
        "aspects,budget,context_chars,max_pages,max_matches",
        [
            ({"a": {"text": "hello world " * 100}}, 500, 120, 20, 20),
            ({"a": {"long_path_" * 40: "first\n\nlast\n" * 100}}, 2000, 0, 10, 20),
            (
                {"documentInfo": {"contents": {"text": "x" * 400_000}}},
                60_000,
                120,
                8,
                20,
            ),
            ({"a": {"text": ("x" * 200 + "\n") * 4000}}, 60_000, 120, 16, 20),
            ({"a": {"text": 'quote "slash\\😀\n' * 1000}}, 1600, 0, 40, 20),
            (
                {
                    "schemaMetadata": _schema_aspect(
                        *[(f"col_{i}", "tenant " * 20) for i in range(500)]
                    )
                },
                60_000,
                120,
                100,
                20,
            ),
            (
                {
                    "schemaMetadata": _schema_aspect(
                        *[(f"col_{i}", "x") for i in range(500)]
                    )
                },
                60_000,
                120,
                6,
                1000,
            ),
        ],
    )
    async def test_full_read_with_dotstar(
        self,
        mock_get_backend: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        aspects: dict[str, object],
        budget: int,
        context_chars: int,
        max_pages: int,
        max_matches: int,
    ) -> None:
        """Paging with .* reads the entire entity."""
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", budget)
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects=aspects,
        )
        stream = mod._build_stream(aspects)
        offset = 0
        covered_until = 0
        for _ in range(max_pages):
            result = await async_background(grep_entities)(
                urn=DATASET_URN,
                pattern=".*",
                start_offset=offset,
                context_chars=context_chars,
                max_matches=max_matches,
            )
            assert len(json.dumps(result)) <= budget
            assert len(result["matches"]) <= max_matches
            for match in result["matches"]:
                assert match["snippet_start"] == covered_until
                snippet = match["snippet"]
                if covered_until:
                    snippet = snippet[3:]
                if snippet.endswith("..."):
                    snippet = snippet[:-3]
                assert snippet
                assert stream[covered_until : covered_until + len(snippet)] == snippet
                covered_until += len(snippet)
            if "next_offset" not in result:
                break
            assert result["next_offset"] == covered_until > offset
            offset = result["next_offset"]
        assert covered_until == result["streamLength"] == len(stream)

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    @pytest.mark.parametrize(
        "text,pattern,hit_count",
        [
            (
                "The event describes a user action with related metadata. " * 15000,
                "(?i)the",
                15000,
            ),
            ("needle " * 120000, "needle", 120000),
        ],
    )
    async def test_dense_separated_hits_remain_limited(
        self, mock_get_backend: MagicMock, text: str, pattern: str, hit_count: int
    ) -> None:
        mock_get_backend.return_value = _FakeReadBackend(aspects={"a": {"text": text}})
        result = await async_background(grep_entities)(urn=DATASET_URN, pattern=pattern)
        assert len(result["matches"]) == 20
        assert result["matchCount"] == hit_count
        assert len(json.dumps(result)) < 10000
        assert "next_offset" in result

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_defers_match_when_only_leading_context_fits(
        self, mock_get_backend: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 850)
        mock_get_backend.return_value = _FakeReadBackend(
            aspects={
                "a": {"text": "A" * 200 + "needle" + "A" * 200},
                "b": {"text": "B" * 200 + "needle" + "B" * 200},
            }
        )
        first = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="needle", context_chars=200
        )
        assert len(first["matches"]) == 1
        assert first["matchCount"] == 2
        second = await async_background(grep_entities)(
            urn=DATASET_URN,
            pattern="needle",
            context_chars=200,
            start_offset=first["next_offset"],
        )
        assert second["matches"][0]["snippet"] == (
            "..." + "B" * 200 + "needle" + "B" * 200
        )
        assert second["matchCount"] == 1
        assert "next_offset" not in second
        assert all(len(json.dumps(page)) <= 850 for page in (first, second))

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_contiguous_hits_share_a_snippet_without_exceeding_the_cap(
        self, mock_get_backend: MagicMock
    ) -> None:
        mock_get_backend.return_value = _FakeReadBackend(
            aspects={"a": {"text": "needle" * 30}, "b": {"text": "needle"}}
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="needle", context_chars=0, max_matches=1
        )
        assert len(result["matches"]) == 1
        assert result["matches"][0]["snippet"].count("needle") == 30
        assert result["matches"][0]["path"] == "a.text"
        assert result["matchCount"] == 31
        assert "next_offset" in result

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    @pytest.mark.parametrize("oversized_pattern", [False, True])
    async def test_unusable_page_raises_bounded_tool_error(
        self,
        mock_get_backend: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        oversized_pattern: bool,
    ) -> None:
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 500)
        mock_get_backend.return_value = _FakeReadBackend(
            aspects={"a": {"long_path" * 100: "needle"}}
        )
        with pytest.raises(ToolError) as exc:
            await async_background(grep_entities)(
                urn=DATASET_URN,
                pattern="needle" * 100 if oversized_pattern else "needle",
            )
        assert len(str(exc.value)) < 500

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    @pytest.mark.parametrize("pattern", ["START.*END", "^START.*END$"])
    async def test_continuation_preserves_match_spanning_cursor(
        self, mock_get_backend: MagicMock, pattern: str
    ) -> None:
        body = "START " + "x" * 80_000 + " END"
        aspects = {"documentInfo": {"contents": {"text": body}}}
        mock_get_backend.return_value = _FakeReadBackend(aspects=aspects)
        first = await async_background(grep_entities)(urn=DATASET_URN, pattern=pattern)
        second = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=pattern, start_offset=first["next_offset"]
        )
        assert second["matches"][0]["snippet"].endswith(" END")
        assert second["matches"][0]["position"] == first["matches"][0]["position"]
        assert second["matches"][0]["snippet_start"] == first["next_offset"]
        assert "next_offset" not in second

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_short_literal_spanning_cursor_is_resumed(
        self, mock_get_backend: MagicMock
    ) -> None:
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        aspects = {"a": {"text": "needle"}}
        mock_get_backend.return_value = _FakeReadBackend(aspects=aspects)
        offset = mod._build_stream(aspects).index("needle") + 1
        second = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="needle", context_chars=0, start_offset=offset
        )
        assert second["matches"][0]["snippet"] == "...eedle"
        assert "next_offset" not in second

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_serialized_budget_and_match_limit_page_all_hits(
        self, mock_get_backend: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        budget = 1600
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", budget)
        aspects = {
            "a": {f"field_{i:02d}_" + "長" * 15: 'needle "\\\n😀' for i in range(45)}
        }
        mock_get_backend.return_value = _FakeReadBackend(aspects=aspects)
        paths = set()
        offset = 0
        for _ in range(100):
            result = await async_background(grep_entities)(
                urn=DATASET_URN,
                pattern="(?s).*",
                context_chars=0,
                start_offset=offset,
                max_matches=2,
            )
            assert len(json.dumps(result)) <= budget
            assert 0 < len(result["matches"]) <= 2
            for match in result["matches"]:
                assert match["snippet"].strip(".")
                paths.add(match["path"])
            if "next_offset" not in result:
                break
            assert result["next_offset"] > offset
            offset = result["next_offset"]
        assert len(paths) == 45

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_default_match_limit(self, mock_get_backend: MagicMock) -> None:
        mock_get_backend.return_value = _FakeReadBackend(
            aspects={"a": {f"field_{i:02d}": "needle" for i in range(30)}}
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="needle", context_chars=0
        )
        assert len(result["matches"]) == 20
        assert result["matchCount"] == 30
        assert "next_offset" in result

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_offset_aliases_and_limits_through_mcp(
        self, mock_get_backend: MagicMock
    ) -> None:
        mock_get_backend.return_value = _FakeReadBackend(
            aspects={"a": {"text": "needle"}}
        )
        tool = Tool.from_function(grep_entities)
        for offset_name in ("start_offset", "offset", "start"):
            result = await tool.run(
                {"urn": DATASET_URN, "pattern": "needle", offset_name: 100}
            )
            assert result.structured_content is not None
            assert result.structured_content["matches"] == []
        for invalid in (
            {"start_offset": -1},
            {"context_chars": -1},
            {"max_matches": 0},
        ):
            with pytest.raises(ValidationError):
                await tool.run({"urn": DATASET_URN, "pattern": "needle", **invalid})

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_stream_order_is_deterministic(self, mock_get_backend):
        padding = "x" * 200
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "z_aspect": {"text": f"{padding} FINDLAST {padding}"},
                "a_aspect": {"text": f"{padding} FINDFIRST {padding}"},
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN,
            pattern="FINDFIRST|FINDLAST",
            context_chars=50,
        )
        assert len(result["matches"]) == 2
        first_pos = result["matches"][0]["position"]
        second_pos = result["matches"][1]["position"]
        assert "FINDFIRST" in result["matches"][0]["snippet"]
        assert "FINDLAST" in result["matches"][1]["snippet"]
        assert first_pos < second_pos

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_unknown_aspect_searched(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={"someNewAspect": {"nested": {"key": "target value here"}}},
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="target"
        )
        assert result["matchCount"] == 1

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_path_names_field_for_match_deep_in_long_value(
        self, mock_get_backend
    ):
        # The match sits far from both ends of a long value, so no path prefix
        # falls inside the snippet window.
        padding = "Identifier kept stable across renames. " * 30
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "schemaMetadata": _schema_aspect(
                    ("col_a", padding + "Generic." + padding),
                    ("col_b", padding + "Holds the customer email." + padding),
                    ("col_c", padding + "Generic." + padding),
                ),
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="customer email"
        )
        assert (
            result["matches"][0]["path"]
            == "schemaMetadata.fields[fieldPath=col_b].description"
        )

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_path_names_matching_item_not_the_next_one(self, mock_get_backend):
        # description is the last key of each item, so the line after the match
        # belongs to the next item.
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "editableSchemaMetadata": {
                    "editableSchemaFieldInfo": [
                        {
                            "fieldPath": "col_a",
                            "description": "Holds the customer email",
                        },
                        {"fieldPath": "col_b", "description": "Generic"},
                    ]
                },
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="customer email"
        )
        assert (
            result["matches"][0]["path"]
            == "editableSchemaMetadata.editableSchemaFieldInfo[fieldPath=col_a].description"
        )

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_path_on_page_that_starts_mid_value(
        self, mock_get_backend, monkeypatch
    ):
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 500)
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={"documentInfo": {"contents": {"text": "tenant " * 1000}}},
        )
        first = await async_background(grep_entities)(urn=DATASET_URN, pattern="tenant")
        second = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="tenant", start_offset=first["next_offset"]
        )
        assert second["matches"][0]["path"] == "documentInfo.contents.text"

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_no_next_offset_when_everything_was_shown(self, mock_get_backend):
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={
                "schemaMetadata": _schema_aspect(
                    *[
                        (f"col_{i}", "tenant attribute" if i == 3 else f"column {i}")
                        for i in range(300)
                    ]
                ),
            },
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="tenant"
        )
        assert len(result["matches"]) == 1
        assert "next_offset" not in result


def _three_columns() -> _FakeReadBackend:
    return _FakeReadBackend(
        exists=True,
        aspects={
            "schemaMetadata": _schema_aspect(
                ("zip_code", "Property ZIP code"),
                ("tenant_name", "Primary tenant"),
                ("created_at", "Row creation timestamp"),
            ),
        },
    )


class TestLineFallback:
    """A pattern that matches no value is retried against whole "path: value" lines."""

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_value_hit_keeps_value_search(self, mock_get_backend):
        # "zip" is also in the column's paths; only the value hits count.
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="zip", context_chars=0
        )
        assert "searched" not in result
        assert result["matchCount"] == 1
        assert result["matches"][0]["path"] == (
            "schemaMetadata.fields[fieldPath=zip_code].fieldPath"
        )
        assert result["matches"][0]["snippet"].endswith(": zip...")

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_mixed_pattern_with_a_value_hit_ignores_paths(self, mock_get_backend):
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=r"fieldPath=created_at|Primary"
        )
        assert "searched" not in result
        assert result["matchCount"] == 1
        assert result["matches"][0]["path"] == (
            "schemaMetadata.fields[fieldPath=tenant_name].description"
        )

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_path_pattern_matches_lines(self, mock_get_backend):
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN,
            pattern=r"fieldPath=(zip_code|created_at)\]\.fieldPath",
            context_chars=0,
        )
        assert result["searched"] == "lines"
        assert result["matchCount"] == 2
        assert [m["path"] for m in result["matches"]] == [
            "schemaMetadata.fields[fieldPath=zip_code].fieldPath",
            "schemaMetadata.fields[fieldPath=created_at].fieldPath",
        ]

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_rendered_line_pattern_matches(self, mock_get_backend):
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=r"\]\.fieldPath: tenant_name$", context_chars=0
        )
        assert result["searched"] == "lines"
        assert result["matchCount"] == 1
        assert result["matches"][0]["path"] == (
            "schemaMetadata.fields[fieldPath=tenant_name].fieldPath"
        )
        assert result["matches"][0]["snippet"] == "...].fieldPath: tenant_name..."

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_value_hit_before_cursor_keeps_value_search(self, mock_get_backend):
        # The mode is decided over the whole stream, not from the cursor onward.
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        backend = _three_columns()
        mock_get_backend.return_value = backend
        cursor = mod._build_stream(backend.aspects(DATASET_URN)).index(
            "schemaMetadata.fields[fieldPath=tenant_name]"
        )
        result = await async_background(grep_entities)(
            urn=DATASET_URN,
            pattern=r"zip_code|fieldPath=created_at\]",
            start_offset=cursor,
        )
        assert "searched" not in result
        assert result["matchCount"] == 0

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_hits_in_one_line_stay_separate(self, mock_get_backend):
        # "fields[fieldPath=zip_code].fieldPath" holds two hits in one line; gaps
        # between them are measured in the searched line, not the value.
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="fieldPath", context_chars=0
        )
        assert result["searched"] == "lines"
        assert len(result["matches"]) == result["matchCount"] == 12

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_line_anchors_are_line_start_and_value_end(self, mock_get_backend):
        mock_get_backend.return_value = _three_columns()
        at_line_start = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=r"^schemaMetadata\.fields\[fieldPath=zip_code\]"
        )
        assert at_line_start["searched"] == "lines"
        assert at_line_start["matchCount"] >= 2
        assert all(
            m["path"].startswith("schemaMetadata.fields[fieldPath=zip_code]")
            for m in at_line_start["matches"]
        )
        # ^ is the start of the line, not of the bracketed key inside the path.
        mid_path = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=r"^fieldPath=zip_code"
        )
        assert mid_path["searched"] == "lines"
        assert mid_path["matchCount"] == 0
        # $ is the end of the value, so a path-only tail never ends a line.
        path_tail = await async_background(grep_entities)(
            urn=DATASET_URN, pattern=r"\[fieldPath=zip_code\]$"
        )
        assert path_tail["matchCount"] == 0

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_miss_everywhere_reports_line_search(self, mock_get_backend):
        mock_get_backend.return_value = _three_columns()
        result = await async_background(grep_entities)(
            urn=DATASET_URN, pattern="nonexistent_xyzzy"
        )
        assert result["searched"] == "lines"
        assert result["matches"] == []
        assert result["matchCount"] == 0

    @patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
    async def test_line_search_pages_consistently(self, mock_get_backend, monkeypatch):
        mod = importlib.import_module("datahub_core_mcp_tools.tools.grep_entities")
        monkeypatch.setattr(mod, "GREP_OUTPUT_CHAR_BUDGET", 3000)
        names = [f"col_{i}" for i in range(120)]
        mock_get_backend.return_value = _FakeReadBackend(
            exists=True,
            aspects={"schemaMetadata": _schema_aspect(*[(n, "x") for n in names])},
        )
        pattern = r"fieldPath=col_\d+\]\.fieldPath"
        seen: list[str] = []
        positions: list[int] = []
        cursor = 0
        first_count = None
        for _ in range(200):
            page = await async_background(grep_entities)(
                urn=DATASET_URN,
                pattern=pattern,
                start_offset=cursor,
                context_chars=0,
                max_matches=7,
            )
            assert page["searched"] == "lines"
            if first_count is None:
                first_count = page["matchCount"]
            seen += [m["path"] for m in page["matches"]]
            positions += [m["position"] for m in page["matches"]]
            if "next_offset" not in page:
                break
            assert page["next_offset"] > cursor
            cursor = page["next_offset"]
        assert first_count == len(names)
        assert positions == sorted(set(positions))
        assert seen == [
            f"schemaMetadata.fields[fieldPath={n}].fieldPath" for n in names
        ]


@pytest.mark.parametrize("value, served", [(None, True), ("false", False)])
async def test_registration_is_gated_by_env_flag(
    monkeypatch: pytest.MonkeyPatch, value: str | None, served: bool
) -> None:
    if value is None:
        monkeypatch.delenv("GREP_ENTITIES_ENABLED", raising=False)
    else:
        monkeypatch.setenv("GREP_ENTITIES_ENABLED", value)
    server = FastMCP("test")
    register_tools(server, is_oss=True)
    async with Client(server) as client:
        names = {tool.name for tool in await client.list_tools()}
    assert ("grep_entities" in names) is served


@patch("datahub_core_mcp_tools.tools.grep_entities.get_read_backend")
async def test_registered_tool_is_read_only_and_answers(
    mock_get_backend: MagicMock,
) -> None:
    mock_get_backend.return_value = _three_columns()
    server = FastMCP("test")
    register_tools(server, is_oss=True)
    with with_datahub_client(MagicMock()):
        async with Client(server) as client:
            tool = next(
                t for t in await client.list_tools() if t.name == "grep_entities"
            )
            result = await client.call_tool(
                "grep_entities", {"dataset_urn": DATASET_URN, "pattern": "(?i)zip"}
            )
    assert tool.annotations is not None and tool.annotations.read_only_hint is True
    assert result.structured_content is not None
    assert result.structured_content["matchCount"] == 2
