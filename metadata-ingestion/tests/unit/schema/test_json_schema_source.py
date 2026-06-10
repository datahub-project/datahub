"""Tests for JsonSchemaSource dataset naming behavior.

Tests the display name generation for JSON schema datasets, covering:
- Default behavior (basename of schema type)
- New dataset_name_strategy options (SCHEMA_ID, TITLE)
- dataset_name_replace_pattern for custom name transformations
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

import datahub.metadata.schema_classes as models
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.schema.json_schema import JsonSchemaSource


def _write_schema(tmp_path: Path, filename: str, schema: Dict) -> Path:
    """Write a JSON schema file and return its path."""
    filepath = tmp_path / filename
    filepath.write_text(json.dumps(schema))
    return filepath


def _get_dataset_properties(
    workunits: List[MetadataWorkUnit],
) -> models.DatasetPropertiesClass:
    """Extract the DatasetPropertiesClass from workunits."""
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, models.DatasetPropertiesClass):
                return wu.metadata.aspect
    raise AssertionError("No DatasetPropertiesClass found in workunits")


def _get_dataset_urn(workunits: List[MetadataWorkUnit]) -> str:
    """Extract the dataset URN from the first workunit."""
    for wu in workunits:
        if isinstance(wu.metadata, MetadataChangeProposalWrapper):
            urn = wu.metadata.entityUrn
            if urn is not None:
                return urn
    raise AssertionError("No MCP found in workunits")


def _run_source(
    tmp_path: Path, config_overrides: Optional[Dict] = None
) -> List[MetadataWorkUnit]:
    """Run JsonSchemaSource and return all workunits."""
    config = {
        "path": str(tmp_path),
        "platform": "test_platform",
        **(config_overrides or {}),
    }
    ctx = PipelineContext(run_id="test")
    source = JsonSchemaSource.create(config, ctx)
    return list(source.get_workunits())


# --- Default behavior (basename) ---


class TestDefaultNaming:
    """Default behavior: display name is basename of schema type."""

    def test_name_from_filename(self, tmp_path: Path) -> None:
        """When no $id, display name comes from filename without .json."""
        _write_schema(
            tmp_path,
            "my-schema.json",
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        )
        workunits = _run_source(tmp_path)
        props = _get_dataset_properties(workunits)
        assert props.name == "my-schema"

    def test_name_from_id_uses_basename(self, tmp_path: Path) -> None:
        """When $id is present, display name is basename of the $id path."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
                "enum": ["a", "b"],
            },
        )
        workunits = _run_source(tmp_path)
        props = _get_dataset_properties(workunits)
        # Current behavior: basename of the $id path → "1.0"
        assert props.name == "1.0"

    def test_urn_contains_full_path(self, tmp_path: Path) -> None:
        """URN should contain the full schema path, not just basename."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
            },
        )
        workunits = _run_source(tmp_path)
        urn = _get_dataset_urn(workunits)
        assert "dev.schema.tui/data-lake/enums/domain/1.0" in urn


# --- New: dataset_name_strategy = SCHEMA_ID ---


class TestSchemaIdNaming:
    """Use the full $id URI as the display name."""

    def test_schema_id_strategy(self, tmp_path: Path) -> None:
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
            },
        )
        workunits = _run_source(tmp_path, {"dataset_name_strategy": "SCHEMA_ID"})
        props = _get_dataset_properties(workunits)
        assert props.name == "https://dev.schema.tui/data-lake/enums/domain/1.0"

    def test_schema_id_fallback_to_filename(self, tmp_path: Path) -> None:
        """When no $id, SCHEMA_ID strategy falls back to filename-based name."""
        _write_schema(
            tmp_path,
            "my-schema.json",
            {
                "type": "object",
                "properties": {"x": {"type": "integer"}},
            },
        )
        workunits = _run_source(tmp_path, {"dataset_name_strategy": "SCHEMA_ID"})
        props = _get_dataset_properties(workunits)
        # No $id → falls back to filename basename
        assert props.name == "my-schema"

    def test_schema_id_does_not_change_urn(self, tmp_path: Path) -> None:
        """URN should be the same regardless of name strategy."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
            },
        )
        default_workunits = _run_source(tmp_path)
        id_workunits = _run_source(tmp_path, {"dataset_name_strategy": "SCHEMA_ID"})
        assert _get_dataset_urn(default_workunits) == _get_dataset_urn(id_workunits)


# --- New: dataset_name_strategy = TITLE ---


class TestTitleNaming:
    """Use the JSON schema 'title' field as the display name."""

    def test_title_strategy(self, tmp_path: Path) -> None:
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "title": "Domain Enum",
                "type": "string",
            },
        )
        workunits = _run_source(tmp_path, {"dataset_name_strategy": "TITLE"})
        props = _get_dataset_properties(workunits)
        assert props.name == "Domain Enum"

    def test_title_fallback_to_basename(self, tmp_path: Path) -> None:
        """When no title, TITLE strategy falls back to basename."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
            },
        )
        workunits = _run_source(tmp_path, {"dataset_name_strategy": "TITLE"})
        props = _get_dataset_properties(workunits)
        # No title → falls back to basename "1.0"
        assert props.name == "1.0"


# --- New: dataset_name_replace_pattern ---


class TestNameReplacePattern:
    """Use match/replace to transform the display name."""

    def test_strip_host_prefix(self, tmp_path: Path) -> None:
        """Strip the host from the schema ID path to get a cleaner name."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://dev.schema.tui/data-lake/enums/domain/1.0",
                "type": "string",
            },
        )
        workunits = _run_source(
            tmp_path,
            {
                "dataset_name_strategy": "SCHEMA_ID",
                "dataset_name_replace_pattern": {
                    "match": "https://dev.schema.tui/",
                    "replace": "",
                },
            },
        )
        props = _get_dataset_properties(workunits)
        assert props.name == "data-lake/enums/domain/1.0"

    def test_replace_pattern_with_default_strategy(self, tmp_path: Path) -> None:
        """Replace pattern works with default BASENAME strategy too."""
        _write_schema(
            tmp_path,
            "my-ugly-schema-name.json",
            {
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        )
        workunits = _run_source(
            tmp_path,
            {
                "dataset_name_replace_pattern": {
                    "match": "-",
                    "replace": " ",
                },
            },
        )
        props = _get_dataset_properties(workunits)
        assert props.name == "my ugly schema name"

    def test_replace_pattern_with_title_strategy(self, tmp_path: Path) -> None:
        """Replace pattern works with TITLE strategy too."""
        _write_schema(
            tmp_path,
            "schema.json",
            {
                "$id": "https://example.com/my-schema/1.0",
                "title": "My Ugly-Schema Name",
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        )
        workunits = _run_source(
            tmp_path,
            {
                "dataset_name_strategy": "TITLE",
                "dataset_name_replace_pattern": {
                    "match": "-",
                    "replace": " ",
                },
            },
        )
        props = _get_dataset_properties(workunits)
        assert props.name == "My Ugly Schema Name"


# --- Mixed schemas in a directory ---


class TestDirectoryWithMixedSchemas:
    """Test directory traversal with schemas that have different naming sources."""

    def test_mixed_schemas_default_strategy(self, tmp_path: Path) -> None:
        """Directory with schemas: one with $id, one without, one with title."""
        _write_schema(
            tmp_path,
            "with-id.json",
            {
                "$id": "https://example.com/schemas/domain/1.0",
                "type": "string",
            },
        )
        _write_schema(
            tmp_path,
            "no-id.json",
            {
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        )
        _write_schema(
            tmp_path,
            "with-title.json",
            {
                "title": "My Title",
                "type": "object",
                "properties": {"y": {"type": "integer"}},
            },
        )
        workunits = _run_source(tmp_path)

        # Collect all display names
        names = []
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if isinstance(wu.metadata.aspect, models.DatasetPropertiesClass):
                    names.append(wu.metadata.aspect.name)

        assert len(names) == 3
        assert "1.0" in names  # basename of $id path
        assert "no-id" in names  # filename without .json
        assert "with-title" in names  # filename without .json (BASENAME ignores title)

    def test_mixed_schemas_schema_id_strategy(self, tmp_path: Path) -> None:
        """SCHEMA_ID strategy: uses $id where available, falls back to basename."""
        _write_schema(
            tmp_path,
            "with-id.json",
            {
                "$id": "https://example.com/schemas/domain/1.0",
                "type": "string",
            },
        )
        _write_schema(
            tmp_path,
            "no-id.json",
            {
                "type": "object",
                "properties": {"x": {"type": "string"}},
            },
        )
        workunits = _run_source(tmp_path, {"dataset_name_strategy": "SCHEMA_ID"})

        names = []
        for wu in workunits:
            if isinstance(wu.metadata, MetadataChangeProposalWrapper):
                if isinstance(wu.metadata.aspect, models.DatasetPropertiesClass):
                    names.append(wu.metadata.aspect.name)

        assert len(names) == 2
        assert "https://example.com/schemas/domain/1.0" in names
        assert "no-id" in names  # fallback to basename
