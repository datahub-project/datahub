"""Tests for _get_columns_to_ignore_sampling in ge_data_profiler."""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("great_expectations")

from datahub.ingestion.source.ge_data_profiler import (
    _get_columns_to_ignore_sampling,
    _normalize_tag,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    EditableSchemaFieldInfo,
    EditableSchemaMetadata,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    StringTypeClass,
    TagAssociationClass,
)

DATASET_NAME = "mydb.myschema.mytable"
PLATFORM = "snowflake"
ENV = "PROD"
PII_TAG_URN = "urn:li:tag:PII"
SENSITIVE_TAG_URN = "urn:li:tag:org_common.tag:sensitive"
TAGS_TO_IGNORE = [
    "PII",
    "org_common.tag:sensitive",
]


def _make_field(field_path: str, tag_urns: list) -> SchemaFieldClass:
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag=urn) for urn in tag_urns])
    return SchemaFieldClass(
        fieldPath=field_path,
        type=SchemaFieldDataTypeClass(type=StringTypeClass()),
        nativeDataType="string",
        globalTags=tags if tag_urns else None,
    )


def _make_editable_field(field_path: str, tag_urns: list) -> EditableSchemaFieldInfo:
    tags = GlobalTagsClass(tags=[TagAssociationClass(tag=urn) for urn in tag_urns])
    return EditableSchemaFieldInfo(
        fieldPath=field_path,
        globalTags=tags if tag_urns else None,
    )


def _make_schema_metadata(fields: list) -> SchemaMetadata:
    return SchemaMetadata(
        schemaName="test",
        platform="urn:li:dataPlatform:snowflake",
        version=0,
        hash="",
        platformSchema=SchemalessClass(),
        fields=fields,
    )


def _make_editable_schema_metadata(fields: list) -> EditableSchemaMetadata:
    return EditableSchemaMetadata(editableSchemaFieldInfo=fields)


def _mock_graph(
    dataset_tags=None,
    schema_metadata=None,
    editable_schema_metadata=None,
) -> MagicMock:
    graph = MagicMock()
    graph.get_tags.return_value = dataset_tags

    def get_aspect(entity_urn, aspect_type):
        if aspect_type is SchemaMetadata:
            return schema_metadata
        if aspect_type is EditableSchemaMetadata:
            return editable_schema_metadata
        return None

    graph.get_aspect.side_effect = get_aspect
    return graph


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_schema_metadata_column_tags_ignored(mock_get_graph):
    """Ingestion-sourced column tags in SchemaMetadata are respected."""
    schema_metadata = _make_schema_metadata(
        [
            _make_field("first_name", [PII_TAG_URN]),
            _make_field("last_name", [PII_TAG_URN, SENSITIVE_TAG_URN]),
            _make_field("age", []),
        ]
    )
    mock_get_graph.return_value = _mock_graph(schema_metadata=schema_metadata)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, TAGS_TO_IGNORE, PLATFORM, ENV
    )

    assert not ignore_table
    assert set(columns) == {"first_name", "last_name"}


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_editable_schema_metadata_column_tags_ignored(mock_get_graph):
    """UI-applied column tags in EditableSchemaMetadata are respected."""
    editable = _make_editable_schema_metadata(
        [
            _make_editable_field("email", [PII_TAG_URN]),
            _make_editable_field("signup_date", []),
        ]
    )
    mock_get_graph.return_value = _mock_graph(editable_schema_metadata=editable)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, TAGS_TO_IGNORE, PLATFORM, ENV
    )

    assert not ignore_table
    assert columns == ["email"]


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_schema_and_editable_metadata_no_duplicates(mock_get_graph):
    """A column tagged in both aspects appears only once."""
    schema_metadata = _make_schema_metadata([_make_field("email", [PII_TAG_URN])])
    editable = _make_editable_schema_metadata(
        [_make_editable_field("email", [PII_TAG_URN])]
    )
    mock_get_graph.return_value = _mock_graph(
        schema_metadata=schema_metadata, editable_schema_metadata=editable
    )

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, TAGS_TO_IGNORE, PLATFORM, ENV
    )

    assert columns.count("email") == 1


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_dataset_level_tag_ignores_table(mock_get_graph):
    """A matching tag on the dataset entity sets ignore_table=True."""
    dataset_tags = MagicMock()
    dataset_tags.tags = [TagAssociationClass(tag=PII_TAG_URN)]
    mock_get_graph.return_value = _mock_graph(dataset_tags=dataset_tags)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, TAGS_TO_IGNORE, PLATFORM, ENV
    )

    assert ignore_table
    assert columns == []


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_no_tags_match_returns_empty(mock_get_graph):
    """No columns are ignored when no tags match."""
    schema_metadata = _make_schema_metadata(
        [
            _make_field("id", ["urn:li:tag:some_other_tag"]),
            _make_field("name", []),
        ]
    )
    mock_get_graph.return_value = _mock_graph(schema_metadata=schema_metadata)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, TAGS_TO_IGNORE, PLATFORM, ENV
    )

    assert not ignore_table
    assert columns == []


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_empty_tags_to_ignore_returns_early(mock_get_graph):
    """Empty tags_to_ignore skips graph calls entirely."""
    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME, None, PLATFORM, ENV
    )

    mock_get_graph.assert_not_called()
    assert not ignore_table
    assert columns == []


def test_normalize_tag_strips_urn_prefix():
    assert _normalize_tag("urn:li:tag:my_tag") == "my_tag"
    assert _normalize_tag("my_tag") == "my_tag"
    assert (
        _normalize_tag("urn:li:tag:org_common.tag.gov_privacy_category:identifier")
        == "org_common.tag.gov_privacy_category:identifier"
    )


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_full_urn_in_recipe_matches(mock_get_graph):
    """Full tag URNs in tags_to_ignore_sampling are accepted."""
    schema_metadata = _make_schema_metadata([_make_field("email", [PII_TAG_URN])])
    mock_get_graph.return_value = _mock_graph(schema_metadata=schema_metadata)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME,
        [PII_TAG_URN],  # full URN instead of just the name
        PLATFORM,
        ENV,
    )

    assert not ignore_table
    assert columns == ["email"]


@patch("datahub.ingestion.source.ge_data_profiler.get_default_graph")
def test_mixed_urn_and_name_formats(mock_get_graph):
    """tags_to_ignore_sampling entries may mix full URNs and plain names."""
    schema_metadata = _make_schema_metadata(
        [
            _make_field("email", [PII_TAG_URN]),
            _make_field("last_name", [SENSITIVE_TAG_URN]),
        ]
    )
    mock_get_graph.return_value = _mock_graph(schema_metadata=schema_metadata)

    ignore_table, columns = _get_columns_to_ignore_sampling(
        DATASET_NAME,
        [
            PII_TAG_URN,  # full URN
            "org_common.tag:sensitive",  # plain name
        ],
        PLATFORM,
        ENV,
    )

    assert not ignore_table
    assert set(columns) == {"email", "last_name"}
