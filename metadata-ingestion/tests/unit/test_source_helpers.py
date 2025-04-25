from unittest.mock import Mock

import pytest

from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source_helpers import auto_fix_duplicate_schema_field_paths
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


@pytest.fixture
def base_schema_metadata():
    return SchemaMetadataClass(
        schemaName="test_schema",
        platform="my_platform",
        version=1,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[],
    )


@pytest.fixture
def string_field():
    def _create_string_field(field_path=""):
        return SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(type=StringTypeClass()),
            nativeDataType="string",
        )

    return _create_string_field


@pytest.fixture
def dataset_urn():
    return make_dataset_urn("my_platform", "my_dataset")


def test_auto_fix_no_duplicate_schema_field_paths(
    base_schema_metadata, string_field, dataset_urn
):
    """Test that non duplicate field paths are keept."""
    schema_metadata = base_schema_metadata
    field1 = string_field("field1")
    field2 = string_field("field2")
    schema_metadata.fields = [field1, field2]

    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 2


def test_auto_fix_duplicate_schema_field_paths_with_telemetry(
    base_schema_metadata, string_field, dataset_urn
):
    """Test that multiple duplicate field paths are handled correctly, keeping one instance of each unique field."""
    schema_metadata = base_schema_metadata
    field1 = string_field("field1")
    field1_dup = string_field("field1")
    field2 = string_field("field2")
    field2_dup = string_field("field2")
    schema_metadata.fields = [field1, field1_dup, field2, field2_dup]

    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 2


def test_auto_fix_duplicate_schema_field_paths_with_none_fields(
    base_schema_metadata, dataset_urn
):
    """Test that empty fields list is handled correctly without errors."""
    schema_metadata = base_schema_metadata
    schema_metadata.fields = []
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 0


def test_auto_fix_duplicate_schema_field_paths_with_none_fieldpath(
    base_schema_metadata, string_field, dataset_urn
):
    """Test that fields with None as fieldPath are preserved in the output."""
    schema_metadata = base_schema_metadata
    field_none = string_field(None)
    schema_metadata.fields = [field_none]

    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 1
    assert aspect.fields[0].fieldPath is None


def test_auto_fix_duplicate_schema_field_paths_with_invalid_metadata(dataset_urn):
    """Test that non-schema metadata aspects are passed through unchanged."""
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(description="test"),
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    assert isinstance(
        new_wus[0].get_aspect_of_type(DatasetPropertiesClass),
        DatasetPropertiesClass,
    )


def test_auto_fix_duplicate_schema_field_paths_with_empty_fields(
    base_schema_metadata, dataset_urn
):
    """Test that schema metadata with empty fields list is handled correctly."""
    schema_metadata = base_schema_metadata
    schema_metadata.fields = []
    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 0


def test_auto_fix_duplicate_schema_field_paths_error_counting(
    base_schema_metadata, string_field, dataset_urn
):
    """Test that a combination of None fieldPath and duplicate fields are handled correctly."""
    schema_metadata = base_schema_metadata
    field_none = string_field(None)
    field1 = string_field("field1")
    field1_dup = string_field("field1")
    schema_metadata.fields = [field_none, field1, field1_dup]

    wus = [
        MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()
    ]
    new_wus = list(auto_fix_duplicate_schema_field_paths(wus))
    assert len(new_wus) == 1
    aspect = new_wus[0].get_aspect_of_type(SchemaMetadataClass)
    assert aspect is not None
    assert len(aspect.fields) == 2
    fields = aspect.fields
    assert any(f.fieldPath is None for f in fields)
    assert any(f.fieldPath == "field1" for f in fields)


def test_auto_fix_duplicate_schema_field_paths_with_exception():
    """Test that exceptions during processing are logged and propagated."""
    # Create a mock work unit that raises an exception when get_aspect_of_type is called
    mock_work_unit = Mock()
    mock_work_unit.get_aspect_of_type.side_effect = Exception("Test exception")

    # The function should raise an exception when processing invalid data
    with pytest.raises(Exception, match="Error processing schema metadata for"):
        list(auto_fix_duplicate_schema_field_paths([mock_work_unit]))
