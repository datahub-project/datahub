from unittest.mock import MagicMock

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit_processor import WorkunitProcessorContext
from datahub.ingestion.workunit_processors.validate_input_fields import (
    ValidateInputFieldsProcessor,
    ValidateInputFieldsProcessorReport,
)
from datahub.metadata.schema_classes import (
    InputFieldClass,
    InputFieldsClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
)

DUMMY_CHART_URN = "urn:li:chart:(grafana,dashboard.123)"
DUMMY_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:grafana,dataset,PROD)"


@pytest.fixture
def ctx():
    return WorkunitProcessorContext(
        source_report=SourceReport(),
        pipeline_context=MagicMock(),
        source_config=None,
        platform=None,
    )


def test_valid_input_fields_pass_through(ctx):
    """Test that valid input fields pass through unchanged."""
    processor = ValidateInputFieldsProcessor.create(ctx)

    # Create input fields with valid fieldPath
    input_fields = InputFieldsClass(
        fields=[
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="valid_field_1",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},valid_field_1)",
            ),
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="valid_field_2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},valid_field_2)",
            ),
        ]
    )

    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN,
        aspect=input_fields,
    )

    out = list(processor.process([mcpw.as_workunit()]))

    assert len(out) == 1
    result_aspect = out[0].get_aspect_of_type(InputFieldsClass)
    assert result_aspect is not None
    assert len(result_aspect.fields) == 2
    assert result_aspect.fields[0].schemaField is not None
    assert result_aspect.fields[0].schemaField.fieldPath == "valid_field_1"
    assert result_aspect.fields[1].schemaField is not None
    assert result_aspect.fields[1].schemaField.fieldPath == "valid_field_2"
    assert len(ctx.source_report.warnings) == 0


def test_empty_field_paths_filtered(ctx):
    """Test that input fields with empty fieldPath values are filtered out."""
    processor = ValidateInputFieldsProcessor.create(ctx)

    # Create mix of valid and invalid input fields
    input_fields = InputFieldsClass(
        fields=[
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="valid_field",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},valid_field)",
            ),
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="",  # Empty fieldPath
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},)",
            ),
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="   ",  # Whitespace-only fieldPath
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},   )",
            ),
        ]
    )

    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN,
        aspect=input_fields,
    )

    out = list(processor.process([mcpw.as_workunit()]))

    assert len(out) == 1
    result_aspect = out[0].get_aspect_of_type(InputFieldsClass)
    assert result_aspect is not None
    assert len(result_aspect.fields) == 1
    assert result_aspect.fields[0].schemaField is not None
    assert result_aspect.fields[0].schemaField.fieldPath == "valid_field"

    # Verify warning was reported
    assert len(ctx.source_report.warnings) == 1
    assert "Invalid input fields filtered" in str(ctx.source_report.warnings)
    assert isinstance(processor.report, ValidateInputFieldsProcessorReport)
    assert processor.report.num_input_fields_filtered == 2
    assert processor.report.num_workunits_with_invalid_fields == 1


def test_all_invalid_fields_skips_workunit(ctx):
    """Test that when all fields are invalid, the workunit is not yielded."""
    processor = ValidateInputFieldsProcessor.create(ctx)

    # Create only invalid input fields
    input_fields = InputFieldsClass(
        fields=[
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="",  # Empty fieldPath
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},)",
            ),
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="   ",  # Whitespace-only fieldPath
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},   )",
            ),
        ]
    )

    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN,
        aspect=input_fields,
    )

    out = list(processor.process([mcpw.as_workunit()]))

    # The workunit should not be yielded at all
    assert len(out) == 0

    assert len(ctx.source_report.warnings) == 1
    assert isinstance(processor.report, ValidateInputFieldsProcessorReport)
    assert processor.report.num_input_fields_filtered == 2
    assert processor.report.num_workunits_with_invalid_fields == 1
    assert processor.report.num_workunits_skipped_entirely == 1


def test_partial_invalid_does_not_increment_skipped_entirely(ctx):
    # num_workunits_skipped_entirely must stay 0 when at least one valid field remains
    processor = ValidateInputFieldsProcessor.create(ctx)

    input_fields = InputFieldsClass(
        fields=[
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="valid",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},valid)",
            ),
            InputFieldClass(
                schemaField=SchemaFieldClass(
                    fieldPath="",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="number",
                ),
                schemaFieldUrn=f"urn:li:schemaField:({DUMMY_DATASET_URN},)",
            ),
        ]
    )
    wu = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN, aspect=input_fields
    ).as_workunit()
    out = list(processor.process([wu]))

    assert len(out) == 1
    assert isinstance(processor.report, ValidateInputFieldsProcessorReport)
    assert processor.report.num_workunits_skipped_entirely == 0
    assert processor.report.num_input_fields_filtered == 1


def test_no_input_fields_aspect(ctx):
    """Test that workunits without InputFieldsClass pass through unchanged."""
    processor = ValidateInputFieldsProcessor.create(ctx)

    # Create workunit without InputFieldsClass
    from datahub.metadata.schema_classes import StatusClass

    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN,
        aspect=StatusClass(removed=False),
    )

    out = list(processor.process([mcpw.as_workunit()]))

    assert len(out) == 1
    assert out[0].get_aspect_of_type(InputFieldsClass) is None
    assert len(ctx.source_report.warnings) == 0
