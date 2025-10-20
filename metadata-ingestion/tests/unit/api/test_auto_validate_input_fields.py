from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.auto_work_units.auto_validate_input_fields import (
    ValidateInputFieldsProcessor,
)
from datahub.ingestion.api.source import SourceReport
from datahub.metadata.schema_classes import (
    InputFieldClass,
    InputFieldsClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
)

DUMMY_CHART_URN = "urn:li:chart:(grafana,dashboard.123)"
DUMMY_DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:grafana,dataset,PROD)"


def test_valid_input_fields_pass_through():
    """Test that valid input fields pass through unchanged."""
    report = SourceReport()
    processor = ValidateInputFieldsProcessor(report)

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

    out = list(processor.validate_input_fields([mcpw.as_workunit()]))

    assert len(out) == 1
    result_aspect = out[0].get_aspect_of_type(InputFieldsClass)
    assert result_aspect is not None
    assert len(result_aspect.fields) == 2
    assert result_aspect.fields[0].schemaField is not None
    assert result_aspect.fields[0].schemaField.fieldPath == "valid_field_1"
    assert result_aspect.fields[1].schemaField is not None
    assert result_aspect.fields[1].schemaField.fieldPath == "valid_field_2"
    assert len(report.warnings) == 0


def test_empty_field_paths_filtered():
    """Test that input fields with empty fieldPath values are filtered out."""
    report = SourceReport()
    processor = ValidateInputFieldsProcessor(report)

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

    out = list(processor.validate_input_fields([mcpw.as_workunit()]))

    assert len(out) == 1
    result_aspect = out[0].get_aspect_of_type(InputFieldsClass)
    assert result_aspect is not None
    assert len(result_aspect.fields) == 1
    assert result_aspect.fields[0].schemaField is not None
    assert result_aspect.fields[0].schemaField.fieldPath == "valid_field"

    # Verify warning was reported
    assert len(report.warnings) == 1
    assert "Invalid input fields filtered" in str(report.warnings)
    # Verify counter was incremented
    assert report.num_input_fields_filtered == 2


def test_all_invalid_fields_skips_workunit():
    """Test that when all fields are invalid, the workunit is not yielded."""
    report = SourceReport()
    processor = ValidateInputFieldsProcessor(report)

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

    out = list(processor.validate_input_fields([mcpw.as_workunit()]))

    # The workunit should not be yielded at all
    assert len(out) == 0

    # Verify warning was reported
    assert len(report.warnings) == 1
    # Verify counter was incremented
    assert report.num_input_fields_filtered == 2


def test_no_input_fields_aspect():
    """Test that workunits without InputFieldsClass pass through unchanged."""
    report = SourceReport()
    processor = ValidateInputFieldsProcessor(report)

    # Create workunit without InputFieldsClass
    from datahub.metadata.schema_classes import StatusClass

    mcpw = MetadataChangeProposalWrapper(
        entityUrn=DUMMY_CHART_URN,
        aspect=StatusClass(removed=False),
    )

    out = list(processor.validate_input_fields([mcpw.as_workunit()]))

    assert len(out) == 1
    assert out[0].get_aspect_of_type(InputFieldsClass) is None
    assert len(report.warnings) == 0
