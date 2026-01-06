"""Tests for schema assertion input validation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional, Union
from unittest.mock import Mock

import pytest

from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY,
    SCHEMA_FIELD_TYPE_TO_CLASS_MAP,
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaFieldDataType,
    _parse_schema_assertion_compatibility,
    _parse_schema_assertion_fields,
    _parse_schema_field_data_type,
    _SchemaAssertionInput,
)
from acryl_datahub_cloud.sdk.errors import SDKUsageError, SDKUsageErrorWithExamples
from datahub.metadata import schema_classes as models


class TestSchemaFieldDataType:
    """Test suite for SchemaFieldDataType enum parsing."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            pytest.param("STRING", SchemaFieldDataType.STRING, id="string_uppercase"),
            pytest.param("boolean", SchemaFieldDataType.BOOLEAN, id="case_insensitive"),
            pytest.param(
                SchemaFieldDataType.NUMBER,
                SchemaFieldDataType.NUMBER,
                id="enum_input",
            ),
            pytest.param("STRUCT", SchemaFieldDataType.STRUCT, id="complex_type"),
        ],
    )
    def test_parse_schema_field_data_type_success(
        self, input_value: str, expected: SchemaFieldDataType
    ) -> None:
        """Test successful parsing of schema field data types."""
        result = _parse_schema_field_data_type(input_value)
        assert result == expected

    @pytest.mark.parametrize(
        "input_value",
        [
            pytest.param("INVALID", id="invalid_type"),
            pytest.param("VARCHAR", id="sql_type_varchar"),
            pytest.param("INT", id="sql_type_int"),
            pytest.param("", id="empty_string"),
        ],
    )
    def test_parse_schema_field_data_type_error(self, input_value: str) -> None:
        """Test parsing errors for invalid schema field data types."""
        with pytest.raises(SDKUsageErrorWithExamples):
            _parse_schema_field_data_type(input_value)


class TestSchemaAssertionCompatibility:
    """Test suite for SchemaAssertionCompatibility enum parsing."""

    @pytest.mark.parametrize(
        "input_value,expected",
        [
            pytest.param(
                "EXACT_MATCH",
                SchemaAssertionCompatibility.EXACT_MATCH,
                id="string_input",
            ),
            pytest.param(
                SchemaAssertionCompatibility.SUPERSET,
                SchemaAssertionCompatibility.SUPERSET,
                id="enum_input",
            ),
            pytest.param(
                None,
                DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY,
                id="none_returns_default",
            ),
        ],
    )
    def test_parse_schema_assertion_compatibility_success(
        self, input_value: Optional[str], expected: SchemaAssertionCompatibility
    ) -> None:
        """Test successful parsing of schema assertion compatibility."""
        result = _parse_schema_assertion_compatibility(input_value)
        assert result == expected

    @pytest.mark.parametrize(
        "input_value",
        [
            pytest.param("INVALID", id="invalid_compatibility"),
            pytest.param("PARTIAL", id="non_existent_compatibility"),
        ],
    )
    def test_parse_schema_assertion_compatibility_error(self, input_value: str) -> None:
        """Test parsing errors for invalid compatibility values."""
        with pytest.raises(SDKUsageErrorWithExamples):
            _parse_schema_assertion_compatibility(input_value)


class TestSchemaAssertionField:
    """Test suite for SchemaAssertionField dataclass."""

    def test_create_from_dict_minimal(self) -> None:
        """Test creating field from dict with minimal fields."""
        data = {"path": "id", "type": "STRING"}
        field = SchemaAssertionField.from_dict(data)
        assert field.path == "id"
        assert field.type == SchemaFieldDataType.STRING
        assert field.native_type is None

    def test_create_from_dict_with_native_type(self) -> None:
        """Test creating field from dict with native_type."""
        data = {"path": "amount", "type": "NUMBER", "native_type": "DECIMAL(10,2)"}
        field = SchemaAssertionField.from_dict(data)
        assert field.path == "amount"
        assert field.type == SchemaFieldDataType.NUMBER
        assert field.native_type == "DECIMAL(10,2)"

    def test_create_from_dict_with_nativeType_camelCase(self) -> None:
        """Test creating field from dict with nativeType (camelCase)."""
        data = {"path": "id", "type": "STRING", "nativeType": "VARCHAR(255)"}
        field = SchemaAssertionField.from_dict(data)
        assert field.native_type == "VARCHAR(255)"

    def test_create_from_dict_missing_path(self) -> None:
        """Test error when path is missing."""
        data = {"type": "STRING"}
        with pytest.raises(SDKUsageError, match="requires 'path'"):
            SchemaAssertionField.from_dict(data)

    def test_create_from_dict_missing_type(self) -> None:
        """Test error when type is missing."""
        data = {"path": "id"}
        with pytest.raises(SDKUsageError, match="requires 'type'"):
            SchemaAssertionField.from_dict(data)


class TestParseSchemaAssertionFields:
    """Test suite for _parse_schema_assertion_fields function."""

    def test_parse_list_of_dicts(self) -> None:
        """Test parsing a list of dictionaries."""
        fields: list[Union[SchemaAssertionField, dict[Any, Any]]] = [
            {"path": "id", "type": "STRING"},
            {"path": "count", "type": "NUMBER"},
        ]
        result = _parse_schema_assertion_fields(fields)
        assert result is not None
        assert len(result) == 2
        assert result[0].path == "id"
        assert result[0].type == SchemaFieldDataType.STRING
        assert result[1].path == "count"
        assert result[1].type == SchemaFieldDataType.NUMBER

    def test_parse_list_of_field_objects(self) -> None:
        """Test parsing a list of SchemaAssertionField objects."""
        fields: list[Union[SchemaAssertionField, dict[Any, Any]]] = [
            SchemaAssertionField(path="id", type=SchemaFieldDataType.STRING),
            SchemaAssertionField(path="count", type=SchemaFieldDataType.NUMBER),
        ]
        result = _parse_schema_assertion_fields(fields)
        assert result is not None
        assert len(result) == 2
        assert result[0] is fields[0]  # Same object
        assert result[1] is fields[1]

    def test_parse_mixed_list(self) -> None:
        """Test parsing a mixed list of dicts and objects."""
        fields: list[Union[dict, SchemaAssertionField]] = [
            {"path": "id", "type": "STRING"},
            SchemaAssertionField(path="count", type=SchemaFieldDataType.NUMBER),
        ]
        result = _parse_schema_assertion_fields(fields)
        assert result is not None
        assert len(result) == 2
        assert result[0].path == "id"
        assert result[1].path == "count"

    def test_parse_none_returns_none(self) -> None:
        """Test that None input returns None."""
        result = _parse_schema_assertion_fields(None)
        assert result is None

    def test_parse_empty_list_raises_error(self) -> None:
        """Test that empty list raises error."""
        with pytest.raises(SDKUsageError, match="cannot be empty"):
            _parse_schema_assertion_fields([])

    def test_parse_not_a_list_raises_error(self) -> None:
        """Test that non-list input raises error."""
        with pytest.raises(SDKUsageErrorWithExamples, match="must be a list"):
            _parse_schema_assertion_fields("not a list")  # type: ignore


class TestSchemaFieldTypeMapping:
    """Test suite for schema field type to class mapping."""

    def test_all_types_have_mapping(self) -> None:
        """Test that all SchemaFieldDataType values have a corresponding mapping."""
        for field_type in SchemaFieldDataType:
            assert field_type in SCHEMA_FIELD_TYPE_TO_CLASS_MAP, (
                f"Missing mapping for {field_type}"
            )


@dataclass
class SchemaAssertionInputTestParams:
    """Test parameters for schema assertion input validation."""

    compatibility: Optional[str]
    fields: Optional[list[Union[SchemaAssertionField, dict[Any, Any]]]]
    expected_error: Optional[str] = None
    should_succeed: bool = True


class TestSchemaAssertionInput:
    """Test suite for _SchemaAssertionInput validation."""

    @pytest.fixture
    def mock_entity_client(self) -> Mock:
        """Create a mock entity client for testing."""
        return Mock()

    @pytest.fixture
    def base_params(self) -> dict[str, Any]:
        """Base parameters for creating _SchemaAssertionInput instances."""
        return {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)",
            "entity_client": Mock(),
            "created_by": "urn:li:corpuser:test_user",
            "created_at": datetime(2021, 1, 1, tzinfo=timezone.utc),
            "updated_by": "urn:li:corpuser:test_user",
            "updated_at": datetime(2021, 1, 1, tzinfo=timezone.utc),
        }

    @pytest.mark.parametrize(
        "test_params",
        [
            # ============ SUCCESSFUL CASES ============
            pytest.param(
                SchemaAssertionInputTestParams(
                    compatibility="EXACT_MATCH",
                    fields=[{"path": "id", "type": "STRING"}],
                    should_succeed=True,
                ),
                id="exact_match_with_fields",
            ),
            pytest.param(
                SchemaAssertionInputTestParams(
                    compatibility="SUPERSET",
                    fields=[
                        {"path": "id", "type": "STRING"},
                        {"path": "count", "type": "NUMBER"},
                    ],
                    should_succeed=True,
                ),
                id="superset_with_multiple_fields",
            ),
            pytest.param(
                SchemaAssertionInputTestParams(
                    compatibility="SUBSET",
                    fields=[{"path": "id", "type": "STRING"}],
                    should_succeed=True,
                ),
                id="subset_with_fields",
            ),
            pytest.param(
                SchemaAssertionInputTestParams(
                    compatibility=None,  # Should default to EXACT_MATCH
                    fields=[{"path": "id", "type": "STRING"}],
                    should_succeed=True,
                ),
                id="none_compatibility_defaults_to_exact_match",
            ),
        ],
    )
    def test_schema_assertion_input_validation(
        self,
        test_params: SchemaAssertionInputTestParams,
        base_params: dict[str, Any],
    ) -> None:
        """Test schema assertion input validation."""
        if test_params.should_succeed:
            assertion_input = _SchemaAssertionInput(
                **base_params,
                compatibility=test_params.compatibility,
                fields=test_params.fields,
            )
            assert assertion_input is not None
            if test_params.compatibility:
                assert assertion_input.compatibility == SchemaAssertionCompatibility(
                    test_params.compatibility.upper()
                )
            else:
                assert (
                    assertion_input.compatibility
                    == DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY
                )
        else:
            with pytest.raises(SDKUsageError, match=test_params.expected_error or ""):
                _SchemaAssertionInput(
                    **base_params,
                    compatibility=test_params.compatibility,
                    fields=test_params.fields,
                )

    def test_create_assertion_info(self, base_params: dict[str, Any]) -> None:
        """Test creating assertion info from input."""
        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=[
                {"path": "id", "type": "STRING"},
                {"path": "count", "type": "NUMBER"},
            ],
        )
        info = assertion_input._create_assertion_info(filter=None)
        assert isinstance(info, models.SchemaAssertionInfoClass)
        assert info.schema is not None
        assert len(info.schema.fields) == 2
        # Verify compatibility enum value is correctly set
        assert (
            info.compatibility == models.SchemaAssertionCompatibilityClass.EXACT_MATCH
        )

    def test_create_monitor_info(self, base_params: dict[str, Any]) -> None:
        """Test creating monitor info from input."""
        from datahub.metadata.urns import AssertionUrn

        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=[{"path": "id", "type": "STRING"}],
        )
        assertion_urn = AssertionUrn("test_assertion")
        status = models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE)
        schedule = models.CronScheduleClass(cron="0 */6 * * *", timezone="UTC")

        monitor_info = assertion_input._create_monitor_info(
            assertion_urn, status, schedule
        )

        assert isinstance(monitor_info, models.MonitorInfoClass)
        assert monitor_info.type == models.MonitorTypeClass.ASSERTION
        assert monitor_info.assertionMonitor is not None
        params = monitor_info.assertionMonitor.assertions[0].parameters
        assert params is not None
        assert (
            params.type == models.AssertionEvaluationParametersTypeClass.DATASET_SCHEMA
        )
        assert params.datasetSchemaParameters is not None
        assert (
            params.datasetSchemaParameters.sourceType
            == models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA
        )

    def test_source_type_always_datahub_schema(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test that schema assertions always use DATAHUB_SCHEMA as source type."""
        from datahub.metadata.urns import AssertionUrn

        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=[{"path": "id", "type": "STRING"}],
        )

        # Verify _convert_assertion_source_type_and_field returns DATAHUB_SCHEMA
        source_type, field = assertion_input._convert_assertion_source_type_and_field()
        assert source_type == models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA
        assert field is None

        # Verify the source type is used in the monitor info
        assertion_urn = AssertionUrn("test_assertion")
        status = models.MonitorStatusClass(mode=models.MonitorModeClass.ACTIVE)
        schedule = models.CronScheduleClass(cron="0 */6 * * *", timezone="UTC")
        monitor_info = assertion_input._create_monitor_info(
            assertion_urn, status, schedule
        )

        # Confirm the evaluation parameters use DATAHUB_SCHEMA, not any detection mechanism
        assert monitor_info.assertionMonitor is not None
        params = monitor_info.assertionMonitor.assertions[0].parameters
        assert params is not None
        assert params.datasetSchemaParameters is not None
        assert (
            params.datasetSchemaParameters.sourceType
            == models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA
        )

    def test_uses_schema_metadata_detection_mechanism(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test that schema assertions use _SchemaMetadata detection mechanism.

        This ensures schema assertions won't fail validation for platforms like Databricks
        that don't support information_schema detection mechanism.
        """
        from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
            _SchemaMetadata,
        )

        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=[{"path": "id", "type": "STRING"}],
        )

        # Verify the detection mechanism is _SchemaMetadata (not _InformationSchema)
        assert assertion_input.detection_mechanism is not None
        assert isinstance(assertion_input.detection_mechanism, _SchemaMetadata)
        assert assertion_input.detection_mechanism.type == "schema_metadata"

    def test_create_schema_metadata(self, base_params: dict[str, Any]) -> None:
        """Test creating schema metadata from fields."""
        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=[
                {"path": "id", "type": "STRING"},
                {"path": "count", "type": "NUMBER", "native_type": "BIGINT"},
            ],
        )
        schema_metadata = assertion_input._create_schema_metadata()
        assert isinstance(schema_metadata, models.SchemaMetadataClass)
        assert len(schema_metadata.fields) == 2

        # Check first field
        assert schema_metadata.fields[0].fieldPath == "id"
        assert isinstance(schema_metadata.fields[0].type.type, models.StringTypeClass)

        # Check second field with native type
        assert schema_metadata.fields[1].fieldPath == "count"
        assert isinstance(schema_metadata.fields[1].type.type, models.NumberTypeClass)
        assert schema_metadata.fields[1].nativeDataType == "BIGINT"

    def test_fields_required_for_assertion_info(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test that fields are required when creating assertion info."""
        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="EXACT_MATCH",
            fields=None,  # No fields
        )
        with pytest.raises(SDKUsageError, match="requires 'fields'"):
            assertion_input._create_assertion_info(filter=None)

    def test_to_assertion_and_monitor_entities(
        self, base_params: dict[str, Any]
    ) -> None:
        """Test converting input to assertion and monitor entities."""
        assertion_input = _SchemaAssertionInput(
            **base_params,
            compatibility="SUPERSET",
            fields=[{"path": "id", "type": "STRING"}],
            display_name="Test Schema Assertion",
            enabled=True,
        )
        assertion_entity, monitor_entity = (
            assertion_input.to_assertion_and_monitor_entities()
        )

        # Check assertion entity
        assert assertion_entity is not None
        assert assertion_entity.description == "Test Schema Assertion"
        assert isinstance(assertion_entity.info, models.SchemaAssertionInfoClass)

        # Check monitor entity
        assert monitor_entity is not None
        assert monitor_entity.info is not None
        assert monitor_entity.info.status.mode == models.MonitorModeClass.ACTIVE
