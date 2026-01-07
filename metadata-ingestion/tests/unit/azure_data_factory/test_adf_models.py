"""Unit tests for Azure Data Factory Pydantic models.

Tests for the Azure API response normalization validators that handle
the Azure API quirk of returning {} instead of null or [] for empty fields.
"""

from datahub.ingestion.source.azure_data_factory.adf_models import (
    Activity,
    DataFlowProperties,
    DatasetProperties,
    LinkedServiceProperties,
    PipelineProperties,
    TriggerProperties,
    _normalize_empty_dict_to_list,
    _normalize_schema_field,
)


class TestAzureApiNormalizationHelpers:
    """Tests for the Azure API normalization helper functions."""

    def test_normalize_empty_dict_to_list_with_empty_dict(self) -> None:
        """Empty dict {} should be converted to empty list []."""
        assert _normalize_empty_dict_to_list({}) == []

    def test_normalize_empty_dict_to_list_with_actual_list(self) -> None:
        """Actual lists should pass through unchanged."""
        test_list = [{"name": "col1", "type": "string"}]
        assert _normalize_empty_dict_to_list(test_list) == test_list

    def test_normalize_empty_dict_to_list_with_none(self) -> None:
        """None should pass through unchanged."""
        assert _normalize_empty_dict_to_list(None) is None

    def test_normalize_empty_dict_to_list_with_non_empty_dict(self) -> None:
        """Non-empty dicts should pass through (for other validators to handle)."""
        non_empty = {"key": "value"}
        assert _normalize_empty_dict_to_list(non_empty) == non_empty

    def test_normalize_schema_field_with_empty_dict(self) -> None:
        """Empty dict {} should be converted to None."""
        assert _normalize_schema_field({}) is None

    def test_normalize_schema_field_with_list(self) -> None:
        """Actual lists (column definitions) should pass through."""
        test_list = [{"name": "col1", "type": "string"}]
        assert _normalize_schema_field(test_list) == test_list

    def test_normalize_schema_field_with_none(self) -> None:
        """None should pass through unchanged."""
        assert _normalize_schema_field(None) is None

    def test_normalize_schema_field_with_json_schema(self) -> None:
        """JSON Schema objects should be converted to None."""
        json_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
            },
        }
        assert _normalize_schema_field(json_schema) is None

    def test_normalize_schema_field_with_expression(self) -> None:
        """Expression objects should be converted to None."""
        expression = {"type": "Expression", "value": "@dataset().schema"}
        assert _normalize_schema_field(expression) is None


class TestDatasetPropertiesEmptyDictHandling:
    """Tests for DatasetProperties handling of Azure API {} quirk.

    This is the confirmed production issue where Azure returns {} for
    schema and structure fields instead of null or [].
    """

    def test_schema_definition_accepts_empty_dict(self) -> None:
        """Empty dict {} for schema should be normalized to None."""
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "schema": {},  # Azure API quirk: {} instead of null
            }
        )
        assert props.schema_definition is None

    def test_schema_definition_accepts_valid_list(self) -> None:
        """Valid schema list should be parsed correctly."""
        schema_cols = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ]
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "schema": schema_cols,
            }
        )
        assert props.schema_definition is not None
        assert len(props.schema_definition) == 2

    def test_schema_definition_accepts_null(self) -> None:
        """Explicit null for schema should remain None."""
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "schema": None,
            }
        )
        assert props.schema_definition is None

    def test_structure_accepts_empty_dict(self) -> None:
        """Empty dict {} for structure should be normalized to None."""
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "structure": {},  # Azure API quirk
            }
        )
        assert props.structure is None

    def test_schema_definition_accepts_json_schema_object(self) -> None:
        """JSON Schema object for schema should be normalized to None.

        Azure API can return schema as a JSON Schema descriptor instead of
        column array. This is the production case from Brookfield.
        """
        json_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "data": {
                    "type": "object",
                    "properties": {"nested": {"type": "string"}},
                },
            },
        }
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "schema": json_schema,  # JSON Schema format, not column array
            }
        )
        assert props.schema_definition is None

    def test_schema_definition_accepts_expression(self) -> None:
        """Expression object for schema should be normalized to None."""
        expression = {"type": "Expression", "value": "@dataset().schema"}
        props = DatasetProperties.model_validate(
            {
                "linkedServiceName": {
                    "referenceName": "test-ls",
                    "type": "LinkedServiceReference",
                },
                "type": "AzureBlobDataset",
                "schema": expression,  # Expression format
            }
        )
        assert props.schema_definition is None


class TestActivityEmptyDictHandling:
    """Tests for Activity model handling of Azure API {} quirk."""

    def test_depends_on_accepts_empty_dict(self) -> None:
        """Empty dict {} for dependsOn should be normalized to []."""
        activity = Activity.model_validate(
            {
                "name": "TestActivity",
                "type": "Copy",
                "dependsOn": {},  # Azure API quirk
            }
        )
        assert activity.depends_on == []

    def test_inputs_accepts_empty_dict(self) -> None:
        """Empty dict {} for inputs should be normalized to []."""
        activity = Activity.model_validate(
            {
                "name": "TestActivity",
                "type": "Copy",
                "inputs": {},  # Azure API quirk
            }
        )
        assert activity.inputs == []

    def test_outputs_accepts_empty_dict(self) -> None:
        """Empty dict {} for outputs should be normalized to []."""
        activity = Activity.model_validate(
            {
                "name": "TestActivity",
                "type": "Copy",
                "outputs": {},  # Azure API quirk
            }
        )
        assert activity.outputs == []

    def test_user_properties_accepts_empty_dict(self) -> None:
        """Empty dict {} for userProperties should be normalized to []."""
        activity = Activity.model_validate(
            {
                "name": "TestActivity",
                "type": "Copy",
                "userProperties": {},  # Azure API quirk
            }
        )
        assert activity.user_properties == []


class TestPipelinePropertiesEmptyDictHandling:
    """Tests for PipelineProperties model handling of Azure API {} quirk."""

    def test_activities_accepts_empty_dict(self) -> None:
        """Empty dict {} for activities should be normalized to []."""
        props = PipelineProperties.model_validate(
            {
                "activities": {},  # Azure API quirk
            }
        )
        assert props.activities == []

    def test_annotations_accepts_empty_dict(self) -> None:
        """Empty dict {} for annotations should be normalized to []."""
        props = PipelineProperties.model_validate(
            {
                "annotations": {},  # Azure API quirk
            }
        )
        assert props.annotations == []


class TestDataFlowPropertiesEmptyDictHandling:
    """Tests for DataFlowProperties model handling of Azure API {} quirk."""

    def test_sources_accepts_empty_dict(self) -> None:
        """Empty dict {} for sources should be normalized to []."""
        props = DataFlowProperties.model_validate(
            {
                "type": "MappingDataFlow",
                "sources": {},  # Azure API quirk
            }
        )
        assert props.sources == []

    def test_sinks_accepts_empty_dict(self) -> None:
        """Empty dict {} for sinks should be normalized to []."""
        props = DataFlowProperties.model_validate(
            {
                "type": "MappingDataFlow",
                "sinks": {},  # Azure API quirk
            }
        )
        assert props.sinks == []

    def test_transformations_accepts_empty_dict(self) -> None:
        """Empty dict {} for transformations should be normalized to []."""
        props = DataFlowProperties.model_validate(
            {
                "type": "MappingDataFlow",
                "transformations": {},  # Azure API quirk
            }
        )
        assert props.transformations == []

    def test_script_lines_accepts_empty_dict(self) -> None:
        """Empty dict {} for scriptLines should be normalized to []."""
        props = DataFlowProperties.model_validate(
            {
                "type": "MappingDataFlow",
                "scriptLines": {},  # Azure API quirk
            }
        )
        assert props.script_lines == []

    def test_annotations_accepts_empty_dict(self) -> None:
        """Empty dict {} for annotations should be normalized to []."""
        props = DataFlowProperties.model_validate(
            {
                "type": "MappingDataFlow",
                "annotations": {},  # Azure API quirk
            }
        )
        assert props.annotations == []


class TestTriggerPropertiesEmptyDictHandling:
    """Tests for TriggerProperties model handling of Azure API {} quirk."""

    def test_annotations_accepts_empty_dict(self) -> None:
        """Empty dict {} for annotations should be normalized to []."""
        props = TriggerProperties.model_validate(
            {
                "type": "ScheduleTrigger",
                "annotations": {},  # Azure API quirk
            }
        )
        assert props.annotations == []

    def test_pipelines_accepts_empty_dict(self) -> None:
        """Empty dict {} for pipelines should be normalized to []."""
        props = TriggerProperties.model_validate(
            {
                "type": "ScheduleTrigger",
                "pipelines": {},  # Azure API quirk
            }
        )
        assert props.pipelines == []


class TestLinkedServicePropertiesEmptyDictHandling:
    """Tests for LinkedServiceProperties model handling of Azure API {} quirk."""

    def test_annotations_accepts_empty_dict(self) -> None:
        """Empty dict {} for annotations should be normalized to []."""
        props = LinkedServiceProperties.model_validate(
            {
                "type": "AzureBlobStorage",
                "annotations": {},  # Azure API quirk
            }
        )
        assert props.annotations == []
