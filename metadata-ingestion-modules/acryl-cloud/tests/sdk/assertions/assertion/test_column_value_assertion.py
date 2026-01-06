"""Tests for ColumnValueAssertion class."""

import pytest

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.column_value_assertion import (
    ColumnValueAssertion,
)
from acryl_datahub_cloud.sdk.assertion_input.assertion_input import (
    AssertionIncidentBehavior,
    DetectionMechanism,
)
from acryl_datahub_cloud.sdk.assertion_input.column_value_assertion_input import (
    FailThresholdType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, TagUrn


class TestColumnValueAssertionFromEntities:
    """Tests for ColumnValueAssertion._from_entities method."""

    def test_from_entities_with_all_fields(
        self,
        column_value_assertion_entity_with_all_fields: Assertion,
        column_value_monitor_with_all_fields: Monitor,
    ) -> None:
        assertion = ColumnValueAssertion._from_entities(
            column_value_assertion_entity_with_all_fields,
            column_value_monitor_with_all_fields,
        )

        # Check basic assertion properties
        assert assertion.urn == column_value_assertion_entity_with_all_fields.urn
        assert assertion.display_name == "Column Value Assertion"
        assert assertion.mode == AssertionMode.ACTIVE

        # Check column value specific properties
        assert assertion.column_name == "string_column"
        assert assertion.operator == models.AssertionStdOperatorClass.GREATER_THAN
        assert assertion.criteria_parameters == 5
        assert assertion.transform == models.FieldTransformTypeClass.LENGTH
        assert assertion.fail_threshold_type == FailThresholdType.PERCENTAGE
        assert assertion.fail_threshold_value == 5
        assert assertion.exclude_nulls is True

        # Check tags
        assert len(assertion.tags) == 1
        assert assertion.tags[0] == TagUrn.from_string(
            "urn:li:tag:column_value_assertion_tag"
        )

        # Check incident behavior
        assert AssertionIncidentBehavior.RAISE_ON_FAIL in assertion.incident_behavior
        assert AssertionIncidentBehavior.RESOLVE_ON_PASS in assertion.incident_behavior

        # Check detection mechanism
        assert isinstance(
            assertion.detection_mechanism, type(DetectionMechanism.ALL_ROWS_QUERY())
        )

    def test_from_entities_with_minimal_fields(
        self,
        column_value_assertion_entity_minimal: Assertion,
        column_value_monitor_with_all_fields: Monitor,
    ) -> None:
        assertion = ColumnValueAssertion._from_entities(
            column_value_assertion_entity_minimal,
            column_value_monitor_with_all_fields,
        )

        # Check basic properties
        assert assertion.urn == column_value_assertion_entity_minimal.urn
        assert assertion.display_name == "Minimal Column Value Assertion"

        # Check column value specific properties with defaults
        assert assertion.column_name == "number_column"
        assert assertion.operator == models.AssertionStdOperatorClass.NOT_NULL
        assert assertion.criteria_parameters is None
        assert assertion.transform is None
        assert assertion.fail_threshold_type == FailThresholdType.COUNT
        assert assertion.fail_threshold_value == 0
        assert assertion.exclude_nulls is True


class TestColumnValueAssertionStaticMethods:
    """Tests for ColumnValueAssertion static methods."""

    def test_get_criteria_parameters_with_type_single_value(
        self, column_value_assertion_entity_with_all_fields: Assertion
    ) -> None:
        result = ColumnValueAssertion._get_criteria_parameters_with_type(
            column_value_assertion_entity_with_all_fields
        )
        assert result is not None
        assert result[0] == "5"
        assert result[1] == models.AssertionStdParameterTypeClass.NUMBER


class TestColumnValueAssertionWithRangeParameters:
    """Tests for ColumnValueAssertion with range parameters."""

    @pytest.fixture
    def range_assertion_entity(self, any_assertion_urn: AssertionUrn) -> Assertion:
        """Create an assertion entity with range parameters."""
        return Assertion(
            id=any_assertion_urn,
            info=models.FieldAssertionInfoClass(
                type=models.FieldAssertionTypeClass.FIELD_VALUES,
                entity="urn:li:dataset:(urn:li:dataPlatform:snowflake,table_name,PROD)",
                fieldValuesAssertion=models.FieldValuesAssertionClass(
                    field=models.SchemaFieldSpecClass(
                        path="number_column", type="NUMBER", nativeType="NUMBER"
                    ),
                    operator=models.AssertionStdOperatorClass.BETWEEN,
                    parameters=models.AssertionStdParametersClass(
                        minValue=models.AssertionStdParameterClass(
                            value="0",
                            type=models.AssertionStdParameterTypeClass.NUMBER,
                        ),
                        maxValue=models.AssertionStdParameterClass(
                            value="100",
                            type=models.AssertionStdParameterTypeClass.NUMBER,
                        ),
                    ),
                    failThreshold=models.FieldValuesFailThresholdClass(
                        type=models.FieldValuesFailThresholdTypeClass.COUNT,
                        value=0,
                    ),
                    excludeNulls=True,
                ),
            ),
            description="Range Column Value Assertion",
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.NATIVE,
                created=models.AuditStampClass(
                    actor="urn:li:corpuser:test",
                    time=1609459200000,
                ),
            ),
            last_updated=models.AuditStampClass(
                actor="urn:li:corpuser:test",
                time=1609545600000,
            ),
        )

    def test_get_criteria_parameters_with_type_range(
        self, range_assertion_entity: Assertion
    ) -> None:
        result = ColumnValueAssertion._get_criteria_parameters_with_type(
            range_assertion_entity
        )
        assert result is not None
        assert result[0] == ("0", "100")
        assert result[1] == (
            models.AssertionStdParameterTypeClass.NUMBER,
            models.AssertionStdParameterTypeClass.NUMBER,
        )

    def test_from_entities_with_range_parameters(
        self,
        range_assertion_entity: Assertion,
        column_value_monitor_with_all_fields: Monitor,
    ) -> None:
        assertion = ColumnValueAssertion._from_entities(
            range_assertion_entity,
            column_value_monitor_with_all_fields,
        )

        assert assertion.operator == models.AssertionStdOperatorClass.BETWEEN
        # Values are converted from strings to integers based on NUMBER type
        assert assertion.criteria_parameters == (0, 100)
