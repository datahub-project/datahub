"""Tests for SchemaAssertion class."""

from datetime import datetime, timezone
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.sdk.assertion.assertion_base import AssertionMode
from acryl_datahub_cloud.sdk.assertion.schema_assertion import SchemaAssertion
from acryl_datahub_cloud.sdk.assertion_input.schema_assertion_input import (
    DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY,
    SchemaAssertionCompatibility,
    SchemaFieldDataType,
)
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, DatasetUrn


class TestSchemaAssertionFromEntities:
    """Test suite for SchemaAssertion._from_entities method."""

    @pytest.fixture
    def dataset_urn(self) -> DatasetUrn:
        """Create a dataset URN for testing."""
        return DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )

    @pytest.fixture
    def assertion_urn(self) -> AssertionUrn:
        """Create an assertion URN for testing."""
        return AssertionUrn("test_schema_assertion")

    def _create_schema_field(
        self,
        path: str,
        type_class: type,
        native_type: str = "",
    ) -> models.SchemaFieldClass:
        """Create a schema field for testing."""
        return models.SchemaFieldClass(
            fieldPath=path,
            type=models.SchemaFieldDataTypeClass(type=type_class()),
            nativeDataType=native_type,
            nullable=False,
        )

    def _create_schema_assertion_info(
        self,
        entity: str,
        compatibility: str,
        fields: list[models.SchemaFieldClass],
    ) -> models.SchemaAssertionInfoClass:
        """Create a SchemaAssertionInfoClass for testing."""
        return models.SchemaAssertionInfoClass(
            entity=entity,
            compatibility=getattr(
                models.SchemaAssertionCompatibilityClass, compatibility
            ),
            schema=models.SchemaMetadataClass(
                schemaName="test-schema",
                platform="urn:li:dataPlatform:snowflake",
                version=0,
                hash="test-hash",
                platformSchema=models.OtherSchemaClass(rawSchema=""),
                fields=fields,
            ),
        )

    def _create_assertion_entity(
        self,
        urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        info: Optional[models.SchemaAssertionInfoClass] = None,
        description: str = "Test Schema Assertion",
    ) -> Assertion:
        """Create an assertion entity for testing."""
        assertion = Assertion(
            id=urn,
            info=info
            or self._create_schema_assertion_info(
                entity=str(dataset_urn),
                compatibility="EXACT_MATCH",
                fields=[
                    self._create_schema_field("id", models.StringTypeClass),
                ],
            ),
            description=description,
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.NATIVE,
                created=models.AuditStampClass(
                    time=1000,
                    actor="urn:li:corpuser:test_user",
                ),
            ),
            last_updated=(
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                "urn:li:corpuser:test_user",
            ),
        )
        return assertion

    def _create_monitor_entity(
        self,
        assertion_urn: AssertionUrn,
        dataset_urn: DatasetUrn,
        mode: str = "ACTIVE",
        schedule: Optional[models.CronScheduleClass] = None,
    ) -> Monitor:
        """Create a monitor entity for testing."""
        return Monitor(
            id=(dataset_urn, assertion_urn),
            info=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(
                    mode=getattr(models.MonitorModeClass, mode)
                ),
                assertionMonitor=models.AssertionMonitorClass(
                    assertions=[
                        models.AssertionEvaluationSpecClass(
                            assertion=str(assertion_urn),
                            schedule=schedule
                            or models.CronScheduleClass(
                                cron="0 */6 * * *", timezone="UTC"
                            ),
                            parameters=models.AssertionEvaluationParametersClass(
                                type=models.AssertionEvaluationParametersTypeClass.DATASET_SCHEMA,
                                datasetSchemaParameters=models.DatasetSchemaAssertionParametersClass(
                                    sourceType=models.DatasetSchemaSourceTypeClass.DATAHUB_SCHEMA
                                ),
                            ),
                        )
                    ]
                ),
            ),
        )

    def test_from_entities_basic(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test basic parsing from entities."""
        info = self._create_schema_assertion_info(
            entity=str(dataset_urn),
            compatibility="EXACT_MATCH",
            fields=[
                self._create_schema_field("id", models.StringTypeClass),
                self._create_schema_field("count", models.NumberTypeClass, "BIGINT"),
            ],
        )
        assertion_entity = self._create_assertion_entity(
            assertion_urn, dataset_urn, info=info
        )
        monitor_entity = self._create_monitor_entity(assertion_urn, dataset_urn)

        result = SchemaAssertion._from_entities(assertion_entity, monitor_entity)

        assert result.urn == assertion_urn
        assert result.dataset_urn == dataset_urn
        assert result.display_name == "Test Schema Assertion"
        assert result.compatibility == SchemaAssertionCompatibility.EXACT_MATCH
        assert len(result.fields) == 2
        assert result.fields[0].path == "id"
        assert result.fields[0].type == SchemaFieldDataType.STRING
        assert result.fields[1].path == "count"
        assert result.fields[1].type == SchemaFieldDataType.NUMBER
        assert result.fields[1].native_type == "BIGINT"

    def test_from_entities_superset_compatibility(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test parsing with SUPERSET compatibility."""
        info = self._create_schema_assertion_info(
            entity=str(dataset_urn),
            compatibility="SUPERSET",
            fields=[self._create_schema_field("id", models.StringTypeClass)],
        )
        assertion_entity = self._create_assertion_entity(
            assertion_urn, dataset_urn, info=info
        )
        monitor_entity = self._create_monitor_entity(assertion_urn, dataset_urn)

        result = SchemaAssertion._from_entities(assertion_entity, monitor_entity)

        assert result.compatibility == SchemaAssertionCompatibility.SUPERSET

    def test_from_entities_subset_compatibility(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test parsing with SUBSET compatibility."""
        info = self._create_schema_assertion_info(
            entity=str(dataset_urn),
            compatibility="SUBSET",
            fields=[self._create_schema_field("id", models.StringTypeClass)],
        )
        assertion_entity = self._create_assertion_entity(
            assertion_urn, dataset_urn, info=info
        )
        monitor_entity = self._create_monitor_entity(assertion_urn, dataset_urn)

        result = SchemaAssertion._from_entities(assertion_entity, monitor_entity)

        assert result.compatibility == SchemaAssertionCompatibility.SUBSET

    def test_from_entities_inactive_mode(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test parsing with INACTIVE monitor mode."""
        assertion_entity = self._create_assertion_entity(assertion_urn, dataset_urn)
        monitor_entity = self._create_monitor_entity(
            assertion_urn, dataset_urn, mode="INACTIVE"
        )

        result = SchemaAssertion._from_entities(assertion_entity, monitor_entity)

        assert result.mode == AssertionMode.INACTIVE

    def test_from_entities_all_field_types(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test parsing with all supported field types."""
        info = self._create_schema_assertion_info(
            entity=str(dataset_urn),
            compatibility="EXACT_MATCH",
            fields=[
                self._create_schema_field("string_col", models.StringTypeClass),
                self._create_schema_field("number_col", models.NumberTypeClass),
                self._create_schema_field("bool_col", models.BooleanTypeClass),
                self._create_schema_field("date_col", models.DateTypeClass),
                self._create_schema_field("time_col", models.TimeTypeClass),
                self._create_schema_field(
                    "struct_col", models.RecordTypeClass
                ),  # STRUCT -> Record
                self._create_schema_field("array_col", models.ArrayTypeClass),
                self._create_schema_field("map_col", models.MapTypeClass),
            ],
        )
        assertion_entity = self._create_assertion_entity(
            assertion_urn, dataset_urn, info=info
        )
        monitor_entity = self._create_monitor_entity(assertion_urn, dataset_urn)

        result = SchemaAssertion._from_entities(assertion_entity, monitor_entity)

        assert len(result.fields) == 8
        expected_types = [
            SchemaFieldDataType.STRING,
            SchemaFieldDataType.NUMBER,
            SchemaFieldDataType.BOOLEAN,
            SchemaFieldDataType.DATE,
            SchemaFieldDataType.TIME,
            SchemaFieldDataType.STRUCT,
            SchemaFieldDataType.ARRAY,
            SchemaFieldDataType.MAP,
        ]
        for i, expected_type in enumerate(expected_types):
            assert result.fields[i].type == expected_type


class TestGetCompatibility:
    """Test suite for SchemaAssertion._get_compatibility method."""

    @pytest.fixture
    def assertion_urn(self) -> AssertionUrn:
        """Create an assertion URN for testing."""
        return AssertionUrn("test_schema_assertion")

    @pytest.fixture
    def dataset_urn(self) -> DatasetUrn:
        """Create a dataset URN for testing."""
        return DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )

    def _create_assertion_with_compatibility(
        self,
        urn: AssertionUrn,
        dataset: DatasetUrn,
        compatibility: Any,
    ) -> Assertion:
        """Create an assertion with a specific compatibility value."""
        info = models.SchemaAssertionInfoClass(
            entity=str(dataset),
            compatibility=compatibility,
            schema=models.SchemaMetadataClass(
                schemaName="test-schema",
                platform="urn:li:dataPlatform:snowflake",
                version=0,
                hash="test-hash",
                platformSchema=models.OtherSchemaClass(rawSchema=""),
                fields=[
                    models.SchemaFieldClass(
                        fieldPath="id",
                        type=models.SchemaFieldDataTypeClass(
                            type=models.StringTypeClass()
                        ),
                        nativeDataType="",
                        nullable=False,
                    )
                ],
            ),
        )
        return Assertion(
            id=urn,
            info=info,
            description="Test Assertion",
            source=models.AssertionSourceClass(
                type=models.AssertionSourceTypeClass.NATIVE,
                created=models.AuditStampClass(
                    time=1000,
                    actor="urn:li:corpuser:test_user",
                ),
            ),
            last_updated=(
                datetime(2021, 1, 1, tzinfo=timezone.utc),
                "urn:li:corpuser:test_user",
            ),
        )

    def test_unknown_enum_value_falls_back_to_default(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test that unknown enum-like compatibility values fall back to default."""
        # Simulate a backend returning an unknown enum value (e.g., "RELAXED")
        unknown_enum = MagicMock()
        unknown_enum.name = (
            "RELAXED"  # Unknown value not in SchemaAssertionCompatibility
        )

        assertion = self._create_assertion_with_compatibility(
            assertion_urn, dataset_urn, unknown_enum
        )

        result = SchemaAssertion._get_compatibility(assertion)

        assert result == DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY

    def test_unknown_string_value_falls_back_to_default(
        self, assertion_urn: AssertionUrn, dataset_urn: DatasetUrn
    ) -> None:
        """Test that unknown string compatibility values fall back to default."""
        assertion = self._create_assertion_with_compatibility(
            assertion_urn,
            dataset_urn,
            "RELAXED",  # Unknown string value
        )

        result = SchemaAssertion._get_compatibility(assertion)

        assert result == DEFAULT_SCHEMA_ASSERTION_COMPATIBILITY
