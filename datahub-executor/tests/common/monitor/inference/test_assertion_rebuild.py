"""
Test suite for assertion rebuild with entity field handling.

Tests that assertion trainers correctly rebuild assertions when AssertionInfo
contains an entity field, ensuring the entity is properly preserved from the
original assertion object rather than duplicated from the info dict.
"""

from typing import cast
from unittest.mock import Mock

import pytest
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    RowCountTotalClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)

from datahub_executor.common.monitor.inference.field_assertion_trainer import (
    FieldAssertionTrainer,
)
from datahub_executor.common.monitor.inference.freshness_assertion_trainer import (
    FreshnessAssertionTrainer,
)
from datahub_executor.common.monitor.inference.sql_assertion_trainer import (
    SqlAssertionTrainer,
)
from datahub_executor.common.monitor.inference.volume_assertion_trainer import (
    VolumeAssertionTrainer,
)
from datahub_executor.common.types import Assertion, AssertionEntity


@pytest.fixture
def mock_assertion() -> Mock:
    """Create a mock assertion for testing."""
    assertion = Mock(spec=Assertion)
    assertion.urn = "urn:li:assertion:test-assertion-123"
    # Create an actual AssertionEntity object instead of Mock
    assertion.entity = AssertionEntity(
        urn="urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
        platform_urn="urn:li:dataPlatform:bigquery",
    )
    assertion.connection_urn = "urn:li:connection:test-connection"
    assertion.raw_info_aspect = Mock()
    return assertion


class TestVolumeAssertionRebuild:
    """Test volume assertion trainer rebuild behavior."""

    def test_rebuild_with_entity_field(self, mock_assertion: Mock) -> None:
        """
        Verify volume assertion rebuilds successfully when AssertionInfo contains entity field.

        The entity from AssertionInfo (URN string) should be excluded from the rebuild dict,
        and the entity from the original assertion (AssertionEntity object) should be preserved.
        """
        # Create trainer
        trainer = VolumeAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        # Create AssertionInfoClass with entity field
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            entity="urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity="urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
                filter=None,
            ),
            description="Row count volume anomaly check",
        )

        # Rebuild assertion - should not raise TypeError
        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )

        # Verify the result
        assert result is not None
        assert isinstance(result, Assertion)

    def test_rebuild_preserves_original_entity(self, mock_assertion: Mock) -> None:
        """Verify original assertion entity is preserved even when AssertionInfo has different entity URN."""
        trainer = VolumeAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        # Create assertion_info with different entity URN than original_assertion
        assertion_info_entity = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
        )
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            entity=assertion_info_entity,
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=assertion_info_entity,
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
            ),
        )

        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )

        # Original assertion entity should be preserved
        assert result.entity == cast(Assertion, mock_assertion).entity
        assert isinstance(result.entity, AssertionEntity)

    def test_rebuild_without_entity_field(self, mock_assertion: Mock) -> None:
        """Verify rebuild succeeds when AssertionInfo does not have entity field set."""
        trainer = VolumeAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        # Create assertion_info without entity field
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            # entity=None,  # Not set
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity="urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)",
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
            ),
        )

        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )

        assert result is not None


class TestFieldAssertionRebuild:
    """Test field assertion trainer rebuild behavior."""

    def test_rebuild_with_entity_field(self, mock_assertion: Mock) -> None:
        """Verify field assertion rebuilds successfully when AssertionInfo contains entity field."""
        trainer = FieldAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        assertion_info = AssertionInfoClass(
            type="FIELD",
            entity="urn:li:dataset:(urn:li:dataPlatform:postgres,db.table,PROD)",
            description="Field assertion test",
        )

        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )
        assert result is not None
        assert isinstance(result, Assertion)


class TestFreshnessAssertionRebuild:
    """Test freshness assertion trainer rebuild behavior."""

    def test_rebuild_with_entity_field(self, mock_assertion: Mock) -> None:
        """Verify freshness assertion rebuilds successfully when AssertionInfo contains entity field."""
        trainer = FreshnessAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        assertion_info = AssertionInfoClass(
            type="FRESHNESS",
            entity="urn:li:dataset:(urn:li:dataPlatform:kafka,topic,PROD)",
            description="Freshness assertion test",
        )

        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )
        assert result is not None
        assert isinstance(result, Assertion)


class TestSqlAssertionRebuild:
    """Test SQL assertion trainer rebuild behavior."""

    def test_rebuild_with_entity_field(self, mock_assertion: Mock) -> None:
        """Verify SQL assertion rebuilds successfully when AssertionInfo contains entity field."""
        trainer = SqlAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        assertion_info = AssertionInfoClass(
            type="SQL",
            entity="urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)",
            description="SQL assertion test",
        )

        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )
        assert result is not None
        assert isinstance(result, Assertion)


class TestDictUnpackingBehavior:
    """Test Python dict unpacking behavior with duplicate keys."""

    def test_duplicate_keys_raise_error(self) -> None:
        """Verify that dict() raises TypeError when the same key appears in both dict and kwargs."""
        # Attempting to pass the same key in both dict and kwargs raises TypeError
        base_dict = {"entity": "value1", "other": "data"}

        with pytest.raises(TypeError, match="multiple values for keyword argument"):
            dict(**base_dict, entity="value2")

    def test_assertion_info_serialization_includes_entity(self) -> None:
        """Verify AssertionInfo.to_obj() includes entity field when set."""
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            entity="urn:li:dataset:test",
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity="urn:li:dataset:test",
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
            ),
        )

        # Convert to dict
        obj_dict = assertion_info.to_obj()

        # Verify entity is in the dict
        assert "entity" in obj_dict
        assert obj_dict["entity"] == "urn:li:dataset:test"

    def test_assertion_deserialization_with_entity_object(
        self, mock_assertion: Mock
    ) -> None:
        """Verify Assertion.model_validate correctly deserializes entity as AssertionEntity object."""
        # Entity must be an AssertionEntity dict with required fields
        assertion_dict = {
            "urn": "urn:li:assertion:test",
            "entity": {
                "urn": "urn:li:dataset:test",
                "platformUrn": "urn:li:dataPlatform:bigquery",
            },
            "type": "VOLUME",
            "volumeAssertion": {
                "type": "ROW_COUNT_TOTAL",
                "entity": "urn:li:dataset:test",  # This one stays as string (for VolumeAssertion)
                "rowCountTotal": {
                    "operator": "BETWEEN",
                    "parameters": {
                        "minValue": {"type": "NUMBER", "value": "100"},
                        "maxValue": {"type": "NUMBER", "value": "200"},
                    },
                },
            },
            "connectionUrn": "urn:li:connection:test",
        }

        # Deserialize and verify entity field
        result = Assertion.model_validate(assertion_dict)
        assert result is not None
        assert result.urn == "urn:li:assertion:test"
        assert isinstance(result.entity, AssertionEntity)
        assert result.entity.urn == "urn:li:dataset:test"


class TestAssertionInfoSchema:
    """Test AssertionInfo schema with entity field."""

    def test_nested_and_top_level_entity_fields(self) -> None:
        """
        Verify AssertionInfo correctly handles both top-level entity field
        and nested entity field within volumeAssertion.
        """
        entity_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,acryl-staging.deploy_test_9k.table_4432,PROD)"

        assertion_info = AssertionInfoClass(
            type="VOLUME",
            entity=entity_urn,  # Top-level entity
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=entity_urn,  # Nested entity in volumeAssertion
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="900"),
                        maxValue=AssertionStdParameterClass(
                            type="NUMBER", value="1100"
                        ),
                    ),
                ),
            ),
            description="Row count volume anomaly check",
        )

        # Both entity fields should be present
        assert assertion_info.entity == entity_urn
        assert assertion_info.volumeAssertion is not None
        assert assertion_info.volumeAssertion.entity == entity_urn

        # to_obj() should include the top-level entity
        obj_dict = assertion_info.to_obj()
        assert "entity" in obj_dict
        assert obj_dict["entity"] == entity_urn

    def test_entity_field_is_optional(self) -> None:
        """Verify entity field is optional and AssertionInfo can be created without it."""
        # Create AssertionInfo without entity field
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            # entity not set
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity="urn:li:dataset:test",
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
            ),
        )

        # Should succeed without entity field
        assert assertion_info.volumeAssertion is not None


class TestProductionScenarios:
    """Test realistic production scenarios with complex assertion data."""

    def test_volume_assertion_rebuild_with_full_data(
        self, mock_assertion: Mock
    ) -> None:
        """
        Verify volume assertion rebuild with complete production-like data including
        entity URN, row count parameters, and description.
        """
        trainer = VolumeAssertionTrainer(
            graph=Mock(),
            metrics_client=Mock(),
            metrics_predictor=Mock(),
            monitor_client=Mock(),
        )

        # Setup production-like URNs
        entity_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,acryl-staging.deploy_test_9k.table_4432,PROD)"
        assertion_urn = "urn:li:assertion:1caec7e9-a356-4ad3-8fd1-19922e144d8f"

        mock_assertion.urn = assertion_urn
        mock_assertion.entity.urn = entity_urn

        # Create assertion info with production data
        assertion_info = AssertionInfoClass(
            type="VOLUME",
            entity=entity_urn,
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=entity_urn,
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="950"),
                        maxValue=AssertionStdParameterClass(
                            type="NUMBER", value="1050"
                        ),
                    ),
                ),
            ),
            description="Row count volume anomaly check",
        )

        # Rebuild assertion
        result = trainer._rebuild_assertion(
            cast(Assertion, mock_assertion),
            assertion_info,
        )

        assert result is not None
        assert result.urn == assertion_urn

    def test_all_trainer_types_rebuild_with_entity(self, mock_assertion: Mock) -> None:
        """Verify all assertion trainer types (Volume, Field, Freshness, SQL) rebuild successfully with entity field."""
        entity_urn = "urn:li:dataset:test"

        # Volume
        volume_trainer = VolumeAssertionTrainer(Mock(), Mock(), Mock(), Mock())
        volume_info = AssertionInfoClass(
            type="VOLUME",
            entity=entity_urn,
            volumeAssertion=VolumeAssertionInfoClass(
                type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
                entity=entity_urn,
                rowCountTotal=RowCountTotalClass(
                    operator=AssertionStdOperatorClass.BETWEEN,
                    parameters=AssertionStdParametersClass(
                        minValue=AssertionStdParameterClass(type="NUMBER", value="100"),
                        maxValue=AssertionStdParameterClass(type="NUMBER", value="200"),
                    ),
                ),
            ),
        )
        volume_result = volume_trainer._rebuild_assertion(
            cast(Assertion, mock_assertion), volume_info
        )
        assert volume_result is not None

        # Field
        field_trainer = FieldAssertionTrainer(Mock(), Mock(), Mock(), Mock())
        # Test with minimal AssertionInfo containing entity field
        field_info = AssertionInfoClass(
            type="FIELD",
            entity=entity_urn,
            description="Field test",
        )
        field_result = field_trainer._rebuild_assertion(
            cast(Assertion, mock_assertion), field_info
        )
        assert field_result is not None

        # Freshness
        freshness_trainer = FreshnessAssertionTrainer(Mock(), Mock(), Mock(), Mock())
        # Test with minimal AssertionInfo containing entity field
        freshness_info = AssertionInfoClass(
            type="FRESHNESS",
            entity=entity_urn,
            description="Freshness test",
        )
        freshness_result = freshness_trainer._rebuild_assertion(
            cast(Assertion, mock_assertion), freshness_info
        )
        assert freshness_result is not None

        # SQL
        sql_trainer = SqlAssertionTrainer(Mock(), Mock(), Mock(), Mock())
        # Test with minimal AssertionInfo containing entity field
        sql_info = AssertionInfoClass(
            type="SQL",
            entity=entity_urn,
            description="SQL test",
        )
        sql_result = sql_trainer._rebuild_assertion(
            cast(Assertion, mock_assertion), sql_info
        )
        assert sql_result is not None
