import json
from unittest.mock import Mock

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
)

from datahub_executor.common.assertion.engine.evaluator.schema_evaluator import (
    SchemaAssertionEvaluator,
)
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionResultType,
    AssertionType,
    DatasetSchemaAssertionParameters,
    DatasetSchemaSourceType,
    SchemaAssertion,
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaFieldDataType,
)


class MockDataType:
    def __init__(self, type_name: str):
        self.type = type(self)
        self.__class__.__name__ = type_name


TEST_START = 1687643700064

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)


class TestSchemaEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.evaluator = SchemaAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
        )
        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.DATA_SCHEMA,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platformUrn="urn:li:dataPlatform:snowflake",
                platformInstance=None,
                subTypes=None,
                table_name="test_table",
                qualifiedName="test_db.public.test_table",
            ),
            connectionUrn="urn:li:dataPlatform:snowflake",
            schemaAssertion=None,
        )
        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_SCHEMA,
            dataset_schema_parameters=DatasetSchemaAssertionParameters(
                sourceType=DatasetSchemaSourceType.DATAHUB_SCHEMA
            ),
        )

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.DATA_SCHEMA

    def test_convert_to_schema_assertion_fields_valid_types(self) -> None:
        schema_metadata_fields = [
            SchemaFieldClass(
                fieldPath="field1",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="VARCHAR",
            ),
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="INTEGER",
            ),
        ]
        expected_field_1 = SchemaAssertionField(
            path="field1", type=SchemaFieldDataType.STRING, nativeType="VARCHAR"
        )
        expected_field_2 = SchemaAssertionField(
            path="field2", type=SchemaFieldDataType.NUMBER, nativeType="INTEGER"
        )
        result = self.evaluator._convert_to_schema_assertion_fields(
            schema_metadata_fields
        )
        assert len(result) == 2
        assert result[0].path == expected_field_1.path
        assert result[0].type == expected_field_1.type
        assert result[0].nativeType == expected_field_1.nativeType
        assert result[1].path == expected_field_2.path
        assert result[1].type == expected_field_2.type
        assert result[1].nativeType == expected_field_2.nativeType

    def test_convert_to_schema_assertion_fields_invalid_types(self) -> None:
        schema_metadata_fields = [
            SchemaFieldClass(
                fieldPath="field2",
                type=SchemaFieldDataTypeClass(type=MockDataType("SomeRandomClass")),  # type: ignore
                nativeDataType="INTEGER",
            )
        ]
        with pytest.raises(InvalidParametersException):
            self.evaluator._convert_to_schema_assertion_fields(schema_metadata_fields)

    # Schemas must match exactly
    def test_evaluate_exact_match_success(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.EXACT_MATCH,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {}

    def test_evaluate_exact_match_fail_extra_fields_in_actual(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.EXACT_MATCH,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path3",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"extra_fields_in_actual": json.dumps(["path3"])}

    def test_evaluate_exact_match_fail_extra_fields_in_expected(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.EXACT_MATCH,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
                SchemaAssertionField(
                    path="path3", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"extra_fields_in_expected": json.dumps(["path3"])}

    def test_evaluate_exact_match_fail_mismatched_field_types(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.EXACT_MATCH,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.NUMBER,
                    nativeType="int(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"mismatched_type_fields": json.dumps(["path1"])}

    # Actual schema must be superset of expected
    def test_evaluate_superset_success_equal(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUPERSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {}

    def test_evaluate_superset_success_superset(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUPERSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path3",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {}

    def test_evaluate_superset_fail_subset(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUPERSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
                SchemaAssertionField(
                    path="path3", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"extra_fields_in_expected": json.dumps(["path3"])}

    def test_evaluate_superset_fail_mismatched_types(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUPERSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"mismatched_type_fields": json.dumps(["path2"])}

    # Actual schema must be a subset of expected schema (not really used but supported for future)
    def test_evaluate_subset_success_equal(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUBSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {}

    def test_evaluate_subset_success_subset(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUBSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
                SchemaAssertionField(
                    path="path3", type=SchemaFieldDataType.NUMBER, nativeType="int(64)"
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {}

    def test_evaluate_subset_fail_extra_fields(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUBSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"extra_fields_in_actual": json.dumps(["path2"])}

    def test_evaluate_subset_fail_mismatched_types(self) -> None:
        schema_assertion = SchemaAssertion(
            compatibility=SchemaAssertionCompatibility.SUBSET,
            fields=[
                SchemaAssertionField(
                    path="path1",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
                SchemaAssertionField(
                    path="path2",
                    type=SchemaFieldDataType.STRING,
                    nativeType="varchar(64)",
                ),
            ],
        )
        self.assertion.schema_assertion = schema_assertion

        # Mock DataHub Graph
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_aspect.return_value = SchemaMetadataClass(
            schemaName="Test",
            platform="Test",
            version=0,
            hash="Test",
            platformSchema=SchemalessClass(),
            fields=[
                SchemaFieldClass(
                    fieldPath="path1",
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="varchar(64)",
                ),
                SchemaFieldClass(
                    fieldPath="path2",
                    type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                    nativeDataType="int(64)",
                ),
            ],
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE
        assert result.parameters == {"mismatched_type_fields": json.dumps(["path2"])}
