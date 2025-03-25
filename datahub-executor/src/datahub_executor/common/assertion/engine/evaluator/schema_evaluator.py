import json
import logging
from typing import List

from datahub.metadata.schema_classes import (
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
    InvalidParametersException,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    DatasetSchemaAssertionParameters,
    DatasetSchemaSourceType,
    SchemaAssertion,
    SchemaAssertionCompatibility,
    SchemaAssertionField,
    SchemaFieldDataType,
)

logger = logging.getLogger(__name__)


class SchemaAssertionEvaluator(AssertionEvaluator):
    """Evaluator for SCHEMA assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.DATA_SCHEMA

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_SCHEMA,
            dataset_schema_parameters=DatasetSchemaAssertionParameters(
                source_type=DatasetSchemaSourceType.DATAHUB_SCHEMA
            ),
        )

    def _convert_schema_field_data_type(
        self, dataType: SchemaFieldDataTypeClass
    ) -> SchemaFieldDataType:
        data_type_map = {
            "BooleanTypeClass": SchemaFieldDataType.BOOLEAN,
            "FixedTypeClass": SchemaFieldDataType.FIXED,
            "StringTypeClass": SchemaFieldDataType.STRING,
            "BytesTypeClass": SchemaFieldDataType.BYTES,
            "NumberTypeClass": SchemaFieldDataType.NUMBER,
            "DateTypeClass": SchemaFieldDataType.DATE,
            "TimeTypeClass": SchemaFieldDataType.TIME,
            "EnumTypeClass": SchemaFieldDataType.ENUM,
            "NullTypeClass": SchemaFieldDataType.NULL,
            "MapTypeClass": SchemaFieldDataType.MAP,
            "ArrayTypeClass": SchemaFieldDataType.ARRAY,
            "UnionTypeClass": SchemaFieldDataType.UNION,
            "RecordTypeClass": SchemaFieldDataType.STRUCT,
        }
        class_name = dataType.type.__class__.__name__
        std_data_type = data_type_map.get(class_name)
        if std_data_type is not None:
            return std_data_type
        raise InvalidParametersException(
            message=f"Unrecognized schema data field type {class_name} provided!",
            parameters={},
        )

    def _convert_to_schema_assertion_fields(
        self, schema_metadata_fields: List[SchemaFieldClass]
    ) -> List[SchemaAssertionField]:
        return [
            SchemaAssertionField(
                path=get_simple_field_path_from_v2_field_path(field.fieldPath),
                type=self._convert_schema_field_data_type(field.type),
                nativeType=field.nativeDataType,
            )
            for field in schema_metadata_fields
        ]

    def _evaluate_subset_match_assertion(
        self,
        expected_fields: List[SchemaAssertionField],
        actual_fields: List[SchemaAssertionField],
    ) -> AssertionEvaluationResult:
        expected_field_path_to_field = {field.path: field for field in expected_fields}

        extra_fields_in_actual: List[str] = []
        mismatched_type_field_paths: List[str] = []

        for field in actual_fields:
            expected_field = expected_field_path_to_field.get(field.path)
            if expected_field is None:
                extra_fields_in_actual.append(field.path)
            elif expected_field.type != field.type:
                mismatched_type_field_paths.append(field.path)

        result = AssertionResultType.SUCCESS
        result_parameters = {}
        if len(extra_fields_in_actual) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["extra_fields_in_actual"] = json.dumps(
                extra_fields_in_actual
            )
        if len(mismatched_type_field_paths) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["mismatched_type_fields"] = json.dumps(
                mismatched_type_field_paths
            )

        return AssertionEvaluationResult(type=result, parameters=result_parameters)

    def _evaluate_superset_match_assertion(
        self,
        expected_fields: List[SchemaAssertionField],
        actual_fields: List[SchemaAssertionField],
    ) -> AssertionEvaluationResult:
        actual_field_path_to_field = {field.path: field for field in actual_fields}

        extra_fields_in_expected: List[str] = []
        mismatched_type_field_paths: List[str] = []

        for field in expected_fields:
            actual_field = actual_field_path_to_field.get(field.path)
            if actual_field is None:
                extra_fields_in_expected.append(field.path)
            elif actual_field.type != field.type:
                mismatched_type_field_paths.append(field.path)

        result = AssertionResultType.SUCCESS
        result_parameters = {}
        if len(extra_fields_in_expected) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["extra_fields_in_expected"] = json.dumps(
                extra_fields_in_expected
            )
        if len(mismatched_type_field_paths) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["mismatched_type_fields"] = json.dumps(
                mismatched_type_field_paths
            )

        return AssertionEvaluationResult(type=result, parameters=result_parameters)

    def _evaluate_exact_match_assertion(
        self,
        expected_fields: List[SchemaAssertionField],
        actual_fields: List[SchemaAssertionField],
    ) -> AssertionEvaluationResult:
        # Fields that were found to be in the actual, but not in the expected.
        extra_fields_in_actual: List[str] = []

        # Fields that were found to be in the expected, but not in the actual.
        extra_fields_in_expected: List[str] = []

        # Fields that were found to be in both the expected and actual schemas.
        matched_field_paths: List[str] = []

        # A list of fields that exist in both actual and expected, but do not share the same type.
        mismatched_type_field_paths: List[str] = []

        # Map of expected field's 'path' to the field object
        expected_field_path_to_field = {field.path: field for field in expected_fields}
        actual_field_path_to_field = {field.path: field for field in actual_fields}

        # Identify mismatches and missing fields
        for path, field in actual_field_path_to_field.items():
            if path in expected_field_path_to_field:
                expected_field = expected_field_path_to_field[path]
                if field.type == expected_field.type:
                    matched_field_paths.append(path)
                else:
                    mismatched_type_field_paths.append(path)
            else:
                extra_fields_in_actual.append(path)

        for path in expected_field_path_to_field.keys():
            if path not in actual_field_path_to_field:
                extra_fields_in_expected.append(path)

        # Determine result type based on discrepancies found
        result = AssertionResultType.SUCCESS
        result_parameters = {}

        if len(extra_fields_in_actual) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["extra_fields_in_actual"] = json.dumps(
                extra_fields_in_actual
            )

        if len(extra_fields_in_expected) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["extra_fields_in_expected"] = json.dumps(
                extra_fields_in_expected
            )

        if len(mismatched_type_field_paths) > 0:
            result = AssertionResultType.FAILURE
            result_parameters["mismatched_type_fields"] = json.dumps(
                mismatched_type_field_paths
            )

        return AssertionEvaluationResult(type=result, parameters=result_parameters)

    def _evaluate_datahub_dataset_schema_metadata_assertion(
        self,
        entity_urn: str,
        schema_assertion: SchemaAssertion,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert isinstance(
            self.connection_provider, DataHubIngestionSourceConnectionProvider
        )
        maybe_schema_metadata_aspect = self.connection_provider.graph.get_aspect(
            entity_urn=entity_urn,
            aspect_type=SchemaMetadataClass,
        )

        if maybe_schema_metadata_aspect is None:
            raise InsufficientDataException(
                message=f"Unable to find DataHub schema for {entity_urn}"
            )

        compatibility = schema_assertion.compatibility
        expected_fields = schema_assertion.fields
        actual_fields = self._convert_to_schema_assertion_fields(
            maybe_schema_metadata_aspect.fields
        )

        if compatibility == SchemaAssertionCompatibility.EXACT_MATCH:
            return self._evaluate_exact_match_assertion(expected_fields, actual_fields)

        elif compatibility == SchemaAssertionCompatibility.SUPERSET:
            return self._evaluate_superset_match_assertion(
                expected_fields, actual_fields
            )

        elif compatibility == SchemaAssertionCompatibility.SUBSET:
            return self._evaluate_subset_match_assertion(expected_fields, actual_fields)

        raise InvalidParametersException(
            message=f"Unsupported compatibility type provided!: {compatibility}"
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.schema_assertion is not None
        assert parameters.dataset_schema_parameters is not None

        entity_urn = assertion.entity.urn
        schema_assertion = assertion.schema_assertion
        dataset_schema_parameters = parameters.dataset_schema_parameters

        if (
            dataset_schema_parameters.source_type
            == DatasetSchemaSourceType.DATAHUB_SCHEMA
        ):
            return self._evaluate_datahub_dataset_schema_metadata_assertion(
                entity_urn, schema_assertion, context
            )

        # We only support DATAHUB_SCHEMA for now
        raise InvalidParametersException(
            message=f"Unsupported source type provided!: {dataset_schema_parameters.source_type}",
            parameters={},
        )
