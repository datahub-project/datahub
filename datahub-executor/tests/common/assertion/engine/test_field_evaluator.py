from unittest.mock import Mock

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
)

from datahub_executor.common.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_executor.common.assertion.types import (
    AssertionDatabaseParams,
    AssertionState,
    AssertionStateType,
)
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InsufficientDataException,
    InvalidParametersException,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.source.source import Source
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
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
    FieldAssertion,
    FieldAssertionType,
    FieldMetricAssertion,
    FieldMetricType,
    FieldValuesAssertion,
    FreshnessFieldSpec,
)

TEST_START = 1687643700064
TEST_END = 1687644000064
TEST_TIME = "2023-10-04 17:12:33.702000"

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)


class TestFreshnessEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.evaluator = FieldAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
        )
        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.DATASET,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platformUrn="urn:li:dataPlatform:snowflake",
                platformInstance=None,
                subTypes=None,
            ),
            connectionUrn="urn:li:dataPlatform:snowflake",
            fieldAssertion=None,
        )
        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.connection_provider.get_connection.return_value = self.connection
        self.source = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = self.source

        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FIELD,
            dataset_field_parameters=DatasetFieldAssertionParameters(
                sourceType=DatasetFieldSourceType.ALL_ROWS_QUERY,
            ),
        )
        self.database_params = Mock(spec=AssertionDatabaseParams)

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.FIELD

    def test_evaluate_internal_no_field_assertion(self) -> None:
        with pytest.raises(Exception):
            self.evaluator._evaluate_internal(
                self.assertion,
                self.params,
                self.context,
            )

    def test_evaluate_internal_no_params(self) -> None:
        self.params.dataset_field_parameters = None
        with pytest.raises(Exception):
            self.evaluator._evaluate_internal(
                self.assertion,
                self.params,
                self.context,
            )

    def test_evaluate_field_values_assertion_exceeded_threshold(self) -> None:
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "0",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_field_values_assertion_equals_threshold(self) -> None:
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "10",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 10
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_field_values_assertion_less_than_threshold(self) -> None:
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "10",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS
        assert self.state_provider.save_state.call_count == 0

    def test_evaluate_field_values_assertion_changed_rows_no_prev_state(self) -> None:
        # TODO - why doesn't mypy see that this is not None because of setup_method?
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "10",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 10
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_field_values_assertion_changed_rows_fail(self) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "10",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 20
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.FAILURE
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_field_values_assertion_changed_rows_success(self) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_VALUES,
            field_values_assertion=FieldValuesAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "my-id-value",
                            "type": "STRING",
                        }
                    },
                    "failThreshold": {
                        "type": "COUNT",
                        "value": "10",
                    },
                    "excludeNulls": "false",
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_values_count.return_value = 1
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_field_values_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters is not None
        assert result.parameters["values_count"] == "1"
        assert result.parameters["threshold_value"] == "10"
        assert self.state_provider.save_state.call_count == 1

    def test_evaluate_field_metric_assertion_success(self) -> None:
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_metric_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_field_metric_assertion_failure(self) -> None:
        assert self.params.dataset_field_parameters is not None

        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 1
        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_field_metric_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_field_metric_assertion_changed_rows_query_no_prev_state(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.CHANGED_ROWS_QUERY
        )
        self.params.dataset_field_parameters.changed_rows_field = FreshnessFieldSpec(
            path="last_updated",
            type="TIMESTAMP",
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.state_provider.get_state.return_value = None
        self.source.get_current_high_watermark_for_column.return_value = TEST_TIME, 10

        result = self.evaluator._evaluate_field_metric_assertion(
            TEST_ENTITY_URN,
            self.assertion.field_assertion,
            self.params.dataset_field_parameters,
            self.source,
            self.database_params,
            self.context,
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_invalid_metric(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.NEGATIVE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        with pytest.raises(InvalidParametersException):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_no_data(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = None

        with pytest.raises(InsufficientDataException):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN, self.assertion.field_assertion
            )

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_no_profile(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(fieldProfiles=[], timestampMillis=TEST_START)
        )

        with pytest.raises(InsufficientDataException):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_no_metric_value(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=None)
                ],
                timestampMillis=TEST_START,
            )
        )

        with pytest.raises(InsufficientDataException):
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_success(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=10)
                ],
                timestampMillis=TEST_START,
            )
        )

        result = (
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )
        )
        assert result.type == AssertionResultType.SUCCESS

    def test_evaluate_field_metric_assertion_datahub_dataset_profile_failure(
        self,
    ) -> None:
        assert self.params.dataset_field_parameters is not None

        self.params.dataset_field_parameters.source_type = (
            DatasetFieldSourceType.DATAHUB_DATASET_PROFILE
        )
        field_assertion = FieldAssertion(
            type=FieldAssertionType.FIELD_METRIC,
            field_metric_assertion=FieldMetricAssertion.parse_obj(
                {
                    "field": {
                        "path": "id",
                        "type": "STRING",
                        "native_type": "STRING",
                    },
                    "metric": FieldMetricType.UNIQUE_COUNT.value,
                    "operator": "EQUAL_TO",
                    "parameters": {
                        "value": {
                            "value": "10",
                            "type": "NUMBER",
                        }
                    },
                }
            ),
        )
        self.assertion.field_assertion = field_assertion

        self.source.get_field_metric_value.return_value = 10
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(
                fieldProfiles=[
                    DatasetFieldProfileClass(fieldPath="id", uniqueCount=100)
                ],
                timestampMillis=TEST_START,
            )
        )

        result = (
            self.evaluator._evaluate_datahub_dataset_profile_field_metric_assertion(
                TEST_ENTITY_URN,
                self.assertion.field_assertion,
            )
        )
        assert result.type == AssertionResultType.FAILURE

    def test__get_field_metric_value_from_dataset_field_profile_unique_count(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", uniqueCount=100
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_COUNT
            )
        )
        assert metric_value == 100.0

    def test__get_field_metric_value_from_dataset_field_profile_unique_count_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", uniqueCount=None
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_COUNT
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_unique_percentage(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", uniqueProportion=0.20
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_PERCENTAGE
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_unique_percentage_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", uniqueProportion=None
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.UNIQUE_PERCENTAGE
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_null_count(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", nullCount=100)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_COUNT
            )
        )
        assert metric_value == 100.0

    def test__get_field_metric_value_from_dataset_field_profile_null_count_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", nullCount=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_COUNT
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_null_percentage(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", nullProportion=0.20
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_PERCENTAGE
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_null_percentage_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(
            fieldPath="id", nullProportion=None
        )
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.NULL_PERCENTAGE
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_min(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", min="20")
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MIN
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_min_none(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", min=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MIN
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_max(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", max="20")
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MAX
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_max_none(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", max=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MAX
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_mean(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", mean="20")
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEAN
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_mean_none(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", mean=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEAN
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_median(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", median="20")
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEDIAN
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_median_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", median=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.MEDIAN
            )
        )
        assert metric_value is None

    def test__get_field_metric_value_from_dataset_field_profile_stdev(self) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", stdev="20")
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.STDDEV
            )
        )
        assert metric_value == 20.0

    def test__get_field_metric_value_from_dataset_field_profile_stdev_none(
        self,
    ) -> None:
        dataset_field_profile = DatasetFieldProfileClass(fieldPath="id", stdev=None)
        metric_value = (
            self.evaluator._get_field_metric_value_from_dataset_field_profile(
                dataset_field_profile, FieldMetricType.STDDEV
            )
        )
        assert metric_value is None
