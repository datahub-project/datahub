from unittest.mock import Mock

import pytest

from datahub_monitors.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_monitors.assertion.types import (
    AssertionDatabaseParams,
    AssertionState,
    AssertionStateType,
)
from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.provider import ConnectionProvider
from datahub_monitors.source.provider import SourceProvider
from datahub_monitors.source.source import Source
from datahub_monitors.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)
from datahub_monitors.types import (
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
        self.connection_provider = Mock(spec=ConnectionProvider)
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
                self.connection,
                self.context,
            )

    def test_evaluate_internal_no_params(self) -> None:
        self.params.dataset_field_parameters = None
        with pytest.raises(Exception):
            self.evaluator._evaluate_internal(
                self.assertion,
                self.params,
                self.connection,
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
