from unittest.mock import Mock

import pytest

from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.monitor.client.client import MonitorClient
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
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    AssertionValueChangeType,
    SQLAssertion,
    SQLAssertionType,
)

TEST_START = 1687643700064

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)
TEST_SQL_STATEMENT = "SELECT SUM(num_items) FROM test_db.public.test_table;"


class TestSQLEvaluator:
    def setup_method(self) -> None:
        from datahub_executor.common.assertion.engine.evaluator.sql_evaluator import (
            SQLAssertionEvaluator,
        )

        self.connection_provider = Mock()
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.monitor_client = Mock(spec=MonitorClient)
        self.metric_client = Mock(spec=MetricClient)
        self.evaluator = SQLAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
            self.monitor_client,
            self.metric_client,
        )
        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.VOLUME,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platform_urn="urn:li:dataPlatform:snowflake",
                platform_instance=None,
                sub_types=None,
                table_name="test_table",
                qualified_name="test_db.public.test_table",
            ),
            connection_urn="urn:li:dataPlatform:snowflake",
        )
        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.connection_provider.get_connection.return_value = self.connection
        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_SQL,
        )

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.SQL

    def test_evaluate_metric_success(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="999", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "metric_value": "999.0",
            "value": "999",
            "min_value": None,
            "max_value": None,
        }

    def test_evaluate_metric_success_not_equal_to(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.NOT_EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="1000", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "metric_value": "999.0",
            "value": "1000",
            "min_value": None,
            "max_value": None,
        }

    def test_evaluate_metric_failure(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="1000", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_runtime_parameters_substitution_in_statement(self) -> None:
        sql_with_vars = "SELECT COUNT(*) FROM test_db.public.test_table WHERE created_at >= ${start} AND id IN (${ids});"
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC,
            statement=sql_with_vars,
            operator=AssertionStdOperator.GREATER_THAN,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="0", type=AssertionStdParameterType.NUMBER
                )
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 10

        # Provide runtime parameters; note values should be pre-quoted if needed
        self.context.runtime_parameters = {
            "start": "'2024-10-01'",
            "ids": "1,2,3",
        }

        _ = self.evaluator._evaluate_internal(self.assertion, self.params, self.context)

        # Validate that substitution occurred as expected
        called_args = source_mock.execute_custom_sql.call_args[0]
        assert (
            called_args[2]
            == "SELECT COUNT(*) FROM test_db.public.test_table WHERE created_at >= '2024-10-01' AND id IN (1,2,3);"
        )

    def test_runtime_parameters_missing_raises(self) -> None:
        sql_with_vars = "SELECT COUNT(*) FROM test_db.public.test_table WHERE created_at >= ${start} AND id IN (${ids});"
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC,
            statement=sql_with_vars,
            operator=AssertionStdOperator.GREATER_THAN,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="0", type=AssertionStdParameterType.NUMBER
                )
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 10

        # Provide only one of the required params
        self.context.runtime_parameters = {
            "start": "'2024-10-01'",
        }

        with pytest.raises(InvalidParametersException):
            _ = self.evaluator._evaluate_internal(
                self.assertion, self.params, self.context
            )

    def test_evaluate_metric_change_absolute_success(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.ABSOLUTE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="100", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "metric_value": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "metric_value": "999.0",
            "prev_metric_value": "899",
        }

    def test_evaluate_metric_change_absolute_fail(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.ABSOLUTE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="200", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "metric_value": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_metric_change_percentage_success(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.PERCENTAGE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 1100

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "metric_value": "1000",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "metric_value": "1100.0",
            "prev_metric_value": "1000",
        }

    def test_evaluate_metric_change_percentage_fail(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.PERCENTAGE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "metric_value": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_metric_change_no_previous_state(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.PERCENTAGE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_metric_change_empty_previous_state(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.PERCENTAGE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_change_no_monitor_urn(self) -> None:
        sql_assertion = SQLAssertion(
            type=SQLAssertionType.METRIC_CHANGE,
            change_type=AssertionValueChangeType.PERCENTAGE,
            statement=TEST_SQL_STATEMENT,
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
            ),
        )
        self.assertion.sql_assertion = sql_assertion
        self.context.monitor_urn = ""

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.execute_custom_sql.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.get_state.call_count == 0
        assert self.state_provider.save_state.call_count == 0
