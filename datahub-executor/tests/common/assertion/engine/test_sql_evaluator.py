from unittest.mock import Mock

from datahub_executor.common.assertion.engine.evaluator.sql_evaluator import (
    SQLAssertionEvaluator,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
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
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.monitor_client = Mock(spec=MonitorClient)
        self.evaluator = SQLAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
            self.monitor_client,
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
            "metric_value": "999",
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
            "metric_value": "999",
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
        assert result.parameters == {"metric_value": "999", "prev_metric_value": "899"}

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
            "metric_value": "1100",
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

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.get_state.call_count == 0
        assert self.state_provider.save_state.call_count == 0
