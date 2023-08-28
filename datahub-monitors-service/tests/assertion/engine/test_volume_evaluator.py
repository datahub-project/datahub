from unittest.mock import Mock

import pytest

from datahub_monitors.assertion.engine.evaluator.volume_evaluator import (
    VolumeAssertionEvaluator,
)
from datahub_monitors.assertion.types import AssertionState, AssertionStateType
from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.provider import ConnectionProvider
from datahub_monitors.exceptions import InvalidParametersException
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
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    RowCountChange,
    RowCountTotal,
    VolumeAssertion,
    VolumeAssertionType,
)

TEST_START = 1687643700064

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)


class TestVolumeEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=ConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.evaluator = VolumeAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
        )
        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.VOLUME,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platformUrn="urn:li:dataPlatform:snowflake",
                platformInstance=None,
                subTypes=None,
                table_name="test_table",
                qualifiedName="test_db.public.test_table",
            ),
            connectionUrn="urn:li:dataPlatform:snowflake",
            freshnessAssertion=None,
            volumeAssertion=None,
        )
        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                sourceType=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
        )

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.VOLUME

    def test_compare_values_between_no_min_value(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.evaluator._compare_values(
                999, AssertionStdOperator.BETWEEN, None, None, 1000
            )

    def test_compare_values_between_no_max_value(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.evaluator._compare_values(
                999,
                AssertionStdOperator.BETWEEN,
                None,
                900,
                None,
            )

    def test_compare_values_between(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.BETWEEN,
            None,
            900,
            1000,
        )
        assert row_count_evaluation is True

    def test_compare_values_greater_than_no_value(self) -> None:
        with pytest.raises(InvalidParametersException):
            self.evaluator._compare_values(
                999, AssertionStdOperator.GREATER_THAN, None, None, None
            )

    def test_compare_values_greater_than(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.GREATER_THAN,
            900,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_greater_than_or_equal1(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            999,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_greater_than_or_equal2(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
            900,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_equal(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.EQUAL_TO,
            999,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_less_than(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.LESS_THAN,
            1000,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_less_than_or_equal1(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            999,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_compare_values_less_than_or_equal2(self) -> None:
        row_count_evaluation = self.evaluator._compare_values(
            999,
            AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
            1000,
            None,
            None,
        )
        assert row_count_evaluation is True

    def test_evaluate_row_count_total_success(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            row_count_total=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="999", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "row_count": 999,
        }

    def test_evaluate_row_count_total_failure(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            row_count_total=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="1000", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_absolute_success(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="100", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {"row_count": 999, "prev_row_count": "899"}

    def test_evaluate_row_count_change_absolute_success_between(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.BETWEEN,
                parameters=AssertionStdParameters(
                    min_value=AssertionStdParameter(value="100", type="NUMBER"),
                    max_value=AssertionStdParameter(value="200", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 1020

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {"row_count": 1020, "prev_row_count": "899"}

    def test_evaluate_row_count_change_absolute_fail(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="200", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_percentage_success(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.PERCENTAGE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="10", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 1100

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "1000",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {"row_count": 1100, "prev_row_count": "1000"}

    def test_evaluate_row_count_change_percentage_success_between(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.PERCENTAGE,
                operator=AssertionStdOperator.BETWEEN,
                parameters=AssertionStdParameters(
                    min_value=AssertionStdParameter(value="10", type="NUMBER"),
                    max_value=AssertionStdParameter(value="15", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 1125

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "1000",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {"row_count": 1125, "prev_row_count": "1000"}

    def test_evaluate_row_count_change_percentage_fail(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="10", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_no_previous_state(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="10", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_change_empty_previous_state(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="10", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={},
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.connection, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_change_no_monitor_urn(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(value="10", type="NUMBER"),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion
        self.context.monitor_urn = ""

        with pytest.raises(InvalidParametersException):
            self.evaluator._evaluate_internal(
                self.assertion, self.params, self.connection, self.context
            )
