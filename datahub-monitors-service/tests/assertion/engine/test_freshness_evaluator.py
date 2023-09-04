from unittest.mock import ANY, Mock, patch

import pytest

from datahub_monitors.assertion.engine.evaluator.freshness_evaluator import (
    FreshnessAssertionEvaluator,
)
from datahub_monitors.assertion.types import AssertionState, AssertionStateType
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
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FreshnessFieldKind,
    SchemaFieldSpec,
)

TEST_START = 1687643700064
TEST_END = 1687644000064


class TestFreshnessEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=ConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.evaluator = FreshnessAssertionEvaluator(
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
            freshnessAssertion=None,
        )
        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                sourceType=DatasetFreshnessSourceType.FIELD_VALUE,
                field=SchemaFieldSpec(
                    path="col_timestamp",
                    type="TIME",
                    native_type="TIMESTAMP",
                    kind=FreshnessFieldKind.HIGH_WATERMARK,
                ),
            ),
        )

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.FRESHNESS

    def test_evaluate_high_watermark_assertion_no_urn(self) -> None:
        self.context = AssertionEvaluationContext()
        with pytest.raises(Exception):
            self.evaluator._evaluate_internal_window_event(
                [TEST_START, TEST_END],
                self.assertion,
                self.params,
                self.connection,
                self.context,
            )

    def test_evaluate_high_watermark_assertion_no_previous_state(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_current_high_watermark_for_column.return_value = "", 0

        self.state_provider.get_state.return_value = None
        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.INIT

    def test_evaluate_high_watermark_assertion_old_previous_state(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_current_high_watermark_for_column.return_value = "", 0

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START - (5 * 60 * 1000) - 1,
            properties={
                "field_value": "",
                "row_count": 0,
            },
        )
        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.INIT

    def test_evaluate_high_watermark_assertion_no_change_assertion_fails(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        entity_state = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START - 10,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
            },
        )
        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-04 12:00:00",
            100,
        )
        self.state_provider.get_state.return_value = entity_state

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.FAILURE

    def test_evaluate_high_watermark_assertion_field_value_blank_assertion_fails(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = "", 100
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.FAILURE

    def test_evaluate_high_watermark_assertion_row_count_zero_assertion_fails(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-05 12:00:00",
            0,
        )
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.FAILURE

    def test_evaluate_high_watermark_assertion_last_state_change_invalid_assertion_fails(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-05 12:00:00",
            0,
        )
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
                "last_state_change": "0",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.FAILURE

    def test_evaluate_high_watermark_assertion_last_state_change_valid_assertion_success(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-05 12:00:00",
            0,
        )
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
                "last_state_change": str(TEST_START),
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.SUCCESS

    def test_evaluate_high_watermark_assertion_field_value_change_assertion_success(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-05 12:00:00",
            100,
        )
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 100,
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.SUCCESS

    def test_evaluate_high_watermark_assertion_row_count_change_assertion_success(
        self,
    ) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-04 12:00:00",
            100,
        )
        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": 200,
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.connection,
            self.context,
        )
        assert eval_result.type == AssertionResultType.SUCCESS

    @patch.object(
        FreshnessAssertionEvaluator,
        "_evaluate_datahub_operation_assertion",
        return_value=None,
    )
    def test_evaluate_datahub_operation_assertion(
        self, mock_eval_datahub_op_assertion: Mock
    ) -> None:
        operation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                sourceType=DatasetFreshnessSourceType.DATAHUB_OPERATION,
            ),
        )
        self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            operation_params,
            self.connection,
            self.context,
        )

        mock_eval_datahub_op_assertion.assert_called_once_with(
            "urn:li:dataset:test", [TEST_START, TEST_END], ANY
        )
