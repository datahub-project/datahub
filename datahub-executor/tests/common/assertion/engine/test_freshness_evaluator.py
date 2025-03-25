import time
from unittest.mock import ANY, Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import OperationClass

from datahub_executor.common.assertion.engine.evaluator.freshness_evaluator import (
    FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS,
    FreshnessAssertionEvaluator,
    InternalAssertionEvaluationResult,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.monitor.client.client import (
    MonitorClient,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.source.source import Source
from datahub_executor.common.state.assertion_state_provider import (
    AssertionStateProvider,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationSpec,
    AssertionInferenceDetails,
    AssertionResultType,
    AssertionType,
    CalendarInterval,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    EntityEvent,
    EntityEventType,
    FixedIntervalSchedule,
    FreshnessAssertion,
    FreshnessAssertionSchedule,
    FreshnessAssertionScheduleType,
    FreshnessAssertionType,
    FreshnessCronSchedule,
    FreshnessFieldKind,
    FreshnessFieldSpec,
)

TEST_START = 1687643700064
TEST_END = 1687644000064


class TestFreshnessEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=AssertionStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.monitor_client = Mock(spec=MonitorClient)
        self.evaluator = FreshnessAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
            self.monitor_client,
        )
        self.assertion = Assertion(
            urn="urn:li:assertion:test",
            type=AssertionType.FRESHNESS,
            entity=AssertionEntity(
                urn="urn:li:dataset:test",
                platform_urn="urn:li:dataPlatform:snowflake",
                platform_instance=None,
                sub_types=None,
                table_name="test_table",
                qualified_name="test_db.public.test_table",
                exists=True,
            ),
            connectionUrn="urn:li:dataPlatform:snowflake",
        )
        self.connection = Connection(
            "urn:li:dataPlatform:snowflake", "urn:li:dataPlatform:snowflake"
        )
        self.connection_provider.get_connection.return_value = self.connection
        self.context = AssertionEvaluationContext(monitor_urn="urn:li:monitor:test")
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                source_type=DatasetFreshnessSourceType.FIELD_VALUE,
                field=FreshnessFieldSpec(
                    path="col_timestamp",
                    type="TIME",
                    native_type="TIMESTAMP",
                    kind=FreshnessFieldKind.HIGH_WATERMARK,
                ),
            ),
        )

    def test_evaluator_type(self) -> None:
        assert self.evaluator.type == AssertionType.FRESHNESS

    def test_default_parameters(self) -> None:
        """Test the default_parameters property."""
        default_params = self.evaluator.default_parameters
        assert (
            default_params.type == AssertionEvaluationParametersType.DATASET_FRESHNESS
        )
        assert default_params.dataset_freshness_parameters is not None
        assert (
            default_params.dataset_freshness_parameters.source_type
            == DatasetFreshnessSourceType.INFORMATION_SCHEMA
        )

    def test_evaluate_high_watermark_assertion_no_urn(self) -> None:
        self.context = AssertionEvaluationContext()
        with pytest.raises(Exception):
            self.evaluator._evaluate_internal_window_event(
                [TEST_START, TEST_END],
                self.assertion,
                self.params,
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
            self.context,
        ).result
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
                "row_count": "0",
            },
        )
        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
        assert eval_result.type == AssertionResultType.INIT

    def test_evaluate_high_watermark_assertion_no_change_assertion_fails(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        entity_state = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START - 10,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": "100",
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
            self.context,
        ).result
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
                "row_count": "100",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
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
                "row_count": "100",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
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
                "row_count": "100",
                "last_state_change": "0",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
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
                "row_count": "100",
                "last_state_change": str(TEST_START),
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
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
                "row_count": "100",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
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
                "row_count": "200",
            },
        )

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
        assert eval_result.type == AssertionResultType.SUCCESS

    @patch.object(
        FreshnessAssertionEvaluator,
        "_evaluate_datahub_operation_assertion",
        return_value=InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters=None
            )
        ),
    )
    def test_evaluate_datahub_operation_assertion(
        self, mock_eval_datahub_op_assertion: Mock
    ) -> None:
        operation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                source_type=DatasetFreshnessSourceType.DATAHUB_OPERATION,
            ),
        )
        result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            operation_params,
            self.context,
        )

        mock_eval_datahub_op_assertion.assert_called_once_with(
            "urn:li:dataset:test", [TEST_START, TEST_END], ANY
        )

        assert (
            result.result.parameters is not None
            and result.result.parameters["window_start_time"] == str(TEST_START)
            and result.result.parameters["window_end_time"] == str(TEST_END)
        )

    def test_evaluate_high_watermark_assertion_no_monitor_urn(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_current_high_watermark_for_column.return_value = (
            "2023-07-04 12:00:00",
            100,
        )

        self.context.monitor_urn = ""

        eval_result = self.evaluator._evaluate_internal_window_event(
            [TEST_START, TEST_END],
            self.assertion,
            self.params,
            self.context,
        ).result
        assert eval_result.type == AssertionResultType.INIT
        assert self.state_provider.get_state.call_count == 0
        assert self.state_provider.save_state.call_count == 0

    def test_evaluate_since_last_check_no_monitor_urn(self) -> None:
        self.context.monitor_urn = ""
        self.assertion.freshness_assertion = FreshnessAssertion(
            type=FreshnessAssertionType.DATASET_CHANGE,
            schedule=FreshnessAssertionSchedule(
                type=FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK,
                fixed_interval=None,
            ),
        )
        result = self.evaluator._evaluate_internal_since_last_check(
            self.assertion, self.params, self.context
        ).result
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.get_state.call_count == 0
        assert self.state_provider.save_state.call_count == 0

    def test_evaluate_since_last_check_no_previous_state(self) -> None:
        self.state_provider.get_state.return_value = None
        self.assertion.freshness_assertion = FreshnessAssertion(
            type=FreshnessAssertionType.DATASET_CHANGE,
            schedule=FreshnessAssertionSchedule(
                type=FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK,
                fixed_interval=None,
            ),
        )
        eval_result_internal = self.evaluator._evaluate_internal_since_last_check(
            self.assertion, self.params, self.context
        )
        assert eval_result_internal.result.type == AssertionResultType.INIT
        assert eval_result_internal.next_assertion_state is not None

    def test_evaluate_since_last_check_highwatermark_change_success(self) -> None:
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock

        high_watermark_column_row_count = 0
        current_field_value = "2023-07-05 12:00:00"
        source_mock.get_current_high_watermark_for_column.return_value = (
            current_field_value,
            high_watermark_column_row_count,
        )
        last_state = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": "100",
                "last_state_change": str(TEST_START),
            },
        )
        self.state_provider.get_state.return_value = last_state

        self.assertion.freshness_assertion = FreshnessAssertion(
            type=FreshnessAssertionType.DATASET_CHANGE,
            schedule=FreshnessAssertionSchedule(
                type=FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK,
                fixed_interval=None,
            ),
        )

        start_millis = int(time.time() * 1000)
        eval_internal_result = self.evaluator._evaluate_internal_since_last_check(
            self.assertion,
            self.params,
            self.context,
        )
        end_millis = int(time.time() * 1000)
        assert eval_internal_result.result.type == AssertionResultType.SUCCESS
        assert eval_internal_result.next_assertion_state is not None
        next_assertion_state_ts = eval_internal_result.next_assertion_state.timestamp
        assert (
            next_assertion_state_ts is not None
            and next_assertion_state_ts >= start_millis
            and next_assertion_state_ts <= end_millis
        )
        assert (
            eval_internal_result.result.parameters is not None
            and eval_internal_result.result.parameters["window_start_time"]
            == str(TEST_START)
            and int(eval_internal_result.result.parameters["window_end_time"])
            >= start_millis
            and int(eval_internal_result.result.parameters["window_end_time"])
            <= end_millis + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS * 1000
        )
        assert eval_internal_result.next_assertion_state.properties["row_count"] == str(
            high_watermark_column_row_count
        )
        assert (
            eval_internal_result.next_assertion_state.properties["field_value"]
            == current_field_value
        )

    @patch.object(
        FreshnessAssertionEvaluator,
        "_evaluate_datahub_operation_assertion",
        return_value=InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters=None
            )
        ),
    )
    def test_evaluate_since_last_check_datahub_operation_success(
        self, mock_eval_datahub_op_assertion: Mock
    ) -> None:
        operation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
            dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
                source_type=DatasetFreshnessSourceType.DATAHUB_OPERATION,
            ),
        )
        last_state = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "field_value": "2023-07-04 12:00:00",
                "row_count": "100",
                "last_state_change": str(TEST_START),
            },
        )
        self.state_provider.get_state.return_value = last_state

        self.assertion.freshness_assertion = FreshnessAssertion(
            type=FreshnessAssertionType.DATASET_CHANGE,
            schedule=FreshnessAssertionSchedule(
                type=FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK,
                fixed_interval=None,
            ),
        )
        start_millis = int(time.time() * 1000)
        result = self.evaluator._evaluate_internal_since_last_check(
            self.assertion,
            operation_params,
            self.context,
        )
        end_millis = int(time.time() * 1000)

        mock_eval_datahub_op_assertion.assert_called_once_with(
            "urn:li:dataset:test", [TEST_START, ANY], ANY
        )

        assert result.next_assertion_state is not None
        next_assertion_state_ts = result.next_assertion_state.timestamp
        assert (
            next_assertion_state_ts is not None
            and next_assertion_state_ts >= start_millis
            and next_assertion_state_ts
            <= end_millis + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS * 1000
        )

    # New tests to improve coverage

    def test_evaluate_high_watermark_freshness(self) -> None:
        """Test the _evaluate_high_watermark_freshness method."""
        # Test: blank current field value fails
        assert not self.evaluator._evaluate_high_watermark_freshness(
            "urn:li:assertion:test", "previous", 100, "", 200
        )

        # Test: zero row count fails
        assert not self.evaluator._evaluate_high_watermark_freshness(
            "urn:li:assertion:test", "previous", 100, "current", 0
        )

        # Test: field value change success
        assert self.evaluator._evaluate_high_watermark_freshness(
            "urn:li:assertion:test", "previous", 100, "current", 200
        )

        # Test: row count change success
        assert self.evaluator._evaluate_high_watermark_freshness(
            "urn:li:assertion:test", "2023-07-04", 100, "2023-07-04", 200
        )

        # Test: no change fails
        assert not self.evaluator._evaluate_high_watermark_freshness(
            "urn:li:assertion:test", "2023-07-04", 100, "2023-07-04", 100
        )

    def test_evaluate_assertion(self) -> None:
        """Test the _evaluate_assertion method."""
        source_mock = Mock(spec=Source)

        # Test with events - success
        events = [EntityEvent(EntityEventType.AUDIT_LOG_OPERATION, TEST_START + 100)]
        source_mock.get_entity_events.return_value = events

        result = self.evaluator._evaluate_assertion(
            source_mock,
            self.assertion,
            "urn:li:dataset:test",
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {},
        )

        assert result.result.type == AssertionResultType.SUCCESS
        assert result.result.parameters is not None
        assert result.result.parameters["events"] == events

        # Test without events - failure
        source_mock.get_entity_events.return_value = []

        result = self.evaluator._evaluate_assertion(
            source_mock,
            self.assertion,
            "urn:li:dataset:test",
            EntityEventType.AUDIT_LOG_OPERATION,
            [TEST_START, TEST_END],
            {},
        )

        assert result.result.type == AssertionResultType.FAILURE

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.freshness_evaluator.get_next_cron_schedule_time"
    )
    @patch(
        "datahub_executor.common.assertion.engine.evaluator.freshness_evaluator.get_prev_cron_schedule_time"
    )
    def test_evaluate_internal_cron(
        self, mock_get_prev: Mock, mock_get_next: Mock
    ) -> None:
        """Test the _evaluate_internal_cron method."""
        # Setup mocks
        mock_get_next.return_value = TEST_END
        mock_get_prev.return_value = TEST_START

        # Create mock for _evaluate_internal_window_event
        with patch.object(
            self.evaluator,
            "_evaluate_internal_window_event",
            return_value=InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(AssertionResultType.SUCCESS)
            ),
        ) as mock_eval_window:
            # Setup assertion with cron schedule
            self.assertion.freshness_assertion = FreshnessAssertion(
                type=FreshnessAssertionType.DATASET_CHANGE,
                schedule=FreshnessAssertionSchedule(
                    type=FreshnessAssertionScheduleType.CRON,
                    cron=FreshnessCronSchedule(
                        cron="0 0 * * *",
                        timezone="UTC",
                        windowStartOffsetMs=3600000,  # 1 hour
                    ),
                    fixedInterval=None,
                ),
            )

            # Execute
            result = self.evaluator._evaluate_internal_cron(
                self.assertion,
                self.params,
                self.context,
            )

            # Verify
            assert result.result.type == AssertionResultType.SUCCESS
            expected_window = [
                TEST_END - 3600000,  # Start time with offset
                TEST_END
                + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS
                * 1000,  # End time with buffer
            ]
            mock_eval_window.assert_called_once_with(
                expected_window, self.assertion, self.params, self.context
            )

            # Test without window_start_offset_ms
            self.assertion.freshness_assertion.schedule.cron.window_start_offset_ms = (  # type: ignore
                None  # type: ignore
            )

            # Reset mock
            mock_eval_window.reset_mock()

            # Execute
            self.evaluator._evaluate_internal_cron(
                self.assertion,
                self.params,
                self.context,
            )

            # Verify - should use prev_cron_schedule_time as start
            expected_window = [
                TEST_START,  # Start time without offset (previous cron time)
                TEST_END
                + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS
                * 1000,  # End time with buffer
            ]
            mock_eval_window.assert_called_once_with(
                expected_window, self.assertion, self.params, self.context
            )

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.freshness_evaluator.get_fixed_interval_start"
    )
    @patch("time.time")
    def test_evaluate_internal_fixed_interval(
        self, mock_time: Mock, mock_get_fixed_interval_start: Mock
    ) -> None:
        """Test the _evaluate_internal_fixed_interval method."""
        # Setup mocks
        mock_current_time_ms = TEST_END
        mock_time.return_value = mock_current_time_ms / 1000  # Time in seconds
        mock_fixed_interval_start = TEST_START
        mock_get_fixed_interval_start.return_value = mock_fixed_interval_start

        # Create mock for _evaluate_internal_window_event
        with patch.object(
            self.evaluator,
            "_evaluate_internal_window_event",
            return_value=InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(AssertionResultType.SUCCESS)
            ),
        ) as mock_eval_window:
            # Setup assertion with fixed interval schedule
            self.assertion.freshness_assertion = FreshnessAssertion(
                type=FreshnessAssertionType.DATASET_CHANGE,
                schedule=FreshnessAssertionSchedule(
                    type=FreshnessAssertionScheduleType.FIXED_INTERVAL,
                    fixedInterval=FixedIntervalSchedule(
                        unit=CalendarInterval.DAY,
                        multiple=1,
                    ),
                ),
            )

            # Execute
            result = self.evaluator._evaluate_internal_fixed_interval(
                self.assertion,
                self.params,
                self.context,
            )

            # Verify
            assert result.result.type == AssertionResultType.SUCCESS
            expected_window = [
                mock_fixed_interval_start,
                mock_current_time_ms
                + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS * 1000,
            ]
            mock_eval_window.assert_called_once_with(
                expected_window, self.assertion, self.params, self.context
            )
            mock_get_fixed_interval_start.assert_called_once_with(
                mock_current_time_ms
                + FRESHNESS_ASSERTION_QUERY_EVALUATION_BUFFER_SECONDS * 1000,
                self.assertion.freshness_assertion.schedule.fixed_interval,  # type: ignore
            )

    def test_evaluate_datahub_operation_assertion_impl(self) -> None:
        """Test the _evaluate_datahub_operation_assertion implementation."""
        # Mock the DataHubGraph
        mock_graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph = mock_graph

        # Setup operation aspects
        operation_aspects = [
            OperationClass(
                timestampMillis=TEST_START + 100,
                operationType="INSERT",
                lastUpdatedTimestamp=TEST_START + 100,
            )
        ]
        mock_graph.get_timeseries_values.return_value = operation_aspects

        # Execute
        window = [TEST_START, TEST_END]
        result = self.evaluator._evaluate_datahub_operation_assertion(
            "urn:li:dataset:test", window, {}
        )

        # Verify success with events
        assert result.result.type == AssertionResultType.SUCCESS
        assert result.result.parameters is not None
        assert len(result.result.parameters["events"]) == 1
        assert (
            result.result.parameters["events"][0].event_type
            == EntityEventType.DATAHUB_OPERATION
        )
        assert result.result.parameters["events"][0].event_time == TEST_START + 100

        # Test failure case
        mock_graph.get_timeseries_values.return_value = []

        # Execute
        result = self.evaluator._evaluate_datahub_operation_assertion(
            "urn:li:dataset:test", window, {}
        )

        # Verify failure
        assert result.result.type == AssertionResultType.FAILURE

    def test_evaluate_assertion_step(self) -> None:
        """Test the _evaluate_assertion_step method."""
        # Patch the different schedule type methods
        with patch.object(
            self.evaluator,
            "_evaluate_internal_cron",
            return_value=InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(AssertionResultType.SUCCESS),
                next_assertion_state=AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=TEST_END,
                    properties={"test": "value"},
                ),
            ),
        ) as mock_eval_cron:
            # Setup assertion with CRON schedule
            self.assertion.freshness_assertion = FreshnessAssertion(
                type=FreshnessAssertionType.DATASET_CHANGE,
                schedule=FreshnessAssertionSchedule(
                    type=FreshnessAssertionScheduleType.CRON,
                    cron=FreshnessCronSchedule(
                        cron="0 0 * * *",
                        timezone="UTC",
                        windowStartOffsetMs=None,
                    ),
                    fixedInterval=None,
                ),
            )

            # Execute
            result = self.evaluator._evaluate_assertion_step(
                self.assertion, self.params, self.context
            )

            # Verify
            assert result.type == AssertionResultType.SUCCESS
            mock_eval_cron.assert_called_once()
            self.state_provider.save_state.assert_called_once()

        # Reset mocks
        self.state_provider.save_state.reset_mock()

        # Test FIXED_INTERVAL type
        with patch.object(
            self.evaluator,
            "_evaluate_internal_fixed_interval",
            return_value=InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(AssertionResultType.SUCCESS),
                next_assertion_state=None,  # Test case when next_assertion_state is None
            ),
        ) as mock_eval_fixed:
            # Setup assertion with FIXED_INTERVAL schedule
            self.assertion.freshness_assertion.schedule.type = (  # type: ignore
                FreshnessAssertionScheduleType.FIXED_INTERVAL
            )  # type: ignore
            self.assertion.freshness_assertion.schedule.fixed_interval = (  # type: ignore
                FixedIntervalSchedule(  # type: ignore
                    unit=CalendarInterval.DAY,
                    multiple=1,
                )
            )

            # Execute
            result = self.evaluator._evaluate_assertion_step(
                self.assertion, self.params, self.context
            )

            # Verify
            assert result.type == AssertionResultType.SUCCESS
            mock_eval_fixed.assert_called_once()
            self.state_provider.save_state.assert_not_called()  # Since next_assertion_state is None

        # Test SINCE_THE_LAST_CHECK type
        with patch.object(
            self.evaluator,
            "_evaluate_internal_since_last_check",
            return_value=InternalAssertionEvaluationResult(
                result=AssertionEvaluationResult(AssertionResultType.FAILURE)
            ),
        ) as mock_eval_since_last:
            # Setup assertion with SINCE_THE_LAST_CHECK schedule
            self.assertion.freshness_assertion.schedule.type = (  # type: ignore
                FreshnessAssertionScheduleType.SINCE_THE_LAST_CHECK
            )  # type: ignore

            # Execute
            result = self.evaluator._evaluate_assertion_step(
                self.assertion, self.params, self.context
            )

            # Verify
            assert result.type == AssertionResultType.FAILURE
            mock_eval_since_last.assert_called_once()

        # Test invalid schedule type
        self.assertion.freshness_assertion.schedule.type = "INVALID_TYPE"  # type: ignore

        with pytest.raises(InvalidParametersException):
            self.evaluator._evaluate_assertion_step(
                self.assertion, self.params, self.context
            )

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.freshness_evaluator.ONLINE_SMART_ASSERTIONS_ENABLED",
        True,
    )
    @patch(
        "datahub_executor.common.assertion.engine.evaluator.freshness_evaluator.is_training_required"
    )
    def test_evaluate_internal_smart_assertions_inference_required(
        self, mock_is_training_required: Mock
    ) -> None:
        """Test _evaluate_internal with smart assertions V2 enabled and inference required."""
        # Setup
        mock_is_training_required.return_value = True

        # No evaluation spec
        self.context.evaluation_spec = Mock(AssertionEvaluationSpec)
        self.context.evaluation_spec.context = None

        # Execute
        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )

        # Verify
        assert result.type == AssertionResultType.INIT

        # With evaluation spec but no inference details
        eval_context = Mock()
        eval_context.inference_details = None
        self.context.evaluation_spec = Mock(spec=AssertionEvaluationSpec)
        self.context.evaluation_spec.context = eval_context

        # Execute
        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )

        # Verify
        assert result.type == AssertionResultType.INIT

        # With inference details but generated_at <= 0
        inference_details = Mock(spec=AssertionInferenceDetails)
        inference_details.generated_at = 0
        eval_context.inference_details = inference_details

        # Execute
        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )

        # Verify
        assert result.type == AssertionResultType.INIT

        # With valid inference details
        inference_details.generated_at = TEST_START

        # Setup for successful evaluation
        with patch.object(
            self.evaluator,
            "_evaluate_assertion_step",
            return_value=AssertionEvaluationResult(AssertionResultType.SUCCESS),
        ) as mock_eval_step:
            # Execute
            result = self.evaluator._evaluate_internal(
                self.assertion, self.params, self.context
            )

            # Verify
            assert result.type == AssertionResultType.SUCCESS
            mock_eval_step.assert_called_once()

    def test_hydrate_window_event_result_with_window_parameters(self) -> None:
        """Test _hydrate_window_event_result_with_window_parameters method."""
        # Test with empty window
        internal_result = InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(AssertionResultType.SUCCESS)
        )
        result = self.evaluator._hydrate_window_event_result_with_window_parameters(
            internal_result, []
        )
        assert result.result.parameters is None

        # Test with window start only
        internal_result = InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(AssertionResultType.SUCCESS)
        )
        result = self.evaluator._hydrate_window_event_result_with_window_parameters(
            internal_result, [TEST_START]
        )
        assert result.result.parameters is not None
        assert result.result.parameters["window_start_time"] == str(TEST_START)
        assert "window_end_time" not in result.result.parameters

        # Test with window start and end
        internal_result = InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(AssertionResultType.SUCCESS)
        )
        result = self.evaluator._hydrate_window_event_result_with_window_parameters(
            internal_result, [TEST_START, TEST_END]
        )
        assert result.result.parameters is not None
        assert result.result.parameters["window_start_time"] == str(TEST_START)
        assert result.result.parameters["window_end_time"] == str(TEST_END)

        # Test with existing parameters
        internal_result = InternalAssertionEvaluationResult(
            result=AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters={"existing": "param"}
            )
        )
        result = self.evaluator._hydrate_window_event_result_with_window_parameters(
            internal_result, [TEST_START, TEST_END]
        )
        assert result.result.parameters is not None
        assert result.result.parameters["existing"] == "param"
        assert result.result.parameters["window_start_time"] == str(TEST_START)
        assert result.result.parameters["window_end_time"] == str(TEST_END)
