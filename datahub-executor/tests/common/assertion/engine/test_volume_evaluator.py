import time
from unittest.mock import Mock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DatasetProfileClass

from datahub_executor.common.assertion.engine.evaluator.volume_evaluator import (
    VolumeAssertionEvaluator,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.connection import Connection
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import InvalidParametersException
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.resolver.resolver import (
    MetricResolver,
)
from datahub_executor.common.monitor.client.client import (
    MonitorClient,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
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
    AssertionEvaluationSpec,
    AssertionEvaluationSpecContext,
    AssertionInferenceDetails,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    AssertionValueChangeType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    RowCountChange,
    RowCountTotal,
    VolumeAssertion,
    VolumeAssertionType,
)

TEST_START = int(time.time() * 1000)

TEST_ENTITY_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.test_table,PROD)"
)


class TestVolumeEvaluator:
    def setup_method(self) -> None:
        self.connection_provider = Mock(spec=DataHubIngestionSourceConnectionProvider)
        self.state_provider = Mock(spec=DataHubMonitorStateProvider)
        self.source_provider = Mock(spec=SourceProvider)
        self.metrics_client = Mock(spec=MetricClient)
        self.graph = Mock(spec=DataHubGraph)

        self.metrics_resolver = MetricResolver(
            self.connection_provider, self.source_provider
        )
        self.metrics_predictor = MetricPredictor()
        self.monitor_client = MonitorClient(graph=self.graph)

        self.evaluator = VolumeAssertionEvaluator(
            self.connection_provider,
            self.state_provider,
            self.source_provider,
            self.metrics_resolver,
            self.metrics_client,
            self.monitor_client,
        )

        print(dir(SourceProvider))  # Check if 'create_source_from_connection' exists

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
        from datahub.metadata.urns import MonitorUrn

        self.context = AssertionEvaluationContext(
            monitor_urn=MonitorUrn(TEST_ENTITY_URN, "__test__").urn()
        )
        self.params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.INFORMATION_SCHEMA
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
                    value=AssertionStdParameter(
                        value="999", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "row_count": 999,
        }
        # Ensure metric context is appended.
        assert result.metric
        assert result.metric.value == 999
        assert result.metric.timestamp_ms is not None

    def test_evaluate_row_count_total_failure(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            row_count_total=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="1000", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_absolute_success(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="100", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
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
                    min_value=AssertionStdParameter(
                        value="100", type=AssertionStdParameterType.NUMBER
                    ),
                    max_value=AssertionStdParameter(
                        value="200", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
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
                    value=AssertionStdParameter(
                        value="200", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_percentage_success(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.PERCENTAGE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
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
                    min_value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                    max_value=AssertionStdParameter(
                        value="15", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
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
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_row_count_change_no_previous_state(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_change_empty_previous_state(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
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
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_total_dataset_profile_success(self) -> None:
        evaluation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
            ),
        )
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            row_count_total=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="999", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(timestampMillis=TEST_START, rowCount=999)
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, evaluation_params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {
            "row_count": 999,
        }
        # Ensure metric context is appended.
        assert result.metric
        assert result.metric.value == 999
        assert result.metric.timestamp_ms is not None

    def test_evaluate_row_count_change_dataset_profile_no_previous_state(self) -> None:
        evaluation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
            ),
        )
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(timestampMillis=TEST_START, rowCount=999)
        )

        self.state_provider.get_state.return_value = None

        result = self.evaluator._evaluate_internal(
            self.assertion, evaluation_params, self.context
        )
        assert result.type == AssertionResultType.INIT

    def test_evaluate_row_count_change_dataset_profile_success(self) -> None:
        evaluation_params = AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.DATAHUB_DATASET_PROFILE
            ),
        )
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="100", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion
        self.connection_provider.graph = Mock(spec=DataHubGraph)
        self.connection_provider.graph.get_latest_timeseries_value.return_value = (
            DatasetProfileClass(timestampMillis=TEST_START, rowCount=999)
        )

        self.state_provider.get_state.return_value = AssertionState(
            type=AssertionStateType.MONITOR_TIMESERIES_STATE,
            timestamp=TEST_START,
            properties={
                "row_count": "899",
            },
        )

        result = self.evaluator._evaluate_internal(
            self.assertion, evaluation_params, self.context
        )
        assert result.type == AssertionResultType.SUCCESS
        assert result.parameters == {"row_count": 999, "prev_row_count": "899"}

    def test_evaluate_row_count_change_no_monitor_urn(self) -> None:
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_CHANGE,
            row_count_change=RowCountChange(
                type=AssertionValueChangeType.ABSOLUTE,
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="10", type=AssertionStdParameterType.NUMBER
                    ),
                ),
            ),
        )
        self.assertion.volume_assertion = volume_assertion
        self.context.monitor_urn = ""

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, self.context
        )
        assert result.type == AssertionResultType.INIT
        assert self.state_provider.get_state.call_count == 0
        assert self.state_provider.save_state.call_count == 0

    def test_default_parameters(self) -> None:
        """Test the default_parameters property."""
        default_params = self.evaluator.default_parameters
        assert default_params.type == AssertionEvaluationParametersType.DATASET_VOLUME
        assert default_params.dataset_volume_parameters is not None
        # Use the source_type attribute instead of sourceType
        assert (
            default_params.dataset_volume_parameters.source_type
            == DatasetVolumeSourceType.INFORMATION_SCHEMA
        )

    def test_evaluate_value_change_absolute(self) -> None:
        """Test _evaluate_value_change with ABSOLUTE change type."""
        # Test equal to
        result = self.evaluator._evaluate_value_change(
            AssertionValueChangeType.ABSOLUTE,
            900,
            1000,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdParameters(
                value=AssertionStdParameter(
                    value="100", type=AssertionStdParameterType.NUMBER
                ),
                minValue=None,
                maxValue=None,
            ),
        )
        assert result is True

        # Test not equal to
        result = self.evaluator._evaluate_value_change(
            AssertionValueChangeType.ABSOLUTE,
            900,
            950,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdParameters(
                value=AssertionStdParameter(
                    value="100", type=AssertionStdParameterType.NUMBER
                ),
                minValue=None,
                maxValue=None,
            ),
        )
        assert result is False

    def test_evaluate_value_change_percentage(self) -> None:
        """Test _evaluate_value_change with PERCENTAGE change type."""
        # Test equal to (10% increase from 1000 to 1100)
        result = self.evaluator._evaluate_value_change(
            AssertionValueChangeType.PERCENTAGE,
            1000,
            1100,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
                minValue=None,
                maxValue=None,
            ),
        )
        assert result is True

        # Test not equal to (5% increase from 1000 to 1050)
        result = self.evaluator._evaluate_value_change(
            AssertionValueChangeType.PERCENTAGE,
            1000,
            1050,
            AssertionStdOperator.EQUAL_TO,
            AssertionStdParameters(
                value=AssertionStdParameter(
                    value="10", type=AssertionStdParameterType.NUMBER
                ),
                minValue=None,
                maxValue=None,
            ),
        )
        assert result is False

    def test_unsupported_volume_assertion_type(self) -> None:
        """Test handling of unsupported volume assertion type."""
        # Create a test volume assertion with a valid enum value
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            rowCountTotal=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="999", type=AssertionStdParameterType.NUMBER
                    ),
                    minValue=None,
                    maxValue=None,
                ),
            ),
            rowCountChange=None,
            incrementingSegmentRowCountChange=None,
            incrementingSegmentRowCountTotal=None,
        )
        self.assertion.volume_assertion = volume_assertion

        # Mock the internal method to validate our case
        with patch.object(
            self.evaluator, "_evaluate_row_count_total_assertion"
        ) as mock_method:
            # Make the patched method raise an exception to simulate an unsupported type
            mock_method.side_effect = InvalidParametersException(
                message="Unsupported volume assertion type",
                parameters={"type": "UNSUPPORTED_TYPE"},
            )

            source_mock = Mock(spec=Source)
            self.source_provider.create_source_from_connection.return_value = (
                source_mock
            )
            source_mock.get_row_count.return_value = 999

            # Now we expect the exception to propagate
            with pytest.raises(InvalidParametersException):
                self.evaluator._evaluate_internal(
                    self.assertion, self.params, self.context
                )

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.volume_evaluator.is_training_required"
    )
    def test_evaluate_smart_assertion_v2_inference_required_no_inferred_at(
        self, mock_is_training_required: Mock
    ) -> None:
        """Test smart assertion v2 logic when inference is required but no last_inferred_at is present."""
        # Setup volume assertion
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            rowCountTotal=None,
            rowCountChange=None,
            incrementingSegmentRowCountChange=None,
            incrementingSegmentRowCountTotal=None,
        )
        self.assertion.volume_assertion = volume_assertion

        # Mock is_training_required to return True
        mock_is_training_required.return_value = True

        # Setup evaluation spec with no inference details
        evaluation_context = Mock(AssertionEvaluationContext)
        evaluation_context.evaluation_spec = Mock(AssertionEvaluationSpec)
        evaluation_context.evaluation_spec.context = Mock(
            AssertionEvaluationSpecContext
        )
        evaluation_context.online_smart_assertions = True
        evaluation_context.evaluation_spec.context.inference_details = (
            None  # No inference details
        )
        from datahub.metadata.urns import MonitorUrn

        evaluation_context.monitor_urn = MonitorUrn(TEST_ENTITY_URN, "__test__").urn()

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        # Execute
        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, evaluation_context
        )

        # Verify that init state is returned
        assert result.type == AssertionResultType.INIT

    @patch(
        "datahub_executor.common.assertion.engine.evaluator.volume_evaluator.is_training_required"
    )
    def test_evaluate_smart_assertion_v2_inference_required_with_inference(
        self, mock_is_training_required: Mock
    ) -> None:
        """Test smart assertion v2 logic when inference is required and last_inferred_at is present."""
        # Setup a volume assertion with valid row_count_total
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            rowCountTotal=RowCountTotal(
                operator=AssertionStdOperator.EQUAL_TO,
                parameters=AssertionStdParameters(
                    value=AssertionStdParameter(
                        value="1000", type=AssertionStdParameterType.NUMBER
                    ),
                    minValue=None,
                    maxValue=None,
                ),
            ),
            rowCountChange=None,
            incrementingSegmentRowCountChange=None,
            incrementingSegmentRowCountTotal=None,
        )
        self.assertion.volume_assertion = volume_assertion

        # Mock is_training_required to return True
        mock_is_training_required.return_value = True

        # Setup evaluation spec with inference details
        inference_details = Mock(spec=AssertionInferenceDetails)
        inference_details.generated_at = int(time.time() * 1000)  # Current time

        # Setup evaluation spec with inference details
        evaluation_context = Mock(AssertionEvaluationContext)
        evaluation_context.evaluation_spec = Mock(AssertionEvaluationSpec)
        evaluation_context.evaluation_spec.context = Mock(
            AssertionEvaluationSpecContext
        )
        evaluation_context.evaluation_spec.context.inference_details = (
            inference_details  # Some infernce details
        )
        from datahub.metadata.urns import MonitorUrn

        evaluation_context.monitor_urn = MonitorUrn(TEST_ENTITY_URN, "__test__").urn()
        evaluation_context.online_smart_assertions = True

        # Create a source mock to return row_count
        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        # This should now use the provided volume_assertion for evaluation
        result = self.evaluator._evaluate_internal(
            self.assertion, self.params, evaluation_context
        )

        # Verify result (should be FAILURE since 999 != 1000)
        assert result.type == AssertionResultType.FAILURE

    def test_evaluate_metric_collection_step(self) -> None:
        """Test the _evaluate_metric_collection_step method."""
        # Setup assertion
        volume_assertion = VolumeAssertion(
            type=VolumeAssertionType.ROW_COUNT_TOTAL,
            rowCountChange=None,
            rowCountTotal=None,
            incrementingSegmentRowCountChange=None,
            incrementingSegmentRowCountTotal=None,
        )
        self.assertion.volume_assertion = volume_assertion

        source_mock = Mock(spec=Source)
        self.source_provider.create_source_from_connection.return_value = source_mock
        source_mock.get_row_count.return_value = 999

        # Test with save=False
        result = self.evaluator._evaluate_metric_collection_step(
            self.assertion, self.params, self.context, save=False
        )
        assert result.value == 999
        self.metrics_client.save_metric_value.assert_not_called()

        # Test with save=True
        self.metrics_client.save_metric_value.reset_mock()
        result = self.evaluator._evaluate_metric_collection_step(
            self.assertion, self.params, self.context, save=True
        )
        assert result.value == 999
        self.metrics_client.save_metric_value.assert_called_once()


# Fixtures for the tests
@pytest.fixture
def evaluator(test_volume_evaluator: TestVolumeEvaluator) -> VolumeAssertionEvaluator:
    """Return the VolumeAssertionEvaluator instance from the existing test class."""
    return test_volume_evaluator.evaluator


@pytest.fixture
def assertion(test_volume_evaluator: TestVolumeEvaluator) -> Assertion:
    """Return the Assertion instance from the existing test class."""
    return test_volume_evaluator.assertion


@pytest.fixture
def context(test_volume_evaluator: TestVolumeEvaluator) -> AssertionEvaluationContext:
    """Return the AssertionEvaluationContext instance from the existing test class."""
    return test_volume_evaluator.context


@pytest.fixture
def params(test_volume_evaluator: TestVolumeEvaluator) -> AssertionEvaluationParameters:
    """Return the AssertionEvaluationParameters instance from the existing test class."""
    return test_volume_evaluator.params
