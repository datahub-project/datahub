from datetime import timedelta
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from datahub.metadata.schema_classes import FixedIntervalScheduleClass

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_helper_classes import (
    MaxNormalIntervalResult,
)
from datahub_executor.common.monitor.inference.metric_projection.config import (
    MetricProjectorConfig,
)

# Updated import path for MetricPredictor and MetricBoundary
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.metric_projection.types import (
    Bucket,
)


class TestMetricBoundary:
    """Tests for the MetricBoundary class."""

    def test_init(self) -> None:
        """Test initialization."""
        lower_bound = Metric(value=1.0, timestamp_ms=1000)
        upper_bound = Metric(value=5.0, timestamp_ms=1000)
        boundary = MetricBoundary(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            start_time_ms=1000,
            end_time_ms=2000,
        )

        assert boundary.lower_bound == lower_bound
        assert boundary.upper_bound == upper_bound
        assert boundary.start_time_ms == 1000
        assert boundary.end_time_ms == 2000

    def test_contains(self) -> None:
        """Test the contains method."""
        boundary = MetricBoundary(
            lower_bound=Metric(value=1.0, timestamp_ms=1000),
            upper_bound=Metric(value=5.0, timestamp_ms=1000),
            start_time_ms=1000,
            end_time_ms=2000,
        )

        # Test timestamp within range
        assert boundary.contains(1500) is True

        # Test boundary timestamps
        assert boundary.contains(1000) is True
        assert boundary.contains(2000) is True

        # Test timestamps outside range
        assert boundary.contains(999) is False
        assert boundary.contains(2001) is False

    def test_repr(self) -> None:
        """Test the string representation."""
        lower_bound = Metric(value=1.0, timestamp_ms=1000)
        upper_bound = Metric(value=5.0, timestamp_ms=1000)
        boundary = MetricBoundary(
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            start_time_ms=1000,
            end_time_ms=2000,
        )

        repr_str = repr(boundary)
        assert "MetricBoundary" in repr_str
        assert "lower_bound" in repr_str
        assert "upper_bound" in repr_str
        assert "start_time_ms" in repr_str
        assert "end_time_ms" in repr_str


class TestMetricPredictor:
    """Tests for the MetricPredictor class."""

    @pytest.fixture
    def predictor(self) -> MetricPredictor:
        """Return a predictor instance."""
        return MetricPredictor()

    @pytest.fixture
    def predictor_with_config(self) -> MetricPredictor:
        """Return a predictor instance with custom config."""
        return MetricPredictor(user_config={"CUSTOM_PARAM": "test_value"})

    @pytest.fixture
    def sample_metrics(self) -> List[Metric]:
        """Return a list of sample metrics."""
        return [
            Metric(value=10.0, timestamp_ms=1000),
            Metric(value=15.0, timestamp_ms=2000),
            Metric(value=13.0, timestamp_ms=3000),
            Metric(value=17.0, timestamp_ms=4000),
            Metric(value=14.0, timestamp_ms=5000),
        ]

    @pytest.fixture
    def sample_operations(self) -> List[Operation]:
        """Return a list of sample operations."""
        return [
            Operation(timestamp_ms=1000, type="CREATE"),
            Operation(timestamp_ms=2000, type="UPDATE", is_anomaly=True),
            Operation(timestamp_ms=3000, type="UPDATE"),
            Operation(timestamp_ms=4000, type="UPDATE", is_anomaly=True),
            Operation(timestamp_ms=5000, type="DELETE"),
        ]

    @pytest.fixture
    def sample_bucket(self) -> Bucket:
        """Return a sample bucket with all required parameters."""
        return Bucket(
            bucket_number=1,
            bucket_size="H",
            bucket_id="bucket_1",
            bucket_start_time=float(0),
            bucket_end_time=float(1),
            min_value=5.0,
            max_value=15.0,
            min_value_with_buffer=3.0,
            max_value_with_buffer=17.0,
        )

    def test_init_default(self, predictor: MetricPredictor) -> None:
        """Test initialization with default values."""
        assert predictor.user_config == {}

    def test_init_custom_config(self, predictor_with_config: MetricPredictor) -> None:
        """Test initialization with custom config."""
        assert predictor_with_config.user_config == {"CUSTOM_PARAM": "test_value"}

    def test_validate_metrics_input_valid(
        self, predictor: MetricPredictor, sample_metrics: List[Metric]
    ) -> None:
        """Test metrics validation with valid input."""
        # Should not raise exception
        predictor._validate_metrics_input(sample_metrics)

    def test_validate_metrics_input_invalid(self, predictor: MetricPredictor) -> None:
        """Test metrics validation with invalid input."""
        with pytest.raises(ValueError) as excinfo:
            predictor._validate_metrics_input([])
        assert "empty metrics list" in str(excinfo.value).lower()

    def test_validate_operations_input_valid(
        self, predictor: MetricPredictor, sample_operations: List[Operation]
    ) -> None:
        """Test operations validation with valid input."""
        # Should not raise exception
        predictor._validate_operations_input(sample_operations)

    def test_validate_operations_input_invalid(
        self, predictor: MetricPredictor
    ) -> None:
        """Test operations validation with invalid input."""
        with pytest.raises(ValueError) as excinfo:
            predictor._validate_operations_input([])
        assert "empty operations list" in str(excinfo.value).lower()

    def test_validate_sensitivity_level_valid(self, predictor: MetricPredictor) -> None:
        """Test sensitivity level validation with valid input."""
        # Should not raise exception for valid range
        for level in range(1, 11):
            predictor._validate_sensitivity_level(level)

    def test_validate_sensitivity_level_invalid(
        self, predictor: MetricPredictor
    ) -> None:
        """Test sensitivity level validation with invalid input."""
        # Below minimum
        with pytest.raises(ValueError) as excinfo:
            predictor._validate_sensitivity_level(0)
        assert "sensitivity level" in str(excinfo.value).lower()

        # Above maximum
        with pytest.raises(ValueError) as excinfo:
            predictor._validate_sensitivity_level(11)
        assert "sensitivity level" in str(excinfo.value).lower()

    def test_build_metric_forecaster_config(self, predictor: MetricPredictor) -> None:
        """Test building metric forecaster config."""
        # Test with lowest sensitivity
        config1 = predictor._build_metric_forecaster_config(1)
        assert isinstance(config1, MetricProjectorConfig)
        assert round(config1.BUFFER_FACTOR, 1) == 1.0  # Max buffer for min sensitivity

        # Test with highest sensitivity
        config10 = predictor._build_metric_forecaster_config(10)
        assert isinstance(config10, MetricProjectorConfig)
        assert round(config10.BUFFER_FACTOR, 1) == 0.1  # Min buffer for max sensitivity

        # Test a middle value
        config5 = predictor._build_metric_forecaster_config(5)
        assert isinstance(config5, MetricProjectorConfig)
        # Should be in between the min and max values
        assert 0.1 < config5.BUFFER_FACTOR < 1.0

    def test_extract_metric_timestamps_and_values(
        self, predictor: MetricPredictor, sample_metrics: List[Metric]
    ) -> None:
        """Test extracting timestamps and values from metrics."""
        timestamps, values = predictor._extract_metric_timestamps_and_values(
            sample_metrics
        )

        assert timestamps == [1000, 2000, 3000, 4000, 5000]
        assert values == [10.0, 15.0, 13.0, 17.0, 14.0]

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricForecaster"
    )
    def test_run_metric_forecaster_success(
        self,
        mock_forecaster_class: MagicMock,
        predictor: MetricPredictor,
        sample_bucket: Bucket,
    ) -> None:
        """Test running the metric forecaster successfully."""
        # Setup mock forecaster
        mock_forecaster = MagicMock()
        mock_forecaster_class.return_value = mock_forecaster

        # Setup mock buckets using the sample_bucket fixture
        mock_buckets = [
            sample_bucket,
            Bucket(
                bucket_number=2,
                bucket_size="H",
                bucket_id="bucket_2",
                bucket_start_time=float(0),
                bucket_end_time=float(1),
                min_value=6.0,
                max_value=16.0,
                min_value_with_buffer=4.0,
                max_value_with_buffer=18.0,
            ),
        ]
        mock_forecaster.forecast_metric.return_value = (mock_buckets, None)

        # Call method
        result = predictor._run_metric_forecaster(
            timestamps=[1000, 2000, 3000],
            metric_values=[10.0, 12.0, 14.0],
            now_time_ms=1000,
            unit=timedelta(hours=1),
            multiple=2,
            config=MetricProjectorConfig(),
        )

        # Verify results
        assert len(result) == 2
        assert isinstance(result[0], MetricBoundary)
        assert result[0].lower_bound.value == 3.0
        assert result[0].upper_bound.value == 17.0
        assert result[0].start_time_ms == 0
        assert result[0].end_time_ms == 1

        assert isinstance(result[1], MetricBoundary)
        assert result[1].lower_bound.value == 4.0
        assert result[1].upper_bound.value == 18.0
        assert result[1].start_time_ms == 0
        assert result[1].end_time_ms == 1

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricForecaster"
    )
    def test_run_metric_forecaster_empty_buckets(
        self, mock_forecaster_class: MagicMock, predictor: MetricPredictor
    ) -> None:
        """Test running the metric forecaster with empty buckets."""
        # Setup mock forecaster
        mock_forecaster = MagicMock()
        mock_forecaster_class.return_value = mock_forecaster

        # Return empty buckets
        mock_forecaster.forecast_metric.return_value = ([], None)

        # Call method and verify exception
        with pytest.raises(Exception) as excinfo:
            predictor._run_metric_forecaster(
                timestamps=[1000, 2000, 3000],
                metric_values=[10.0, 12.0, 14.0],
                now_time_ms=1000,
                unit=timedelta(hours=1),
                multiple=2,
                config=MetricProjectorConfig(),
            )

        assert "forecast returned no buckets" in str(excinfo.value).lower()

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricForecaster"
    )
    def test_run_metric_forecaster_exception(
        self, mock_forecaster_class: MagicMock, predictor: MetricPredictor
    ) -> None:
        """Test running the metric forecaster with an exception."""
        # Setup mock forecaster to raise an exception
        mock_forecaster = MagicMock()
        mock_forecaster_class.return_value = mock_forecaster

        test_error = ValueError("Test forecaster error")
        mock_forecaster.forecast_metric.side_effect = test_error

        # Call method and verify exception
        with pytest.raises(Exception) as excinfo:
            predictor._run_metric_forecaster(
                timestamps=[1000, 2000, 3000],
                metric_values=[10.0, 12.0, 14.0],
                now_time_ms=1000,
                unit=timedelta(hours=1),
                multiple=2,
                config=MetricProjectorConfig(),
            )

        # Check that the original error message is preserved in the exception
        error_message = str(excinfo.value).lower()
        assert "failed to generate metric predictions" in error_message
        assert "test forecaster error" in error_message

    def test_build_metric_boundaries(
        self, predictor: MetricPredictor, sample_bucket: Bucket
    ) -> None:
        """Test building metric boundaries from buckets."""
        # Test with valid buckets using the sample_bucket fixture
        buckets = [
            sample_bucket,
            Bucket(
                bucket_number=2,
                bucket_size="H",
                bucket_id="bucket_2",
                bucket_start_time=float(0),
                bucket_end_time=float(1),
                min_value=6.0,
                max_value=16.0,
                min_value_with_buffer=4.0,
                max_value_with_buffer=18.0,
            ),
        ]

        boundaries = predictor._build_metric_boundaries(buckets)

        assert len(boundaries) == 2
        assert isinstance(boundaries[0], MetricBoundary)
        assert boundaries[0].lower_bound.value == 3.0
        assert boundaries[0].upper_bound.value == 17.0
        assert boundaries[0].start_time_ms == float(0)
        assert boundaries[0].end_time_ms == float(1)

        assert isinstance(boundaries[1], MetricBoundary)
        assert boundaries[1].lower_bound.value == 4.0
        assert boundaries[1].upper_bound.value == 18.0
        assert boundaries[1].start_time_ms == float(0)
        assert boundaries[1].end_time_ms == float(1)

        # Test with None values in buckets
        buckets_with_nones = [
            Bucket(
                bucket_number=1,
                bucket_size="H",
                bucket_id="bucket_1",
                bucket_start_time=float(0),
                bucket_end_time=float(1),
                min_value=None,
                max_value=None,
                min_value_with_buffer=None,
                max_value_with_buffer=None,
            ),
        ]

        boundaries = predictor._build_metric_boundaries(buckets_with_nones)

        assert len(boundaries) == 1
        assert boundaries[0].lower_bound.value == 0.0
        assert boundaries[0].upper_bound.value == 0.0
        assert boundaries[0].start_time_ms == 0
        assert boundaries[0].end_time_ms == 1

    def test_extract_current_and_future_metric_boundaries(
        self, predictor: MetricPredictor
    ) -> None:
        """Test extracting current and future boundaries."""
        boundaries = [
            MetricBoundary(
                lower_bound=Metric(value=1.0, timestamp_ms=1000),
                upper_bound=Metric(value=5.0, timestamp_ms=1000),
                start_time_ms=1000,
                end_time_ms=2000,
            ),
            MetricBoundary(
                lower_bound=Metric(value=2.0, timestamp_ms=2000),
                upper_bound=Metric(value=6.0, timestamp_ms=2000),
                start_time_ms=2000,
                end_time_ms=3000,
            ),
            MetricBoundary(
                lower_bound=Metric(value=3.0, timestamp_ms=3000),
                upper_bound=Metric(value=7.0, timestamp_ms=3000),
                start_time_ms=3000,
                end_time_ms=4000,
            ),
        ]

        current, future = predictor._extract_current_and_future_metric_boundaries(
            boundaries
        )

        assert current == boundaries[0]
        assert future == boundaries[1:]

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricPredictor._run_metric_forecaster"
    )
    def test_predict_metric_boundaries(
        self,
        mock_run_forecaster: MagicMock,
        predictor: MetricPredictor,
        sample_metrics: List[Metric],
    ) -> None:
        """Test predicting metric boundaries."""
        # Setup mock boundaries
        mock_boundaries = [
            MetricBoundary(
                lower_bound=Metric(value=1.0, timestamp_ms=1000),
                upper_bound=Metric(value=5.0, timestamp_ms=1000),
                start_time_ms=1000,
                end_time_ms=2000,
            ),
            MetricBoundary(
                lower_bound=Metric(value=2.0, timestamp_ms=2000),
                upper_bound=Metric(value=6.0, timestamp_ms=2000),
                start_time_ms=2000,
                end_time_ms=3000,
            ),
        ]
        mock_run_forecaster.return_value = mock_boundaries

        # Call method
        result = predictor.predict_metric_boundaries(
            recent_metrics=sample_metrics,
            unit=timedelta(hours=1),
            multiple=2,
            sensitivity_level=5,
        )

        # Verify correct parameters were passed to run_metric_forecaster
        mock_run_forecaster.assert_called_once()
        # Instead of checking all arguments, check just that the function was called
        assert mock_run_forecaster.called

        # Verify the result
        assert result == mock_boundaries

    def test_predict_metric_boundaries_invalid_sensitivity(
        self, predictor: MetricPredictor, sample_metrics: List[Metric]
    ) -> None:
        """Test predicting metric boundaries with invalid sensitivity."""
        with pytest.raises(ValueError) as excinfo:
            predictor.predict_metric_boundaries(
                recent_metrics=sample_metrics,
                unit=timedelta(hours=1),
                multiple=2,
                sensitivity_level=0,  # Invalid
            )

        assert "sensitivity level" in str(excinfo.value).lower()

    def test_predict_metric_boundaries_empty_metrics(
        self, predictor: MetricPredictor
    ) -> None:
        """Test predicting metric boundaries with empty metrics."""
        with pytest.raises(ValueError) as excinfo:
            predictor.predict_metric_boundaries(
                recent_metrics=[],
                unit=timedelta(hours=1),
                multiple=2,
                sensitivity_level=5,
            )

        assert "empty metrics list" in str(excinfo.value).lower()

    def test_calculate_fixed_interval_buffer_ratio(
        self, predictor: MetricPredictor
    ) -> None:
        """Test calculating fixed interval buffer ratio."""
        # Test with lowest sensitivity (should give highest buffer ratio)
        ratio1 = predictor._calculate_fixed_interval_buffer_ratio(1)
        assert ratio1 == 0.9  # Highest buffer ratio for min sensitivity

        # Test with highest sensitivity (should give lowest buffer ratio)
        ratio10 = predictor._calculate_fixed_interval_buffer_ratio(10)
        assert ratio10 == 0.0  # Lowest buffer ratio for max sensitivity

        # Test a middle value
        ratio5 = predictor._calculate_fixed_interval_buffer_ratio(5)
        # Should be in between the min and max values
        assert 0.0 < ratio5 < 0.9

    def test_extract_operation_timestamps(
        self, predictor: MetricPredictor, sample_operations: List[Operation]
    ) -> None:
        """Test extracting timestamps from operations."""
        timestamps = predictor._extract_operation_timestamps(sample_operations)
        assert timestamps == [1000, 2000, 3000, 4000, 5000]

    def test_extract_anomaly_operation_timestamps(
        self, predictor: MetricPredictor, sample_operations: List[Operation]
    ) -> None:
        """Test extracting timestamps from operations."""
        timestamps = predictor._extract_anomaly_operation_timestamps(sample_operations)
        assert timestamps == [2000, 4000]

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.predict_max_normal_interval"
    )
    def test_predict_fixed_interval(
        self, mock_predict: MagicMock, predictor: MetricPredictor
    ) -> None:
        """Test predicting fixed interval."""
        # Setup mock result
        mock_result = MaxNormalIntervalResult(
            max_normal_interval=60,
            max_normal_interval_with_buffer=90,
        )
        mock_predict.return_value = mock_result

        # Call method
        result = predictor._predict_fixed_interval(
            operations=[
                Operation(type="INSERT", timestamp_ms=1000),
                Operation(type="INSERT", timestamp_ms=2000),
                Operation(type="INSERT", timestamp_ms=3000),
            ],
            buffer_ratio=0.5,
        )

        # Verify mock was called with correct parameters
        mock_predict.assert_called_once_with(
            timestamps=[1000, 2000, 3000], buffer_ratio=0.5, known_anomaly_timestamps=[]
        )

        # Verify result
        assert result == mock_result

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.predict_max_normal_interval"
    )
    def test_predict_fixed_interval_exception(
        self, mock_predict: MagicMock, predictor: MetricPredictor
    ) -> None:
        """Test predicting fixed interval with an exception."""
        # Setup mock to raise an exception
        test_error = ValueError("Test predict error")
        mock_predict.side_effect = test_error

        # Call method and verify exception
        with pytest.raises(Exception) as excinfo:
            predictor._predict_fixed_interval(
                operations=[
                    Operation(type="INSERT", timestamp_ms=1000),
                    Operation(type="INSERT", timestamp_ms=2000),
                    Operation(type="INSERT", timestamp_ms=3000),
                ],
                buffer_ratio=0.5,
            )

        # Check for the error message
        error_message = str(excinfo.value).lower()
        assert "failed to predict fixed interval" in error_message
        assert "test predict error" in error_message

    def test_build_fixed_interval_schedule_minutes(
        self, predictor: MetricPredictor
    ) -> None:
        """Test building fixed interval schedule with minutes."""
        schedule = predictor._build_fixed_interval_schedule(
            interval_minutes=30,
            operations=[],  # Not used by the method
        )

        assert isinstance(schedule, FixedIntervalScheduleClass)
        assert schedule.unit == "MINUTE"
        assert schedule.multiple == 30

    def test_build_fixed_interval_schedule_hours(
        self, predictor: MetricPredictor
    ) -> None:
        """Test building fixed interval schedule with hours."""
        schedule = predictor._build_fixed_interval_schedule(
            interval_minutes=120,  # 2 hours
            operations=[],  # Not used by the method
        )

        assert isinstance(schedule, FixedIntervalScheduleClass)
        assert schedule.unit == "HOUR"
        assert schedule.multiple == 2

    def test_build_fixed_interval_schedule_days(
        self, predictor: MetricPredictor
    ) -> None:
        """Test building fixed interval schedule with days."""
        schedule = predictor._build_fixed_interval_schedule(
            interval_minutes=2880,  # 2 days
            operations=[],  # Not used by the method
        )

        assert isinstance(schedule, FixedIntervalScheduleClass)
        assert schedule.unit == "DAY"
        assert schedule.multiple == 2

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricPredictor._predict_fixed_interval"
    )
    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricPredictor._build_fixed_interval_schedule"
    )
    def test_predict_fixed_interval_schedule(
        self,
        mock_build_schedule: MagicMock,
        mock_predict_interval: MagicMock,
        predictor: MetricPredictor,
        sample_operations: List[Operation],
    ) -> None:
        """Test predicting fixed interval schedule."""
        # Setup mocks
        mock_result = MaxNormalIntervalResult(
            max_normal_interval=60,
            max_normal_interval_with_buffer=90,
        )
        mock_predict_interval.return_value = mock_result

        mock_schedule = FixedIntervalScheduleClass(unit="HOUR", multiple=1)
        mock_build_schedule.return_value = mock_schedule

        # Call method
        result = predictor.predict_fixed_interval_schedule(
            operations=sample_operations,
            sensitivity_level=5,
        )

        # Verify that the mocks were called at all
        mock_predict_interval.assert_called_once()
        mock_build_schedule.assert_called_once()

        # Verify result
        assert result == mock_schedule

    def test_predict_fixed_interval_schedule_empty_operations(
        self, predictor: MetricPredictor
    ) -> None:
        """Test predicting fixed interval schedule with empty operations."""
        with pytest.raises(ValueError) as excinfo:
            predictor.predict_fixed_interval_schedule(
                operations=[],
                sensitivity_level=5,
            )

        assert "empty operations list" in str(excinfo.value).lower()

    def test_predict_fixed_interval_schedule_invalid_sensitivity(
        self, predictor: MetricPredictor, sample_operations: List[Operation]
    ) -> None:
        """Test predicting fixed interval schedule with invalid sensitivity."""
        with pytest.raises(ValueError) as excinfo:
            predictor.predict_fixed_interval_schedule(
                operations=sample_operations,
                sensitivity_level=0,  # Invalid
            )

        assert "sensitivity level" in str(excinfo.value).lower()

    @patch(
        "datahub_executor.common.monitor.inference.metric_projection.metric_predictor.MetricPredictor._predict_fixed_interval"
    )
    def test_predict_fixed_interval_schedule_none_result(
        self,
        mock_predict_interval: MagicMock,
        predictor: MetricPredictor,
        sample_operations: List[Operation],
    ) -> None:
        """Test predicting fixed interval schedule with None result."""
        # Setup mock to return None for max_normal_interval_with_buffer
        mock_result = MaxNormalIntervalResult(
            max_normal_interval=60,
            max_normal_interval_with_buffer=None,  # None result
        )
        mock_predict_interval.return_value = mock_result

        # Call method and verify exception
        with pytest.raises(Exception) as excinfo:
            predictor.predict_fixed_interval_schedule(
                operations=sample_operations,
                sensitivity_level=5,
            )

        assert (
            "failed to generate fixed interval prediction" in str(excinfo.value).lower()
        )
