from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from datahub.metadata.schema_classes import FixedIntervalScheduleClass

from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_helper_classes import (
    MaxNormalIntervalResult,
)
from datahub_executor.common.monitor.inference.fixed_interval.fixed_interval_inference import (
    predict_max_normal_interval,
)
from datahub_executor.common.monitor.inference.metric_projection.config import (
    MetricProjectorConfig,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_forecaster import (
    MetricForecaster,
)
from datahub_executor.common.monitor.inference.metric_projection.types import (
    Bucket,
)

logger = logging.getLogger(__name__)


class MetricBoundary:
    def __init__(
        self,
        lower_bound: Metric,
        upper_bound: Metric,
        start_time_ms: int,
        end_time_ms: int,
    ):
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.start_time_ms = start_time_ms
        self.end_time_ms = end_time_ms

    def contains(self, timestamp: int) -> bool:
        """Check if a timestamp falls within the boundary range."""
        return self.start_time_ms <= timestamp <= self.end_time_ms

    def __repr__(self) -> str:
        return (
            f"MetricBoundary(lower_bound={self.lower_bound}, "
            f"upper_bound={self.upper_bound}, "
            f"start_time_ms={self.start_time_ms}, "
            f"end_time_ms={self.end_time_ms})"
        )


class MetricPredictor:
    """
    Uses MetricForecaster to predict future metric boundaries.
    Leverages Prophet model to forecast metric min and max values for specified intervals.
    """

    # Constants for sensitivity validation
    MIN_SENSITIVITY_LEVEL = 1
    MAX_SENSITIVITY_LEVEL = 10
    BUFFER_FACTOR_MULTIPLIER = 1

    def __init__(self, user_config: Optional[Dict] = None):
        """
        Initialize the MetricPredictor with optional configuration.

        Args:
            user_config: Optional configuration dictionary to customize MetricForecaster behavior.
        """
        self.user_config = user_config if user_config is not None else {}

    """
    The following methods pertain to METRIC forecasting.

    These are used to generate volume, column metric, and custom metric training predictions. 
    """

    def predict_metric_boundaries(
        self,
        recent_metrics: List[Metric],
        unit: timedelta,
        multiple: int,
        sensitivity_level: int,
        start_time: Optional[datetime] = None,
    ) -> List[MetricBoundary]:
        """
        Given some recent metrics, produce current and future metric boundaries.

        Args:
            recent_metrics: List of recent Metric objects containing historical data.
            unit: timedelta indicating the prediction interval size.
            multiple: Number of time units to predict into the future.
            sensitivity_level: Sensitivity level (1-10) to adjust prediction confidence intervals.
                               Higher values produce narrower bounds (more sensitive).
            start_time: Optional start time for prediction. If None, current time is used.

        Returns:
            List of predicted MetricBoundary objects for requested intervals.

        Raises:
            ValueError: If recent_metrics is empty or sensitivity_level is outside the valid range.
        """
        logger.debug(
            "MetricPredictor: Predicting based on %s recent metrics",
            len(recent_metrics),
        )

        # Validate we have metrics to predict from
        self._validate_metrics_input(recent_metrics)

        # Validate sensitivity level
        self._validate_sensitivity_level(sensitivity_level)

        if not start_time:
            now_time_ms = int(time.time() * 1000)
        else:
            now_time_ms = int(start_time.timestamp() * 1000)

        # Prepare config and extract data
        config = self._build_metric_forecaster_config(sensitivity_level)
        timestamps, values = self._extract_metric_timestamps_and_values(recent_metrics)

        # Run the forecaster to obtain boundaries
        metric_boundaries = self._run_metric_forecaster(
            timestamps,
            values,
            now_time_ms,
            unit,
            multiple,
            config,
        )

        self._log_metric_prediction_result(metric_boundaries)

        return metric_boundaries

    def _validate_metrics_input(self, metrics: List[Metric]) -> None:
        """
        Ensure we have at least one metric to predict from.

        Args:
            metrics: List of metrics to validate.

        Raises:
            ValueError: If metrics list is empty.
        """
        if not metrics:
            raise ValueError("Cannot predict with an empty metrics list.")

    def _validate_sensitivity_level(self, sensitivity_level: int) -> None:
        """
        Ensure sensitivity level is within the valid range.

        Args:
            sensitivity_level: Sensitivity level to validate.

        Raises:
            ValueError: If sensitivity level is outside the valid range.
        """
        if not (
            self.MIN_SENSITIVITY_LEVEL
            <= sensitivity_level
            <= self.MAX_SENSITIVITY_LEVEL
        ):
            raise ValueError(
                f"Sensitivity level must be between {self.MIN_SENSITIVITY_LEVEL} and {self.MAX_SENSITIVITY_LEVEL}, "
                f"got {sensitivity_level}"
            )

    def _build_metric_forecaster_config(
        self, sensitivity_level: int
    ) -> MetricProjectorConfig:
        """
        Builds the forecaster config by merging user_config with sensitivity-specific parameters.
        Higher sensitivity means narrower bounds (smaller buffer).

        Args:
            sensitivity_level: Sensitivity level (1-10), where higher values mean narrower bounds.

        Returns:
            MetricProjectorConfig: The configuration for the metric forecaster.
        """
        config = dict(self.user_config)

        # Sensitivity affects the buffer factor - higher sensitivity => narrower bounds (smaller buffer)
        # Linear relationship: sensitivity 1 -> buffer 3, sensitivity 10 -> buffer 0.3
        normalized_sensitivity = (sensitivity_level - self.MIN_SENSITIVITY_LEVEL) / (
            self.MAX_SENSITIVITY_LEVEL - self.MIN_SENSITIVITY_LEVEL
        )
        config["BUFFER_FACTOR"] = self.BUFFER_FACTOR_MULTIPLIER * (
            1 - normalized_sensitivity * 0.9
        )  # Scaling to range (BUFFER_FACTOR_MULTIPLIER x [0.1, 1])

        return MetricProjectorConfig(**config)

    def _extract_metric_timestamps_and_values(
        self, metrics: List[Metric]
    ) -> Tuple[List[int], List[float]]:
        """
        Extracts timestamps and values from recent metrics.

        Args:
            metrics: List of metrics to extract from.

        Returns:
            Tuple containing lists of timestamps and values.
        """
        timestamps = [metric.timestamp_ms for metric in metrics]
        values = [metric.value for metric in metrics]
        return timestamps, values

    def _run_metric_forecaster(
        self,
        timestamps: List[int],
        metric_values: List[float],
        now_time_ms: int,
        unit: timedelta,
        multiple: int,
        config: MetricProjectorConfig,
    ) -> List[MetricBoundary]:
        """
        Creates a MetricForecaster, forecasts metric boundaries, and converts them to MetricBoundary objects.

        Args:
            timestamps: List of historical timestamp values.
            metric_values: List of historical metric values.
            now_time_ms: Current time in milliseconds.
            unit: The time unit for prediction intervals.
            multiple: Number of intervals to predict.
            config: Configuration for the forecaster.

        Returns:
            List of MetricBoundary objects.

        Raises:
            Exception: If forecasting fails or returns no buckets.
        """
        try:
            # +1 in num_intervals to include the *current* interval as well
            forecaster = MetricForecaster(
                timestamps=timestamps,
                metric_values=metric_values,
                start_time=now_time_ms,
                prediction_interval_size=unit,
                num_intervals=multiple + 1,
                config=config,
            )

            buckets, _ignored = forecaster.forecast_metric()

            if not buckets:
                raise ValueError("Forecast returned no buckets")

            return self._build_metric_boundaries(buckets)
        except Exception as e:
            logger.exception("Failed to generate metric predictions")
            raise Exception(f"Failed to generate metric predictions: {str(e)}") from e

    def _build_metric_boundaries(self, buckets: List[Bucket]) -> List[MetricBoundary]:
        """
        Converts forecasting 'buckets' into a list of MetricBoundary objects.
        Handles potential None values safely.

        Args:
            buckets: List of prediction buckets.

        Returns:
            List of MetricBoundary objects.
        """
        boundaries = []
        for bucket in buckets:
            # Handle possible None values safely
            min_value = (
                0.0
                if bucket.min_value_with_buffer is None
                else bucket.min_value_with_buffer
            )
            max_value = (
                0.0
                if bucket.max_value_with_buffer is None
                else bucket.max_value_with_buffer
            )
            start_time = (
                0 if bucket.bucket_start_time is None else int(bucket.bucket_start_time)
            )
            end_time = (
                0 if bucket.bucket_end_time is None else int(bucket.bucket_end_time)
            )

            lower_bound = Metric(
                value=min_value,
                timestamp_ms=start_time,
            )
            upper_bound = Metric(
                value=max_value,
                timestamp_ms=start_time,
            )
            boundary = MetricBoundary(
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                start_time_ms=start_time,
                end_time_ms=end_time,
            )
            boundaries.append(boundary)

        return boundaries

    def _extract_current_and_future_metric_boundaries(
        self, boundaries: List[MetricBoundary]
    ) -> Tuple[MetricBoundary, List[MetricBoundary]]:
        """
        Splits the first boundary as 'current' and the rest as 'future'.

        Args:
            boundaries: List of metric boundaries.

        Returns:
            Tuple containing current boundary and list of future boundaries.
        """
        current = boundaries[0]
        future = boundaries[1:]
        return current, future

    def _log_metric_prediction_result(
        self, metric_boundaries: List[MetricBoundary]
    ) -> None:
        """
        Logs a brief message on how many future boundaries were produced.

        Args:
            metric_boundaries: List of predicted metric boundaries.
        """
        logger.info(
            "MetricPredictor: Generated %s metric boundaries",
            len(metric_boundaries),
        )

    """
    The following methods pertain to OPERATION forecasting.
    
    These are used to generate freshness training predictions. 
    """

    def predict_fixed_interval_schedule(
        self, operations: List[Operation], sensitivity_level: int
    ) -> FixedIntervalScheduleClass:
        """
        Predicts a fixed interval schedule based on operations and sensitivity level.

        Args:
            operations: List of Operation objects.
            sensitivity_level: Sensitivity level (1-10), where higher sensitivity means narrower bounds.

        Returns:
            FixedIntervalScheduleClass defining the schedule.

        Raises:
            ValueError: If operations list is empty or sensitivity level is invalid.
            Exception: If prediction fails.
        """
        # Validate operations list
        self._validate_operations_input(operations)

        # Validate sensitivity level
        self._validate_sensitivity_level(sensitivity_level)

        buffer_ratio = self._calculate_fixed_interval_buffer_ratio(sensitivity_level)

        logger.info(f"Generated fixed interval buffer ratio of {buffer_ratio}")

        prediction_result = self._predict_fixed_interval(operations, buffer_ratio)

        logger.info(
            f"Fixed interval schedule prediction result is: {prediction_result}"
        )

        if prediction_result.max_normal_interval_with_buffer is None:
            raise Exception(
                f"Failed to generate fixed interval prediction from {len(operations)} operations. "
                f"Skipping smart assertion generation!"
            )

        return self._build_fixed_interval_schedule(
            prediction_result.max_normal_interval_with_buffer, operations
        )

    def _validate_operations_input(self, operations: List[Operation]) -> None:
        """
        Ensure we have at least one operation to predict from.

        Args:
            operations: List of operations to validate.

        Raises:
            ValueError: If operations list is empty.
        """
        if not operations:
            raise ValueError("Cannot predict with an empty operations list.")

    def _calculate_fixed_interval_buffer_ratio(self, sensitivity_level: int) -> float:
        """
        Converts sensitivity level (1-10) into a buffer ratio.
        Higher sensitivity (higher value) means smaller buffer (smaller interval).

        Args:
            sensitivity_level: Sensitivity level (1-10).

        Returns:
            Buffer ratio as a float.
        """
        # Convert sensitivity to buffer ratio:
        # sensitivity 1 (least sensitive) -> buffer ratio 0.9 (largest buffer)
        # sensitivity 10 (most sensitive) -> buffer ratio 0.0 (smallest buffer)
        normalized_sensitivity = (sensitivity_level - self.MIN_SENSITIVITY_LEVEL) / (
            self.MAX_SENSITIVITY_LEVEL - self.MIN_SENSITIVITY_LEVEL
        )
        return 0.9 - (normalized_sensitivity * 0.9)

    def _extract_operation_timestamps(self, operations: List[Operation]) -> List[int]:
        """
        Extracts timestamps from operations.

        Args:
            operations: List of operations to extract from.

        Returns:
            List of timestamps.
        """
        return [operation.timestamp_ms for operation in operations]

    def _extract_anomaly_operation_timestamps(
        self, operations: List[Operation]
    ) -> List[int]:
        """
        Extracts timestamps from operations where the operation is an anomaly window close.

        Args:
            operations: List of operations to extract from.

        Returns:
            List of timestamps for any operations marked as anomaly.
        """
        return [
            operation.timestamp_ms for operation in operations if operation.is_anomaly
        ]

    def _predict_fixed_interval(
        self, operations: List[Operation], buffer_ratio: float
    ) -> MaxNormalIntervalResult:
        """
        Uses a prediction model to estimate the maximum normal interval.

        Args:
            timestamps: List of historical timestamps.
            buffer_ratio: Buffer ratio to use in prediction.

        Returns:
            MaxNormalIntervalResult with prediction results.
        """
        try:
            timestamps = self._extract_operation_timestamps(operations)
            known_anomaly_timestamps = self._extract_anomaly_operation_timestamps(
                operations
            )
            return predict_max_normal_interval(
                timestamps=timestamps,
                buffer_ratio=buffer_ratio,
                known_anomaly_timestamps=known_anomaly_timestamps,
            )
        except Exception as e:
            logger.exception("Failed to predict fixed interval")
            raise Exception(f"Failed to predict fixed interval: {str(e)}") from e

    def _build_fixed_interval_schedule(
        self, interval_minutes: int, operations: List[Operation]
    ) -> FixedIntervalScheduleClass:
        """
        Converts an interval in minutes into a well-rounded time unit (days, hours, or minutes).

        Args:
            interval_minutes: Interval in minutes.
            operations: List of operations (for logging purposes).

        Returns:
            FixedIntervalScheduleClass defining the schedule.
        """
        if interval_minutes >= 1440 and interval_minutes % 1440 == 0:
            unit = "DAY"
            multiple = interval_minutes // 1440
        elif interval_minutes >= 60 and interval_minutes % 60 == 0:
            unit = "HOUR"
            multiple = interval_minutes // 60
        else:
            unit = "MINUTE"
            multiple = interval_minutes

        logger.info(
            f"Predicted fixed interval schedule with unit {unit} multiple {multiple} from interval minutes {interval_minutes}"
        )

        return FixedIntervalScheduleClass(unit=unit, multiple=multiple)
