import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Generic, List, Optional, TypeVar

import pytz
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInferenceDetailsClass,
)

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    AssertionExclusionWindow,
    Monitor,
)
from datahub_executor.config import (
    ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS,
)

logger = logging.getLogger(__name__)

# Define a generic type for events (Metric or Operation)
Event = TypeVar("Event", Metric, Operation)


class BaseAssertionTrainer(Generic[Event], ABC):
    """
    Base class for all assertion trainers with common functionality.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        metrics_client: MetricClient,
        metrics_predictor: MetricPredictor,
        monitor_client: MonitorClient,
    ) -> None:
        self.graph = graph
        self.metrics_client = metrics_client
        self.metrics_predictor = metrics_predictor
        self.monitor_client = monitor_client

    @abstractmethod
    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """Train an assertion and update its boundaries."""
        pass

    @abstractmethod
    def should_perform_inference(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> bool:
        """Determine if inference should be performed based on time since last inference."""
        pass

    @abstractmethod
    def get_min_training_samples(self) -> int:
        """Get the minimum number of samples required for training."""
        pass

    @abstractmethod
    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Event]:
        """Fetch metric or operation data for training."""
        pass

    @abstractmethod
    def remove_inferred_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """Remove inferred assertion when not enough samples are available."""
        pass

    @abstractmethod
    def train_and_update_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        events: List[Event],
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> Assertion:
        """Train and update assertion with new boundaries."""
        pass

    def perform_training(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Common training flow for all assertion types.
        """
        monitor_urn = monitor.urn
        if not monitor_urn:
            logger.warning(
                f"No monitor URN found for assertion {assertion.urn}. Cannot train."
            )
            return

        # Check if enough time has passed for re-training
        if not self.should_perform_inference(evaluation_spec):
            logger.debug(
                f"Skipping retraining for assertion.. Already retrained within the retraining time window {assertion.urn}"
            )
            return

        # Extract training params
        maybe_adjustment_settings = (
            monitor.assertion_monitor.settings.inference_settings
            if monitor.assertion_monitor and monitor.assertion_monitor.settings
            else None
        )

        # Fetch historical metrics/operations
        historical_data = self.get_metric_data(
            monitor, assertion, maybe_adjustment_settings
        )

        logger.debug(
            f"Fetched {len(historical_data)} training samples before applying exclusions"
        )

        # Filter by exclusion windows if configured
        filtered_data = self.filter_training_timeseries(
            historical_data, maybe_adjustment_settings
        )

        logger.debug(
            f"Fetched {len(filtered_data)} training samples after applying exclusions."
        )

        # Check sample size
        if len(filtered_data) < self.get_min_training_samples():
            # Not enough data: remove inferred assertions and go back to training state
            self.remove_inferred_assertion(monitor, assertion, evaluation_spec)
        else:
            # Enough data: proceed with training
            self.train_and_update_assertion(
                monitor,
                assertion,
                filtered_data,
                maybe_adjustment_settings,
                evaluation_spec,
            )

    @classmethod
    def extract_lookback_days_from_adjustment_settings(
        cls, adjustment_settings: Optional[AssertionAdjustmentSettings]
    ) -> int:
        """
        Extract the lookback window in days from adjustment settings or use the default.
        """
        if (
            adjustment_settings
            and adjustment_settings.training_data_lookback_window_days
        ):
            return adjustment_settings.training_data_lookback_window_days
        return ASSERTION_MONITOR_DEFAULT_TRAINING_LOOKBACK_WINDOW_DAYS

    @classmethod
    def filter_training_timeseries(
        cls,
        timeseries: List[Event],
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Event]:
        """
        Filter raw timeseries events according to user-defined exclusion windows.
        """
        if not adjustment_settings or not adjustment_settings.exclusion_windows:
            return timeseries  # no exclusion specified

        filtered: List[Event] = []
        for event in timeseries:
            exclude = False
            for excl_window in adjustment_settings.exclusion_windows:
                if cls._is_excluded_by_fixed_range(event, excl_window):
                    exclude = True
                    break
                if cls._is_excluded_by_weekly(event, excl_window):
                    exclude = True
                    break
            if not exclude:
                filtered.append(event)

        return filtered

    @classmethod
    def _is_excluded_by_fixed_range(
        cls, event: Event, exclusion_window: AssertionExclusionWindow
    ) -> bool:
        """
        Check if an event is excluded by a fixed time range.
        """
        if exclusion_window.fixed_range:
            start_ms = exclusion_window.fixed_range.startTimeMillis
            end_ms = exclusion_window.fixed_range.endTimeMillis
            return start_ms <= event.timestamp_ms <= end_ms
        return False

    @classmethod
    def _is_excluded_by_weekly(
        cls,
        event: Event,
        exclusion_window: AssertionExclusionWindow,
    ) -> bool:
        """
        Check if an event is excluded by a weekly pattern.
        """
        if not exclusion_window.weekly:
            return False

        weekly = exclusion_window.weekly
        if not weekly.days_of_week:
            return False

        metric_utc_dt = datetime.fromtimestamp(
            event.timestamp_ms / 1000, tz=timezone.utc
        )

        # Convert to local tz
        exclusion_tz = weekly.timezone or "UTC"
        local_tz = pytz.timezone(exclusion_tz)
        local_metric_time = metric_utc_dt.astimezone(local_tz)

        # e.g. if weekly.days_of_week=["SATURDAY"] and we match
        if local_metric_time.strftime("%A").upper() in weekly.days_of_week:
            start_time_str = weekly.start_time or "00:00"
            end_time_str = weekly.end_time or "23:59"

            start_time = datetime.strptime(start_time_str, "%H:%M").time()
            end_time = datetime.strptime(end_time_str, "%H:%M").time()
            return start_time <= local_metric_time.time() <= end_time

        return False

    def create_inference_details(
        self, generated_at_ms: int = 0
    ) -> AssertionInferenceDetailsClass:
        """
        Create inference details with the given generation timestamp.
        Set to 0 to indicate no assertion is available.
        """
        return AssertionInferenceDetailsClass(
            modelId="online-predictor-v1",
            modelVersion="v1",
            generatedAt=generated_at_ms,
            confidence=None,
            parameters=None,
            adjustmentSettings=None,
        )

    def create_empty_context(
        self, generated_at_ms: int = 0
    ) -> AssertionEvaluationContextClass:
        """
        Create an empty evaluation context with the given generation timestamp.
        """
        inference_details = self.create_inference_details(generated_at_ms)
        return AssertionEvaluationContextClass(
            embeddedAssertions=[], inferenceDetails=inference_details
        )

    def get_metric_cube_urn(self, monitor_urn: str) -> str:
        """
        Get the metric cube URN for a monitor.
        """
        from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
            encode_monitor_urn,
        )

        return f"urn:li:dataHubMetricCube:{encode_monitor_urn(monitor_urn)}"
