import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Generic, List, Optional

import pytz
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInferenceDetailsClass,
    AssertionMonitorMetricsCubeBootstrapStateClass,
    AssertionMonitorMetricsCubeBootstrapStatusClass,
)

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.types import Event
from datahub_executor.common.monitor.inference.utils import (
    check_is_metrics_cube_bootstrapped,
    get_event_timespan_seconds,
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
    ONLINE_SMART_ASSERTIONS_ENABLED,
)

logger = logging.getLogger(__name__)


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
    def get_min_training_samples_timespan_seconds(self) -> int:
        """Get the minimum timespan between first & last sample required for training."""
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
    def try_get_historical_data_for_bootstrap(
        self,
        assertion: Assertion,
        maybe_adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> Optional[List[Metric]]:
        """Get historical data to bootstrap cube for training."""
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

        # Check if online smart assertions enabled, if not we skip training.
        if not ONLINE_SMART_ASSERTIONS_ENABLED:
            logger.debug(
                "Skipping retraining for assertion.. Online smart assertions is not enabled."
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

        # Bootstrap historical data if needed
        self.try_bootstrap_historical_data(
            monitor, assertion, maybe_adjustment_settings
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
        if (
            len(filtered_data) < self.get_min_training_samples()
            or get_event_timespan_seconds(filtered_data)
            < self.get_min_training_samples_timespan_seconds()
        ):
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

    def try_bootstrap_historical_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        maybe_adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> None:
        """
        Bootstrap historical data for training.
        """

        # 1. Check if the metrics cube has already been bootstrapped
        if check_is_metrics_cube_bootstrapped(monitor):
            logger.debug(
                f"Metrics cube for assertion {assertion.urn} has already been bootstrapped. Skipping bootstrap."
            )
            return

        # 2. Fetch the historical data
        metrics_data = self.try_get_historical_data_for_bootstrap(
            assertion, maybe_adjustment_settings
        )
        if metrics_data is None or len(metrics_data) == 0:
            logger.debug(
                f"No historical data found for assertion {assertion.urn}. Skipping bootstrap."
            )
            return

        # 3. Bootstrap the metrics cube with the data
        self.bootstrap_metrics_cube_with_data(monitor, metrics_data)

    def bootstrap_metrics_cube_with_data(
        self,
        monitor: Monitor,
        datahub_profile_metrics: List[Metric],
    ) -> None:
        if len(datahub_profile_metrics) > 0:
            # 1. Store the metrics in the metrics cube
            metric_cube_urn = self.get_metric_cube_urn(monitor.urn)
            self.metrics_client.save_metric_values(
                metric_cube_urn,
                datahub_profile_metrics,
            )

            # 2. Mark the metrics cube as bootstrapped
            metrics_cube_bootstrap_status = (
                AssertionMonitorMetricsCubeBootstrapStatusClass(
                    state=AssertionMonitorMetricsCubeBootstrapStateClass.COMPLETED,
                )
            )
            self.monitor_client.patch_assertion_monitor_metrics_cube_bootstrap_status(
                monitor.urn,
                metrics_cube_bootstrap_status,
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
