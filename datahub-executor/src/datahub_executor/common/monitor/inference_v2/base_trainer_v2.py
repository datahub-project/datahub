"""
Base trainer V2 for observe-models integration.

This provides a simplified base class that:
1. Fetches data from MetricClient and MonitorClient
2. Builds a DataFrame with anomaly markers
3. Delegates to ObserveAdapter for preprocessing/forecasting/anomaly detection
4. Uses persistence utilities for assertion updates
"""

import logging
import time
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Callable, List, Optional

import pandas as pd
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import MonitorErrorTypeClass

from datahub_executor.common.exceptions import (
    InsufficientSamplesException,
    TrainingErrorException,
)
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.adjustment_utils import (
    extract_lookback_days,
    get_metric_cube_urn,
    get_sensitivity_level,
)
from datahub_executor.common.monitor.client.client import MonitorClient
from datahub_executor.common.monitor.inference_v2.inference_utils import (
    AnomalyAssertions,
    ModelConfig,
    build_evaluation_context,
    get_default_prediction_num_intervals,
    parse_inference_details,
    timestamp_ms_to_ds,
)
from datahub_executor.common.monitor.inference_v2.observe_adapter import ObserveAdapter
from datahub_executor.common.monitor.inference_v2.observe_adapter.defaults import (
    InputDataContext,
)
from datahub_executor.common.monitor.inference_v2.types import (
    AssertionTrainingContext,
    TrainingResult,
)
from datahub_executor.common.types import (
    Anomaly,
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    Monitor,
)

logger = logging.getLogger(__name__)


class BaseTrainerV2(ABC):
    """
    Base class for V2 assertion trainers using observe-models.

    Subclasses should implement:
    - _train_internal(): The actual training logic
    - get_assertion_category(): Return the assertion category string
    - get_min_training_interval_seconds(): Return minimum interval between training runs
    - get_retune_interval_seconds(): Return interval after which hyperparameters are stale
    - get_training_context(): Return training context with floor/ceiling values

    Convenience methods available for subclasses:
    - _fetch_metrics(): Fetch metrics from the metrics cube
    - _fetch_anomalies(): Fetch anomalies with status from the monitor
    - _build_training_dataframe(): Build training and ground truth DataFrames
    - _run_training_pipeline(): Run the observe-models training pipeline
    - _update_assertion(): Update assertion with training results

    The base class train() method handles the inference interval check before
    calling _train_internal(). TODO: Move interval check to scheduler.
    """

    def __init__(
        self,
        graph: DataHubGraph,
        metrics_client: MetricClient,
        monitor_client: MonitorClient,
    ) -> None:
        self.graph = graph
        self.metrics_client = metrics_client
        self.monitor_client = monitor_client

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Train an assertion using observe-models.

        This handles the interval check before delegating to _train_internal().
        TODO: The interval check should be moved to the scheduler.
        """
        if not self._should_perform_inference(evaluation_spec):
            logger.debug(
                f"[V2] Skipping training - already trained within interval for {assertion.urn}"
            )
            return

        self._train_internal(monitor, assertion, evaluation_spec)

    @abstractmethod
    def _train_internal(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Perform the actual training logic.

        Subclasses implement this with their specific training behavior.
        """
        pass

    @abstractmethod
    def get_assertion_category(self) -> str:
        """Return the assertion category (e.g., 'volume', 'field')."""
        pass

    @abstractmethod
    def get_min_training_interval_seconds(self) -> int:
        """Return the minimum time between training runs in seconds."""
        pass

    @abstractmethod
    def get_retune_interval_seconds(self) -> int:
        """Return the interval after which hyperparameters should be retuned.

        When hyperparameters are older than this interval, the adapter will
        force fresh tuning instead of using the existing hyperparameters.
        Typically longer than get_min_training_interval_seconds() (e.g., 6 days).
        """
        pass

    @abstractmethod
    def get_training_context(
        self,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> AssertionTrainingContext:
        """Return the training context with entity URN and bounds."""
        pass

    def _extract_existing_model_config(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> Optional[ModelConfig]:
        """Extract existing model config from evaluation spec if available."""
        if evaluation_spec.context and evaluation_spec.context.inference_details:
            # inference_details is a permissive pydantic model that includes schema class attributes
            return parse_inference_details(evaluation_spec.context.inference_details)  # type: ignore[arg-type]
        return None

    def _should_perform_inference(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> bool:
        """
        Check if enough time has passed since last training.

        TODO: also ensure we have valid predictions for the next X time period.
        Also if we have new feedback on anomalies, we should train again.
        """
        if evaluation_spec.context and evaluation_spec.context.inference_details:
            last_inferred_at_ms = (
                evaluation_spec.context.inference_details.generated_at or 0
            )
            now_ms = int(time.time() * 1000)
            min_interval_ms = self.get_min_training_interval_seconds() * 1000
            return (now_ms - last_inferred_at_ms) >= min_interval_ms

        # No previous inference - should train
        return True

    def _should_force_retune(
        self, existing_model_config: Optional[ModelConfig]
    ) -> bool:
        """
        Check if hyperparameters are stale and need retuning.

        Compares the generated_at timestamp from the existing model config against
        the retune interval. If hyperparameters are older than the retune
        interval, returns True to signal that fresh tuning should be performed.

        Args:
            existing_model_config: Previously trained model config, if available.

        Returns:
            True if hyperparameters are stale and should be retuned,
            False if they are still fresh or no previous config exists.
        """
        if existing_model_config is None:
            # No previous config - don't force retune (will tune fresh anyway)
            return False

        generated_at_ms = existing_model_config.generated_at or 0
        if generated_at_ms == 0:
            # No timestamp - don't force retune
            return False

        now_ms = int(time.time() * 1000)
        retune_interval_ms = self.get_retune_interval_seconds() * 1000
        return (now_ms - generated_at_ms) >= retune_interval_ms

    def _run_with_training_error_handling(
        self, *, assertion_urn: str, fn: Callable[[], None]
    ) -> None:
        """
        Run a training step with standardized error handling.

        Subclasses historically duplicated this error mapping logic; centralize it here
        so V2 trainers behave consistently.
        """
        try:
            fn()
        except TrainingErrorException as e:
            logger.exception(f"[V2] Training failed for assertion {assertion_urn}: {e}")
            raise
        except RuntimeError as e:
            logger.exception(f"[V2] Training failed for assertion {assertion_urn}: {e}")
            raise TrainingErrorException(
                message=str(e),
                error_type=MonitorErrorTypeClass.UNKNOWN,
            ) from e
        except Exception as e:
            logger.exception(f"[V2] Training failed for assertion {assertion_urn}: {e}")
            raise TrainingErrorException(
                message=str(e),
                error_type=MonitorErrorTypeClass.UNKNOWN,
            ) from e

    def _build_base_training_context(
        self,
        *,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
        assertion_category: str,
        default_sensitivity_level: int,
        interval_hours: int = 1,
    ) -> AssertionTrainingContext:
        """
        Build the common portion of AssertionTrainingContext for all v2 trainers.

        Individual trainers should use this and then override/add their specific
        fields (floor/ceiling/is_delta/etc).
        """
        return AssertionTrainingContext(
            entity_urn=assertion.entity.urn,
            interval_hours=int(interval_hours),
            num_intervals=get_default_prediction_num_intervals(
                interval_hours=int(interval_hours)
            ),
            sensitivity_level=get_sensitivity_level(
                adjustment_settings, default_sensitivity_level
            ),
            assertion_category=assertion_category,
            existing_model_config=self._extract_existing_model_config(evaluation_spec),
        )

    def _run_training_pipeline(
        self,
        df: pd.DataFrame,
        context: AssertionTrainingContext,
        ground_truth: Optional[pd.DataFrame] = None,
    ) -> TrainingResult:
        """
        Run the observe-models training pipeline.

        Args:
            df: DataFrame with 'ds', 'y' columns containing all metrics.
            context: Training context with intervals, sensitivity, existing_model_config,
                assertion_category, and is_dataframe_cumulative.
            ground_truth: Optional DataFrame with 'ds', 'is_anomaly_gt' columns for
                anomaly feedback. is_anomaly_gt should be True for confirmed anomalies
                (status != REJECTED).

        Returns:
            TrainingResult with forecast and optional anomaly detection results
        """
        min_samples = context.min_training_samples
        if min_samples and len(df) < min_samples:
            raise InsufficientSamplesException(
                min_samples=min_samples,
                actual_samples=len(df),
                properties={
                    "component": "trainer_v2",
                    "assertion_category": context.assertion_category,
                },
            )

        # Determine if we should force retuning (hyperparameters are stale)
        force_retune = self._should_force_retune(context.existing_model_config)

        if force_retune:
            logger.info(
                f"Forcing retune - hyperparameters are stale for {context.entity_urn}"
            )

        # Build input data context - describes the shape of the input data
        input_context = InputDataContext(
            assertion_category=context.assertion_category,
            is_dataframe_cumulative=context.is_dataframe_cumulative,
            is_delta=context.is_delta,
        )

        adapter = ObserveAdapter()

        return adapter.run_training_pipeline(
            df=df,
            ground_truth=ground_truth,
            input_data_context=input_context,
            num_intervals=context.num_intervals,
            interval_hours=context.interval_hours,
            force_retune=force_retune,
            sensitivity_level=context.sensitivity_level,
            existing_model_config=context.existing_model_config,
        )

    def _get_adjustment_settings(
        self, monitor: Monitor
    ) -> Optional[AssertionAdjustmentSettings]:
        """Extract adjustment settings from a monitor."""
        if monitor.assertion_monitor and monitor.assertion_monitor.settings:
            return monitor.assertion_monitor.settings.inference_settings
        return None

    def _fetch_metrics(
        self,
        monitor: Monitor,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Metric]:
        """Fetch metrics from the metrics cube."""
        metric_cube_urn = get_metric_cube_urn(monitor.urn)
        lookback_days = extract_lookback_days(adjustment_settings)

        try:
            return self.metrics_client.fetch_metric_values(
                metric_cube_urn,
                lookback=timedelta(days=lookback_days),
                limit=2000,
            )
        except Exception as e:
            raise TrainingErrorException(
                message=f"Failed to fetch metrics for {metric_cube_urn}: {e}",
                error_type=MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT,
                properties={
                    "step": "fetch_metrics",
                    "metric_cube_urn": metric_cube_urn,
                },
            ) from e

    def _fetch_anomalies(
        self,
        monitor: Monitor,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Anomaly]:
        """Fetch anomalies from the monitor for ground truth.

        Returns non-rejected anomalies which are treated as confirmed ground truth.
        """
        lookback_days = extract_lookback_days(adjustment_settings)

        try:
            return self.monitor_client.fetch_monitor_anomalies(
                urn=monitor.urn,
                lookback=timedelta(days=lookback_days),
                limit=2000,
            )
        except Exception as e:
            raise TrainingErrorException(
                message=f"Failed to fetch anomalies for {monitor.urn}: {e}",
                error_type=MonitorErrorTypeClass.INPUT_DATA_INSUFFICIENT,
                properties={
                    "step": "fetch_anomalies",
                    "monitor_urn": monitor.urn,
                },
            ) from e

    def _build_training_dataframe(
        self,
        metrics: List[Metric],
        anomalies: List[Anomaly],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Build training DataFrame and ground truth DataFrame for observe-models.

        Returns:
            A tuple of (training_df, ground_truth_df):
            - training_df: DataFrame with 'ds', 'y' columns containing all metrics.
            - ground_truth_df: DataFrame with 'ds', 'is_anomaly_gt' columns for
              anomaly feedback. All anomalies passed here are non-rejected
              (rejected anomalies are filtered out upstream).
        """
        try:
            # Build training dataframe with all metrics
            training_records = []
            for metric in metrics:
                training_records.append(
                    {
                        "timestamp_ms": metric.timestamp_ms,
                        "y": metric.value,
                    }
                )

            training_df = pd.DataFrame(training_records)
            if not training_df.empty:
                training_df = (
                    timestamp_ms_to_ds(training_df)
                    .sort_values("ds")
                    .reset_index(drop=True)
                )

            # Build ground truth dataframe from anomalies
            # All anomalies here are non-rejected (filtered upstream)
            ground_truth_records = []
            for anomaly in anomalies:
                ground_truth_records.append(
                    {
                        "timestamp_ms": anomaly.timestamp_ms,
                        "is_anomaly_gt": anomaly.is_confirmed,
                    }
                )

            ground_truth_df = pd.DataFrame(ground_truth_records)
            if not ground_truth_df.empty:
                ground_truth_df = (
                    timestamp_ms_to_ds(ground_truth_df)
                    .sort_values("ds")
                    .reset_index(drop=True)
                )

            return training_df, ground_truth_df
        except Exception as e:
            raise TrainingErrorException(
                message=f"Failed to build training dataframes: {e}",
                error_type=MonitorErrorTypeClass.INPUT_DATA_INVALID,
                properties={"step": "build_training_dataframe"},
            ) from e

    def _update_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
        training_result: TrainingResult,
        context: AssertionTrainingContext,
    ) -> None:
        """
        Update the assertion with training results using persistence utilities.
        """
        if training_result.prediction_df is None and getattr(
            training_result, "scores_only_persist", False
        ):
            # Quality below threshold: persist inference_details (scores) only, no forecast.
            try:
                evaluation_context = build_evaluation_context(
                    model_config=training_result.model_config,
                    embedded_assertions=[],
                    generated_at_millis=int(time.time() * 1000),
                )
                self.monitor_client.patch_volume_monitor_evaluation_context(
                    monitor.urn, assertion.urn, evaluation_context, evaluation_spec
                )
            except Exception as e:
                raise TrainingErrorException(
                    message=f"Failed to persist scores-only outputs for {assertion.urn}: {e}",
                    error_type=MonitorErrorTypeClass.PERSISTENCE_FAILED,
                    properties={
                        "step": "patch_volume_monitor_evaluation_context",
                        "monitor_urn": monitor.urn,
                        "assertion_urn": assertion.urn,
                    },
                ) from e
            return

        if training_result.prediction_df is None:
            prediction_error = None
            if "prediction" in training_result.step_results:
                prediction_error = training_result.step_results["prediction"].error
            raise TrainingErrorException(
                message="Prediction output missing (prediction_df is None)",
                error_type=MonitorErrorTypeClass.MODEL_TRAINING_FAILED,
                properties={
                    "step": "prediction",
                    "component": "trainer_v2",
                    "prediction_error": str(prediction_error),
                    "assertion_category": context.assertion_category,
                    "forecast_model_key": _format_model_key(
                        training_result.model_config.forecast_model_name,
                        training_result.model_config.forecast_model_version,
                    ),
                    "anomaly_model_key": _format_model_key(
                        training_result.model_config.anomaly_model_name,
                        training_result.model_config.anomaly_model_version,
                    ),
                    "forecast_score": str(training_result.model_config.forecast_score),
                    "anomaly_score": str(training_result.model_config.anomaly_score),
                },
            )

        try:
            embedded_assertions = AnomalyAssertions.from_df(
                training_result.prediction_df,
                entity_urn=context.entity_urn,
                window_size_seconds=context.interval_hours * 3600,
            )
            evaluation_context = build_evaluation_context(
                model_config=training_result.model_config,
                embedded_assertions=embedded_assertions,
                generated_at_millis=int(time.time() * 1000),
            )
            self.monitor_client.patch_volume_monitor_evaluation_context(
                monitor.urn, assertion.urn, evaluation_context, evaluation_spec
            )
        except Exception as e:
            raise TrainingErrorException(
                message=f"Failed to persist training outputs for {assertion.urn}: {e}",
                error_type=MonitorErrorTypeClass.PERSISTENCE_FAILED,
                properties={
                    "step": "patch_volume_monitor_evaluation_context",
                    "monitor_urn": monitor.urn,
                    "assertion_urn": assertion.urn,
                },
            ) from e


def _format_model_key(name: Optional[str], version: Optional[str]) -> str:
    if not name:
        return ""
    if version:
        return f"{name}@{version}"
    return name
