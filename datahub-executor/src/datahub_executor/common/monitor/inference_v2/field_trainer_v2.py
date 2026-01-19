"""
Field assertion trainer V2 using observe-models.
"""

import logging
from typing import Optional

from datahub_executor.common.monitor.adjustment_utils import get_sensitivity_level
from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import BaseTrainerV2
from datahub_executor.common.monitor.inference_v2.types import AssertionTrainingContext
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    Monitor,
)
from datahub_executor.config import (
    FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL,
    FIELD_METRIC_MIN_TRAINING_INTERVAL_SECONDS,
    FIELD_METRIC_MIN_TRAINING_SAMPLES,
    FIELD_METRIC_RETUNE_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

# Mapping from FieldMetricType values to assertion category strings.
# The field trainer resolves the specific category (volume, rate, statistic, length)
# based on the metric type, rather than passing "field" to the adapter.
FIELD_METRIC_TO_CATEGORY: dict[str, str] = {
    # Count metrics -> volume
    "UNIQUE_COUNT": "volume",
    "NULL_COUNT": "volume",
    "NEGATIVE_COUNT": "volume",
    "ZERO_COUNT": "volume",
    "EMPTY_COUNT": "volume",
    # Percentage metrics -> rate
    "UNIQUE_PERCENTAGE": "rate",
    "NULL_PERCENTAGE": "rate",
    "NEGATIVE_PERCENTAGE": "rate",
    "ZERO_PERCENTAGE": "rate",
    "EMPTY_PERCENTAGE": "rate",
    # Statistics metrics -> statistic
    "MIN": "statistic",
    "MAX": "statistic",
    "MEAN": "statistic",
    "MEDIAN": "statistic",
    "STDDEV": "statistic",
    # Length metrics -> length
    "MIN_LENGTH": "length",
    "MAX_LENGTH": "length",
}

# Field metrics with floor value of 0.0 (counts/percentages cannot be negative)
FLOOR_ZERO_METRICS = {
    "EMPTY_COUNT",
    "EMPTY_PERCENTAGE",
    "NULL_COUNT",
    "NULL_PERCENTAGE",
    "NEGATIVE_COUNT",
    "NEGATIVE_PERCENTAGE",
    "UNIQUE_COUNT",
    "UNIQUE_PERCENTAGE",
    "ZERO_COUNT",
    "ZERO_PERCENTAGE",
    "MAX_LENGTH",
    "MIN_LENGTH",
}

# Field metrics with ceiling value of 100.0 (percentages cannot exceed 100)
CEILING_100_METRICS = {
    "EMPTY_PERCENTAGE",
    "NULL_PERCENTAGE",
    "NEGATIVE_PERCENTAGE",
    "UNIQUE_PERCENTAGE",
    "ZERO_PERCENTAGE",
}


class FieldTrainerV2(BaseTrainerV2):
    """
    V2 trainer for field assertions using observe-models.

    Field assertions predict metric boundaries for individual columns.
    Floor/ceiling values depend on the specific metric type.
    """

    def _train_internal(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """Train a field assertion using observe-models."""
        logger.debug(
            f"[V2] Training field assertion {assertion.urn} under monitor {monitor.urn}"
        )

        adjustment_settings = self._get_adjustment_settings(monitor)

        # Fetch metric data
        metrics = self._fetch_metrics(monitor, adjustment_settings)
        # TODO: Move this minimum samples check out of the trainer and let _run_training_pipeline
        # throw an InsufficientSamplesException so we can handle it properly at the coordinator level.
        if len(metrics) < FIELD_METRIC_MIN_TRAINING_SAMPLES:
            logger.warning(
                f"[V2] Insufficient samples ({len(metrics)}) for assertion {assertion.urn}"
            )
            return

        # Fetch anomalies and build training dataframe + ground truth
        anomalies = self._fetch_anomalies(monitor, adjustment_settings)
        df, ground_truth = self._build_training_dataframe(metrics, anomalies)

        # Get training context and run pipeline
        context = self.get_training_context(
            assertion, adjustment_settings, evaluation_spec
        )

        try:
            # TODO: if this throws an error because of not confident enough predictions, we should report that to the monitor status.
            training_result = self._run_training_pipeline(
                df=df,
                context=context,
                ground_truth=ground_truth,
            )

            self._update_assertion(
                monitor, assertion, evaluation_spec, training_result, context
            )

            num_predictions = (
                len(training_result.prediction_df)
                if training_result.prediction_df is not None
                else 0
            )
            logger.info(
                f"[V2] Successfully trained field assertion {assertion.urn} "
                f"with {num_predictions} predictions"
            )

        except Exception as e:
            logger.exception(f"[V2] Training failed for assertion {assertion.urn}: {e}")

    def get_assertion_category(self) -> str:
        return "field"

    def get_min_training_interval_seconds(self) -> int:
        return FIELD_METRIC_MIN_TRAINING_INTERVAL_SECONDS

    def get_retune_interval_seconds(self) -> int:
        return FIELD_METRIC_RETUNE_INTERVAL_SECONDS

    def get_training_context(
        self,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> AssertionTrainingContext:
        # Determine floor/ceiling and allow_negative based on metric type
        floor_value: Optional[float] = None
        ceiling_value: Optional[float] = None
        allow_negative: Optional[bool] = None

        metric_type = self._get_metric_type(assertion)
        if metric_type in FLOOR_ZERO_METRICS:
            floor_value = 0.0
            # Metrics with floor=0 should not allow negative values during preprocessing
            allow_negative = False
        if metric_type in CEILING_100_METRICS:
            ceiling_value = 100.0

        # Resolve the assertion category based on metric type
        assertion_category = self._get_category_for_metric_type(metric_type)

        return AssertionTrainingContext(
            entity_urn=assertion.entity.urn,
            num_intervals=24,  # 24 hours of predictions
            interval_hours=1,
            sensitivity_level=get_sensitivity_level(
                adjustment_settings, FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL
            ),
            floor_value=floor_value,
            ceiling_value=ceiling_value,
            assertion_category=assertion_category,
            metric_type=metric_type,
            existing_model_config=self._extract_existing_model_config(evaluation_spec),
            allow_negative=allow_negative,
        )

    def _get_metric_type(self, assertion: Assertion) -> str:
        """Extract the metric type from a field assertion."""
        if (
            assertion.field_assertion
            and assertion.field_assertion.field_metric_assertion
        ):
            if assertion.field_assertion.field_metric_assertion.metric:
                return assertion.field_assertion.field_metric_assertion.metric.name
        return ""

    def _get_category_for_metric_type(self, metric_type: str) -> str:
        """
        Get the assertion category based on the field metric type.

        Returns the specific category (volume, rate, statistic, length) that
        corresponds to the metric type, defaulting to 'rate' for unknown types.
        """
        return FIELD_METRIC_TO_CATEGORY.get(metric_type.upper(), "rate")
