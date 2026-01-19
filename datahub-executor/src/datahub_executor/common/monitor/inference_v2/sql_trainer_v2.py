"""
SQL assertion trainer V2 using observe-models.
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
    SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL,
    SQL_METRIC_MIN_TRAINING_INTERVAL_SECONDS,
    SQL_METRIC_MIN_TRAINING_SAMPLES,
    SQL_METRIC_RETUNE_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)


class SqlTrainerV2(BaseTrainerV2):
    """
    V2 trainer for SQL assertions using observe-models.

    SQL assertions predict boundaries for custom SQL metric values.
    No floor/ceiling constraints since SQL metrics can have any range.
    """

    def _train_internal(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """Train a SQL assertion using observe-models."""
        logger.debug(
            f"[V2] Training SQL assertion {assertion.urn} under monitor {monitor.urn}"
        )

        adjustment_settings = self._get_adjustment_settings(monitor)

        # Fetch metric data
        metrics = self._fetch_metrics(monitor, adjustment_settings)
        # TODO: Move this minimum samples check out of the trainer and let _run_training_pipeline
        # throw an InsufficientSamplesException so we can handle it properly at the coordinator level.
        if len(metrics) < SQL_METRIC_MIN_TRAINING_SAMPLES:
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
                f"[V2] Successfully trained SQL assertion {assertion.urn} "
                f"with {num_predictions} predictions"
            )

        except Exception as e:
            logger.exception(f"[V2] Training failed for assertion {assertion.urn}: {e}")

    def get_assertion_category(self) -> str:
        # SQL assertions use volume preprocessing (similar time series patterns)
        return "volume"

    def get_min_training_interval_seconds(self) -> int:
        return SQL_METRIC_MIN_TRAINING_INTERVAL_SECONDS

    def get_retune_interval_seconds(self) -> int:
        return SQL_METRIC_RETUNE_INTERVAL_SECONDS

    def get_training_context(
        self,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> AssertionTrainingContext:
        return AssertionTrainingContext(
            entity_urn=assertion.entity.urn,
            num_intervals=24,  # 24 hours of predictions
            interval_hours=1,
            sensitivity_level=get_sensitivity_level(
                adjustment_settings, SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL
            ),
            floor_value=None,  # SQL metrics have no floor
            ceiling_value=None,  # SQL metrics have no ceiling
            assertion_category=self.get_assertion_category(),
            existing_model_config=self._extract_existing_model_config(evaluation_spec),
        )
