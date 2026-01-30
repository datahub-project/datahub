"""
SQL assertion trainer V2 using observe-models.
"""

import dataclasses
import logging
from typing import Optional

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

        # Fetch anomalies and build training dataframe + ground truth
        anomalies = self._fetch_anomalies(monitor, adjustment_settings)
        df, ground_truth = self._build_training_dataframe(metrics, anomalies)

        # Get training context and run pipeline
        context = self.get_training_context(
            assertion, adjustment_settings, evaluation_spec
        )

        def _do_train() -> None:
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

        self._run_with_training_error_handling(
            assertion_urn=assertion.urn, fn=_do_train
        )

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
        base = self._build_base_training_context(
            assertion=assertion,
            adjustment_settings=adjustment_settings,
            evaluation_spec=evaluation_spec,
            assertion_category=self.get_assertion_category(),
            default_sensitivity_level=SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL,
            interval_hours=1,
        )
        return dataclasses.replace(
            base,
            floor_value=None,  # SQL metrics have no floor
            ceiling_value=None,  # SQL metrics have no ceiling
            min_training_samples=SQL_METRIC_MIN_TRAINING_SAMPLES,
        )
