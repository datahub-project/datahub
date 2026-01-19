"""
Volume assertion trainer V2 using observe-models.
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
    VolumeAssertionType,
)
from datahub_executor.config import (
    VOLUME_DEFAULT_SENSITIVITY_LEVEL,
    VOLUME_MIN_TRAINING_INTERVAL_SECONDS,
    VOLUME_MIN_TRAINING_SAMPLES,
    VOLUME_RETUNE_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)


class VolumeTrainerV2(BaseTrainerV2):
    """
    V2 trainer for volume assertions using observe-models.

    Volume assertions predict row count boundaries for datasets.
    Floor value is 0.0 (row counts cannot be negative).
    """

    def _train_internal(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """Train a volume assertion using observe-models."""
        logger.debug(
            f"[V2] Training volume assertion {assertion.urn} under monitor {monitor.urn}"
        )

        adjustment_settings = self._get_adjustment_settings(monitor)

        # Fetch metric data
        metrics = self._fetch_metrics(monitor, adjustment_settings)
        # TODO: Move this minimum samples check out of the trainer and let _run_training_pipeline
        # throw an InsufficientSamplesException so we can handle it properly at the coordinator level.
        if len(metrics) < VOLUME_MIN_TRAINING_SAMPLES:
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
                f"[V2] Successfully trained volume assertion {assertion.urn} "
                f"with {num_predictions} predictions"
            )

        except Exception as e:
            logger.exception(f"[V2] Training failed for assertion {assertion.urn}: {e}")

    def get_assertion_category(self) -> str:
        return "volume"

    def get_min_training_interval_seconds(self) -> int:
        return VOLUME_MIN_TRAINING_INTERVAL_SECONDS

    def get_retune_interval_seconds(self) -> int:
        return VOLUME_RETUNE_INTERVAL_SECONDS

    def get_training_context(
        self,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> AssertionTrainingContext:
        return AssertionTrainingContext(
            entity_urn=assertion.entity.urn,
            num_intervals=48,  # 48 hours of predictions
            interval_hours=1,
            sensitivity_level=get_sensitivity_level(
                adjustment_settings, VOLUME_DEFAULT_SENSITIVITY_LEVEL
            ),
            floor_value=0.0,  # Row counts cannot be negative
            ceiling_value=None,
            assertion_category=self.get_assertion_category(),
            existing_model_config=self._extract_existing_model_config(evaluation_spec),
            is_dataframe_cumulative=self._is_dataframe_cumulative(assertion),
        )

    def _is_dataframe_cumulative(self, assertion: Assertion) -> bool:
        """
        Check if assertion uses cumulative row counts that need differencing.

        ROW_COUNT_TOTAL means the data represents total row counts (cumulative),
        which needs differencing to convert to incremental values for training.

        ROW_COUNT_CHANGE means the data is already incremental, no differencing needed.
        """
        if assertion.volume_assertion is None:
            return False
        # ROW_COUNT_TOTAL means data is cumulative, needs differencing
        return assertion.volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL
