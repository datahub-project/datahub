"""
Volume assertion trainer V2 using observe-models.
"""

import dataclasses
import logging
from typing import Optional

from datahub_executor.common.monitor.inference_v2.base_trainer_v2 import BaseTrainerV2
from datahub_executor.common.monitor.inference_v2.types import AssertionTrainingContext
from datahub_executor.common.monitor.inference_v2.volume_semantics import (
    resolve_volume_series_semantics,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    Monitor,
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
                f"[V2] Successfully trained volume assertion {assertion.urn} "
                f"with {num_predictions} predictions"
            )

        self._run_with_training_error_handling(
            assertion_urn=assertion.urn, fn=_do_train
        )

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
        is_dataframe_cumulative, is_delta = self._get_volume_data_characteristics(
            assertion
        )
        base = self._build_base_training_context(
            assertion=assertion,
            adjustment_settings=adjustment_settings,
            evaluation_spec=evaluation_spec,
            assertion_category=self.get_assertion_category(),
            default_sensitivity_level=VOLUME_DEFAULT_SENSITIVITY_LEVEL,
            interval_hours=1,
        )
        return dataclasses.replace(
            base,
            floor_value=0.0,  # Row counts cannot be negative
            ceiling_value=None,
            is_dataframe_cumulative=is_dataframe_cumulative,
            is_delta=is_delta,
            min_training_samples=VOLUME_MIN_TRAINING_SAMPLES,
        )

    def _get_volume_data_characteristics(
        self, assertion: Assertion
    ) -> tuple[bool, Optional[bool]]:
        """
        Derive volume time series semantics from the assertion subtype.

        We treat TOTAL variants as cumulative inputs (needs differencing) and
        CHANGE variants as delta inputs (no differencing).
        """
        if assertion.volume_assertion is None:
            return False, None
        return resolve_volume_series_semantics(assertion.volume_assertion.type)
