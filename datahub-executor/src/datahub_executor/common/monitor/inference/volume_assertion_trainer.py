import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional

from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    RowCountTotalClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)

from datahub_executor.common.aspect_builder import get_assertion_info
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricBoundary,
)
from datahub_executor.common.monitor.inference.utils import (
    build_std_parameters,
    create_embedded_assertion,
    create_inference_source,
    is_metric_anomaly,
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
    VOLUME_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS,
)

logger = logging.getLogger(__name__)

DAYS_TO_MILLIS = 24 * 60 * 60 * 1000


class VolumeAssertionTrainer(BaseAssertionTrainer[Metric]):
    """
    Trainer for volume assertions.
    """

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Train a volume assertion.
        """
        logger.debug(
            f"Performing training for volume assertion {assertion.urn} under monitor {monitor.urn}"
        )
        self.perform_training(monitor, assertion, evaluation_spec)

    def should_perform_inference(
        self, evaluation_spec: AssertionEvaluationSpec
    ) -> bool:
        """
        Determine if inference should be performed based on time since last inference.
        """
        if evaluation_spec.context and evaluation_spec.context.inference_details:
            last_inferred_at_ms = (
                evaluation_spec.context.inference_details.generated_at or 0
            )
            now_ms = int(time.time() * 1000)
            return (now_ms - last_inferred_at_ms) >= (
                VOLUME_MIN_TRAINING_INTERVAL_SECONDS * 1000
            )
        # No previous inference, we should continue with inference to generate our first predictions.
        return True

    def get_min_training_samples(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return VOLUME_MIN_TRAINING_SAMPLES

    def get_min_training_samples_timespan_seconds(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return VOLUME_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS

    def _check_can_bootstrap_with_dataset_profile(self, assertion: Assertion) -> bool:
        """
        Check if the assertion can be bootstrapped with dataset profile row counts.
        """
        if assertion.volume_assertion is None:
            logger.warning(
                f"Assertion {assertion.urn} does not have a volume assertion. Skipping bootstrap."
            )
            return False
        if assertion.volume_assertion.filter is not None:
            logger.warning(
                f"Assertion {assertion.urn} has a custom SQL filter clause. Skipping bootstrap."
            )
            return False
        if assertion.volume_assertion.row_count_total is None:
            logger.warning(
                f"Assertion {assertion.urn} is not for row count total. Skipping bootstrap."
            )
            return False
        return True

    def try_get_historical_data_for_bootstrap(
        self,
        assertion: Assertion,
        maybe_adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> Optional[List[Metric]]:
        # 1. Ensure this assertion can be bootstrapped with dataset profile row counts
        if not self._check_can_bootstrap_with_dataset_profile(assertion):
            return None

        # 2. Fetch and return the historical data
        if assertion.source_created_time is None:
            logger.warning(
                f"No source created time found for assertion {assertion.urn}. Skipping bootstrap."
            )
            return None
        end_time_millis = assertion.source_created_time
        lookback_days = self.extract_lookback_days_from_adjustment_settings(
            maybe_adjustment_settings
        )
        start_time_millis = end_time_millis - (lookback_days * DAYS_TO_MILLIS)

        return self.metrics_client.fetch_row_counts_from_dataset_profile(
            assertion.entity.urn,
            start_time=datetime.fromtimestamp(start_time_millis / 1000),
            end_time=datetime.fromtimestamp(end_time_millis / 1000),
        )

    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Metric]:
        """
        Fetch metric data for volume training.
        """
        # Construct the metric cube URN
        metric_cube_urn = self.get_metric_cube_urn(monitor.urn)

        # Calculate lookback period
        lookback_days = self.extract_lookback_days_from_adjustment_settings(
            adjustment_settings
        )
        training_window_duration = timedelta(days=lookback_days)
        min_window_duration = timedelta(
            seconds=self.get_min_training_samples_timespan_seconds() + 60 * 60
        )  # Min timespan + 1 hr buffer

        # Prevent User Error: Ensure that we always fetch a timespan larger than the minimum required to train!
        final_window_duration = (
            training_window_duration
            if training_window_duration > min_window_duration
            else min_window_duration
        )

        # Fetch metrics
        metrics = self.metrics_client.fetch_metric_values(
            metric_cube_urn,
            lookback=final_window_duration,
            limit=2000,
        )

        # Fetch anomalies
        anomalies = self.monitor_client.fetch_monitor_anomalies(
            urn=monitor.urn,
            lookback=training_window_duration,
            limit=2000,
        )

        # Filter out anomalies to avoid using in training
        metrics_without_anomalies = [
            metric for metric in metrics if not is_metric_anomaly(metric, anomalies)
        ]

        return metrics_without_anomalies

    def remove_inferred_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Remove inferred assertion when not enough samples are available.
        """
        logger.warning(
            f"Insufficient samples to train volume assertion {assertion.urn}. Resetting boundaries by updating context."
        )

        # Create empty context to reset the generated_at time to 0
        new_context = self.create_empty_context(0)

        # Update the assertion inference details
        self.monitor_client.patch_volume_monitor_evaluation_context(
            monitor.urn, assertion.urn, new_context, evaluation_spec
        )

    def train_and_update_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        events: List[Metric],
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> Assertion:
        """
        Train and update a volume assertion with new boundaries.
        """
        # 1) Determine sensitivity level
        sensitivity = self._get_sensitivity_level(adjustment_settings)

        # 2) Predict boundaries
        boundaries = self.metrics_predictor.predict_metric_boundaries(
            events, timedelta(hours=1), 24, sensitivity
        )
        current_boundary = boundaries[0]  # First boundary is the current one
        future_boundaries = boundaries[1:]  # Rest are future predictions

        # 3) Build a new VolumeAssertionInfoClass with updated boundaries
        assertion_info = self._get_assertion_info(assertion)

        # 4) Build new volume assertion with updated boundaries
        new_vol_assertion = self._build_volume_assertion_info(
            assertion.entity.urn,
            current_boundary,
        )
        assertion_info.volumeAssertion = new_vol_assertion

        # 5) Update the monitor with embedded assertions for future boundaries
        self._update_volume_monitor_evaluation_context(
            assertion.urn,
            monitor.urn,
            assertion.entity.urn,
            future_boundaries,
            evaluation_spec,
            assertion_info.description,
        )

        # 6) Persist the updated assertion info
        self.monitor_client.update_assertion_info(assertion.urn, assertion_info)

        # 7) Return updated assertion
        updated_assertion = self._rebuild_assertion(assertion, assertion_info)

        logger.debug(
            f"Trained & updated assertion {assertion.urn}, new boundaries: "
            f"[{current_boundary.lower_bound.value}, {current_boundary.upper_bound.value}]"
        )
        return updated_assertion

    def _get_sensitivity_level(
        self, adjustment_settings: Optional[AssertionAdjustmentSettings]
    ) -> int:
        """
        Get the sensitivity level from settings or use the default.
        """
        if (
            adjustment_settings
            and adjustment_settings.sensitivity
            and adjustment_settings.sensitivity.level is not None
        ):
            return adjustment_settings.sensitivity.level
        return VOLUME_DEFAULT_SENSITIVITY_LEVEL

    def _get_assertion_info(self, assertion: Assertion) -> AssertionInfoClass:
        """
        Get assertion info from an assertion or raise an error.
        """
        assertion_info = get_assertion_info(assertion.raw_info_aspect)
        if not assertion_info:
            raise RuntimeError(
                f"Missing raw assertionInfo aspect for assertion {assertion.urn}"
            )
        return assertion_info

    def _build_volume_assertion_info(
        self,
        entity_urn: str,
        boundary: MetricBoundary,
    ) -> VolumeAssertionInfoClass:
        """
        Build a volume assertion info with updated boundaries.
        """
        return VolumeAssertionInfoClass(
            type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
            entity=entity_urn,
            rowCountTotal=RowCountTotalClass(
                operator=AssertionStdOperatorClass.BETWEEN,
                parameters=build_std_parameters(boundary),
            ),
            rowCountChange=None,
            incrementingSegmentRowCountChange=None,
            incrementingSegmentRowCountTotal=None,
        )

    def _update_volume_monitor_evaluation_context(
        self,
        assertion_urn: str,
        monitor_urn: str,
        entity_urn: str,
        boundaries: List[MetricBoundary],
        evaluation_spec: AssertionEvaluationSpec,
        assertion_description: Optional[str],
    ) -> None:
        """
        Update the monitor's embedded assertions with future boundary predictions.
        """
        # 1) Create embedded assertions for each boundary
        embedded_assertions = []
        window_size = timedelta(hours=1)

        for boundary in boundaries:
            # Build volume assertion info for this boundary
            vol_assertion_info = self._build_volume_assertion_info(entity_urn, boundary)

            # Create assertion info
            assertion_info = AssertionInfoClass(
                type="VOLUME",
                volumeAssertion=vol_assertion_info,
                description=assertion_description,
                source=create_inference_source(),
            )

            # Create embedded assertion
            embedded_assertions.append(
                create_embedded_assertion(
                    assertion_info, boundary, int(window_size.total_seconds())
                )
            )

        # 2) Create evaluation context with current timestamp
        inference_details = self.create_inference_details(int(time.time() * 1000))
        new_context = AssertionEvaluationContextClass(
            embeddedAssertions=embedded_assertions, inferenceDetails=inference_details
        )

        # 3) Update the monitor's evaluation context
        logger.debug(
            f"Updating monitor embedded assertions for {monitor_urn} with {len(embedded_assertions)} boundaries."
        )
        try:
            self.monitor_client.patch_volume_monitor_evaluation_context(
                monitor_urn, assertion_urn, new_context, evaluation_spec
            )
        except Exception as e:
            logger.exception(
                f"Failed to update embedded assertions for {monitor_urn}: {e}"
            )

    def _rebuild_assertion(
        self, original_assertion: Assertion, assertion_info: AssertionInfoClass
    ) -> Assertion:
        """
        Rebuild an assertion with updated info.
        """
        return Assertion.parse_obj(
            dict(
                **dict(assertion_info.to_obj()),
                urn=original_assertion.urn,
                entity=original_assertion.entity,
                connectionUrn=original_assertion.connection_urn,
                raw_info_aspect=None,
            )
        )
