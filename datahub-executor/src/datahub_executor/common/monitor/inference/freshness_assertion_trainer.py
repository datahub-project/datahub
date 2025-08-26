import logging
import time
from datetime import timedelta
from typing import List, Optional

from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    FixedIntervalScheduleClass,
    FreshnessAssertionInfoClass,
    FreshnessAssertionScheduleClass,
    FreshnessAssertionScheduleTypeClass,
    FreshnessAssertionTypeClass,
)

from datahub_executor.common.aspect_builder import get_assertion_info
from datahub_executor.common.metric.types import Metric, Operation
from datahub_executor.common.monitor.inference.base_assertion_trainer import (
    BaseAssertionTrainer,
)
from datahub_executor.common.monitor.inference.utils import (
    annotate_operations_with_anomalies,
    build_evaluation_schedule_from_fixed_interval,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentSettings,
    AssertionEvaluationSpec,
    CronSchedule,
    Monitor,
)
from datahub_executor.config import (
    FRESHNESS_DEFAULT_EVALUATION_CRON_SCHEDULE,
    FRESHNESS_DEFAULT_SENSITIVITY_LEVEL,
    FRESHNESS_MIN_TRAINING_INTERVAL_SECONDS,
    FRESHNESS_MIN_TRAINING_SAMPLES,
    FRESHNESS_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS,
)

logger = logging.getLogger(__name__)

# Operations to ignore during freshness training
FRESHNESS_OPERATION_TYPES_TO_IGNORE = ["DELETE", "DROP", "SELECT"]

# TODO: We need to dynamically update the run interval based on the predicted
# interval - We must run at least as frequently as the predicted interval.
# For our anomaly strategy to hold, this must be the case.


class FreshnessAssertionTrainer(BaseAssertionTrainer[Operation]):
    """
    Trainer for freshness assertions.
    """

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Train a freshness assertion.
        """
        logger.debug(
            f"Performing training for freshness assertion {assertion.urn} under monitor {monitor.urn}"
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
                FRESHNESS_MIN_TRAINING_INTERVAL_SECONDS * 1000
            )
        # No previous inference, we should continue with inference to generate our first predictions.
        return True

    def get_min_training_samples(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return FRESHNESS_MIN_TRAINING_SAMPLES

    def get_min_training_samples_timespan_seconds(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return FRESHNESS_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS

    def try_get_historical_data_for_bootstrap(
        self,
        assertion: Assertion,
        maybe_adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> Optional[List[Metric]]:
        """No bootstrap needed for freshness assertions as they are based on the Operation aspect."""
        return None

    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        prefetched_metrics_data: Optional[List[Metric]],
    ) -> List[Operation]:
        """
        Fetch operation data for freshness training.
        """
        entity_urn = assertion.entity.urn

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

        logger.debug(
            f"Fetching historical operations for the past {lookback_days} days, ignoring {FRESHNESS_OPERATION_TYPES_TO_IGNORE} for entity {entity_urn}"
        )

        # Fetch operations
        operations = self.metrics_client.fetch_operations(
            entity_urn=entity_urn,
            lookback=final_window_duration,
            limit=2000,
            ignore_types=FRESHNESS_OPERATION_TYPES_TO_IGNORE,
        )

        # Fetch anomalies
        anomalies = self.monitor_client.fetch_monitor_anomalies(
            urn=monitor.urn,
            lookback=training_window_duration,
            limit=2000,
        )

        # Now, merge the anomalies and the operations
        # to mark operations following anomaly intervals.
        return annotate_operations_with_anomalies(operations, anomalies)

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
            f"Insufficient samples to train freshness assertion {assertion.urn}. Resetting boundaries by updating context."
        )

        # Create empty context to reset the generated_at time to 0
        new_context = self.create_empty_context(0)

        # Update the assertion inference details. Reset run schedule to hourly.
        new_schedule = CronSchedule(
            cron=FRESHNESS_DEFAULT_EVALUATION_CRON_SCHEDULE,
            timezone="UTC",
        )
        self.monitor_client.patch_freshness_monitor_evaluation_spec(
            monitor.urn, assertion.urn, new_context, new_schedule, evaluation_spec
        )

    def train_and_update_assertion(
        self,
        monitor: Monitor,
        assertion: Assertion,
        events: List[Operation],
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> Assertion:
        """
        Train and update a freshness assertion with a fixed interval schedule.
        """
        # 1) Determine sensitivity level
        sensitivity = self._get_sensitivity_level(adjustment_settings)

        # 2) Predict the fixed interval schedule
        fixed_interval = self.metrics_predictor.predict_fixed_interval_schedule(
            events, sensitivity
        )

        # 3) Get assertion info
        assertion_info = self._get_assertion_info(assertion)

        # 4) Build new freshness assertion with updated schedule
        new_freshness_assertion = self._build_fixed_interval_freshness_assertion_info(
            assertion.entity.urn,
            fixed_interval,
        )
        assertion_info.freshnessAssertion = new_freshness_assertion

        # 5) Build new run schedule from inferred freshness assertion
        new_schedule = build_evaluation_schedule_from_fixed_interval(fixed_interval)

        logger.debug(
            f"Saving assertion info for urn {assertion.urn} {assertion_info.freshnessAssertion}"
        )

        # 5) Update the monitor evaluation context
        self._update_freshness_monitor_evaluation_spec(
            monitor,
            assertion,
            new_schedule,
            evaluation_spec,
        )

        # 6) Persist the updated assertion info
        self.monitor_client.update_assertion_info(assertion.urn, assertion_info)

        # 7) Return updated assertion
        updated_assertion = self._rebuild_assertion(assertion, assertion_info)

        logger.debug(
            f"Trained & updated assertion {assertion.urn}, new fixed interval predicted: "
            f"{fixed_interval}"
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
        return FRESHNESS_DEFAULT_SENSITIVITY_LEVEL

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

    def _build_fixed_interval_freshness_assertion_info(
        self,
        entity_urn: str,
        fixed_interval: FixedIntervalScheduleClass,
    ) -> FreshnessAssertionInfoClass:
        """
        Build a freshness assertion info with a fixed interval schedule.
        """
        return FreshnessAssertionInfoClass(
            type=FreshnessAssertionTypeClass.DATASET_CHANGE,
            entity=entity_urn,
            schedule=FreshnessAssertionScheduleClass(
                type=FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
                fixedInterval=fixed_interval,
            ),
        )

    def _update_freshness_monitor_evaluation_spec(
        self,
        monitor: Monitor,
        assertion: Assertion,
        new_schedule: CronSchedule,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Update the freshness monitor evaluation context with current timestamp.
        """
        # Create evaluation context with current timestamp
        inference_details = self.create_inference_details(int(time.time() * 1000))

        new_context = AssertionEvaluationContextClass(
            embeddedAssertions=[],  # No embedded assertions for freshness monitors (yet)
            inferenceDetails=inference_details,
        )

        # Update the monitor's evaluation context
        self.monitor_client.patch_freshness_monitor_evaluation_spec(
            monitor.urn,
            assertion.urn,
            new_context,
            new_schedule,
            evaluation_spec,
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
