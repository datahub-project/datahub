import logging
import time
from datetime import timedelta
from typing import List, Optional

from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
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
    SQLAssertionType,
)
from datahub_executor.config import (
    SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL,
    SQL_METRIC_MIN_TRAINING_INTERVAL_SECONDS,
    SQL_METRIC_MIN_TRAINING_SAMPLES,
    SQL_METRIC_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS,
)

logger = logging.getLogger(__name__)


class SqlAssertionTrainer(BaseAssertionTrainer[Metric]):
    """
    Trainer for SQL metric assertions with anomaly detection.
    """

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Train a SQL metric assertion.
        """
        logger.debug(
            f"Performing training for SQL metric assertion {assertion.urn} under monitor {monitor.urn}"
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
                SQL_METRIC_MIN_TRAINING_INTERVAL_SECONDS * 1000
            )
        return True

    def get_min_training_samples(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return SQL_METRIC_MIN_TRAINING_SAMPLES

    def get_min_training_samples_timespan_seconds(self) -> int:
        """
        Get the minimum timespan required for training samples.
        """
        return SQL_METRIC_MIN_TRAINING_SAMPLES_TIMESPAN_SECONDS

    def try_get_historical_data_for_bootstrap(
        self,
        assertion: Assertion,
        maybe_adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> Optional[List[Metric]]:
        """
        Not possible to boostrap data for SQL assertions as this informtion
        is not contained within the dataset profiles.
        """
        return None

    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
        prefetched_metrics_data: List[Metric],
    ) -> List[Metric]:
        """
        Fetch metric data for SQL metric training.
        """
        metric_cube_urn = self.get_metric_cube_urn(monitor.urn)

        lookback_days = self.extract_lookback_days_from_adjustment_settings(
            adjustment_settings
        )
        training_window_duration = timedelta(days=lookback_days)
        min_window_duration = timedelta(
            seconds=self.get_min_training_samples_timespan_seconds() + 60 * 60
        )

        final_window_duration = (
            training_window_duration
            if training_window_duration > min_window_duration
            else min_window_duration
        )

        metrics = self.metrics_client.fetch_metric_values(
            metric_cube_urn,
            lookback=final_window_duration,
            limit=2000,
        )

        anomalies = self.monitor_client.fetch_monitor_anomalies(
            urn=monitor.urn,
            lookback=training_window_duration,
            limit=2000,
        )

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
            f"Insufficient samples to train SQL metric assertion {assertion.urn}. Resetting boundaries by updating context."
        )

        new_context = self.create_empty_context(0)

        self.monitor_client.patch_sql_metric_monitor_evaluation_context(
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
        Train and update a SQL metric assertion with new boundaries.
        """
        # Validate SQL assertion exists
        if not assertion.sql_assertion:
            raise RuntimeError(f"Missing SQL assertion for assertion {assertion.urn}")

        # 1) Determine teh sensitivty level
        sensitivity = self._get_sensitivity_level(adjustment_settings)

        # 2) Predict boundaries
        boundaries = self.metrics_predictor.predict_metric_boundaries(
            events,
            timedelta(hours=1),
            24,
            sensitivity,
            floor_value=None,  # SQL metrics can be negative
            ceiling_value=None,  # SQL metrics have no fixed ceiling
        )
        current_boundary = boundaries[0]
        future_boundaries = boundaries[1:]

        # 3) Build a new SQLAssertionInfoClass with updated boundaries
        assertion_info = self._get_assertion_info(assertion)

        # Convert change_type enum to string if present
        change_type_str = (
            assertion.sql_assertion.change_type.value
            if assertion.sql_assertion.change_type
            else None
        )

        # 4) Buil da new sql assertion with updated boundaries
        new_sql_assertion = self._build_sql_assertion_info(
            assertion.entity.urn,
            assertion.sql_assertion.statement,
            assertion.sql_assertion.type,
            current_boundary,
            change_type_str,
        )
        assertion_info.sqlAssertion = new_sql_assertion

        # 5) Update the monitor with embedded assertions for future boundaries
        self._update_sql_metric_monitor_evaluation_context(
            assertion.urn,
            monitor.urn,
            assertion.entity.urn,
            assertion.sql_assertion.statement,
            assertion.sql_assertion.type,
            future_boundaries,
            evaluation_spec,
            assertion_info.description,
            change_type_str,
        )

        # 6) Persist the updated assertion info
        self.monitor_client.update_assertion_info(assertion.urn, assertion_info)

        # 7) Retunr teh updated assertion
        updated_assertion = self._rebuild_assertion(assertion, assertion_info)

        logger.debug(
            f"Trained & updated SQL metric assertion {assertion.urn}, new boundaries: "
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
        return SQL_METRIC_DEFAULT_SENSITIVITY_LEVEL

    def _get_assertion_info(self, assertion: Assertion) -> AssertionInfoClass:
        """
        Get SQL assertion details from an assertion.
        """
        assertion_info = get_assertion_info(assertion.raw_info_aspect)
        if not assertion_info or not assertion_info.sqlAssertion:
            raise RuntimeError(
                f"Missing raw assertionInfo aspect or SQL assertion info for assertion {assertion.urn}"
            )
        return assertion_info

    def _build_sql_assertion_info(
        self,
        entity_urn: str,
        statement: str,
        assertion_type: SQLAssertionType,
        boundary: MetricBoundary,
        change_type: Optional[str],
    ) -> SqlAssertionInfoClass:
        """
        Build a SQL assertion info with updated boundaries.
        """
        return SqlAssertionInfoClass(
            type=SqlAssertionTypeClass.METRIC,
            entity=entity_urn,
            statement=statement,
            operator=AssertionStdOperatorClass.BETWEEN,
            parameters=build_std_parameters(boundary),
            changeType=change_type,
        )

    def _update_sql_metric_monitor_evaluation_context(
        self,
        assertion_urn: str,
        monitor_urn: str,
        entity_urn: str,
        statement: str,
        assertion_type: SQLAssertionType,
        boundaries: List[MetricBoundary],
        evaluation_spec: AssertionEvaluationSpec,
        assertion_description: Optional[str],
        change_type: Optional[str],
    ) -> None:
        """
        Update the monitor's embedded assertions with future boundary predictions.
        """
        embedded_assertions = []
        window_size = timedelta(hours=1)

        for boundary in boundaries:
            sql_assertion_info = self._build_sql_assertion_info(
                entity_urn, statement, assertion_type, boundary, change_type
            )

            assertion_info = AssertionInfoClass(
                type="SQL",
                sqlAssertion=sql_assertion_info,
                description=assertion_description,
                source=create_inference_source(),
            )

            embedded_assertions.append(
                create_embedded_assertion(
                    assertion_info, boundary, int(window_size.total_seconds())
                )
            )

        inference_details = self.create_inference_details(int(time.time() * 1000))
        new_context = AssertionEvaluationContextClass(
            embeddedAssertions=embedded_assertions, inferenceDetails=inference_details
        )

        logger.debug(
            f"Updating monitor embedded assertions for {monitor_urn} with {len(embedded_assertions)} boundaries."
        )
        try:
            self.monitor_client.patch_sql_metric_monitor_evaluation_context(
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

        Note: We must exclude 'entity' from assertion_info.to_obj() because:
        - AssertionInfoClass.entity is a URN string (from PDL)
        - Assertion.entity expects an AssertionEntity object
        - original_assertion.entity already has the correct type
        """
        info_dict = dict(assertion_info.to_obj())
        info_dict.pop("entity", None)  # Remove entity URN string

        return Assertion.parse_obj(
            dict(
                **info_dict,
                urn=original_assertion.urn,
                entity=original_assertion.entity,  # Use AssertionEntity object
                connectionUrn=original_assertion.connection_urn,
                raw_info_aspect=None,
            )
        )
