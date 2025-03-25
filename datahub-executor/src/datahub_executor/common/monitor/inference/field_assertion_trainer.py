import logging
import time
from datetime import timedelta
from typing import List, Optional, Union

from datahub.metadata.schema_classes import (
    AssertionEvaluationContextClass,
    AssertionInfoClass,
    AssertionStdOperatorClass,
    FieldAssertionInfoClass,
    FieldAssertionTypeClass,
    FieldMetricAssertionClass,
    FieldMetricTypeClass,
    SchemaFieldSpecClass,
)

from datahub_executor.common.aspect_builder import get_assertion_info
from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    is_field_metric_assertion,
)
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
)
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
)

logger = logging.getLogger(__name__)


class FieldAssertionTrainer(BaseAssertionTrainer[Metric]):
    """
    Trainer for field metric assertions.
    """

    def train(
        self,
        monitor: Monitor,
        assertion: Assertion,
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Train a field metric assertion.
        """
        # Check if this is a field metric assertion that requires inference
        if not is_field_metric_assertion(assertion):
            logger.info(
                f"Skipping training for non-field-metric assertion {assertion.urn}"
            )
            return

        logger.info(
            f"Performing training for field metric assertion {assertion.urn} under monitor {monitor.urn}"
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
                FIELD_METRIC_MIN_TRAINING_INTERVAL_SECONDS * 1000
            )
        # No previous inference, we should continue with inference to generate our first predictions.
        return True

    def get_min_training_samples(self) -> int:
        """
        Get the minimum number of samples required for training.
        """
        return FIELD_METRIC_MIN_TRAINING_SAMPLES

    def get_metric_data(
        self,
        monitor: Monitor,
        assertion: Assertion,
        adjustment_settings: Optional[AssertionAdjustmentSettings],
    ) -> List[Metric]:
        """
        Fetch metric data for field metric training.
        """
        # Construct the metric cube URN
        metric_cube_urn = self.get_metric_cube_urn(monitor.urn)

        # Calculate lookback period
        lookback_days = self.extract_lookback_days_from_adjustment_settings(
            adjustment_settings
        )
        training_window_duration = timedelta(days=lookback_days)

        # Fetch metrics
        return self.metrics_client.fetch_metric_values(
            metric_cube_urn,
            lookback=training_window_duration,
            limit=2000,
        )

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
            f"Insufficient samples to train field metric assertion {assertion.urn}. Resetting boundaries by updating context."
        )

        # Create empty context to reset the generated_at time to 0
        new_context = self.create_empty_context(0)

        # Update the assertion inference details
        self.monitor_client.patch_field_metric_monitor_evaluation_context(
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
        Train and update a field metric assertion with new boundaries.
        """
        # 1) Determine sensitivity level
        sensitivity = self._get_sensitivity_level(adjustment_settings)

        # 2) Predict boundaries
        boundaries = self.metrics_predictor.predict_metric_boundaries(
            events, timedelta(hours=1), 24, sensitivity
        )
        current_boundary = boundaries[0]  # First boundary is the current one
        future_boundaries = boundaries[1:]  # Rest are future predictions

        # 3) Get assertion info and extract field and metric information
        assertion_info, field, metric = self._get_field_assertion_details(assertion)

        # 4) Build new field metric assertion with updated boundaries
        new_field_metric_assertion = self._build_field_metric_assertion_info(
            assertion.entity.urn,
            field,
            metric,
            current_boundary,
        )
        assertion_info.fieldAssertion = new_field_metric_assertion

        # 5) Update the monitor with embedded assertions for future boundaries
        self._update_field_metric_monitor_evaluation_context(
            assertion.urn,
            monitor.urn,
            assertion.entity.urn,
            field,
            metric,
            future_boundaries,
            evaluation_spec,
        )

        # 6) Persist the updated assertion info
        self.monitor_client.update_assertion_info(assertion.urn, assertion_info)

        # 7) Return updated assertion
        updated_assertion = self._rebuild_assertion(assertion, assertion_info)

        logger.info(
            f"Trained & updated field metric assertion {assertion.urn}, new boundaries: "
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
        return FIELD_METRIC_DEFAULT_SENSITIVITY_LEVEL

    def _get_field_assertion_details(
        self, assertion: Assertion
    ) -> tuple[
        AssertionInfoClass, SchemaFieldSpecClass, Union[str, FieldMetricTypeClass]
    ]:
        """
        Get field assertion details from an assertion.
        """
        assertion_info = get_assertion_info(assertion.raw_info_aspect)
        if (
            not assertion_info
            or not assertion_info.fieldAssertion
            or not assertion_info.fieldAssertion.fieldMetricAssertion
        ):
            raise RuntimeError(
                f"Missing raw assertionInfo aspect or field assertion info for assertion {assertion.urn}"
            )

        field = assertion_info.fieldAssertion.fieldMetricAssertion.field
        metric = assertion_info.fieldAssertion.fieldMetricAssertion.metric

        return assertion_info, field, metric

    def _build_field_metric_assertion_info(
        self,
        entity_urn: str,
        field: SchemaFieldSpecClass,
        metric: Union[str, FieldMetricTypeClass],
        boundary: MetricBoundary,
    ) -> FieldAssertionInfoClass:
        """
        Build a field metric assertion info with updated boundaries.
        """
        return FieldAssertionInfoClass(
            type=FieldAssertionTypeClass.FIELD_METRIC,
            entity=entity_urn,
            fieldMetricAssertion=FieldMetricAssertionClass(
                field=field,
                metric=metric,
                operator=AssertionStdOperatorClass.BETWEEN,
                parameters=build_std_parameters(boundary),
            ),
        )

    def _update_field_metric_monitor_evaluation_context(
        self,
        assertion_urn: str,
        monitor_urn: str,
        entity_urn: str,
        field: SchemaFieldSpecClass,
        metric: Union[str, FieldMetricTypeClass],
        boundaries: List[MetricBoundary],
        evaluation_spec: AssertionEvaluationSpec,
    ) -> None:
        """
        Update the monitor's embedded assertions with future boundary predictions.
        """
        # 1) Create embedded assertions for each boundary
        embedded_assertions = []
        window_size = timedelta(hours=1)

        for boundary in boundaries:
            # Build field metric assertion info for this boundary
            field_metric_assertion_info = self._build_field_metric_assertion_info(
                entity_urn, field, metric, boundary
            )

            # Create assertion info
            assertion_info = AssertionInfoClass(
                type="FIELD",
                fieldAssertion=field_metric_assertion_info,
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
            self.monitor_client.patch_field_metric_monitor_evaluation_context(
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
