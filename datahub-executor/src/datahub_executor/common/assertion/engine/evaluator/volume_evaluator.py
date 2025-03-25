import logging
import time

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils import (
    get_database_parameters,
)
from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    encode_monitor_urn,
    is_training_required,
)
from datahub_executor.common.assertion.engine.evaluator.utils.volume import (
    convert_volume_parameters_to_metric_resolver_strategy,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InvalidParametersException,
)
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.resolver.resolver import (
    MetricResolver,
)
from datahub_executor.common.metric.types import (
    Metric,
)
from datahub_executor.common.monitor.client.client import (
    MonitorClient,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.state.assertion_state_provider import (
    AssertionStateProvider,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    VolumeAssertion,
    VolumeAssertionType,
)
from datahub_executor.config import (
    ONLINE_SMART_ASSERTIONS_ENABLED,
)

logger = logging.getLogger(__name__)


#
# VolumeAssertionEvaluator Implementation
#
class VolumeAssertionEvaluator(AssertionEvaluator):
    """Evaluator for VOLUME assertions."""

    metric_resolver: MetricResolver
    metric_client: MetricClient

    def __init__(
        self,
        connection_provider: DataHubIngestionSourceConnectionProvider,
        state_provider: AssertionStateProvider,
        source_provider: SourceProvider,
        metric_resolver: MetricResolver,
        metric_client: MetricClient,
        monitor_client: MonitorClient,
    ) -> None:
        super().__init__(
            connection_provider, state_provider, source_provider, monitor_client
        )
        self.metric_resolver = metric_resolver
        self.metric_client = metric_client

    @property
    def type(self) -> AssertionType:
        return AssertionType.VOLUME

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                source_type=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
        )

    def _evaluate_row_count_total_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        row_count: int,
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_total is not None
        logger.debug("Evaluating ROW_COUNT_TOTAL assertion on row_count=%d", row_count)
        row_count_evaluation = self._evaluate_value(
            row_count,
            volume_assertion.row_count_total.operator,
            volume_assertion.row_count_total.parameters,
        )
        if row_count_evaluation is True:
            logger.debug("ROW_COUNT_TOTAL assertion succeeded.")
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters={"row_count": row_count}
            )
        else:
            logger.debug("ROW_COUNT_TOTAL assertion failed.")
            return AssertionEvaluationResult(
                AssertionResultType.FAILURE, parameters={"row_count": row_count}
            )

    def _evaluate_row_count_change_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        row_count: int,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_change is not None
        logger.debug("Evaluating ROW_COUNT_CHANGE assertion on row_count=%d", row_count)

        previous_state = (
            self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            if context.monitor_urn
            else None
        )

        if previous_state:
            prev_row_count = previous_state.properties.get("row_count", None)
            if prev_row_count is None:
                logger.debug("No previous row_count found; assertion state is INIT.")
                assertion_evaluation_result = AssertionEvaluationResult(
                    AssertionResultType.INIT, parameters={"row_count": row_count}
                )
            else:
                row_count_evaluation = self._evaluate_value_change(
                    volume_assertion.row_count_change.type,
                    int(prev_row_count),
                    row_count,
                    volume_assertion.row_count_change.operator,
                    volume_assertion.row_count_change.parameters,
                )
                if row_count_evaluation is True:
                    logger.debug(
                        "ROW_COUNT_CHANGE assertion succeeded. prev_row_count=%s",
                        prev_row_count,
                    )
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.SUCCESS,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
                else:
                    logger.warning(
                        "ROW_COUNT_CHANGE assertion failed. prev_row_count=%s",
                        prev_row_count,
                    )
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.FAILURE,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
        else:
            logger.debug("No previous assertion state found; assertion state is INIT.")
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"row_count": row_count}
            )

        if context.monitor_urn:
            logger.debug("Saving new assertion state with row_count=%d", row_count)
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={
                        "row_count": str(row_count),
                    },
                ),
            )

        return assertion_evaluation_result

    def _evaluate_metric_collection_step(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
        save: bool,
    ) -> Metric:
        """
        This method should use the MetricsResolver to fetch a row count using the parameters inside
        the evaluation parameters. It optionally saves the metric to DataHub using the MetricsClient.
        """
        logger.debug(
            "Starting Metric Collection Step for assertion %s",
            assertion.urn or "unknown",
        )

        entity_urn = assertion.entity.urn
        database_params = get_database_parameters(assertion)
        filter_params = (
            assertion.volume_assertion.filter if assertion.volume_assertion else None
        )

        # Step 1: Fetch metric via MetricResolver
        # Need some way to fetch dataset profile from DataHub.
        metric = self.metric_resolver.get_metric(
            entity_urn,
            "row_count",
            database_params,
            filter_params,
            convert_volume_parameters_to_metric_resolver_strategy(parameters),
        )

        # Step 2: Optionally save the fetched metric to the metric cube.
        if save and context.monitor_urn:
            metric_cube_urn = (
                f"urn:li:dataHubMetricCube:{encode_monitor_urn(context.monitor_urn)}"
            )
            self.metric_client.save_metric_value(metric_cube_urn, metric)

        # Step 3: Return the metric
        logger.debug("Completed metric collection step with metric=%s", metric)
        return metric

    def _evaluate_assertion_step(
        self,
        row_count: int,
        assertion: Assertion,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        """
        Evaluate the assertion using the final parameters (possibly updated in the inference step).
        """
        logger.debug(
            "Starting Final Assertion Evaluation Step, row_count=%d", row_count
        )

        entity_urn = assertion.entity.urn
        volume_assertion = assertion.volume_assertion

        if ONLINE_SMART_ASSERTIONS_ENABLED and is_training_required(assertion):
            # If we have smart assertions v2, there might not be an assertion present yet.
            if context.evaluation_spec:
                last_inferred_at = (
                    context.evaluation_spec.context.inference_details.generated_at
                    if context.evaluation_spec.context
                    and context.evaluation_spec.context.inference_details
                    else None
                )

                if not last_inferred_at or last_inferred_at <= 0:
                    return AssertionEvaluationResult(
                        AssertionResultType.INIT, parameters={"row_count": row_count}
                    )

        if not volume_assertion:
            raise Exception(
                "Volume assertion is required to evaluate! Missing volume assertion info."
            )

        if volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL:
            return self._evaluate_row_count_total_assertion(
                entity_urn,
                volume_assertion,
                row_count,
            )
        elif volume_assertion.type == VolumeAssertionType.ROW_COUNT_CHANGE:
            return self._evaluate_row_count_change_assertion(
                entity_urn,
                volume_assertion,
                row_count,
                context,
            )
        else:
            logger.error(
                "Unsupported VOLUME Assertion Type %s provided.", volume_assertion.type
            )
            raise InvalidParametersException(
                message=f"Failed to evaluate VOLUME Assertion. Unsupported VOLUME Assertion Type {volume_assertion.type} provided.",
                parameters=volume_assertion.__dict__,
            )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        logger.debug(
            "Beginning Volume Assertion evaluation for %s", assertion.urn or "unknown"
        )

        # Determine if this is a smart assertion, requiring stored sampling.
        is_training_required_for_assertion = is_training_required(assertion=assertion)

        logger.debug("About to run metric collection step...")

        # Stage 1: Collect metrics & save to DataHub
        row_count_metric = self._evaluate_metric_collection_step(
            assertion=assertion,
            parameters=parameters,
            context=context,
            save=is_training_required_for_assertion,
        )

        logger.debug("About to evaluate assertion...")

        # Stage 2: Perform final evaluation
        return self._evaluate_assertion_step(
            row_count=int(row_count_metric.value),
            assertion=assertion,
            context=context,
        )
