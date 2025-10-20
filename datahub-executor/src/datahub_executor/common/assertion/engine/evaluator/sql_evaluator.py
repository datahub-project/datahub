import logging
import time
from typing import cast

from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.utils import (
    get_database_parameters,
)
from datahub_executor.common.assertion.engine.evaluator.utils.shared import (
    is_training_required,
    make_monitor_metric_cube_urn,
)
from datahub_executor.common.assertion.types import AssertionState, AssertionStateType
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.exceptions import (
    InvalidParametersException,
    SourceConnectionErrorException,
)
from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.monitor.client.client import MonitorClient
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
    SQLAssertion,
    SQLAssertionType,
)

logger = logging.getLogger(__name__)


class SQLAssertionEvaluator(AssertionEvaluator):
    """Evaluator for SQL assertions."""

    metric_client: MetricClient

    def __init__(
        self,
        connection_provider: DataHubIngestionSourceConnectionProvider,
        state_provider: AssertionStateProvider,
        source_provider: SourceProvider,
        monitor_client: MonitorClient,
        metric_client: MetricClient,
    ) -> None:
        super().__init__(
            connection_provider, state_provider, source_provider, monitor_client
        )
        self.metric_client = metric_client

    @property
    def type(self) -> AssertionType:
        return AssertionType.SQL

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_SQL
        )

    def _evaluate_metric_assertion(
        self,
        sql_assertion: SQLAssertion,
        metric_value: float,
        assertion: Assertion,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        # If smart assertions are enabled and this assertion is in training (no conditions inferred),
        # short-circuit with INIT to mirror field/freshness behavior.
        if context.online_smart_assertions and is_training_required(assertion):
            if context.evaluation_spec:
                last_inferred_at = (
                    context.evaluation_spec.context.inference_details.generated_at
                    if context.evaluation_spec.context
                    and context.evaluation_spec.context.inference_details
                    else None
                )
                if not last_inferred_at or last_inferred_at <= 0:
                    return AssertionEvaluationResult(
                        AssertionResultType.INIT,
                        parameters={"metric_value": str(metric_value)},
                    )

        metric_evaluation = self._evaluate_value(
            metric_value, sql_assertion.operator, sql_assertion.parameters
        )
        params = {
            "metric_value": str(metric_value),
            "value": (
                str(sql_assertion.parameters.value.value)
                if sql_assertion.parameters.value
                else None
            ),
            "min_value": (
                str(sql_assertion.parameters.min_value.value)
                if sql_assertion.parameters.min_value
                else None
            ),
            "max_value": (
                str(sql_assertion.parameters.max_value.value)
                if sql_assertion.parameters.max_value
                else None
            ),
        }
        return AssertionEvaluationResult(
            AssertionResultType.SUCCESS
            if metric_evaluation
            else AssertionResultType.FAILURE,
            parameters=params,
        )

    def _evaluate_metric_change_assertion_with_previous_state(
        self,
        sql_assertion: SQLAssertion,
        previous_state: AssertionState,
        metric_value: float,
    ) -> AssertionEvaluationResult:
        assert sql_assertion.change_type is not None

        prev_metric_value = previous_state.properties.get("metric_value", None)
        if prev_metric_value is None:
            return AssertionEvaluationResult(
                AssertionResultType.INIT,
                parameters={"metric_value": str(metric_value)},
            )

        metric_evaluation = self._evaluate_value_change(
            sql_assertion.change_type,
            float(prev_metric_value),
            metric_value,
            sql_assertion.operator,
            sql_assertion.parameters,
        )
        if metric_evaluation is True:
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS,
                parameters={
                    "metric_value": str(metric_value),
                    "prev_metric_value": prev_metric_value,
                },
            )

        return AssertionEvaluationResult(
            AssertionResultType.FAILURE,
            parameters={
                "metric_value": str(metric_value),
                "prev_metric_value": prev_metric_value,
            },
        )

    def _evaluate_metric_change_assertion(
        self,
        entity_urn: str,
        sql_assertion: SQLAssertion,
        metric_value: float,
        context: AssertionEvaluationContext,
        assertion: Assertion,
    ) -> AssertionEvaluationResult:
        # If smart assertions are enabled and this assertion is in training (no conditions inferred),
        # short-circuit with INIT to mirror field/freshness behavior.
        if context.online_smart_assertions and is_training_required(assertion):
            if context.evaluation_spec:
                last_inferred_at = (
                    context.evaluation_spec.context.inference_details.generated_at
                    if context.evaluation_spec.context
                    and context.evaluation_spec.context.inference_details
                    else None
                )
                if not last_inferred_at or last_inferred_at <= 0:
                    return AssertionEvaluationResult(
                        AssertionResultType.INIT,
                        parameters={"metric_value": str(metric_value)},
                    )
        previous_state = (
            self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            if context.monitor_urn
            else None
        )

        if previous_state:
            assertion_evaluation_result = (
                self._evaluate_metric_change_assertion_with_previous_state(
                    sql_assertion,
                    previous_state,
                    metric_value,
                )
            )
        else:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT,
                parameters={"metric_value": str(metric_value)},
            )

        if context.monitor_urn:
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={
                        "metric_value": str(metric_value),
                    },
                ),
            )

        return assertion_evaluation_result

    def _evaluate_metric_collection_step(
        self,
        assertion: Assertion,
        context: AssertionEvaluationContext,
        save: bool,
    ) -> Metric:
        """
        Collects the metric value by executing the custom SQL and optionally saves it to the
        metric cube. Returns a Metric object with timestamp and value.
        """
        assert assertion.sql_assertion is not None
        assert assertion.connection_urn

        connection = self.connection_provider.get_connection(
            cast(str, assertion.entity.urn)
        )

        if connection is None:
            raise SourceConnectionErrorException(
                message=(
                    f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}"
                ),
                connection_urn=assertion.connection_urn,
            )

        entity_urn = assertion.entity.urn
        sql_assertion = assertion.sql_assertion

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        metric_value = source.execute_custom_sql(
            entity_urn,
            database_params,
            sql_assertion.statement,
        )

        metric = Metric(timestamp_ms=int(time.time() * 1000), value=metric_value)

        if save and context.monitor_urn:
            metric_cube_urn = make_monitor_metric_cube_urn(context.monitor_urn)
            self.metric_client.save_metric_value(metric_cube_urn, metric)

        return metric

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.sql_assertion is not None
        assert assertion.connection_urn

        sql_assertion = assertion.sql_assertion
        entity_urn = assertion.entity.urn

        # Determine if this is a smart assertion (AI-inferred), requiring training.
        is_training_required_for_assertion = is_training_required(assertion=assertion)

        # Stage 1: Collect metric & optionally save for training
        metric = self._evaluate_metric_collection_step(
            assertion=assertion,
            context=context,
            save=is_training_required_for_assertion,
        )

        if sql_assertion.type == SQLAssertionType.METRIC:
            result = self._evaluate_metric_assertion(
                sql_assertion,
                float(metric.value),
                assertion,
                context,
            )
        elif sql_assertion.type == SQLAssertionType.METRIC_CHANGE:
            result = self._evaluate_metric_change_assertion(
                entity_urn,
                sql_assertion,
                float(metric.value),
                context,
                assertion,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate SQL Assertion. Unsupported SQL Assertion Type {assertion.sql_assertion.type} provided.",
                parameters=sql_assertion.model_dump(),
            )

        # Only populate the metric on the result for smart assertions.
        if assertion.is_inferred:
            result.metric = metric
        return result
