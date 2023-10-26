import logging
import time

from datahub_monitors.assertion.engine.evaluator.evaluator import AssertionEvaluator
from datahub_monitors.assertion.engine.evaluator.utils import get_database_parameters
from datahub_monitors.assertion.types import (
    AssertionDatabaseParams,
    AssertionState,
    AssertionStateType,
)
from datahub_monitors.connection.connection import Connection
from datahub_monitors.exceptions import InvalidParametersException
from datahub_monitors.source.source import Source
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    DatasetFieldAssertionParameters,
    DatasetFieldSourceType,
    EntityEventType,
    FieldAssertion,
    FieldAssertionType,
    FieldValuesFailThresholdType,
)

logger = logging.getLogger(__name__)


class FieldAssertionEvaluator(AssertionEvaluator):
    """Evaluator for FIELD assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.FIELD

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_FIELD,
            dataset_field_parameters=DatasetFieldAssertionParameters(
                sourceType=DatasetFieldSourceType.ALL_ROWS_QUERY
            ),
        )

    def _evaluate_field_values_assertion(
        self,
        entity_urn: str,
        field_assertion: FieldAssertion,
        dataset_field_parameters: DatasetFieldAssertionParameters,
        source: Source,
        database_params: AssertionDatabaseParams,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert field_assertion.field_values_assertion is not None
        # TODO - remove this later when we support PERCENTAGE
        assert (
            field_assertion.field_values_assertion.fail_threshold.type
            == FieldValuesFailThresholdType.COUNT
        )

        previous_state = None
        prev_high_watermark_value = None

        if (
            dataset_field_parameters.source_type
            == DatasetFieldSourceType.CHANGED_ROWS_QUERY
            and context.monitor_urn
        ):
            assert dataset_field_parameters.changed_rows_field is not None

            # we get the prev state, if available
            previous_state = self.state_provider.get_state(
                context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
            )
            # grab the previous high watermark value if we stored one.
            if previous_state:
                prev_high_watermark_value = previous_state.properties.get(
                    "high_watermark_value", None
                )

            # get the current high watermark value for this field
            high_watermark_value, _ = source.get_current_high_watermark_for_column(
                entity_urn,
                EntityEventType.FIELD_UPDATE,
                [0, 0],
                {
                    "path": dataset_field_parameters.changed_rows_field.path,
                    "type": dataset_field_parameters.changed_rows_field.type,
                    "native_type": dataset_field_parameters.changed_rows_field.native_type,
                    "filter": field_assertion.filter.__dict__
                    if field_assertion.filter
                    else None,
                    "database": database_params,
                },
                prev_high_watermark_value,
            )

            # save state for this assertion/monitor, including the current high watermark value
            # for the next assertion run.
            self.state_provider.save_state(
                context.monitor_urn,
                AssertionState(
                    type=AssertionStateType.MONITOR_TIMESERIES_STATE,
                    timestamp=int(time.time() * 1000),
                    properties={"high_watermark_value": str(high_watermark_value)},
                ),
            )

        values_count = source.get_field_values_count(
            entity_urn,
            database_params,
            field_assertion.field_values_assertion.field,
            field_assertion.field_values_assertion.operator,
            field_assertion.field_values_assertion.parameters,
            field_assertion.field_values_assertion.exclude_nulls,
            field_assertion.field_values_assertion.transform,
            field_assertion.filter,
            prev_high_watermark_value,
            dataset_field_parameters.changed_rows_field,
        )

        fail_threshold_value = (
            field_assertion.field_values_assertion.fail_threshold.value
        )

        if (
            dataset_field_parameters.source_type
            == DatasetFieldSourceType.CHANGED_ROWS_QUERY
            and previous_state is None
        ):
            return AssertionEvaluationResult(
                AssertionResultType.INIT, parameters={"values_count": str(values_count)}
            )

        return AssertionEvaluationResult(
            AssertionResultType.FAILURE
            if values_count > fail_threshold_value
            else AssertionResultType.SUCCESS,
            parameters={
                "threshold_value": str(fail_threshold_value),
                "values_count": str(values_count),
            },
        )

    def _evaluate_field_metric_assertion(
        self,
        entity_urn: str,
        field_assertion: FieldAssertion,
        dataset_field_parameters: DatasetFieldAssertionParameters,
        source: Source,
        database_params: AssertionDatabaseParams,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        raise NotImplementedError()

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.field_assertion is not None
        assert parameters.dataset_field_parameters is not None

        entity_urn = assertion.entity.urn
        field_assertion = assertion.field_assertion
        dataset_field_parameters = parameters.dataset_field_parameters

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        if field_assertion.type == FieldAssertionType.FIELD_VALUES:
            return self._evaluate_field_values_assertion(
                entity_urn,
                field_assertion,
                dataset_field_parameters,
                source,
                database_params,
                context,
            )
        elif field_assertion.type == FieldAssertionType.FIELD_METRIC:
            return self._evaluate_field_metric_assertion(
                entity_urn,
                field_assertion,
                dataset_field_parameters,
                source,
                database_params,
                context,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate FIELD Assertion. Unsupported FIELD Assertion Source Type {field_assertion.type} provided.",
                parameters=dataset_field_parameters.__dict__,
            )
