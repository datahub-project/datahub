import logging
import time
from typing import Optional, Union

from datahub_monitors.assertion.engine.evaluator.evaluator import AssertionEvaluator
from datahub_monitors.assertion.engine.evaluator.utils import get_database_parameters
from datahub_monitors.assertion.engine.evaluator.utils.volume import (
    get_filter_parameters,
)
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
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
    DatasetVolumeAssertionParameters,
    DatasetVolumeSourceType,
    VolumeAssertion,
    VolumeAssertionType,
)

logger = logging.getLogger(__name__)


class VolumeAssertionEvaluator(AssertionEvaluator):
    """Evaluator for VOLUME assertions."""

    @property
    def type(self) -> AssertionType:
        return AssertionType.VOLUME

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        return AssertionEvaluationParameters(
            type=AssertionEvaluationParametersType.DATASET_VOLUME,
            dataset_volume_parameters=DatasetVolumeAssertionParameters(
                sourceType=DatasetVolumeSourceType.INFORMATION_SCHEMA
            ),
        )

    def _compare_values(
        self,
        current_row_count: Union[int, float],
        operator: AssertionStdOperator,
        value: Optional[Union[int, float]],
        min_value: Optional[Union[int, float]],
        max_value: Optional[Union[int, float]],
    ) -> bool:
        if operator == AssertionStdOperator.BETWEEN:
            if max_value is None or min_value is None:
                raise InvalidParametersException(
                    message="Failed to evaluate VOLUME Assertion (BETWEEN) both minValue and maxValue are required.",
                    parameters={
                        "operator": operator,
                        "parameters": {
                            "value": value,
                            "min_value": min_value,
                            "max_value": max_value,
                        },
                    },
                )
            return current_row_count >= min_value and current_row_count <= max_value
        else:
            if value is None:
                raise InvalidParametersException(
                    message=f"Failed to evaluate VOLUME Assertion. ({operator.name}) and no value supplied",
                    parameters={
                        "operator": operator,
                        "parameters": {
                            "value": value,
                            "min_value": min_value,
                            "max_value": max_value,
                        },
                    },
                )

            if operator == AssertionStdOperator.GREATER_THAN:
                return current_row_count > value
            if operator == AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO:
                return current_row_count >= value
            if operator == AssertionStdOperator.EQUAL_TO:
                return current_row_count == value
            if operator == AssertionStdOperator.LESS_THAN:
                return current_row_count < value
            if operator == AssertionStdOperator.LESS_THAN_OR_EQUAL_TO:
                return current_row_count <= value

        raise InvalidParametersException(
            message=f"Failed to evaluate VOLUME Assertion. Unsupported AssertionStdOperator {operator} provided.",
            parameters={
                "operator": operator,
                "parameters": {
                    "value": value,
                    "min_value": min_value,
                    "max_value": max_value,
                },
            },
        )

    def _evaluate_row_count_total(
        self,
        current_row_count: int,
        operator: AssertionStdOperator,
        parameters: AssertionStdParameters,
    ) -> bool:
        value = int(parameters.value.value) if parameters.value else None
        min_value = int(parameters.min_value.value) if parameters.min_value else None
        max_value = int(parameters.max_value.value) if parameters.max_value else None

        return self._compare_values(
            current_row_count, operator, value, min_value, max_value
        )

    def _evaluate_row_count_change(
        self,
        change_type: AssertionValueChangeType,
        previous_row_count: int,
        current_row_count: int,
        operator: AssertionStdOperator,
        parameters: AssertionStdParameters,
    ) -> bool:
        value = float(parameters.value.value) if parameters.value else None
        max_value = float(parameters.max_value.value) if parameters.max_value else None
        min_value = float(parameters.min_value.value) if parameters.min_value else None

        if change_type == AssertionValueChangeType.ABSOLUTE:
            new_value = previous_row_count + value if value else None
            new_max_value = previous_row_count + max_value if max_value else None
            new_min_value = previous_row_count + min_value if min_value else None
        else:
            new_value = previous_row_count * (1.0 + value / 100.0) if value else None
            new_max_value = (
                previous_row_count * (1.0 + max_value / 100.0) if max_value else None
            )
            new_min_value = (
                previous_row_count * (1.0 + min_value / 100.0) if min_value else None
            )

        return self._compare_values(
            current_row_count, operator, new_value, new_min_value, new_max_value
        )

    def _evaluate_row_count_total_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        source: Source,
        database_params: AssertionDatabaseParams,
        volume_parameters: DatasetVolumeAssertionParameters,
        filter_params: Optional[dict],
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_total is not None
        row_count = source.get_row_count(
            entity_urn, database_params, volume_parameters, filter_params
        )
        row_count_evaluation = self._evaluate_row_count_total(
            row_count,
            volume_assertion.row_count_total.operator,
            volume_assertion.row_count_total.parameters,
        )
        if row_count_evaluation is True:
            return AssertionEvaluationResult(
                AssertionResultType.SUCCESS, parameters={"row_count": row_count}
            )
        else:
            return AssertionEvaluationResult(
                AssertionResultType.FAILURE, parameters={"row_count": row_count}
            )

    def _evaluate_row_count_change_assertion(
        self,
        entity_urn: str,
        volume_assertion: VolumeAssertion,
        source: Source,
        database_params: AssertionDatabaseParams,
        volume_parameters: DatasetVolumeAssertionParameters,
        filter_params: Optional[dict],
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert volume_assertion.row_count_change is not None

        if not context.monitor_urn:
            raise InvalidParametersException(
                message=f"_evaluate_row_count_change_assertion for {entity_urn} requires a monitor_urn",
                parameters={"database_params": database_params, "context": context},
            )

        previous_state = self.state_provider.get_state(
            context.monitor_urn, AssertionStateType.MONITOR_TIMESERIES_STATE
        )
        row_count = source.get_row_count(
            entity_urn, database_params, volume_parameters, filter_params
        )

        if previous_state:
            prev_row_count = previous_state.properties.get("row_count", None)
            if prev_row_count is None:
                assertion_evaluation_result = AssertionEvaluationResult(
                    AssertionResultType.INIT, parameters=None
                )
            else:
                row_count_evaluation = self._evaluate_row_count_change(
                    volume_assertion.row_count_change.type,
                    int(prev_row_count),
                    row_count,
                    volume_assertion.row_count_change.operator,
                    volume_assertion.row_count_change.parameters,
                )
                if row_count_evaluation is True:
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.SUCCESS,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
                else:
                    assertion_evaluation_result = AssertionEvaluationResult(
                        AssertionResultType.FAILURE,
                        parameters={
                            "row_count": row_count,
                            "prev_row_count": prev_row_count,
                        },
                    )
        else:
            assertion_evaluation_result = AssertionEvaluationResult(
                AssertionResultType.INIT, parameters=None
            )

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

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        assert assertion.volume_assertion is not None
        assert parameters.dataset_volume_parameters is not None

        entity_urn = assertion.entity.urn
        volume_assertion = assertion.volume_assertion
        dataset_volume_parameters = parameters.dataset_volume_parameters
        filter_params = get_filter_parameters(assertion)

        source = self.source_provider.create_source_from_connection(connection)
        database_params = get_database_parameters(assertion)

        if volume_assertion.type == VolumeAssertionType.ROW_COUNT_TOTAL:
            return self._evaluate_row_count_total_assertion(
                entity_urn,
                volume_assertion,
                source,
                database_params,
                dataset_volume_parameters,
                filter_params,
            )
        elif volume_assertion.type == VolumeAssertionType.ROW_COUNT_CHANGE:
            return self._evaluate_row_count_change_assertion(
                entity_urn,
                volume_assertion,
                source,
                database_params,
                dataset_volume_parameters,
                filter_params,
                context,
            )
        else:
            raise InvalidParametersException(
                message=f"Failed to evaluate VOLUME Assertion. Unsupported VOLUME Assertion Type {assertion.volume_assertion.type} provided.",
                parameters=volume_assertion.__dict__,
            )
