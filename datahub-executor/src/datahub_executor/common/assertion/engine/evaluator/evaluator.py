import logging
import traceback
from typing import Optional, Union

from datahub_executor.common.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_executor.common.connection.provider import ConnectionProvider
from datahub_executor.common.exceptions import (
    AssertionResultException,
    InvalidParametersException,
)
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.state.assertion_state_provider import (
    AssertionStateProvider,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionStdOperator,
    AssertionStdParameters,
    AssertionType,
    AssertionValueChangeType,
)

logger = logging.getLogger(__name__)


class AssertionEvaluator:
    """Base class for all assertion evaluators."""

    @property
    def type(self) -> AssertionType:
        raise NotImplementedError()

    @property
    def default_parameters(self) -> AssertionEvaluationParameters:
        raise NotImplementedError()

    def __init__(
        self,
        connection_provider: ConnectionProvider,
        state_provider: AssertionStateProvider,
        source_provider: SourceProvider,
    ):
        self.connection_provider = connection_provider
        self.state_provider = state_provider
        self.source_provider = source_provider

    def _compare_values(
        self,
        current_value: Union[int, float],
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
            return current_value >= min_value and current_value <= max_value
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
                return current_value > value
            if operator == AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO:
                return current_value >= value
            if operator == AssertionStdOperator.EQUAL_TO:
                return current_value == value
            if operator == AssertionStdOperator.NOT_EQUAL_TO:
                return current_value != value
            if operator == AssertionStdOperator.LESS_THAN:
                return current_value < value
            if operator == AssertionStdOperator.LESS_THAN_OR_EQUAL_TO:
                return current_value <= value

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

    def _evaluate_value(
        self,
        current_value: Union[int, float],
        operator: AssertionStdOperator,
        parameters: AssertionStdParameters,
    ) -> bool:
        value = float(parameters.value.value) if parameters.value else None
        min_value = float(parameters.min_value.value) if parameters.min_value else None
        max_value = float(parameters.max_value.value) if parameters.max_value else None

        return self._compare_values(
            current_value, operator, value, min_value, max_value
        )

    def _evaluate_value_change(
        self,
        change_type: AssertionValueChangeType,
        previous_value: Union[int, float],
        current_value: Union[int, float],
        operator: AssertionStdOperator,
        parameters: AssertionStdParameters,
    ) -> bool:
        value = float(parameters.value.value) if parameters.value else None
        max_value = float(parameters.max_value.value) if parameters.max_value else None
        min_value = float(parameters.min_value.value) if parameters.min_value else None

        if change_type == AssertionValueChangeType.ABSOLUTE:
            new_value = previous_value + value if value else None
            new_max_value = previous_value + max_value if max_value else None
            new_min_value = previous_value + min_value if min_value else None
        else:
            new_value = previous_value * (1.0 + value / 100.0) if value else None
            new_max_value = (
                previous_value * (1.0 + max_value / 100.0) if max_value else None
            )
            new_min_value = (
                previous_value * (1.0 + min_value / 100.0) if min_value else None
            )

        return self._compare_values(
            current_value, operator, new_value, new_min_value, new_max_value
        )

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        raise NotImplementedError()

    def evaluate(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        try:
            return self._evaluate_internal(
                assertion,
                parameters if parameters is not None else self.default_parameters,
                context,
            )
        except AssertionResultException as e:
            error = extract_assertion_evaluation_result_error(e)
            result = AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=error,
            )
            logger.exception(
                f"Caught error of type {error.type} when attempting to evaluate assertion with urn {assertion.urn} and properties {error.properties}. Caused by: {e}"
            )
            return result
        except Exception as e:
            logger.exception(
                f"An unknown error occurred when attempting to evaluate assertion with urn {assertion.urn} and parameters {parameters}. Caused by: {e}"
            )
            stack_trace_string = traceback.format_exc(limit=2)
            return AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=AssertionEvaluationResultError(
                    type=AssertionResultErrorType.UNKNOWN_ERROR,
                    properties={
                        "assertion_urn": assertion.urn,
                        "message": repr(e),
                        "stacktrace": stack_trace_string,
                    },
                ),
            )
