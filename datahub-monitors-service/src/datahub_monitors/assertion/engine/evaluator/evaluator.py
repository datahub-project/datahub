import logging
from typing import Optional, cast

from datahub_monitors.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_monitors.connection.connection import Connection
from datahub_monitors.connection.provider import ConnectionProvider
from datahub_monitors.exceptions import (
    AssertionResultException,
    SourceConnectionErrorException,
)
from datahub_monitors.source.provider import SourceProvider
from datahub_monitors.state.assertion_state_provider import AssertionStateProvider
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultErrorType,
    AssertionResultType,
    AssertionType,
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

    def _evaluate_internal(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        connection: Connection,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        raise NotImplementedError()

    def evaluate(
        self,
        assertion: Assertion,
        parameters: Optional[AssertionEvaluationParameters],
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        try:
            assert assertion.connection_urn
            connection = self.connection_provider.get_connection(
                cast(str, assertion.connection_urn)
            )

            if connection is None:
                raise SourceConnectionErrorException(
                    message=f"Unable to retrieve valid connection for Data Platform with urn {assertion.connection_urn}",
                    connection_urn=assertion.connection_urn,
                )

            return self._evaluate_internal(
                assertion,
                parameters if parameters is not None else self.default_parameters,
                connection,
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
            return AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=AssertionEvaluationResultError(
                    type=AssertionResultErrorType.UNKNOWN_ERROR,
                    properties={"assertion_urn": assertion.urn},
                ),
            )
