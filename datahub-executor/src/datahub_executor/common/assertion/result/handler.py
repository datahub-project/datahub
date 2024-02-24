from abc import ABC, abstractmethod

from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
)


class AssertionResultHandler(ABC):
    """Base class for all assertion result handlers."""

    @abstractmethod
    def handle(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        result: AssertionEvaluationResult,
        context: AssertionEvaluationContext,
    ) -> None:
        raise NotImplementedError()
