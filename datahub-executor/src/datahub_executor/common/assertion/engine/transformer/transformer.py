from abc import abstractmethod
from typing import Tuple

from datahub_executor.common.graph import DataHubAssertionGraph
from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
)


class AssertionTransformer:
    @abstractmethod
    def transform(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> Tuple[Assertion, AssertionEvaluationParameters, AssertionEvaluationContext]:
        """
        Transforms an assertion.
        :param assertion: the assertion to be transformed
        :param parameters: the parameters required to evaluate assertion
        :param context: the context required to evaluate (+ transform) assertion
        :return: Tuple of transformed (assertion, parameters, context)
        """

    @classmethod
    @abstractmethod
    def create(cls, graph: DataHubAssertionGraph) -> "AssertionTransformer":
        pass
