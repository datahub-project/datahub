from typing import Dict, Optional, Sequence

from datahub_monitors.assertion.engine.evaluator.evaluator import AssertionEvaluator
from datahub_monitors.assertion.result.handler import AssertionResultHandler
from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationResult,
)


class AssertionEngine:
    """Singleton class used to evaluate assertions."""

    evaluators: Dict[str, AssertionEvaluator]
    result_handlers: Sequence[AssertionResultHandler]

    def __init__(
        self,
        evaluators: Sequence[AssertionEvaluator],
        result_handlers: Optional[Sequence[AssertionResultHandler]] = None,
    ):
        """Create a new instance if one doesn't exist, otherwise return the existing instance."""
        self.evaluators = {}
        for evaluator in evaluators:
            self.evaluators[evaluator.type.name] = evaluator
        self.result_handlers = result_handlers if result_handlers is not None else []

    def evaluate(
        self,
        assertion: Assertion,
        parameters: AssertionEvaluationParameters,
        context: AssertionEvaluationContext,
    ) -> AssertionEvaluationResult:
        """
        Evaluate an assertion with the given context.

        :param assertion: The assertion to be evaluated.
        :param parameters: The parameters required to evaluate the assertion.
        :param context: The context for the evaluation.
        """
        evaluator = self.evaluators.get(assertion.type.name)
        if evaluator is None:
            raise ValueError(f"No evaluator found for assertion type {assertion.type}")

        result = evaluator.evaluate(assertion, parameters, context)

        if not context.dry_run:
            # Execute the result handlers.
            for result_handler in self.result_handlers:
                result_handler.handle(assertion, parameters, result, context)

        return result
