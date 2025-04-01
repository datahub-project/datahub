import logging
from typing import Optional

import fastapi

from .assertion_handlers import (
    handle_evaluate_assertion,
    handle_evaluate_assertion_urn,
    handle_evaluate_assertion_urns,
    handle_train_assertion_monitor,
)
from .types import (
    AssertionResultSchema,
    AssertionsResultSchema,
    EvaluateAssertionInputSchema,
    EvaluateAssertionUrnInputSchema,
    EvaluateAssertionUrnsInputSchema,
    TrainAssertionMonitorInputSchema,
    TrainAssertionMonitorResultSchema,
)

logger = logging.getLogger(__name__)

assertions_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@assertions_router.post("/evaluate_assertion")
def evaluate_assertion(
    assertion_input: EvaluateAssertionInputSchema,
) -> Optional[AssertionResultSchema]:
    global graph, engine

    return handle_evaluate_assertion(assertion_input)


@assertions_router.post("/evaluate_assertion_urns")
def evaluate_assertion_urns(
    input: EvaluateAssertionUrnsInputSchema,
) -> Optional[AssertionsResultSchema]:
    return handle_evaluate_assertion_urns(input)


@assertions_router.post("/evaluate_assertion_urn")
def evaluate_assertion_urn(
    assertion_urn_input: EvaluateAssertionUrnInputSchema,
) -> AssertionResultSchema:
    return handle_evaluate_assertion_urn(assertion_urn_input, False)


@assertions_router.post("/train_assertion_monitor")
def evaluate_train_assertion_monitor(
    train_assertion_monitor_input: TrainAssertionMonitorInputSchema,
) -> TrainAssertionMonitorResultSchema:
    return handle_train_assertion_monitor(train_assertion_monitor_input)


if __name__ == "__main__":
    # For development only
    pass
