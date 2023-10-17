import fastapi

from datahub_monitors.app.schemas import (
    AssertionEvaluationParametersSchema,
    AssertionResultSchema,
    EvaluateAssertionInputSchema,
    EvaluateAssertionUrnInputSchema,
)
from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.common.aspect_builder import build_assertion_run_event
from datahub_monitors.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionSourceType,
)


def _evaluate_assertion(
    engine: AssertionEngine,
    assertion: Assertion,
    parameters: AssertionEvaluationParametersSchema,
    dry_run: bool,
) -> AssertionResultSchema:
    assertion_evaluation_parameters = parameters.to_internal_params()

    # call the engine evaluate method to run the actual assertion
    result = engine.evaluate(
        assertion,
        assertion_evaluation_parameters,
        AssertionEvaluationContext(dry_run=dry_run),
    )

    # convert assertion and evaluation result to run event
    run_event = build_assertion_run_event(assertion, result)

    # convert run_event to a pydantic schema for API return data
    return AssertionResultSchema.from_orm(run_event.result)


def handle_post_evaluate_assertion(
    assertion_input: EvaluateAssertionInputSchema,
    engine: AssertionEngine,
) -> AssertionResultSchema:
    # Setup the test assertion
    entity = AssertionEntity(
        urn=assertion_input.entityUrn,
        platformUrn=assertion_input.connectionUrn,  # this makes the assumption that connectionUrn coming from the front-end is the same as the platformUrn
        platformInstance=None,
        subTypes=None,
    )
    assertion = Assertion(
        urn="urn:li:assertion:test",
        connectionUrn=assertion_input.connectionUrn,
        type=assertion_input.type,
        entity=entity,
        freshnessAssertion=assertion_input.assertion.freshness_assertion,
        volumeAssertion=assertion_input.assertion.volume_assertion,
        sqlAssertion=assertion_input.assertion.sql_assertion,
    )

    return _evaluate_assertion(
        engine=engine,
        assertion=assertion,
        parameters=assertion_input.parameters,
        dry_run=assertion_input.dryRun,
    )


def handle_post_evaluate_assertion_urn(
    assertion: Assertion,
    assertion_urn_input: EvaluateAssertionUrnInputSchema,
    engine: AssertionEngine,
) -> AssertionResultSchema:
    if not assertion.source_type == AssertionSourceType.NATIVE:
        raise fastapi.HTTPException(
            status_code=422, detail="Can only evaluate native defined assertions"
        )

    return _evaluate_assertion(
        engine=engine,
        assertion=assertion,
        parameters=assertion_urn_input.parameters,
        dry_run=assertion_urn_input.dryRun,
    )
