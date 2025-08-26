from unittest.mock import Mock

import pytest

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.assertion.engine.evaluator.evaluator import (
    AssertionEvaluator,
)
from datahub_executor.common.assertion.result.handler import AssertionResultHandler
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionResultType,
    AssertionType,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FreshnessFieldKind,
    FreshnessFieldSpec,
)

# Sample Assertion and Context
entity = AssertionEntity(
    urn="urn:li:dataset:test",
    platform_urn="urn:li:dataPlatform:snowflake",
)
assertion = Assertion(
    urn="urn:li:assertion:test",
    type=AssertionType.DATASET,
    entity=entity,
    connection_urn="urn:li:dataPlatform:snowflake",
)
parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
        source_type=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
    ),
)
context = AssertionEvaluationContext()


def test_evaluate_assertion() -> None:
    # Mock the AssertionEvaluator
    evaluator = Mock(spec=AssertionEvaluator)
    evaluator.type = AssertionType.DATASET
    evaluator.evaluate.return_value = AssertionEvaluationResult(
        type=AssertionResultType.SUCCESS, parameters=None
    )

    # Mock the AssertionResultHandler
    result_handler = Mock(spec=AssertionResultHandler)

    # Create AssertionEngine instance with the evaluator and result handler
    engine = AssertionEngine([evaluator], [result_handler])

    # Evaluate the Assertion
    result = engine.evaluate(
        assertion=assertion, parameters=parameters, context=context
    )

    # Check the evaluator's evaluate method was called with correct parameters
    evaluator.evaluate.assert_called_once_with(assertion, parameters, context)

    # Check the result handler's handle method was called with correct parameters
    result_handler.handle.assert_called_once_with(
        assertion, parameters, result, context
    )

    # Assert the result of evaluation is as expected
    assert result.type == AssertionResultType.SUCCESS


def test_evaluate_stateful_assertion() -> None:
    # Mock the AssertionEvaluator
    evaluator = Mock(spec=AssertionEvaluator)
    evaluator.type = AssertionType.DATASET
    evaluator.is_stateful = True
    evaluator.evaluate.return_value = AssertionEvaluationResult(
        type=AssertionResultType.SUCCESS, parameters=None
    )
    parameters = AssertionEvaluationParameters(
        type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
        dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
            source_type=DatasetFreshnessSourceType.FIELD_VALUE,
            field=FreshnessFieldSpec(
                path="col_timestamp",
                type="TIME",
                native_type="TIMESTAMP",
                kind=FreshnessFieldKind.HIGH_WATERMARK,
            ),
        ),
    )
    # Mock the AssertionResultHandler
    result_handler = Mock(spec=AssertionResultHandler)

    # Create AssertionEngine instance with the evaluator and result handler
    engine = AssertionEngine([evaluator], [result_handler])

    # Evaluate the Assertion
    result = engine.evaluate(
        assertion=assertion, parameters=parameters, context=context
    )

    # Check the evaluator's evaluate method was called with correct parameters
    evaluator.evaluate.assert_called_once_with(assertion, parameters, context)

    # Check the result handler's handle method was called with correct parameters
    result_handler.handle.assert_called_once_with(
        assertion, parameters, result, context
    )

    # Assert the result of evaluation is as expected
    assert result.type == AssertionResultType.SUCCESS


def test_evaluate_assertion_no_evaluator() -> None:
    # Create AssertionEngine instance without any evaluators
    engine = AssertionEngine([])

    # Attempting to evaluate the Assertion should raise a ValueError
    with pytest.raises(ValueError) as e:
        engine.evaluate(assertion, parameters, context)

    # Check the exception message
    assert str(e.value) == f"No evaluator found for assertion type {assertion.type}"
