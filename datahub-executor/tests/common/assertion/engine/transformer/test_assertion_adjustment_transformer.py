from unittest.mock import MagicMock, patch

import pytest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.transformer.assertion_adjustment.algorithm import (
    IQRAdjustmentAlgorithm,
    StdDevAdjustmentAlgorithm,
)
from datahub_executor.common.assertion.engine.transformer.assertion_adjustment_transformer import (
    AssertionAdjustmentTransformer,
    get_adjustment_algorithm_from_settings,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationSpec,
    AssertionEvaluationSpecContext,
    AssertionSourceType,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    FieldAssertion,
    FieldAssertionType,
    FieldMetricAssertion,
    FieldMetricType,
    RowCountTotal,
    SchemaFieldSpec,
    VolumeAssertion,
    VolumeAssertionType,
)

# Initialize sample objects
entity = AssertionEntity(
    urn="urn:li:dataset:test",
    platform_urn="urn:li:dataPlatform:snowflake",
    platform_instance=None,
    sub_types=None,
)

eval_parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
        source_type=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
        field=None,
        audit_log=None,
    ),
)
schedule = CronSchedule(cron="* * * * *", timezone="America/Los_Angeles")
assertion_context = AssertionEvaluationContext()
assertion_context_online_smart_assertions_enabled = AssertionEvaluationContext(
    online_smart_assertions=True
)


@pytest.fixture
def volume_assertion() -> VolumeAssertion:
    return VolumeAssertion(
        type=VolumeAssertionType.ROW_COUNT_TOTAL,
        row_count_total=RowCountTotal(
            operator=AssertionStdOperator.BETWEEN,
            parameters=AssertionStdParameters(
                max_value=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="100"
                ),
                min_value=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="90"
                ),
            ),
        ),
    )


@pytest.fixture
def field_metric_assertion() -> FieldAssertion:
    return FieldAssertion(
        type=FieldAssertionType.FIELD_METRIC,
        field_metric_assertion=FieldMetricAssertion(
            field=SchemaFieldSpec(
                path="column", type="string", native_type="varchar", kind=None
            ),
            metric=FieldMetricType.EMPTY_COUNT,
            operator=AssertionStdOperator.BETWEEN,
            parameters=AssertionStdParameters(
                max_value=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="1000"
                ),
                min_value=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="800"
                ),
                value=None,
            ),
        ),
        field_values_assertion=None,
    )


@pytest.fixture
def assertion_no_adjustment(volume_assertion: VolumeAssertion) -> Assertion:
    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        source_type=AssertionSourceType.NATIVE,
        connection_urn="urn:li:dataPlatform:snowflake",
        volume_assertion=volume_assertion,
        raw_info_aspect=None,
    )


@pytest.fixture
def assertion_default_adjustment(volume_assertion: VolumeAssertion) -> Assertion:
    assertion = Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        source_type=AssertionSourceType.INFERRED,
        connection_urn="urn:li:dataPlatform:snowflake",
        volume_assertion=volume_assertion,
        raw_info_aspect=None,
    )
    return assertion


@pytest.fixture
def field_assertion_default_adjustment(
    field_metric_assertion: FieldAssertion,
) -> Assertion:
    assertion = Assertion(
        urn="urn:li:assertion:test-field",
        type=AssertionType.FIELD,
        entity=entity,
        source_type=AssertionSourceType.INFERRED,
        connection_urn="urn:li:dataPlatform:snowflake",
        field_assertion=field_metric_assertion,
        raw_info_aspect=None,
    )
    return assertion


def test_assertion_tranformer_no_adjustment(assertion_no_adjustment: Assertion) -> None:
    """Test that non-inferred assertions are not transformed"""
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_no_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion == assertion_no_adjustment
    assert parameters == eval_parameters
    assert context == assertion_context


def test_assertion_tranformer_default_adjustment(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test default IQR adjustment for inferred volume assertions"""
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion.volume_assertion
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            max_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="125.0"
            ),
            min_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="65.0"
            ),
        )
    )
    assert parameters == eval_parameters


def test_assertion_tranformer_field_metric_adjustment(
    field_assertion_default_adjustment: Assertion,
) -> None:
    """Test default IQR adjustment for inferred field metric assertions"""
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        field_assertion_default_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion.field_assertion
    assert new_assertion.field_assertion.field_metric_assertion
    assert (
        new_assertion.field_assertion.field_metric_assertion.parameters
        == AssertionStdParameters(
            max_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="1500.0"
            ),
            min_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="300.0"
            ),
        )
    )
    assert parameters == eval_parameters


def test_assertion_tranformer_default_adjustment_std_dev(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test standard deviation adjustment with provided std_dev value"""
    assertion_context_with_std = AssertionEvaluationContext(
        assertion_evaluation_spec=AssertionEvaluationSpec(
            assertion=assertion_default_adjustment,
            schedule=CronSchedule(cron="* * * * *", timezone="America/Los_Angeles"),
            parameters=AssertionEvaluationParameters(
                type=AssertionEvaluationParametersType.DATASET_VOLUME,
            ),
            raw_parameters=None,
            context=AssertionEvaluationSpecContext(
                embedded_assertions=[], std_dev=5.0, inference_details=None
            ),
        )
    )

    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context_with_std
    )

    assert new_assertion.volume_assertion
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            max_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="101.25"
            ),
            min_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="88.75"
            ),
        )
    )
    assert parameters == eval_parameters


def test_assertion_tranformer_default_adjustment_std_dev_with_floor(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test standard deviation adjustment with very large std_dev that would make min value negative"""
    assertion_context_with_std = AssertionEvaluationContext(
        assertion_evaluation_spec=AssertionEvaluationSpec(
            assertion=assertion_default_adjustment,
            schedule=CronSchedule(cron="* * * * *", timezone="America/Los_Angeles"),
            parameters=AssertionEvaluationParameters(
                type=AssertionEvaluationParametersType.DATASET_VOLUME,
            ),
            raw_parameters=None,
            context=AssertionEvaluationSpecContext(
                embedded_assertions=[], std_dev=500, inference_details=None
            ),
        )
    )

    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context_with_std
    )

    assert new_assertion.volume_assertion
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            max_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="225.0"
            ),
            min_value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="0"
            ),
        )
    )
    assert parameters == eval_parameters


def test_assertion_transformer_no_adjustment_with_v2_enabled(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test that when online_smart_assertions is True, no adjustments are made"""
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment,
        eval_parameters,
        assertion_context_online_smart_assertions_enabled,
    )

    # Should not be adjusted when V2 is enabled
    assert new_assertion == assertion_default_adjustment
    assert parameters == eval_parameters
    assert context == assertion_context_online_smart_assertions_enabled


def test_get_adjustment_algorithm_from_settings_std_dev(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test that std_dev context creates StdDevAdjustmentAlgorithm"""
    assertion_context_with_std = AssertionEvaluationContext(
        assertion_evaluation_spec=AssertionEvaluationSpec(
            assertion=assertion_default_adjustment,
            schedule=CronSchedule(cron="* * * * *", timezone="America/Los_Angeles"),
            parameters=AssertionEvaluationParameters(
                type=AssertionEvaluationParametersType.DATASET_VOLUME,
            ),
            rawParameters=None,
            context=AssertionEvaluationSpecContext(
                embedded_assertions=[], std_dev=3.0, inference_details=None
            ),
        )
    )

    algorithm = get_adjustment_algorithm_from_settings(assertion_context_with_std)
    assert isinstance(algorithm, StdDevAdjustmentAlgorithm)
    # Access the std_dev attribute using the property name from the implementation
    assert algorithm.std_dev == 3.0


def test_get_adjustment_algorithm_from_settings_default() -> None:
    """Test that context without std_dev creates IQRAdjustmentAlgorithm"""
    context = AssertionEvaluationContext()

    algorithm = get_adjustment_algorithm_from_settings(context)
    assert isinstance(algorithm, IQRAdjustmentAlgorithm)


def test_assertion_transformer_with_exception(
    assertion_default_adjustment: Assertion,
) -> None:
    """Test that exceptions during adjustment are handled properly"""
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)

    # Force an exception during adjustment
    with patch(
        "datahub_executor.common.assertion.engine.transformer.assertion_adjustment_transformer.get_adjustment_algorithm_from_settings",
        side_effect=Exception("Test error"),
    ):
        new_assertion, parameters, context = transformer.transform(
            assertion_default_adjustment, eval_parameters, assertion_context
        )

        # Should return original values without adjustments
        assert new_assertion == assertion_default_adjustment
        assert parameters == eval_parameters
        assert context == assertion_context
