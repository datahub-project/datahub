import json
from unittest.mock import MagicMock

import pytest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    RowCountTotalClass,
    VolumeAssertionInfoClass,
)

from datahub_executor.common.aspect_builder import get_assertion_std_parameters
from datahub_executor.common.assertion.engine.transformer.assertion_adjustment_transformer import (
    AssertionAdjustmentTransformer,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionAdjustmentAlgorithm,
    AssertionAdjustmentSettings,
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
    RawAspect,
    RowCountTotal,
    VolumeAssertion,
    VolumeAssertionType,
)

# Initialize sample objects
entity = AssertionEntity(
    urn="urn:li:dataset:test",
    platformUrn="urn:li:dataPlatform:snowflake",
    platformInstance=None,
    subTypes=None,
)

eval_parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    datasetFreshnessParameters=DatasetFreshnessAssertionParameters(
        sourceType=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
        field=None,
        auditLog=None,
    ),
)
schedule = CronSchedule(cron="* * * * *", timezone="America/Los_Angeles")
assertion_context = AssertionEvaluationContext()


@pytest.fixture
def volume_assertion() -> VolumeAssertion:
    return VolumeAssertion(
        type=VolumeAssertionType.ROW_COUNT_TOTAL,
        rowCountTotal=RowCountTotal(
            operator=AssertionStdOperator.BETWEEN,
            parameters=AssertionStdParameters(
                maxValue=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="100"
                ),
                minValue=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="90"
                ),
            ),
        ),
    )


@pytest.fixture
def assertion_custom_adjustment(volume_assertion: VolumeAssertion) -> Assertion:
    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.INFERRED,
        connectionUrn="urn:li:dataPlatform:snowflake",
        volumeAssertion=volume_assertion,
        adjustmentSettings=AssertionAdjustmentSettings(
            algorithm=AssertionAdjustmentAlgorithm.CUSTOM,
            algorithmName="IQR",
            context={
                "K": 0.5,
            },
        ),
        raw_info_aspect=get_raw_info_aspect_for_volume_assertion(volume_assertion),
    )


@pytest.fixture
def assertion_malformed_custom_adjustment(
    volume_assertion: VolumeAssertion,
) -> Assertion:
    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.INFERRED,
        connectionUrn="urn:li:dataPlatform:snowflake",
        volumeAssertion=volume_assertion,
        adjustmentSettings=AssertionAdjustmentSettings(
            algorithm=AssertionAdjustmentAlgorithm.CUSTOM,
            algorithmName="IQR",
            context={"K": "K", "somerandomstuff": "xyz"},
        ),
        raw_info_aspect=get_raw_info_aspect_for_volume_assertion(volume_assertion),
    )


@pytest.fixture
def assertion_no_adjustment(volume_assertion: VolumeAssertion) -> Assertion:
    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.NATIVE,
        connectionUrn="urn:li:dataPlatform:snowflake",
        volumeAssertion=volume_assertion,
        adjustmentSettings=AssertionAdjustmentSettings(
            algorithm=AssertionAdjustmentAlgorithm.CUSTOM,
            algorithmName="IQR",
            context={
                "K": 0.5,
            },
        ),
        raw_info_aspect=get_raw_info_aspect_for_volume_assertion(volume_assertion),
    )


@pytest.fixture
def assertion_default_adjustment(volume_assertion: VolumeAssertion) -> Assertion:
    return Assertion(
        urn="urn:li:assertion:test",
        type=AssertionType.VOLUME,
        entity=entity,
        sourceType=AssertionSourceType.INFERRED,
        connectionUrn="urn:li:dataPlatform:snowflake",
        volumeAssertion=volume_assertion,
        raw_info_aspect=get_raw_info_aspect_for_volume_assertion(volume_assertion),
    )


def get_raw_info_aspect_for_volume_assertion(
    volume_assertion: VolumeAssertion,
) -> RawAspect:
    return RawAspect(
        aspectName="assertionInfo",
        payload=json.dumps(
            AssertionInfoClass(
                type=AssertionType.VOLUME.value,
                volumeAssertion=VolumeAssertionInfoClass(
                    entity=entity.urn,
                    type=volume_assertion.type.value,
                    rowCountTotal=RowCountTotalClass(
                        operator=volume_assertion.row_count_total.operator.value,  # type:ignore
                        parameters=get_assertion_std_parameters(
                            volume_assertion.row_count_total.parameters  # type:ignore
                        ),
                    ),
                ),
            ).to_obj()
        ),
    )


def test_assertion_tranformer_no_adjustment(assertion_no_adjustment: Assertion) -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_no_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion == assertion_no_adjustment
    assert new_assertion.raw_info_aspect
    assert parameters == eval_parameters
    assert context == assertion_context


def test_assertion_tranformer_default_adjustment(
    assertion_default_adjustment: Assertion,
) -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion.volume_assertion
    assert new_assertion.raw_info_aspect
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="125.0"
            ),
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="65.0"
            ),
        )
    )
    assert parameters == eval_parameters
    assert context.base_assertion_info != new_assertion.raw_info_aspect


def test_assertion_tranformer_default_adjustment_std_dev(
    assertion_default_adjustment: Assertion,
) -> None:
    assertion_context_with_std = AssertionEvaluationContext(
        assertion_evaluation_spec=AssertionEvaluationSpec(
            assertion=assertion_default_adjustment,
            schedule=CronSchedule(cron="* * * * *", timezone="America/Los_Angeles"),
            parameters=AssertionEvaluationParameters(
                type=AssertionEvaluationParametersType.DATASET_VOLUME,
                datasetFieldParameters=None,
                datasetSchemaParameters=None,
                datasetVolumeParameters=None,
                datasetFreshnessParameters=None,
            ),
            rawParameters=None,
            context=AssertionEvaluationSpecContext(embeddedAssertions=[], stdDev=5.0),
        )
    )

    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context_with_std
    )

    assert new_assertion.volume_assertion
    assert new_assertion.raw_info_aspect
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="101.25"
            ),
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="88.75"
            ),
        )
    )
    assert parameters == eval_parameters
    assert context.base_assertion_info != new_assertion.raw_info_aspect


def test_assertion_tranformer_default_adjustment_std_dev_with_floor(
    assertion_default_adjustment: Assertion,
) -> None:
    assertion_context_with_std = AssertionEvaluationContext(
        assertion_evaluation_spec=AssertionEvaluationSpec(
            assertion=assertion_default_adjustment,
            schedule=CronSchedule(cron="* * * * *", timezone="America/Los_Angeles"),
            parameters=AssertionEvaluationParameters(
                type=AssertionEvaluationParametersType.DATASET_VOLUME,
                datasetFieldParameters=None,
                datasetSchemaParameters=None,
                datasetVolumeParameters=None,
                datasetFreshnessParameters=None,
            ),
            rawParameters=None,
            context=AssertionEvaluationSpecContext(embeddedAssertions=[], stdDev=500),
        )
    )

    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_default_adjustment, eval_parameters, assertion_context_with_std
    )

    assert new_assertion.volume_assertion
    assert new_assertion.raw_info_aspect
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="225.0"
            ),
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="0"
            ),
        )
    )
    assert parameters == eval_parameters
    assert context.base_assertion_info != new_assertion.raw_info_aspect


def test_assertion_tranformer_custom_adjustment(
    assertion_custom_adjustment: Assertion,
) -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_custom_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion.volume_assertion
    assert new_assertion.raw_info_aspect
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="105.0"
            ),
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="85.0"
            ),
        )
    )
    assert parameters == eval_parameters
    assert context.base_assertion_info
    assert context.base_assertion_info != new_assertion.raw_info_aspect


def test_assertion_tranformer_malformed_adjustment(
    assertion_malformed_custom_adjustment: Assertion,
) -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = AssertionAdjustmentTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_malformed_custom_adjustment, eval_parameters, assertion_context
    )

    assert new_assertion.volume_assertion
    assert new_assertion.raw_info_aspect
    assert new_assertion.volume_assertion.row_count_total
    assert (
        new_assertion.volume_assertion.row_count_total.parameters
        == AssertionStdParameters(
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="125.0"
            ),
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="65.0"
            ),
        )
    )
    assert parameters == eval_parameters
    assert context.base_assertion_info != new_assertion.raw_info_aspect
