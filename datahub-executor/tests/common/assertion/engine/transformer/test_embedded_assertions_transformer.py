from unittest.mock import MagicMock

from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.transformer.embedded_assertions_transformer import (
    EmbeddedAssertionsTransformer,
)
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationSpec,
    AssertionEvaluationSpecContext,
    AssertionInfo,
    AssertionSourceType,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    EmbeddedAssertion,
    RawAspect,
    RowCountTotal,
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
assertion = Assertion(
    urn="urn:li:assertion:test",
    type=AssertionType.VOLUME,
    entity=entity,
    source_type=AssertionSourceType.INFERRED,
    connection_urn="urn:li:dataPlatform:snowflake",
    volume_assertion=VolumeAssertion(
        type=VolumeAssertionType.ROW_COUNT_TOTAL,
        row_count_total=RowCountTotal(
            operator=AssertionStdOperator.EQUAL_TO,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    type=AssertionStdParameterType.NUMBER, value="100"
                )
            ),
        ),
    ),
    raw_info_aspect=RawAspect(
        aspectName="assertionInfo",
        payload='{"type":"VOLUME","volumeAssertion":{"type":"ROW_COUNT_TOTAL","entity":"urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)","rowCountTotal":{"operator":"EQUAL_TO","parameters":{"value":{"value":"100.0","type":"NUMBER"}}}},"source":{"type":"INFERRED"}}',
    ),
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
assertion_spec = AssertionEvaluationSpec(
    assertion=assertion, schedule=schedule, parameters=eval_parameters, context=None
)
assertion_context = AssertionEvaluationContext(assertion_evaluation_spec=assertion_spec)
new_volume_assertion = VolumeAssertion(
    type=VolumeAssertionType.ROW_COUNT_TOTAL,
    row_count_total=RowCountTotal(
        operator=AssertionStdOperator.EQUAL_TO,
        parameters=AssertionStdParameters(
            value=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="200"
            )
        ),
    ),
)

embedded_assertion = EmbeddedAssertion(
    assertion=AssertionInfo(
        type=AssertionType.VOLUME,
        volume_assertion=new_volume_assertion,
        source_type=AssertionSourceType.INFERRED,
    ),
    raw_assertion='{"type":"VOLUME","volumeAssertion":{"type":"ROW_COUNT_TOTAL","entity":"urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)","rowCountTotal":{"operator":"EQUAL_TO","parameters":{"value":{"value":"200.0","type":"NUMBER"}}}},"source":{"type":"INFERRED"}}',
    evaluation_time_window=None,
)

smart_assertion_spec = AssertionEvaluationSpec(
    assertion=assertion,
    schedule=schedule,
    parameters=eval_parameters,
    context=AssertionEvaluationSpecContext(
        embedded_assertions=[embedded_assertion], stdDev=None
    ),
)
smart_assertion_context = AssertionEvaluationContext(
    assertion_evaluation_spec=smart_assertion_spec
)


def test_assertion_tranformer_no_change() -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = EmbeddedAssertionsTransformer(graph)
    new_assertion, parameters, context = transformer.transform(
        assertion_spec.assertion, assertion_spec.parameters, assertion_context
    )

    assert new_assertion == assertion
    assert new_assertion.raw_info_aspect
    assert parameters == assertion_spec.parameters
    assert context == assertion_context


def test_assertion_transformer_assertion_updated() -> None:
    graph = MagicMock(spec=DataHubGraph)
    transformer = EmbeddedAssertionsTransformer(graph)

    new_assertion, parameters, context = transformer.transform(
        smart_assertion_spec.assertion,
        smart_assertion_spec.parameters,
        smart_assertion_context,
    )

    graph.emit_mcp.assert_called_once()

    assert new_assertion != assertion

    assert new_assertion.raw_info_aspect
    assert assertion.raw_info_aspect

    assert new_assertion.source_type
    assert new_assertion.volume_assertion
    assert new_assertion.volume_assertion.row_count_total
    assert new_assertion.volume_assertion == new_volume_assertion
    assert parameters == assertion_spec.parameters
    assert context == smart_assertion_context
