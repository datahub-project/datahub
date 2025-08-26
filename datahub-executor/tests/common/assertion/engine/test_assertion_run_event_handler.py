from unittest.mock import Mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import AssertionRunEventClass

from datahub_executor.common.assertion.result.assertion_run_event_handler import (
    AssertionRunEventResultHandler,
)
from datahub_executor.common.metric.types import Metric
from datahub_executor.common.types import (
    Assertion,
    AssertionEntity,
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationSpec,
    AssertionResultType,
    AssertionType,
    CronSchedule,
    DatasetFreshnessAssertionParameters,
    DatasetFreshnessSourceType,
    RawAspect,
)

# Sample Assertion and Context
entity = AssertionEntity(
    urn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
    platform_urn="urn:li:dataPlatform:hive",
    platform_instance=None,
    sub_types=None,
)
assertion = Assertion(
    urn="urn:li:assertion:e3663fd5-8477-4d18-adea-62b48c0de1f9",
    type=AssertionType.FRESHNESS,
    entity=entity,
    connection_urn="urn:li:dataPlatform:hive",
    freshness_assertion=None,
    raw_info_aspect=RawAspect(
        aspectName="assertionInfo",
        payload='{"type":"FRESHNESS","freshnessAssertion":{"type":"DATASET_CHANGE","schedule":{"type":"CRON","cron":{"cron":"0 */6 * * *","timezone":"Asia/Calcutta"}},"entity":"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"},"source":{"type":"NATIVE"}}',
    ),
)
assertion_nonparseable = Assertion(
    urn="urn:li:assertion:e3663fd5-8477-4d18-adea-62b48c0de1f9",
    type=AssertionType.FRESHNESS,
    entity=entity,
    connection_urn="urn:li:dataPlatform:hive",
    freshness_assertion=None,
    raw_info_aspect=RawAspect(
        aspectName="assertionInfo",
        payload='{"freshnessAssertion":{"type":"DATASET_CHANGE","schedule":{"type":"CRON","cron":{"cron":"0 */6 * * *","timezone":"Asia/Calcutta"}},"entity":"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"},"source":{"type":"NATIVE"}}',
    ),
)
parameters = AssertionEvaluationParameters(
    type=AssertionEvaluationParametersType.DATASET_FRESHNESS,
    dataset_freshness_parameters=DatasetFreshnessAssertionParameters(
        source_type=DatasetFreshnessSourceType.INFORMATION_SCHEMA,
        field=None,
        audit_log=None,
    ),
)
context = AssertionEvaluationContext(
    monitor_urn="urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD),9b7074de-46a4-4715-9383-32b90bed4632)",
    assertion_evaluation_spec=AssertionEvaluationSpec(
        assertion=assertion,
        schedule=CronSchedule(cron="0 * * * *", timezone="America/Los_Angeles"),
        parameters=parameters,
        raw_parameters='{"datasetFreshnessParameters":{"sourceType":"DATAHUB_OPERATION"},"type":"DATASET_FRESHNESS"}',
    ),
    base_assertion=RawAspect(
        aspectName="assertionInfo",
        payload='{"type":"FRESHNESS","freshnessAssertion":{"type":"DATASET_CHANGE","schedule":{"type":"CRON","cron":{"cron":"0 */6 * * *","timezone":"Asia/Calcutta"}},"entity":"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"},"source":{"type":"NATIVE"}}',
    ),
)
context_nonparseable = AssertionEvaluationContext(
    monitor_urn="urn:li:monitor:(urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD),9b7074de-46a4-4715-9383-32b90bed4632)",
    assertion_evaluation_spec=AssertionEvaluationSpec(
        assertion=assertion,
        schedule=CronSchedule(cron="0 * * * *", timezone="America/Los_Angeles"),
        parameters=parameters,
        raw_parameters='{"datasetFreshnessParameters":{"sourceType":"OPERATION"},"type":null}',
    ),
    base_assertion=RawAspect(
        aspectName="assertionInfo",
        payload='{"freshnessAssertion":{"type":"DATASET_CHANGE","schedule":{"type":"CRON","cron":{"cron":"0 */6 * * *","timezone":"Asia/Calcutta"}},"entity":"urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"},"source":{"type":"NATIVE"}}',
    ),
)
result = AssertionEvaluationResult(
    type=AssertionResultType.SUCCESS,
    parameters=None,
    metric=Metric(timestamp_ms=124, value=12.0),
)


def test_handle_assertion_run_event() -> None:
    graph = Mock(spec=DataHubGraph)

    handler = AssertionRunEventResultHandler(graph)
    handler.handle(assertion, parameters, result, context)

    graph.emit_mcp.assert_called_once()
    args, _ = graph.emit_mcp.call_args
    mcpw = args[0]
    assert isinstance(mcpw, MetadataChangeProposalWrapper)
    assert mcpw.entityUrn == assertion.urn
    assert mcpw.aspectName == "assertionRunEvent"
    assert isinstance(mcpw.aspect, AssertionRunEventClass)
    assert mcpw.aspect.asserteeUrn == entity.urn

    assert mcpw.aspect.result

    # These should be populated
    assert mcpw.aspect.result.assertion
    assert mcpw.aspect.result.parameters
    assert mcpw.aspect.result.baseAssertion
    assert mcpw.aspect.result.metric
    assert mcpw.aspect.result.metric.value == 12.0
    assert mcpw.aspect.result.metric.timestampMs == 124


def test_handle_assertion_run_event_parse_failure() -> None:
    graph = Mock(spec=DataHubGraph)

    handler = AssertionRunEventResultHandler(graph)
    handler.handle(assertion_nonparseable, parameters, result, context_nonparseable)

    graph.emit_mcp.assert_called_once()
    args, _ = graph.emit_mcp.call_args
    mcpw = args[0]
    assert isinstance(mcpw, MetadataChangeProposalWrapper)
    assert mcpw.entityUrn == assertion.urn
    assert mcpw.aspectName == "assertionRunEvent"
    assert isinstance(mcpw.aspect, AssertionRunEventClass)
    assert mcpw.aspect.asserteeUrn == entity.urn

    assert mcpw.aspect.result

    # These should not be populated
    assert not mcpw.aspect.result.assertion
    assert not mcpw.aspect.result.parameters
    assert not mcpw.aspect.result.baseAssertion
