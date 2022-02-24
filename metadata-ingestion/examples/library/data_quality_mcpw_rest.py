import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    DatasetAssertionScope,
    AssertionStdOperator,
    AssertionType,
    DatasetAssertionInfo,
    DatasetColumnAssertion,
    DatasetColumnStdAggFunc,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.schema_classes import AssertionRunEventClass, PartitionSpecClass


def datasetUrn(tbl: str) -> str:
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl: str, fld: str) -> str:
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"


def assertionUrn(info: AssertionInfo) -> str:
    return "urn:li:assertion:432475190cc846f2894b5b3aa4d55af2"


def emitAssertionResult(assertionResult: AssertionResult) -> None:

    dataset_assertionRunEvent_mcp = MetadataChangeProposalWrapper(
        entityType="assertion",
        changeType=ChangeType.UPSERT,
        entityUrn=assertionResult.assertionUrn,
        aspectName="assertionRunEvent",
        aspect=assertionResult,
    )

    # Emit BatchAssertion Result! (timseries aspect)
    emitter.emit_mcp(dataset_assertionRunEvent_mcp)


# Construct an assertion object.
assertion_maxVal = AssertionInfo(
    type=AssertionType.DATASET,
    datasetAssertion=DatasetAssertionInfo(
        scope=DatasetAssertionScope.DATASET_COLUMN,
        columnAssertion=DatasetColumnAssertion(
            stdOperator=AssertionStdOperator.LESS_THAN,
            nativeType="column_value_is_less_than",
            stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
        ),
        fields=[fldUrn("bazTable", "col1")],
        datasets=[datasetUrn("bazTable")],
    ),
    parameters={"max_value": "99"},
    customProperties={"suite_name": "demo_suite"},
)

# Construct a MetadataChangeProposalWrapper object.
assertion_maxVal_mcp = MetadataChangeProposalWrapper(
    entityType="assertion",
    changeType=ChangeType.UPSERT,
    entityUrn=assertionUrn(assertion_maxVal),
    aspectName="assertionInfo",
    aspect=assertion_maxVal,
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit Assertion entity info object!
emitter.emit_mcp(assertion_maxVal_mcp)

# Construct batch assertion result object for partition 1 batch
assertionResult_maxVal_batch_partition1 = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpecClass(partition=str([{"country": "IN"}])),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.SUCCESS,
        externalUrl="http://example.com/uuid1",
        actualAggValue=90,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition1,
)

# Construct batch assertion result object for partition 2 batch
assertionResult_maxVal_batch_partition2 = AssertionRunEventClass(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpecClass(partition=str([{"country": "US"}])),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.FAILURE,
        externalUrl="http://example.com/uuid1",
        actualAggValue=101,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition2,
)

# Construct batch assertion result object for full table batch.
assertionResult_maxVal_batch_fulltable = AssertionRunEventClass(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    runId="uuid1",
    status=AssertionRunStatus.COMPLETE,
    result=AssertionResult(
        type=AssertionResultType.SUCCESS,
        externalUrl="http://example.com/uuid1",
        actualAggValue=93,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_fulltable,
)
