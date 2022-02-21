import time

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionScope,
    AssertionStdOperator,
    AssertionType,
    BatchAssertionResult,
    DatasetColumnAssertion,
    DatasetColumnStdAggFunc,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.schema_classes import PartitionSpecClass


def datasetUrn(tbl: str) -> str:
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl: str, fld: str) -> str:
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"


def assertionUrn(info: AssertionInfo) -> str:
    assertionId = builder.datahub_guid(info.to_obj())
    return builder.make_assertion_urn(assertionId)


def emitAssertionResult(assertionResult: AssertionResult, datasetUrn: str) -> None:

    dataset_assertionResult_mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeType.UPSERT,
        entityUrn=datasetUrn,
        aspectName="assertionResult",
        aspect=assertionResult,
    )

    # Emit BatchAssertion Result! (timseries aspect)
    emitter.emit_mcp(dataset_assertionResult_mcp)


# Construct an assertion object.
assertion_maxVal = AssertionInfo(
    datasetFields=[fldUrn("fooTable", "col1")],
    datasets=[datasetUrn("fooTable")],
    assertionType=AssertionType(
        scope=AssertionScope.DATASET_COLUMN,
        datasetColumnAssertion=DatasetColumnAssertion(
            stdOperator=AssertionStdOperator.LESS_THAN,
            nativeOperator="column_value_is_less_than",
            stdAggFunc=DatasetColumnStdAggFunc.IDENTITY,
        ),
    ),
    assertionParameters={"max_value": "99"},
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
assertionResult_maxVal_batch_partition1 = AssertionResult(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("fooTable"),
    partitionSpec=PartitionSpecClass(partition=str([{"country": "IN"}])),
    nativeEvaluatorRunId="uuid1",
    batchAssertionResult=BatchAssertionResult(
        success=True,
        externalUrl="http://example.com/uuid1",
        actualAggValue=90,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition1,
    datasetUrn("fooTable"),
)

# Construct batch assertion result object for partition 2 batch
assertionResult_maxVal_batch_partition2 = AssertionResult(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("fooTable"),
    partitionSpec=PartitionSpecClass(partition=str([{"country": "US"}])),
    nativeEvaluatorRunId="uuid1",
    batchAssertionResult=BatchAssertionResult(
        success=False,
        externalUrl="http://example.com/uuid1",
        actualAggValue=101,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_partition2,
    datasetUrn("fooTable"),
)

# Construct batch assertion result object for full table batch.
assertionResult_maxVal_batch_fulltable = AssertionResult(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("fooTable"),
    nativeEvaluatorRunId="uuid1",
    batchAssertionResult=BatchAssertionResult(
        success=True,
        externalUrl="http://example.com/uuid1",
        actualAggValue=93,
    ),
)

emitAssertionResult(
    assertionResult_maxVal_batch_fulltable,
    datasetUrn("fooTable"),
)
