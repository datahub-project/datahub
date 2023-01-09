import json
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
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import PartitionSpec


def datasetUrn(tbl: str) -> str:
    return builder.make_dataset_urn("postgres", tbl)


def fldUrn(tbl: str, fld: str) -> str:
    return f"urn:li:schemaField:({datasetUrn(tbl)}, {fld})"


def assertionUrn(info: AssertionInfo) -> str:
    return "urn:li:assertion:432475190cc846f2894b5b3aa4d55af2"


def emitAssertionResult(assertionResult: AssertionRunEvent) -> None:
    dataset_assertionRunEvent_mcp = MetadataChangeProposalWrapper(
        entityUrn=assertionResult.assertionUrn,
        aspect=assertionResult,
    )

    # Emit BatchAssertion Result! (timeseries aspect)
    emitter.emit_mcp(dataset_assertionRunEvent_mcp)


# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

datasetProperties = DatasetProperties(
    name="bazTable",
)
# Construct a MetadataChangeProposalWrapper object for dataset
dataset_mcp = MetadataChangeProposalWrapper(
    entityUrn=datasetUrn("bazTable"),
    aspect=datasetProperties,
)

# Emit Dataset entity properties aspect! (Skip if dataset is already present)
emitter.emit_mcp(dataset_mcp)

# Construct an assertion object.
assertion_maxVal = AssertionInfo(
    type=AssertionType.DATASET,
    datasetAssertion=DatasetAssertionInfo(
        scope=DatasetAssertionScope.DATASET_COLUMN,
        operator=AssertionStdOperator.BETWEEN,
        nativeType="expect_column_max_to_be_between",
        aggregation=AssertionStdAggregation.MAX,
        fields=[fldUrn("bazTable", "col1")],
        dataset=datasetUrn("bazTable"),
        nativeParameters={"max_value": "99", "min_value": "89"},
        parameters=AssertionStdParameters(
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="89"
            ),
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value="99"
            ),
        ),
    ),
    customProperties={"suite_name": "demo_suite"},
)

# Construct a MetadataChangeProposalWrapper object.
assertion_maxVal_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertionUrn(assertion_maxVal),
    aspect=assertion_maxVal,
)

# Emit Assertion entity info aspect!
emitter.emit_mcp(assertion_maxVal_mcp)

# Construct an assertion platform object.
assertion_dataPlatformInstance = DataPlatformInstance(
    platform=builder.make_data_platform_urn("great-expectations")
)

# Construct a MetadataChangeProposalWrapper object for assertion platform
assertion_dataPlatformInstance_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertionUrn(assertion_maxVal),
    aspect=assertion_dataPlatformInstance,
)
# Emit Assertion entity platform aspect!
emitter.emit(assertion_dataPlatformInstance_mcp)


# Construct batch assertion result object for partition 1 batch
assertionResult_maxVal_batch_partition1 = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpec(partition=json.dumps([{"country": "IN"}])),
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
assertionResult_maxVal_batch_partition2 = AssertionRunEvent(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertionUrn(assertion_maxVal),
    asserteeUrn=datasetUrn("bazTable"),
    partitionSpec=PartitionSpec(partition=json.dumps([{"country": "US"}])),
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
assertionResult_maxVal_batch_fulltable = AssertionRunEvent(
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
