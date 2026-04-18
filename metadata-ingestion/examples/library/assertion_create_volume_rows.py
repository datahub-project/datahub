# metadata-ingestion/examples/library/assertion_volume_rows.py
import os

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    RowCountTotalClass,
    VolumeAssertionInfoClass,
    VolumeAssertionTypeClass,
)

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

dataset_urn = builder.make_dataset_urn(
    platform="bigquery", name="project.dataset.orders"
)

volume_assertion_info = VolumeAssertionInfoClass(
    type=VolumeAssertionTypeClass.ROW_COUNT_TOTAL,
    entity=dataset_urn,
    rowCountTotal=RowCountTotalClass(
        operator=AssertionStdOperatorClass.BETWEEN,
        parameters=AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                type=AssertionStdParameterTypeClass.NUMBER,
                value="1000",
            ),
            maxValue=AssertionStdParameterClass(
                type=AssertionStdParameterTypeClass.NUMBER,
                value="1000000",
            ),
        ),
    ),
)

assertion_info = AssertionInfoClass(
    type=AssertionTypeClass.VOLUME,
    volumeAssertion=volume_assertion_info,
    description="Orders table must contain between 1,000 and 1,000,000 rows",
)

assertion_urn = builder.make_assertion_urn(
    builder.datahub_guid({"entity": dataset_urn, "type": "row-count-range"})
)

assertion_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_info,
)

emitter.emit_mcp(assertion_info_mcp)
print(f"Created volume assertion: {assertion_urn}")
