# metadata-ingestion/examples/library/assertion_sql_metric.py
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
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
)

emitter = DatahubRestEmitter(
    gms_server=os.getenv("DATAHUB_GMS_URL", "http://localhost:8080"),
    token=os.getenv("DATAHUB_GMS_TOKEN"),
)

dataset_urn = builder.make_dataset_urn(platform="postgres", name="public.transactions")

sql_assertion_info = SqlAssertionInfoClass(
    type=SqlAssertionTypeClass.METRIC,
    entity=dataset_urn,
    statement="SELECT SUM(amount) FROM public.transactions WHERE status = 'completed' AND date = CURRENT_DATE",
    operator=AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
    parameters=AssertionStdParametersClass(
        value=AssertionStdParameterClass(
            type=AssertionStdParameterTypeClass.NUMBER,
            value="0",
        )
    ),
)

assertion_info = AssertionInfoClass(
    type=AssertionTypeClass.SQL,
    sqlAssertion=sql_assertion_info,
    description="Total completed transaction amount today must be non-negative",
)

assertion_urn = builder.make_assertion_urn(
    builder.datahub_guid(
        {"entity": dataset_urn, "type": "sql-completed-transactions-sum"}
    )
)

assertion_info_mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_info,
)

emitter.emit_mcp(assertion_info_mcp)
print(f"Created SQL assertion: {assertion_urn}")
