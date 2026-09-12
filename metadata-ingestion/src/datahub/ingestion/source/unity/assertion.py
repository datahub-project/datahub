from typing import Dict, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_source,
    make_assertion_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)

# The provider shown as the assertion's category in DataHub. Mirrors how other
# native integrations tag their source (dbt -> "dbt", Great Expectations ->
# "GREAT_EXPECTATIONS"); the specific check is carried in `nativeType` instead.
DATABRICKS_ASSERTION_PROVIDER = "Databricks"

# Native check name shown in the assertion details (analogous to a dbt test name).
COMPLETENESS_NATIVE_TYPE = "completeness"

DATABRICKS_PLATFORM = "databricks"


class DataQualityAssertion(BaseModel):
    # (dataset, column, metric) drives the assertion URN and excludes the run
    # window, so re-ingesting the same check is idempotent: same assertion, new
    # run event.
    column: str
    metric: str
    threshold: float
    observed: Optional[float]
    passed: bool
    timestamp_millis: int
    run_id: str
    native_results: Dict[str, str] = {}


def _format_threshold(threshold: float) -> str:
    return str(int(threshold)) if threshold == int(threshold) else str(threshold)


def make_dq_assertion_urn(dataset_urn: str, column: str, metric: str) -> str:
    # Key off the dataset URN (not the qualified name), so the assertion inherits
    # the same platform instance, metastore, and env the dataset URN already
    # encodes — otherwise assertions from different workspaces could collide.
    key = {
        "platform": DATABRICKS_PLATFORM,
        "dataset": dataset_urn,
        "column": column,
        "metric": metric,
    }
    return make_assertion_urn(datahub_guid(key))


def build_assertion_info_mcp(
    result: DataQualityAssertion,
    assertion_urn: str,
    dataset_urn: str,
) -> MetadataChangeProposalWrapper:
    field_urn = make_schema_field_urn(dataset_urn, result.column)
    # Structured custom assertion (matching the dbt connector's shape): the
    # scope/aggregation/operator/parameters/field drive DataHub's column-aware
    # rendering ("Null count for column X is equal to 0"), rather than a
    # hand-written description that omits the column.
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customProperties={"metric": result.metric, "threshold": str(result.threshold)},
        source=make_assertion_source(),
        customAssertion=CustomAssertionInfoClass(
            type=DATABRICKS_ASSERTION_PROVIDER,
            entity=dataset_urn,
            field=field_urn,
            fields=[field_urn],
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            aggregation=AssertionStdAggregationClass.NULL_COUNT,
            operator=AssertionStdOperatorClass.EQUAL_TO,
            parameters=AssertionStdParametersClass(
                value=AssertionStdParameterClass(
                    value=_format_threshold(result.threshold),
                    type=AssertionStdParameterTypeClass.NUMBER,
                )
            ),
            nativeType=COMPLETENESS_NATIVE_TYPE,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=assertion_info)


def build_assertion_run_event_mcp(
    result: DataQualityAssertion,
    assertion_urn: str,
    dataset_urn: str,
) -> MetadataChangeProposalWrapper:
    run_event = AssertionRunEventClass(
        timestampMillis=result.timestamp_millis,
        assertionUrn=assertion_urn,
        asserteeUrn=dataset_urn,
        runId=result.run_id,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=(
                AssertionResultTypeClass.SUCCESS
                if result.passed
                else AssertionResultTypeClass.FAILURE
            ),
            actualAggValue=result.observed,
            nativeResults=result.native_results or None,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event)
