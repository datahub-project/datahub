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
    AssertionStdOperatorClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)

# Shown in DataHub as the assertion's origin/type. Databricks data-quality
# monitors were formerly branded "Lakehouse Monitoring".
CUSTOM_ASSERTION_TYPE = "Databricks Data Quality"

DATABRICKS_PLATFORM = "databricks"

# Completeness is expressed as "null count == 0"; kept as symbols so the model
# owns the DataHub-facing representation of its single operator.
_OPERATOR = AssertionStdOperatorClass.EQUAL_TO
_OPERATOR_SYMBOL = "=="


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

    @property
    def logic(self) -> str:
        return f"{self.metric} {_OPERATOR_SYMBOL} {self.threshold} on {self.column}"


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
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        customProperties={"metric": result.metric, "threshold": str(result.threshold)},
        source=make_assertion_source(),
        description="Completeness",
        customAssertion=CustomAssertionInfoClass(
            type=CUSTOM_ASSERTION_TYPE,
            entity=dataset_urn,
            field=field_urn,
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=_OPERATOR,
            logic=result.logic,
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
