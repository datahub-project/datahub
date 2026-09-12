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
    AssertionResultSeverityClass,
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

# Shown as the assertion's category in DataHub. We name the specific source of the
# check (the monitor) rather than a generic "Databricks" so that different
# Databricks-sourced checks stay distinguishable; the specific check is carried in
# `nativeType`.
LAKEHOUSE_MONITOR_ASSERTION_TYPE = "Databricks Lakehouse Monitor"

# Native check name shown in the assertion details.
COMPLETENESS_NATIVE_TYPE = "completeness"

# The assertion "name" shown in the DataHub assertions list. The list renders a
# custom assertion's name from `description` when set, otherwise it falls back to
# the `type` label; it does not fetch the structured scope/aggregation fields, so
# the column would never appear in the name unless we put it here.
COMPLETENESS_ASSERTION_DESCRIPTION = "Null count for column {column} is {threshold}"

# Lakeflow Declarative Pipelines (formerly Delta Live Tables) expectations.
EXPECTATION_ASSERTION_TYPE = "Databricks Pipeline Expectation"

# Assertion name shown in the list (same rationale as COMPLETENESS_ASSERTION_DESCRIPTION):
# carries the expectation name so each pipeline expectation is distinguishable.
EXPECTATION_ASSERTION_DESCRIPTION = "Rows meet expectation {expectation}"

# A DLT expectation's action determines how bad a violation is: `expect_or_fail`
# aborts the update and `expect_or_drop` discards the offending rows (both hard
# failures), while a plain `expect` keeps the rows and only warns. Severity is only
# meaningful on a FAILURE result.
EXPECTATION_ACTION_SEVERITY = {
    "FAIL": AssertionResultSeverityClass.HIGH,
    "DROP": AssertionResultSeverityClass.HIGH,
    "ALLOW": AssertionResultSeverityClass.LOW,
}

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
    # Structured custom assertion. The scope/aggregation/operator/parameters/field
    # fully describe the check, but the assertions list renders a custom assertion's
    # name from `description` and never fetches those structured fields — so we also
    # set a column-aware `description` to surface the column in the name.
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        description=COMPLETENESS_ASSERTION_DESCRIPTION.format(
            column=result.column, threshold=_format_threshold(result.threshold)
        ),
        customProperties={"metric": result.metric, "threshold": str(result.threshold)},
        source=make_assertion_source(),
        customAssertion=CustomAssertionInfoClass(
            type=LAKEHOUSE_MONITOR_ASSERTION_TYPE,
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


class PipelineExpectationAssertion(BaseModel):
    # (dataset, pipeline, expectation) drives the assertion URN and excludes the
    # update, so re-ingesting the same expectation is idempotent: same assertion,
    # new run event. An expectation passes when no records failed it.
    name: str
    pipeline_id: str
    failed_records: int
    passed_records: Optional[int]
    # DLT expectation action (ALLOW / DROP / FAIL); drives failure severity.
    action: Optional[str] = None
    timestamp_millis: int
    run_id: str
    native_results: Dict[str, str] = {}

    @property
    def passed(self) -> bool:
        return self.failed_records == 0

    @property
    def failure_severity(self) -> Optional[str]:
        # Only meaningful on a FAILURE result; None when the action is unknown.
        if self.passed or not self.action:
            return None
        return EXPECTATION_ACTION_SEVERITY.get(self.action.upper())

    @property
    def logic(self) -> str:
        return f"{self.name}: failed_records == 0"


def make_expectation_assertion_urn(
    dataset_urn: str, pipeline_id: str, expectation: str
) -> str:
    # Key off the dataset URN (not the qualified name) for the same reason as
    # data-quality assertions: inherit the dataset's platform instance / env and
    # avoid cross-workspace collisions.
    key = {
        "platform": DATABRICKS_PLATFORM,
        "dataset": dataset_urn,
        "pipeline": pipeline_id,
        "expectation": expectation,
    }
    return make_assertion_urn(datahub_guid(key))


def build_expectation_info_mcp(
    result: PipelineExpectationAssertion,
    assertion_urn: str,
    dataset_urn: str,
) -> MetadataChangeProposalWrapper:
    # Structured custom assertion (matching the dbt connector's row-level native
    # test). The scope/operator/aggregation/nativeType are populated for
    # future-proofing, but the assertions list renders a custom assertion's name
    # from `description` and never fetches those structured fields — so we set an
    # explicit `description` carrying the expectation name. The DLT action is
    # surfaced as a custom property.
    custom_properties = {
        "expectation": result.name,
        "pipeline_id": result.pipeline_id,
    }
    if result.action:
        custom_properties["action"] = result.action
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        description=EXPECTATION_ASSERTION_DESCRIPTION.format(expectation=result.name),
        customProperties=custom_properties,
        source=make_assertion_source(),
        customAssertion=CustomAssertionInfoClass(
            type=EXPECTATION_ASSERTION_TYPE,
            entity=dataset_urn,
            scope=DatasetAssertionScopeClass.DATASET_ROWS,
            aggregation=AssertionStdAggregationClass._NATIVE_,
            operator=AssertionStdOperatorClass._NATIVE_,
            nativeType=result.name,
            logic=result.logic,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=assertion_info)


def build_expectation_run_event_mcp(
    result: PipelineExpectationAssertion,
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
            severity=result.failure_severity,
            actualAggValue=float(result.failed_records),
            nativeResults=result.native_results or None,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event)
