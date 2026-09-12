from typing import Dict, Optional

from pydantic import BaseModel

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_source,
    make_assertion_urn,
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
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)

# Shown as the assertion's category in DataHub (CustomAssertionInfo.type). The
# specific check is carried in `nativeType`.
LAKEHOUSE_MONITOR_ASSERTION_TYPE = "Databricks Lakehouse Monitor"

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

# Databricks anomaly-detection statuses. These are verdicts the platform already
# computed; we map them to DataHub run-event result types and never invent one.
# `Training` and `Unknown` are neither pass nor fail, so they produce no run event.
_STATUS_HEALTHY = "healthy"
_STATUS_UNHEALTHY = "unhealthy"
_STATUS_ERROR = "error"
# Model-fit failure surfaces as an error_code rather than a status; treat it as ERROR.
_ERROR_CODE_FAILED_TO_FIT_MODEL = "failed_to_fit_model"


def map_status_to_result_type(
    status: Optional[str], error_code: Optional[str]
) -> Optional[str]:
    """Map a Databricks check status/error_code to an AssertionResultType.

    Returns None for statuses that are not a pass/fail verdict (``Training``,
    ``Unknown``, missing), so the caller skips emitting a run event for them.
    """
    if error_code and error_code.strip().lower() == _ERROR_CODE_FAILED_TO_FIT_MODEL:
        return AssertionResultTypeClass.ERROR
    normalized = (status or "").strip().lower()
    if normalized == _STATUS_HEALTHY:
        return AssertionResultTypeClass.SUCCESS
    if normalized == _STATUS_UNHEALTHY:
        return AssertionResultTypeClass.FAILURE
    if normalized == _STATUS_ERROR:
        return AssertionResultTypeClass.ERROR
    return None


class CheckSpec(BaseModel):
    # Static identity of a Databricks anomaly-detection check. `key` is the stable
    # part of the assertion URN (dataset + key), so each scan appends a run event to
    # the same assertion rather than creating a new one. Predicted bounds change every
    # scan and therefore live on the run event's nativeResults, not here.
    key: str
    native_type: str
    title: str
    aggregation: Optional[str] = None
    scope: Optional[str] = None


# One assertion per table per check. The group rollups (`freshness.status`,
# `completeness.status`) and the table-level `status` are deliberately skipped so the
# same failure is not counted twice.
COMMIT_FRESHNESS_CHECK = CheckSpec(
    key="freshness.commit_freshness",
    native_type="commit_freshness",
    title="Databricks commit freshness anomaly detection",
    # Freshness is evaluated natively by Databricks; last/predicted values are
    # timestamps, so there is no standard numeric aggregation to report.
    aggregation=None,
    scope=None,
)
DAILY_ROW_COUNT_CHECK = CheckSpec(
    key="completeness.daily_row_count",
    native_type="daily_row_count",
    title="Databricks daily row count anomaly detection",
    aggregation=AssertionStdAggregationClass.ROW_COUNT,
    scope=DatasetAssertionScopeClass.DATASET_ROWS,
)
TOTAL_ROW_COUNT_CHECK = CheckSpec(
    key="completeness.total_row_count",
    native_type="total_row_count",
    title="Databricks total row count anomaly detection",
    aggregation=AssertionStdAggregationClass.ROW_COUNT,
    scope=DatasetAssertionScopeClass.DATASET_ROWS,
)


class CheckRun(BaseModel):
    # A single scan's outcome for one check. `result_type` is Databricks' own verdict
    # (mapped via map_status_to_result_type); `native_results` carries the per-scan
    # predicted bounds / observed timestamps that do not belong on the assertion
    # definition.
    result_type: str
    timestamp_millis: int
    run_id: str
    actual_agg_value: Optional[float] = None
    native_results: Dict[str, str] = {}


def make_dq_assertion_urn(dataset_urn: str, check_key: str) -> str:
    # Key off the dataset URN (not the qualified name) and the check, so the assertion
    # inherits the platform instance, metastore, and env the dataset URN already
    # encodes — otherwise assertions from different workspaces could collide — and so
    # re-ingestion is idempotent (the window/event_time is intentionally excluded).
    key = {
        "platform": DATABRICKS_PLATFORM,
        "dataset": dataset_urn,
        "check": check_key,
    }
    return make_assertion_urn(datahub_guid(key))


def build_assertion_info_mcp(
    spec: CheckSpec,
    assertion_urn: str,
    dataset_urn: str,
) -> MetadataChangeProposalWrapper:
    custom_assertion = CustomAssertionInfoClass(
        type=LAKEHOUSE_MONITOR_ASSERTION_TYPE,
        entity=dataset_urn,
        scope=spec.scope,
        aggregation=spec.aggregation,
        # Databricks produces the verdict; DataHub does not re-evaluate a threshold.
        operator=AssertionStdOperatorClass._NATIVE_,
        nativeType=spec.native_type,
    )
    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        # A short fixed title: the assertions list renders a custom assertion's name
        # from its description. Per-scan values stay on the run event.
        description=spec.title,
        customProperties={"check": spec.key},
        source=make_assertion_source(),
        customAssertion=custom_assertion,
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=assertion_info)


def build_assertion_run_event_mcp(
    run: CheckRun,
    assertion_urn: str,
    dataset_urn: str,
) -> MetadataChangeProposalWrapper:
    run_event = AssertionRunEventClass(
        timestampMillis=run.timestamp_millis,
        assertionUrn=assertion_urn,
        asserteeUrn=dataset_urn,
        runId=run.run_id,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=run.result_type,
            actualAggValue=run.actual_agg_value,
            nativeResults=run.native_results or None,
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
