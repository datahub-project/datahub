import functools
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Type

from datahub.emitter.mce_builder import (
    make_assertion_source,
    make_assertion_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import DatahubKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.montecarlo.client import (
    MonteCarloAlert,
    MonteCarloAssertionDef,
    MonteCarloComparison,
    MonteCarloJobExecution,
    MonteCarloMetricPoint,
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.constants import (
    CUSTOM_METRIC_PREFIX,
    MC_METRIC_TO_AGG_VALUE,
    MC_METRIC_TO_RESULT_SLOT,
    MC_METRIC_TO_STD_AGGREGATION,
    MC_OPERATOR_TO_STD_OPERATOR,
    TABLE_METRICS_TO_FETCH,
)
from datahub.ingestion.source.montecarlo.mcon_resolver import MconResolver
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    DatasetAssertionScopeClass,
)
from datahub.utilities.time import datetime_to_ts_millis

PLATFORM = "montecarlo"

# Sentinel for MC operators/metrics with no clean DataHub equivalent. Using the
# real enum member (not a bare string) keeps the generated aspect consistent
# with dbt's unknown-test fallback and lets the UI render a native description.
_NATIVE_OPERATOR = AssertionStdOperatorClass._NATIVE_
_NATIVE_AGGREGATION = AssertionStdAggregationClass._NATIVE_


@dataclass(frozen=True)
class _IngestedAssertion:
    """An assertion emitted for a monitor, plus the dataset it targets and the
    resolved mcon / definition, so a later run-event phase can fetch measured
    metric values via getMetricsV4 (which needs the mcon) and know which metric
    names to query (from the definition's comparisons)."""

    assertion_urn: str
    dataset_urn: str
    mcon: Optional[str]
    definition: MonteCarloAssertionDef


@functools.lru_cache(maxsize=1)
def _load_cloud_assertion_class() -> Optional[Type]:
    """Return the DataHub Cloud ``Assertion`` entity class if available, cached.

    The connector prefers the Cloud SDK's assertion entity (it manages the
    assertionInfo + dataPlatformInstance aspects). When ``acryl-datahub-cloud`` is
    not installed we fall back to emitting equivalent OSS aspects directly, mirroring
    the optional-import pattern in ``datahub.sdk.main_client``. Only ImportError (the
    "not installed" case) is caught here -- any other exception (e.g. a broken or
    incompatible install) propagates up to the caller, where source.py's per-item
    error handling reports it instead of silently downgrading to the OSS path.
    """
    try:
        from acryl_datahub_cloud.sdk.entities.assertion import (  # type: ignore[import-not-found]
            Assertion,
        )

        return Assertion
    except ImportError:
        return None


def _string_map(value: Dict[str, Any]) -> Dict[str, str]:
    """Coerce a dict's values to strings, dropping None values — nativeParameters
    is ``map[string, string]`` so non-string/None values must be normalized."""
    return {k: str(v) for k, v in value.items() if v is not None}


def _native_parameters(definition: MonteCarloAssertionDef) -> Dict[str, str]:
    """Native MC fields that don't map to the structured assertion slots, carried
    on nativeParameters so the UI can render them. severity / data_quality_dimension
    / resource_id are monitor-level; comparisonType / metric / customMetric are
    added by the comparisons path when present."""
    params: Dict[str, Any] = {
        "severity": definition.severity,
        "data_quality_dimension": definition.data_quality_dimension,
        "resource_id": definition.resource_id,
    }
    return _string_map(params)


def _field_urns(dataset_urn: str, comparison: MonteCarloComparison) -> List[str]:
    """Build schema-field URNs for the comparison's column references. MC's
    ``field`` is a single column, ``fields`` is multi-column; both are optional
    (table-level / row-predicate checks carry neither). Field URNs are best-effort
    by name (CustomAssertionInfo.field/fields have @UrnValidation exist:false)."""
    names: List[str] = []
    if comparison.field:
        names.append(comparison.field)
    names.extend(f for f in comparison.fields if f)
    return [make_schema_field_urn(dataset_urn, name) for name in names]


def _std_parameters(
    operator: str, comparison: MonteCarloComparison
) -> Optional[AssertionStdParametersClass]:
    """Map MC thresholds onto AssertionStdParameters for the operators that need
    them. BETWEEN (INSIDE_RANGE) needs minValue+maxValue; the scalar comparison
    operators (EQUAL_TO/GREATER_THAN/...) need a single value. Operators that take
    no parameters (NULL/NOT_NULL) return None. Unmapped/_NATIVE_ operators also
    return None — the threshold is preserved on nativeParameters instead."""
    if operator == AssertionStdOperatorClass.BETWEEN:
        lo = comparison.lower_threshold
        hi = comparison.upper_threshold
        if lo is None or hi is None:
            return None
        return AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(value=str(lo), type="NUMBER"),
            maxValue=AssertionStdParameterClass(value=str(hi), type="NUMBER"),
        )
    scalar = comparison.threshold
    if scalar is None:
        return None
    return AssertionStdParametersClass(
        value=AssertionStdParameterClass(value=str(scalar), type="NUMBER"),
    )


def _make_custom_assertion_info(
    *,
    entity_urn: str,
    native_type: str,
    definition: MonteCarloAssertionDef,
) -> CustomAssertionInfoClass:
    """Build a CustomAssertionInfo from a Monte Carlo monitor/rule.

    Only ``comparisons[0]`` is mapped onto the structured slots
    (scope/operator/aggregation/fields/parameters); any remaining comparisons
    are folded into ``logic`` as JSON so a compound rule is still fully
    represented. Scope is DATASET_COLUMN when ``comparisons[0]`` carries a
    column reference (field/fields) and DATASET_ROWS otherwise.

    The no-comparisons path falls back to DATASET_ROWS with ``_NATIVE_``
    operator/aggregation (mirroring dbt's unknown-test-no-column pattern) so
    the shared structured-rendering path still fires, and populates
    nativeType/nativeParameters so native MC fields remain available on the
    profile page.
    """
    native_parameters = _native_parameters(definition)
    logic: Optional[str] = definition.custom_sql

    comparisons = definition.comparisons
    if not comparisons:
        # No comparisons → no field/fields, so scope is DATASET_ROWS. Mirrors
        # dbt's unknown-test-no-column fallback (dbt_tests.py: DATASET_ROWS +
        # _NATIVE_ operator/aggregation), so the shared structured-rendering
        # path fires cleanly (DATASET_ROWS + _NATIVE_ aggregation → 'rows'
        # descriptor, no UI console-error) and nativeType/nativeParameters
        # still carry the native MC fields.
        return CustomAssertionInfoClass(
            type=native_type,
            entity=entity_urn,
            scope=DatasetAssertionScopeClass.DATASET_ROWS,
            operator=_NATIVE_OPERATOR,
            aggregation=_NATIVE_AGGREGATION,
            nativeType=native_type,
            nativeParameters=native_parameters or None,
            logic=logic,
        )

    first = comparisons[0]
    field_urns = _field_urns(entity_urn, first)
    has_column = bool(field_urns)
    scope = (
        DatasetAssertionScopeClass.DATASET_COLUMN
        if has_column
        else DatasetAssertionScopeClass.DATASET_ROWS
    )
    operator = (
        MC_OPERATOR_TO_STD_OPERATOR.get(
            (first.operator or "").upper(), _NATIVE_OPERATOR
        )
        if first.operator
        else _NATIVE_OPERATOR
    )
    aggregation = (
        MC_METRIC_TO_STD_AGGREGATION.get(
            (first.metric or "").lower(), _NATIVE_AGGREGATION
        )
        if first.metric
        else _NATIVE_AGGREGATION
    )
    parameters = _std_parameters(operator, first)

    # Fold any remaining comparisons into logic so a compound rule is fully
    # represented. Keep custom_sql first when present, then the JSON.
    extra = comparisons[1:]
    if extra:
        rest_json = json.dumps(
            [c.model_dump(mode="json") for c in extra], separators=(",", ":")
        )
        logic = rest_json if not logic else f"{logic}\n{rest_json}"

    # Carry the first comparison's native specifics on nativeParameters too, so
    # they're renderable even when operator/aggregation fall back to _NATIVE_.
    # Only non-None string values are added — nativeParameters is map[str,str].
    if first.comparison_type:
        native_parameters["comparison_type"] = first.comparison_type
    if first.metric:
        native_parameters["metric"] = first.metric
    if first.custom_metric:
        native_parameters["custom_metric"] = first.custom_metric

    return CustomAssertionInfoClass(
        type=native_type,
        entity=entity_urn,
        field=field_urns[0] if field_urns else None,
        fields=field_urns or None,
        scope=scope,
        operator=operator,
        aggregation=aggregation,
        parameters=parameters,
        nativeType=native_type,
        nativeParameters=native_parameters or None,
        logic=logic,
    )


class MonteCarloAssertionKey(DatahubKey):
    """Key for deterministic, stable assertion GUIDs across ingestion runs."""

    platform: str = PLATFORM
    monitor_uuid: str
    instance: Optional[str] = None


class MonteCarloAssertionBuilder:
    """Builds DataHub assertion workunits from Monte Carlo monitors/rules and alerts."""

    def __init__(
        self,
        config: MonteCarloSourceConfig,
        report: MonteCarloSourceReport,
        resolver: MconResolver,
    ) -> None:
        self.config = config
        self.report = report
        self.resolver = resolver
        # Maps a monitor/rule uuid to the assertion (and its target dataset) we
        # emitted for it, so alerts can attach run events to the same entities.
        self._ingested_by_monitor: Dict[str, _IngestedAssertion] = {}

    def _assertion_urn(self, monitor_uuid: str) -> str:
        key = MonteCarloAssertionKey(
            monitor_uuid=monitor_uuid,
            instance=self.config.platform_instance,
        )
        return make_assertion_urn(key.guid())

    def build_assertion(
        self, definition: MonteCarloAssertionDef
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.monitor_pattern.allowed(definition.name or definition.uuid):
            self.report.report_dropped(definition.name or definition.uuid)
            return

        # Resolve the monitored asset. We use the first MCON that resolves to a URN;
        # a monitor without a resolvable asset is skipped with a warning.
        if not definition.entity_mcons:
            self.report.warning(
                title="Monitor has no monitored entities",
                message="Skipping monitor with no entity_mcons; cannot build a dataset URN.",
                context=f"monitor_uuid={definition.uuid}",
            )
            return

        dataset_urn: Optional[str] = None
        resolved_mcon: Optional[str] = None
        for mcon in definition.entity_mcons:
            dataset_urn = self.resolver.dataset_urn_for_mcon(mcon)
            if dataset_urn:
                resolved_mcon = mcon
                break
        if dataset_urn is None:
            return

        assertion_urn = self._assertion_urn(definition.uuid)
        self._ingested_by_monitor[definition.uuid] = _IngestedAssertion(
            assertion_urn=assertion_urn,
            dataset_urn=dataset_urn,
            mcon=resolved_mcon,
            definition=definition,
        )

        # customProperties keeps only the DataHub-side correlation key; native MC
        # fields move to nativeType/nativeParameters. mc_monitor_uuid is a
        # DataHub-internal key for alert/run-event wiring, not a native field.
        custom_properties: Dict[str, str] = {"mc_monitor_uuid": definition.uuid}

        custom_assertion = _make_custom_assertion_info(
            entity_urn=dataset_urn,
            native_type=definition.native_type,
            definition=definition,
        )
        yield from self._emit_assertion(
            assertion_urn=assertion_urn,
            custom_assertion=custom_assertion,
            description=definition.description,
            custom_properties=custom_properties,
        )
        self.report.report_assertion_emitted()

    def _emit_assertion(
        self,
        assertion_urn: str,
        custom_assertion: CustomAssertionInfoClass,
        description: Optional[str],
        custom_properties: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        assertion_cls = _load_cloud_assertion_class()
        if assertion_cls is not None:
            assertion = assertion_cls(
                id=assertion_urn,
                info=custom_assertion,
                description=description,
                custom_properties=custom_properties,
                source=make_assertion_source(),
                platform=PLATFORM,
                platform_instance=self.config.platform_instance,
            )
            yield from assertion.as_workunits()
        else:
            yield from self._emit_assertion_oss(
                assertion_urn, custom_assertion, description, custom_properties
            )

    def _emit_assertion_oss(
        self,
        assertion_urn: str,
        custom_assertion: CustomAssertionInfoClass,
        description: Optional[str],
        custom_properties: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """Fallback used when acryl-datahub-cloud is not installed.

        Emits the same assertionInfo + dataPlatformInstance aspects (as primary
        workunits) that the Cloud ``Assertion`` entity emits, so the connector
        output is identical whether or not the Cloud SDK is present. Emitting as
        primary also lets ``auto_status_aspect`` add the status aspect.
        """
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.CUSTOM,
            customAssertion=custom_assertion,
            source=make_assertion_source(),
            description=description,
            customProperties=custom_properties,
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        ).as_workunit()

        platform_instance = DataPlatformInstance(
            platform=make_data_platform_urn(PLATFORM),
            instance=(
                make_dataplatform_instance_urn(PLATFORM, self.config.platform_instance)
                if self.config.platform_instance
                else None
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=platform_instance,
        ).as_workunit()

    def build_run_event(self, alert: MonteCarloAlert) -> Iterable[MetadataWorkUnit]:
        if alert.monitor_uuid is None:
            return
        ingested = self._ingested_by_monitor.get(alert.monitor_uuid)
        if ingested is None:
            # Alert for a monitor we didn't ingest (filtered out, unresolved asset).
            return
        assertion_urn = ingested.assertion_urn
        dataset_urn = ingested.dataset_urn
        if alert.created_time is None:
            self.report.warning(
                title="Alert skipped: missing timestamp",
                message="Alert has no createdTime and cannot be emitted as a run event.",
                context=f"alert_uuid={alert.uuid}",
            )
            return

        native_results: Dict[str, str] = {}
        if alert.severity:
            native_results["severity"] = alert.severity
        if alert.priority:
            native_results["priority"] = alert.priority
        if alert.sub_types:
            native_results["subType"] = ",".join(alert.sub_types)

        run_event = AssertionRunEvent(
            timestampMillis=datetime_to_ts_millis(alert.created_time),
            runId=alert.uuid,
            asserteeUrn=dataset_urn,
            status=AssertionRunStatus.COMPLETE,
            assertionUrn=assertion_urn,
            result=AssertionResult(
                type=AssertionResultType.FAILURE,
                nativeResults=native_results or None,
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=run_event,
        ).as_workunit(is_primary_source=False)
        self.report.report_run_event_emitted()

    def build_run_events_from_execution(
        self,
        execution: MonteCarloJobExecution,
        metric_points: List[MonteCarloMetricPoint],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit an AssertionRunEvent for a SUCCESS monitor run, carrying the
        measured metric values on AssertionResult.

        The measured value is attached to the latest SUCCESS run as a best-effort
        temporal correlation. getMetricsV4 does not populate jobExecutionUuid
        for table-level metrics, so a per-run join is not possible — the value
        is "the most recent measurement" on "the most recent successful run",
        approximately but not provably from the same run.

        Emitted with is_primary_source=False so stale entity removal never
        touches run events — they are not tracked in the stale-removal state
        and are added to urns_to_skip, matching the alert path (build_run_event).
        The assertion entity itself is still subject to stale removal via the
        primary-source definition path (build_assertion), which is correct: if
        the monitor disappears from Monte Carlo, the assertion should be
        soft-deleted.
        """
        ingested = self._ingested_by_monitor.get(execution.monitor_uuid)
        if ingested is None:
            return
        assertion_urn = ingested.assertion_urn
        dataset_urn = ingested.dataset_urn

        ts = execution.start_time or execution.end_time
        if ts is None:
            self.report.warning(
                title="Run event skipped: missing timestamp",
                message="Job execution has no startTime/endTime; cannot emit "
                "a run event.",
                context=f"job_execution_uuid={execution.job_execution_uuid}",
            )
            return

        native_results: Dict[str, str] = {}
        row_count: Optional[int] = None
        missing_count: Optional[int] = None
        unexpected_count: Optional[int] = None
        actual_agg: Optional[float] = None

        for mp in metric_points:
            slot = MC_METRIC_TO_RESULT_SLOT.get(mp.metric.lower())
            key = mp.field or mp.metric
            if slot == "rowCount":
                row_count = int(mp.value)
            elif slot == "missingCount":
                missing_count = int(mp.value)
            elif slot == "unexpectedCount":
                unexpected_count = int(mp.value)
            elif mp.metric.lower() in MC_METRIC_TO_AGG_VALUE:
                actual_agg = mp.value
            else:
                native_results[key] = str(mp.value)
            if mp.upper_threshold is not None:
                native_results[f"{key}_threshold_upper"] = str(mp.upper_threshold)
            if mp.lower_threshold is not None:
                native_results[f"{key}_threshold_lower"] = str(mp.lower_threshold)

        if execution.exceptions:
            native_results["exceptions"] = execution.exceptions
        if execution.total_result_count is not None:
            native_results["totalResultCount"] = str(execution.total_result_count)
        if execution.evaluated_record_count is not None:
            native_results["evaluatedRecordCount"] = str(
                execution.evaluated_record_count
            )

        run_event = AssertionRunEvent(
            timestampMillis=datetime_to_ts_millis(ts),
            runId=execution.job_execution_uuid,
            asserteeUrn=dataset_urn,
            status=AssertionRunStatus.COMPLETE,
            assertionUrn=assertion_urn,
            result=AssertionResult(
                type=AssertionResultType.SUCCESS,
                rowCount=row_count,
                missingCount=missing_count,
                unexpectedCount=unexpected_count,
                actualAggValue=actual_agg,
                nativeResults=native_results or None,
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=run_event,
        ).as_workunit(is_primary_source=False)
        self.report.report_run_event_emitted()

    def metric_names_for_monitor(self, definition: MonteCarloAssertionDef) -> List[str]:
        """Return the getMetricsV4 metricName values to fetch for a monitor,
        filtered per decision 1c: skip custom_value_based_metric_* (returns 0
        points), and for TABLE monitors fetch only total_row_count (the other
        three comparison metrics are non-standard names that return 0 points).
        """
        if not definition.comparisons:
            return []
        is_table = (definition.monitor_type or "").upper() == "TABLE"
        names: List[str] = []
        seen: Set[str] = set()
        for comp in definition.comparisons:
            metric = (comp.metric or "").lower()
            if not metric or metric.startswith(CUSTOM_METRIC_PREFIX):
                continue
            if is_table and metric not in TABLE_METRICS_TO_FETCH:
                continue
            if metric not in seen:
                seen.add(metric)
                names.append(metric)
        return names

    @staticmethod
    def field_for_metric(
        definition: MonteCarloAssertionDef, metric_name: str
    ) -> Optional[str]:
        """Return the field for a given metric name from the definition's
        comparisons, for field-level metrics (null_rate, distinct_count, etc.)
        that require a field filter on getMetricsV4."""
        target = metric_name.lower()
        for comp in definition.comparisons:
            if (comp.metric or "").lower() == target:
                if comp.field:
                    return comp.field
                if comp.fields:
                    return comp.fields[0]
        return None

    def iter_ingested_monitors(
        self,
    ) -> Iterable[tuple]:
        """Yield (monitor_uuid, _IngestedAssertion) pairs for all monitors
        ingested in this run, so the source's run-event phase can iterate
        them without accessing the private map directly."""
        yield from self._ingested_by_monitor.items()
