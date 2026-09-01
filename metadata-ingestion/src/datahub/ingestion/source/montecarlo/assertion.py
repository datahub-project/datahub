import functools
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Type

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
)
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.constants import (
    MC_METRIC_TO_STD_AGGREGATION,
    MC_OPERATOR_TO_STD_OPERATOR,
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
    """An assertion emitted for a monitor, plus the dataset it targets, so a later
    alert can build a run event against both."""

    assertion_urn: str
    dataset_urn: str


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
    # _std_parameters only populates the structured slots for the standard
    # operators (BETWEEN / scalar). For unmapped / _NATIVE_ operators it
    # returns None when there's no scalar threshold — but a range-style
    # native operator (e.g. OUTSIDE_RANGE) sets lower_threshold / upper_threshold
    # instead of threshold, so those would be silently dropped. Surface them on
    # nativeParameters so native range comparisons keep their bounds. The
    # scalar threshold case is already handled by _std_parameters' fallthrough
    # (parameters.value), so it is not duplicated here.
    # Coerced to str since nativeParameters is map[str, str].
    if operator is _NATIVE_OPERATOR:
        if first.lower_threshold is not None:
            native_parameters["lower_threshold"] = str(first.lower_threshold)
        if first.upper_threshold is not None:
            native_parameters["upper_threshold"] = str(first.upper_threshold)

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
        for mcon in definition.entity_mcons:
            dataset_urn = self.resolver.dataset_urn_for_mcon(mcon)
            if dataset_urn:
                break
        if dataset_urn is None:
            return

        assertion_urn = self._assertion_urn(definition.uuid)

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
        # Register only after the emit succeeded: if _emit_assertion raises, the
        # assertion is never created this run, and a dangling entry here would let
        # build_run_event attach a failure run event to a non-existent assertion.
        self._ingested_by_monitor[definition.uuid] = _IngestedAssertion(
            assertion_urn=assertion_urn, dataset_urn=dataset_urn
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
        # An alert can reference multiple monitors; use the first one we actually
        # ingested an assertion for. Storing only monitor_uuids[0] previously
        # dropped incidents whose first listed monitor was filtered out /
        # unresolved even when a later one was ingested.
        ingested: Optional[_IngestedAssertion] = None
        for monitor_uuid in alert.monitor_uuids:
            candidate = self._ingested_by_monitor.get(monitor_uuid)
            if candidate is not None:
                ingested = candidate
                break
        if ingested is None:
            # Alert references no monitor we ingested (all filtered out or
            # unresolved assets).
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
