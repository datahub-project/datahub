import logging
import re
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

from datahub.ingestion.source.montecarlo.queries import (
    ASSET_FILTER_INLINE_SELECTION,
    TABLE_MONITOR_QUERY,
)
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)


class MonteCarloGraphQLType(StrEnum):
    MONITOR = "Monitor"
    CUSTOM_RULE = "CustomRule"
    ALERT = "Alert"
    WAREHOUSE_TABLE = "WarehouseTable"
    TABLE_MONITOR = "TableMonitor"
    ASSET_SELECTION = "AssetSelection"
    ASSET_SELECTION_RESULT = "AssetSelectionResult"
    SCHEMA_CHANGE = "SchemaChange"
    JOB_EXECUTION_HISTORY_LOG = "JobExecutionHistoryLog"
    METRIC = "TableMetricV2"


@dataclass(frozen=True)
class FieldSpec:
    """One GraphQL field the connector may request and parse."""

    name: str
    aliases: Tuple[str, ...] = ()
    critical: bool = False
    impact: str = ""
    in_fallback: bool = True
    # Union member fields selected only via inline fragments; never drift-diffed.
    fragment_only: bool = False
    children: Tuple["FieldSpec", ...] = ()


@dataclass(frozen=True)
class TypeSpec:
    type_name: MonteCarloGraphQLType
    fields: Tuple[FieldSpec, ...]


_CUSTOM_METRIC_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec(name="uuid"),
    FieldSpec(name="metricName"),
)

COMPARISONS_FIELD = FieldSpec(
    name="comparisons",
    impact="assertion operator, thresholds and column scope; measured run events",
    in_fallback=True,
    children=(
        FieldSpec(name="comparisonType"),
        FieldSpec(name="operator"),
        FieldSpec(name="metric"),
        FieldSpec(
            name="customMetric",
            children=_CUSTOM_METRIC_FIELDS,
        ),
        FieldSpec(name="field"),
        FieldSpec(name="fields"),
        FieldSpec(name="threshold"),
        FieldSpec(name="upperThreshold"),
        FieldSpec(name="lowerThreshold"),
    ),
)

MONITOR_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.MONITOR,
    fields=(
        # uuid and entityMcons are required to build assertions and attach datasets.
        FieldSpec(name="uuid", critical=True),
        FieldSpec(name="name"),
        FieldSpec(name="description"),
        FieldSpec(
            name="monitorType",
            impact="assertion type, monitor_type_pattern filtering, TABLE monitor scope",
        ),
        FieldSpec(
            name="whereCondition",
            impact="row filter on nativeParameters",
        ),
        FieldSpec(name="entityMcons", critical=True),
        FieldSpec(
            name="resourceId",
            impact="nativeParameters resource_id slot",
        ),
        FieldSpec(
            name="priority",
            aliases=("severity",),
            impact="nativeParameters severity slot",
        ),
        FieldSpec(name="dataQualityDimension", in_fallback=True),
        COMPARISONS_FIELD,
    ),
)

CUSTOM_RULE_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.CUSTOM_RULE,
    fields=(
        FieldSpec(name="uuid", critical=True),
        FieldSpec(name="ruleName"),
        FieldSpec(name="ruleType"),
        FieldSpec(name="description"),
        FieldSpec(name="customSql", impact="assertion logic (the rule's SQL)"),
        FieldSpec(name="entityMcons", critical=True),
        FieldSpec(
            name="priority",
            aliases=("severity",),
            impact="nativeParameters severity slot",
        ),
        COMPARISONS_FIELD,
    ),
)

ALERT_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.ALERT,
    fields=(
        FieldSpec(name="id", critical=True),
        FieldSpec(name="type"),
        FieldSpec(name="subTypes"),
        FieldSpec(name="severity"),
        FieldSpec(name="priority"),
        FieldSpec(name="status"),
        # build_run_event cannot emit a run event without a timestamp.
        FieldSpec(name="createdTime", critical=True),
        FieldSpec(name="title"),
        FieldSpec(name="monitorUuids", critical=True),
        FieldSpec(
            name="assets",
            children=(FieldSpec(name="mcon"),),
        ),
    ),
)

_ASSET_FILTER_INTERFACE_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec(name="type", critical=True),
    FieldSpec(name="negated"),
)

_ASSET_FILTER_FRAGMENT_FIELDS: Tuple[FieldSpec, ...] = (
    FieldSpec(name="tableName", fragment_only=True, in_fallback=False),
    FieldSpec(name="tableNameOperator", fragment_only=True, in_fallback=False),
    FieldSpec(name="fullTableId", fragment_only=True, in_fallback=False),
    FieldSpec(name="tableType", fragment_only=True, in_fallback=False),
    FieldSpec(name="tableTags", fragment_only=True, in_fallback=False),
    FieldSpec(name="tableTagsOperator", fragment_only=True, in_fallback=False),
    FieldSpec(name="readDays", fragment_only=True, in_fallback=False),
    FieldSpec(name="writeDays", fragment_only=True, in_fallback=False),
    FieldSpec(name="readWriteDays", fragment_only=True, in_fallback=False),
    FieldSpec(name="readAndWriteDays", fragment_only=True, in_fallback=False),
    FieldSpec(name="volumeChangeDays", fragment_only=True, in_fallback=False),
    FieldSpec(name="readActivityIsNull", fragment_only=True, in_fallback=False),
    FieldSpec(name="writeActivityIsNull", fragment_only=True, in_fallback=False),
    FieldSpec(name="readWriteActivityIsNull", fragment_only=True, in_fallback=False),
    FieldSpec(name="readAndWriteActivityIsNull", fragment_only=True, in_fallback=False),
)

_ASSET_FILTER_MEMBER_FIELDS: Tuple[FieldSpec, ...] = (
    _ASSET_FILTER_INTERFACE_FIELDS + _ASSET_FILTER_FRAGMENT_FIELDS
)

ASSET_SELECTION_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.ASSET_SELECTION,
    fields=(
        FieldSpec(name="filtersJoiner"),
        FieldSpec(name="exclusionsJoiner"),
        FieldSpec(
            name="databases",
            children=(
                FieldSpec(name="name"),
                FieldSpec(name="schemas"),
            ),
        ),
        FieldSpec(name="filters", children=_ASSET_FILTER_MEMBER_FIELDS),
        FieldSpec(name="exclusions", children=_ASSET_FILTER_MEMBER_FIELDS),
    ),
)

TABLE_MONITOR_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.TABLE_MONITOR,
    fields=(
        FieldSpec(name="warehouseUuid", critical=True),
        FieldSpec(
            name="assetSelection",
            critical=True,
            children=ASSET_SELECTION_TYPE_SPEC.fields,
        ),
    ),
)

WAREHOUSE_TABLE_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.WAREHOUSE_TABLE,
    fields=(
        FieldSpec(name="mcon"),
        FieldSpec(name="fullTableId", critical=True),
        FieldSpec(
            name="warehouse",
            children=(FieldSpec(name="connectionType"),),
        ),
    ),
)

ASSET_SELECTION_RESULT_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.ASSET_SELECTION_RESULT,
    fields=(
        FieldSpec(name="selected", critical=True),
        FieldSpec(name="mcon", critical=True),
    ),
)

SCHEMA_CHANGE_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.SCHEMA_CHANGE,
    fields=(
        FieldSpec(name="mcon", critical=True),
        FieldSpec(name="startTime", critical=True),
        FieldSpec(name="fieldsAdded"),
        FieldSpec(name="fieldsRemoved"),
        FieldSpec(
            name="fieldTypeChanges",
            children=(
                FieldSpec(name="fieldName"),
                FieldSpec(name="oldFieldType"),
                FieldSpec(name="newFieldType"),
            ),
        ),
    ),
)

JOB_EXECUTION_HISTORY_LOG_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.JOB_EXECUTION_HISTORY_LOG,
    fields=(
        FieldSpec(name="jobExecutionUuid", critical=True),
        FieldSpec(name="startTime", critical=True),
        FieldSpec(name="endTime"),
        FieldSpec(name="status"),
        FieldSpec(name="exceptions"),
        FieldSpec(name="totalResultCount"),
        FieldSpec(name="evaluatedRecordCount"),
    ),
)

METRIC_TYPE_SPEC = TypeSpec(
    type_name=MonteCarloGraphQLType.METRIC,
    fields=(
        FieldSpec(name="metric", critical=True),
        FieldSpec(name="value", critical=True),
        FieldSpec(name="field"),
        FieldSpec(name="timestamp"),
        FieldSpec(name="measurementTimestamp"),
        FieldSpec(
            name="thresholds",
            children=(
                FieldSpec(name="upper"),
                FieldSpec(name="lower"),
            ),
        ),
        FieldSpec(name="jobExecutionUuid"),
    ),
)

TYPE_SPECS: Dict[MonteCarloGraphQLType, TypeSpec] = {
    MonteCarloGraphQLType.MONITOR: MONITOR_TYPE_SPEC,
    MonteCarloGraphQLType.CUSTOM_RULE: CUSTOM_RULE_TYPE_SPEC,
    MonteCarloGraphQLType.ALERT: ALERT_TYPE_SPEC,
    MonteCarloGraphQLType.WAREHOUSE_TABLE: WAREHOUSE_TABLE_TYPE_SPEC,
    MonteCarloGraphQLType.TABLE_MONITOR: TABLE_MONITOR_TYPE_SPEC,
    MonteCarloGraphQLType.ASSET_SELECTION: ASSET_SELECTION_TYPE_SPEC,
    MonteCarloGraphQLType.ASSET_SELECTION_RESULT: ASSET_SELECTION_RESULT_TYPE_SPEC,
    MonteCarloGraphQLType.SCHEMA_CHANGE: SCHEMA_CHANGE_TYPE_SPEC,
    MonteCarloGraphQLType.JOB_EXECUTION_HISTORY_LOG: JOB_EXECUTION_HISTORY_LOG_TYPE_SPEC,
    MonteCarloGraphQLType.METRIC: METRIC_TYPE_SPEC,
}


def type_spec_by_graphql_name(name: str) -> Optional[TypeSpec]:
    try:
        return TYPE_SPECS.get(MonteCarloGraphQLType(name))
    except ValueError:
        return None


_CAMEL_TO_SNAKE = re.compile(r"(?<!^)(?=[A-Z])")


def _camel_to_snake(name: str) -> str:
    return _CAMEL_TO_SNAKE.sub("_", name).lower()


def spec_value(raw: Optional[Dict[str, object]], spec: FieldSpec) -> object:
    """Read a field from a pycarlo snake_cased row (canonical name, then aliases)."""
    if not isinstance(raw, dict):
        return None
    for graphql_name in (spec.name, *spec.aliases):
        key = _camel_to_snake(graphql_name)
        if key in raw:
            return raw[key]
    return None


def field_spec_by_name(type_spec: TypeSpec, name: str) -> FieldSpec:
    for spec in type_spec.fields:
        if spec.name == name:
            return spec
    raise KeyError(f"no FieldSpec named {name!r} on {type_spec.type_name}")


def field_spec_at_path(type_spec: TypeSpec, dotted_path: str) -> Optional[FieldSpec]:
    parts = dotted_path.split(".")
    fields: Tuple[FieldSpec, ...] = type_spec.fields
    spec: Optional[FieldSpec] = None
    for part in parts:
        spec = None
        for candidate in fields:
            if candidate.name == part:
                spec = candidate
                break
        if spec is None:
            return None
        fields = spec.children
    return spec


def format_degraded_missing_message(
    type_spec: TypeSpec, missing_paths: Iterable[str]
) -> str:
    lines: list = []
    for path in missing_paths:
        spec = field_spec_at_path(type_spec, path)
        if spec is not None and spec.impact:
            lines.append(f"{path} ({spec.impact})")
        else:
            lines.append(path)
    return ", ".join(lines)


def iter_leaf_field_specs(
    fields: Tuple[FieldSpec, ...], prefix: str = ""
) -> Iterable[Tuple[str, FieldSpec]]:
    for spec in fields:
        path = f"{prefix}.{spec.name}" if prefix else spec.name
        if spec.children:
            yield from iter_leaf_field_specs(spec.children, path)
        else:
            yield path, spec


def _all_graphql_names(spec: FieldSpec) -> Tuple[str, ...]:
    return (spec.name, *spec.aliases)


def _live_graphql_name(
    spec: FieldSpec, live_field_names: Mapping[str, Any]
) -> Optional[str]:
    for name in _all_graphql_names(spec):
        if name in live_field_names:
            return name
    return None


def _spec_graphql_names(type_spec: TypeSpec) -> frozenset:
    names: set = set()

    def walk(fields: Tuple[FieldSpec, ...]) -> None:
        for spec in fields:
            names.update(_all_graphql_names(spec))
            if spec.children:
                walk(spec.children)

    walk(type_spec.fields)
    return frozenset(names)


class DriftVerdict(StrEnum):
    """Outcome of a schema-drift check for one type.

    PROCEED: no drift; the live schema exposes every desired field.
    DEGRADED: optional fields are missing; ingestion proceeds without them (the
      query builder already dropped them, and every consumer handles None). A
      warning is reported naming the dropped fields.
    ABORT: a critical field is missing, or strict_schema_drift is enabled and any
      drift was found. The phase must stop cleanly rather than emit
      malformed/empty records.
    """

    PROCEED = "PROCEED"
    DEGRADED = "DEGRADED"
    ABORT = "ABORT"


@dataclass
class TypeDrift:
    """Per-type drift result. missing = desired fields absent from the live
    schema; new_fields = live fields the connector does not request (an under-
    ingestion signal, surfaced as info, not actionable for the operator).
    critical_missing = the subset of missing that is critical for this type."""

    type_name: str
    verdict: DriftVerdict
    missing: List[str] = field(default_factory=list)
    critical_missing: List[str] = field(default_factory=list)
    new_fields: List[str] = field(default_factory=list)
    type_absent: bool = False
    introspection_failed: bool = False

    def summary(self) -> str:
        parts = [f"type={self.type_name}", f"verdict={self.verdict.value}"]
        if self.missing:
            parts.append(f"missing={','.join(self.missing)}")
        if self.critical_missing:
            parts.append(f"critical_missing={','.join(self.critical_missing)}")
        if self.new_fields:
            parts.append(f"new_uningested={','.join(self.new_fields)}")
        return ", ".join(parts)


def drift_abort_title(type_drift: TypeDrift, strict: bool) -> str:
    if type_drift.type_absent:
        return (
            f"Monte Carlo schema drift: type {type_drift.type_name} not found "
            "in the Monte Carlo schema"
        )
    if type_drift.introspection_failed:
        return "Monte Carlo schema drift: could not verify schema"
    if type_drift.critical_missing:
        fields = ", ".join(type_drift.critical_missing)
        return f"Monte Carlo schema drift: critical fields missing ({fields})"
    if strict and type_drift.missing:
        return "Monte Carlo schema drift: strict mode: drift detected"
    return "Monte Carlo schema drift: schema drift aborted ingestion"


@dataclass
class SchemaDrift:
    """Aggregate drift across all checked types, with the overall verdict (the
    worst per-type verdict). ABORT dominates DEGRADED dominates PROCEED."""

    per_type: Dict[str, TypeDrift] = field(default_factory=dict)

    @property
    def verdict(self) -> DriftVerdict:
        if not self.per_type:
            return DriftVerdict.PROCEED
        if any(t.verdict == DriftVerdict.ABORT for t in self.per_type.values()):
            return DriftVerdict.ABORT
        if any(t.verdict == DriftVerdict.DEGRADED for t in self.per_type.values()):
            return DriftVerdict.DEGRADED
        return DriftVerdict.PROCEED

    def summary(self) -> str:
        return "; ".join(t.summary() for t in self.per_type.values())


_INTROSPECT_QUERY = """
query mcIntrospect($name: String!) {
  mcType: __type(name: $name) {
    fields {
      name
      type {
        kind
        name
        ofType { kind name ofType { kind name ofType { kind name } } }
      }
    }
  }
}
"""


@dataclass
class _TypeField:
    """One field of an introspected GraphQL type. is_composite is True when the
    field's (unwrapped) type is OBJECT, INTERFACE, or UNION; object_type_name
    is the GraphQL type name to introspect for sub-selection / drift."""

    name: str
    is_object: bool
    is_composite: bool
    object_type_name: Optional[str] = None


@dataclass
class _TypeShape:
    fields: Dict[str, _TypeField] = field(default_factory=dict)
    introspection_failed: bool = False
    type_absent: bool = False


def _unwrap_type(type_dict: Optional[Dict[str, object]]) -> tuple:
    node: Optional[Dict[str, object]] = type_dict
    while node is not None and node.get("kind") in ("NON_NULL", "LIST"):
        next_node = node.get("of_type") or node.get("ofType")
        if not isinstance(next_node, dict):
            node = None
            break
        node = next_node
    if node is None:
        return ("SCALAR", None)
    return (node.get("kind"), node.get("name"))


def _parse_type_shape(response: Dict[str, object]) -> _TypeShape:
    shape = _TypeShape()
    type_node = response.get("mc_type") or response.get("__type")
    if not isinstance(type_node, dict):
        return shape
    for f in type_node.get("fields") or []:
        if not isinstance(f, dict):
            continue
        name = f.get("name")
        if not name:
            continue
        kind, type_name = _unwrap_type(f.get("type"))
        is_object = kind == "OBJECT"
        is_composite = kind in ("OBJECT", "INTERFACE", "UNION")
        shape.fields[name] = _TypeField(
            name=name,
            is_object=is_object,
            is_composite=is_composite,
            object_type_name=type_name if is_composite else None,
        )
    return shape


def _emit_fallback_selection(
    fields: Tuple[FieldSpec, ...],
    indent: str,
) -> str:
    lines: list = []
    for spec in fields:
        if not spec.in_fallback:
            continue
        if spec.children:
            lines.append(f"{indent}{spec.name} {{")
            lines.append(_emit_fallback_selection(spec.children, indent + "    "))
            lines.append(f"{indent}}}")
        else:
            lines.append(f"{indent}{spec.name}")
    return "\n".join(lines)


def _emit_desired_selection(
    fields: Tuple[FieldSpec, ...],
    indent: str,
) -> str:
    """Emit a GraphQL selection from the spec without intersecting a live shape."""
    lines: list = []
    for spec in fields:
        if spec.children:
            lines.append(f"{indent}{spec.name} {{")
            lines.append(_emit_desired_selection(spec.children, indent + "    "))
            lines.append(f"{indent}}}")
        else:
            lines.append(f"{indent}{spec.name}")
    return "\n".join(lines)


def _build_selection_with_subshapes(
    fields: Tuple[FieldSpec, ...],
    shape: _TypeShape,
    object_shapes: Dict[str, _TypeShape],
    indent: str,
) -> str:
    lines: list = []
    for spec in fields:
        live_name = _live_graphql_name(spec, shape.fields)
        if live_name is None:
            continue
        live = shape.fields[live_name]
        if not spec.children:
            lines.append(f"{indent}{live_name}")
            continue
        if not live.is_composite or not live.object_type_name:
            continue
        sub_shape = object_shapes.get(live.object_type_name, _TypeShape())
        if sub_shape.introspection_failed:
            sub_lines = _emit_desired_selection(spec.children, indent + "    ")
        else:
            sub_lines = _build_selection_with_subshapes(
                spec.children, sub_shape, object_shapes, indent + "    "
            )
        if not sub_lines.strip():
            continue
        lines.append(f"{indent}{live_name} {{")
        lines.append(sub_lines)
        lines.append(f"{indent}}}")
    return "\n".join(lines)


def _collect_missing_paths(
    fields: Tuple[FieldSpec, ...],
    shape: _TypeShape,
    object_shapes: Dict[str, _TypeShape],
    prefix: str,
    critical_only: bool,
) -> Tuple[List[str], List[str]]:
    missing: List[str] = []
    critical_missing: List[str] = []
    for spec in fields:
        if spec.fragment_only:
            continue
        path = f"{prefix}.{spec.name}" if prefix else spec.name
        live_name = _live_graphql_name(spec, shape.fields)
        if live_name is None:
            if spec.critical or not critical_only:
                missing.append(path)
            if spec.critical:
                critical_missing.append(path)
            continue
        if not spec.children:
            continue
        live = shape.fields[live_name]
        if not live.is_composite or not live.object_type_name:
            if spec.critical or not critical_only:
                missing.append(path)
            if spec.critical:
                critical_missing.append(path)
            continue
        sub_shape = object_shapes.get(live.object_type_name)
        if sub_shape is None or sub_shape.introspection_failed:
            continue
        sub_missing, sub_critical = _collect_missing_paths(
            spec.children,
            sub_shape,
            object_shapes,
            path,
            critical_only=False,
        )
        missing.extend(sub_missing)
        critical_missing.extend(sub_critical)
    return missing, critical_missing


def _diff_drift(
    type_spec: TypeSpec,
    shape: _TypeShape,
    strict: bool,
    object_shapes: Optional[Dict[str, _TypeShape]] = None,
) -> TypeDrift:
    if shape.type_absent:
        return TypeDrift(
            type_name=type_spec.type_name.value,
            verdict=DriftVerdict.ABORT,
            type_absent=True,
        )
    shapes = object_shapes if object_shapes is not None else {}
    missing, critical_missing = _collect_missing_paths(
        type_spec.fields, shape, shapes, prefix="", critical_only=False
    )
    desired_names = _spec_graphql_names(type_spec)
    new_fields = [f for f in shape.fields if f not in desired_names]
    if critical_missing or missing and strict:
        verdict = DriftVerdict.ABORT
    elif missing:
        verdict = DriftVerdict.DEGRADED
    else:
        verdict = DriftVerdict.PROCEED
    return TypeDrift(
        type_name=type_spec.type_name.value,
        verdict=verdict,
        missing=missing,
        critical_missing=critical_missing,
        new_fields=new_fields,
    )


class QueryBuilder(Protocol):
    def monitors_query(self) -> str: ...

    def custom_rules_query(self) -> str: ...

    def alerts_query(self) -> str: ...

    def get_table_query(self) -> str: ...

    def table_monitor_query(self) -> str: ...

    def evaluate_asset_selection_query(self) -> str: ...

    def get_schema_changes_query(self) -> str: ...

    def get_job_executions_query(self) -> str: ...

    def get_metrics_v4_query(self) -> str: ...

    def check_drift(
        self,
        strict: bool = False,
        types: Optional[Sequence[MonteCarloGraphQLType]] = None,
    ) -> SchemaDrift: ...


_MONITORS_ENVELOPE = (
    "query getMonitors($domainIds: [UUID!], $limit: Int, $offset: Int) "
    "{{\n  getMonitors(domainIds: $domainIds, limit: $limit, offset: $offset) {{\n"
    "    {selection}\n  }}\n}}"
)
_CUSTOM_RULES_ENVELOPE = (
    "query getCustomRules($first: Int, $after: String) {{\n"
    "  getCustomRules(first: $first, after: $after) {{\n    edges {{\n"
    "      node {{\n        {selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)
_ALERTS_ENVELOPE = (
    "query getAlerts($first: Int, $after: String, $createdTime: DateTimeRangeInput) {{\n"
    "  getAlerts(first: $first, after: $after, createdTime: $createdTime) {{\n"
    "    edges {{\n      node {{\n        {selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)

_GET_TABLE_ENVELOPE = "query getTable($mcon: String) {{\n  getTable(mcon: $mcon) {{\n    {selection}\n  }}\n}}"
_TABLE_MONITOR_ENVELOPE = (
    "query getTableMonitor($monitorUuid: UUID!) {{\n"
    "  getTableMonitor(monitorUuid: $monitorUuid) {{\n    {selection}\n  }}\n}}"
)
_EVALUATE_ASSET_SELECTION_ENVELOPE = (
    "query evaluateAssetSelection(\n"
    "  $warehouseUuid: UUID!\n"
    "  $assetSelection: AssetSelectionInput!\n"
    "  $monitorUuid: UUID\n"
    "  $limit: Int\n"
    "  $offset: Int\n"
    ") {{\n"
    "  evaluateAssetSelection(\n"
    "    warehouseUuid: $warehouseUuid\n"
    "    assetSelection: $assetSelection\n"
    "    assetSelectionLevel: TABLE\n"
    "    monitorUuid: $monitorUuid\n"
    "    limit: $limit\n"
    "    offset: $offset\n"
    "  ) {{\n    {selection}\n  }}\n}}"
)
_GET_SCHEMA_CHANGES_ENVELOPE = (
    "query getSchemaChanges(\n"
    "  $mcon: String!\n"
    "  $startTime: DateTime\n"
    "  $endTime: DateTime\n"
    "  $first: Int\n"
    ") {{\n"
    "  getSchemaChanges(\n"
    "    mcon: $mcon\n"
    "    startTime: $startTime\n"
    "    endTime: $endTime\n"
    "    first: $first\n"
    "  ) {{\n"
    "    edges {{\n      node {{\n        {node_selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)
_GET_JOB_EXECUTIONS_ENVELOPE = (
    "query getJobExecutions($monitorUuid: String, $historyDays: Int, $first: Int) {{\n"
    "  getJobExecutions(monitorUuid: $monitorUuid, historyDays: $historyDays, "
    "first: $first) {{\n"
    "    edges {{\n      node {{\n        {node_selection}\n      }}\n    }}\n"
    "    pageInfo {{ hasNextPage endCursor }}\n  }}\n}}"
)
_GET_METRICS_V4_ENVELOPE = (
    "query getMetricsV4($metricName: String!, $metricsFilter: MetricsFilter!, "
    "$startTime: DateTime!, $endTime: DateTime, $first: Int, "
    "$deduplicateValues: Boolean) {{\n"
    "  getMetricsV4(metricName: $metricName, metricsFilter: $metricsFilter, "
    "startTime: $startTime, endTime: $endTime, first: $first, "
    "deduplicateValues: $deduplicateValues) {{\n"
    "    metrics {{\n      {metric_selection}\n    }}\n  }}\n}}"
)


def _wrap(envelope: str, selection: str) -> str:
    return envelope.format(selection=selection)


def monitors_fallback_query() -> str:
    selection = _emit_fallback_selection(MONITOR_TYPE_SPEC.fields, "    ")
    return _wrap(_MONITORS_ENVELOPE, selection)


_MONITOR_SELECTION_INDENT = "        "
_MONITOR_FALLBACK_INDENT = "    "


def custom_rules_fallback_query() -> str:
    selection = _emit_fallback_selection(CUSTOM_RULE_TYPE_SPEC.fields, "        ")
    return _wrap(_CUSTOM_RULES_ENVELOPE, selection)


def alerts_fallback_query() -> str:
    selection = _emit_fallback_selection(ALERT_TYPE_SPEC.fields, "        ")
    return _wrap(_ALERTS_ENVELOPE, selection)


def get_table_fallback_query() -> str:
    selection = _emit_fallback_selection(WAREHOUSE_TABLE_TYPE_SPEC.fields, "    ")
    return _wrap(_GET_TABLE_ENVELOPE, selection)


def evaluate_asset_selection_fallback_query() -> str:
    selection = _emit_fallback_selection(
        ASSET_SELECTION_RESULT_TYPE_SPEC.fields, "    "
    )
    return _wrap(_EVALUATE_ASSET_SELECTION_ENVELOPE, selection)


def get_schema_changes_fallback_query() -> str:
    node = _emit_fallback_selection(SCHEMA_CHANGE_TYPE_SPEC.fields, "        ")
    return _GET_SCHEMA_CHANGES_ENVELOPE.format(node_selection=node)


def get_job_executions_fallback_query() -> str:
    node = _emit_fallback_selection(
        JOB_EXECUTION_HISTORY_LOG_TYPE_SPEC.fields, "        "
    )
    return _GET_JOB_EXECUTIONS_ENVELOPE.format(node_selection=node)


def get_metrics_v4_fallback_query() -> str:
    metric = _emit_fallback_selection(METRIC_TYPE_SPEC.fields, "      ")
    return _GET_METRICS_V4_ENVELOPE.format(metric_selection=metric)


def _emit_asset_selection_selection(
    shape: _TypeShape,
    object_shapes: Dict[str, _TypeShape],
    indent: str,
    filter_inline: str,
) -> str:
    lines: list = []
    if shape.introspection_failed:
        for spec in ASSET_SELECTION_TYPE_SPEC.fields:
            if spec.name in ("filters", "exclusions"):
                lines.append(f"{indent}{spec.name} {{")
                lines.append(filter_inline.rstrip("\n"))
                lines.append(f"{indent}}}")
                continue
            if spec.children:
                lines.append(f"{indent}{spec.name} {{")
                lines.append(_emit_desired_selection(spec.children, indent + "  "))
                lines.append(f"{indent}}}")
            else:
                lines.append(f"{indent}{spec.name}")
    else:
        for spec in ASSET_SELECTION_TYPE_SPEC.fields:
            live_name = _live_graphql_name(spec, shape.fields)
            if live_name is None:
                continue
            if spec.name in ("filters", "exclusions"):
                lines.append(f"{indent}{live_name} {{")
                lines.append(filter_inline.rstrip("\n"))
                lines.append(f"{indent}}}")
                continue
            if not spec.children:
                lines.append(f"{indent}{live_name}")
                continue
            live = shape.fields[live_name]
            if not live.is_composite or not live.object_type_name:
                continue
            sub_shape = object_shapes.get(live.object_type_name, _TypeShape())
            if sub_shape.introspection_failed:
                sub_lines = _emit_desired_selection(spec.children, indent + "  ")
            else:
                sub_lines = _build_selection_with_subshapes(
                    spec.children, sub_shape, object_shapes, indent + "  "
                )
            if not sub_lines.strip():
                continue
            lines.append(f"{indent}{live_name} {{")
            lines.append(sub_lines)
            lines.append(f"{indent}}}")
    return "\n".join(lines)


def _build_table_monitor_selection(
    shape: _TypeShape,
    object_shapes: Dict[str, _TypeShape],
    filter_inline: str,
    indent: str = "    ",
) -> str:
    lines: list = []
    for spec in TABLE_MONITOR_TYPE_SPEC.fields:
        live_name = _live_graphql_name(spec, shape.fields)
        if live_name is None:
            continue
        if spec.name != "assetSelection":
            lines.append(f"{indent}{live_name}")
            continue
        live = shape.fields[live_name]
        if not live.is_composite or not live.object_type_name:
            continue
        sub_shape = object_shapes.get(live.object_type_name, _TypeShape())
        inner = _emit_asset_selection_selection(
            sub_shape, object_shapes, indent + "  ", filter_inline
        )
        if not inner.strip():
            continue
        lines.append(f"{indent}{live_name} {{")
        lines.append(inner)
        lines.append(f"{indent}}}")
    return "\n".join(lines)


class IntrospectingQueryBuilder:
    """Builds GraphQL queries dynamically from the live MCD schema, dropping
    fields the gateway no longer exposes so schema drift degrades gracefully
    instead of failing the whole fetch with a 400."""

    def __init__(
        self,
        call: "Callable[..., Dict[str, object]]",
        warner: "Callable[..., None]",
        fatal_error_types: tuple = (),
    ) -> None:
        self._call = call
        self._warner = warner
        self._fatal_error_types = fatal_error_types
        self._shapes: Dict[str, _TypeShape] = {}

    def _introspect(self, type_name: str) -> _TypeShape:
        if type_name in self._shapes:
            return self._shapes[type_name]
        try:
            response = self._call(_INTROSPECT_QUERY, {"name": type_name})
        except self._fatal_error_types:
            raise
        except Exception as e:
            self._warner(
                title="Monte Carlo schema introspection failed",
                message=(
                    f"Could not introspect the {type_name} type; falling back to a "
                    "minimal field selection. Schema drift will not be detected this "
                    f"run for {type_name}."
                ),
                context=f"type={type_name}",
                exc=e,
            )
            self._shapes[type_name] = _TypeShape(introspection_failed=True)
            return self._shapes[type_name]
        type_node = response.get("mc_type")
        if type_node is None:
            type_node = response.get("__type")
        if type_node is None:
            self._shapes[type_name] = _TypeShape(type_absent=True)
            return self._shapes[type_name]
        shape = _parse_type_shape(response)
        self._shapes[type_name] = shape
        return shape

    def _introspect_spec_subtypes(
        self, fields: Tuple[FieldSpec, ...], shape: _TypeShape
    ) -> None:
        for spec in fields:
            if not spec.children:
                continue
            live_name = _live_graphql_name(spec, shape.fields)
            if live_name is None:
                continue
            live = shape.fields[live_name]
            if not live.is_composite or not live.object_type_name:
                continue
            sub_shape = self._introspect(live.object_type_name)
            self._introspect_spec_subtypes(spec.children, sub_shape)

    def _object_shapes(self) -> Dict[str, _TypeShape]:
        return self._shapes

    def _build(
        self,
        type_spec: TypeSpec,
        envelope: str,
        selection_indent: str,
        fallback_indent: Optional[str] = None,
    ) -> str:
        type_name = type_spec.type_name.value
        shape = self._introspect(type_name)
        fb_indent = fallback_indent if fallback_indent is not None else selection_indent
        fallback_selection = _emit_fallback_selection(type_spec.fields, fb_indent)
        if shape.introspection_failed:
            return _wrap(envelope, fallback_selection)
        self._introspect_spec_subtypes(type_spec.fields, shape)
        selection = _build_selection_with_subshapes(
            type_spec.fields, shape, self._object_shapes(), selection_indent
        )
        if not selection.strip():
            self._warner(
                title="Monte Carlo schema drift: no requested fields available",
                message=(
                    f"None of the requested {type_name} fields exist on the live "
                    "schema; falling back to a minimal selection."
                ),
                context=f"type={type_name}",
            )
            return _wrap(envelope, fallback_selection)
        return _wrap(envelope, selection)

    def monitors_query(self) -> str:
        return self._build(
            MONITOR_TYPE_SPEC,
            _MONITORS_ENVELOPE,
            _MONITOR_SELECTION_INDENT,
            fallback_indent=_MONITOR_FALLBACK_INDENT,
        )

    def custom_rules_query(self) -> str:
        return self._build(CUSTOM_RULE_TYPE_SPEC, _CUSTOM_RULES_ENVELOPE, "        ")

    def alerts_query(self) -> str:
        return self._build(ALERT_TYPE_SPEC, _ALERTS_ENVELOPE, "        ")

    def get_table_query(self) -> str:
        return self._build(WAREHOUSE_TABLE_TYPE_SPEC, _GET_TABLE_ENVELOPE, "    ")

    def table_monitor_query(self) -> str:
        type_name = TABLE_MONITOR_TYPE_SPEC.type_name.value
        shape = self._introspect(type_name)
        if shape.introspection_failed:
            return TABLE_MONITOR_QUERY
        self._introspect_spec_subtypes(TABLE_MONITOR_TYPE_SPEC.fields, shape)
        selection = _build_table_monitor_selection(
            shape,
            self._object_shapes(),
            ASSET_FILTER_INLINE_SELECTION,
        )
        if not selection.strip():
            return TABLE_MONITOR_QUERY
        return _wrap(_TABLE_MONITOR_ENVELOPE, selection)

    def evaluate_asset_selection_query(self) -> str:
        return self._build(
            ASSET_SELECTION_RESULT_TYPE_SPEC, _EVALUATE_ASSET_SELECTION_ENVELOPE, "    "
        )

    def get_schema_changes_query(self) -> str:
        return self._build_connection_node_query(
            SCHEMA_CHANGE_TYPE_SPEC, _GET_SCHEMA_CHANGES_ENVELOPE
        )

    def get_job_executions_query(self) -> str:
        return self._build_connection_node_query(
            JOB_EXECUTION_HISTORY_LOG_TYPE_SPEC, _GET_JOB_EXECUTIONS_ENVELOPE
        )

    def get_metrics_v4_query(self) -> str:
        type_name = METRIC_TYPE_SPEC.type_name.value
        shape = self._introspect(type_name)
        fallback = get_metrics_v4_fallback_query()
        if shape.introspection_failed:
            return fallback
        self._introspect_spec_subtypes(METRIC_TYPE_SPEC.fields, shape)
        metric_selection = _build_selection_with_subshapes(
            METRIC_TYPE_SPEC.fields,
            shape,
            self._object_shapes(),
            "      ",
        )
        if not metric_selection.strip():
            return fallback
        return _GET_METRICS_V4_ENVELOPE.format(metric_selection=metric_selection)

    def _build_connection_node_query(self, type_spec: TypeSpec, envelope: str) -> str:
        type_name = type_spec.type_name.value
        shape = self._introspect(type_name)
        node_fallback = _emit_fallback_selection(type_spec.fields, "        ")
        fallback = envelope.format(node_selection=node_fallback)
        if shape.introspection_failed:
            return fallback
        self._introspect_spec_subtypes(type_spec.fields, shape)
        node_selection = _build_selection_with_subshapes(
            type_spec.fields, shape, self._object_shapes(), "        "
        )
        if not node_selection.strip():
            return fallback
        return envelope.format(node_selection=node_selection)

    def check_drift(
        self,
        strict: bool = False,
        types: Optional[Sequence[MonteCarloGraphQLType]] = None,
    ) -> SchemaDrift:
        drift = SchemaDrift()
        for graphql_type in types if types is not None else list(TYPE_SPECS):
            type_spec = TYPE_SPECS[graphql_type]
            type_name = type_spec.type_name.value
            shape = self._introspect(type_name)
            if shape.introspection_failed:
                drift.per_type[type_name] = TypeDrift(
                    type_name=type_name,
                    verdict=(DriftVerdict.ABORT if strict else DriftVerdict.PROCEED),
                    introspection_failed=True,
                )
                continue
            self._introspect_spec_subtypes(type_spec.fields, shape)
            if type_name in (
                MonteCarloGraphQLType.TABLE_MONITOR.value,
                MonteCarloGraphQLType.ASSET_SELECTION.value,
            ):
                self._introspect("AssetFilterInterface")
            drift.per_type[type_name] = _diff_drift(
                type_spec,
                shape,
                strict,
                object_shapes=self._object_shapes(),
            )
        return drift
