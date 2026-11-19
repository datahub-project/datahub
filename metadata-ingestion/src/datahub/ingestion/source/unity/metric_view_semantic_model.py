import copy
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
from sqlglot import exp as sqlglot_exp

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DatasetLineageTypeClass,
    DialectClass,
    DomainsClass,
    ERModelRelationshipCardinalityClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    OwnershipClass,
    SemanticFieldTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    _Aspect,
)
from datahub.metadata.urns import MetricUrn, SchemaFieldUrn, SemanticModelUrn
from datahub.sdk._semantic_shared import AiContextInput
from datahub.sdk.entity import Entity
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    DialectExpressionInput,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

logger = logging.getLogger(__name__)

DATABRICKS_PLATFORM = "databricks"

# MEASURE(name) is a Databricks-specific token: https://docs.databricks.com/aws/en/business-semantics/metric-views/advanced-techniques
MEASURE_REF_RE = re.compile(
    r"\bMEASURE\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", re.IGNORECASE
)

# Databricks exposes the top-level `source` of a metric view under this alias;
# unqualified columns resolve to it too.
ROOT_ALIAS = "source"

# Key prefix of the per-field custom properties built by the source
# (metric_view.field.<lowercase name>.*), used to route them to the logical
# dataset that carries the field.
FIELD_CUSTOM_PROPERTY_PREFIX = "metric_view.field."
JOIN_CUSTOM_PROPERTY_PREFIX = "metric_view.join."

_JOIN_CARDINALITY = "cardinality"
_JOIN_RELY = "rely"
# YAML 1.1 join cardinality; a join with none is many-to-one.
_JOIN_CARDINALITIES: Dict[str, str] = {
    "many_to_one": ERModelRelationshipCardinalityClass.N_ONE,
    "one_to_many": ERModelRelationshipCardinalityClass.ONE_N,
}

_DIMENSIONS_KEY = "dimensions"
# The documented key for dimensions; `dimensions` is its backward-compatible
# synonym. Folded into `dimensions` when the spec is loaded.
_FIELDS_KEY = "fields"
_MEASURES_KEY = "measures"
_TIME_TYPE_PREFIXES = ("date", "timestamp")
_DISPLAY_NAME_MAX_LEN = 255
_SYNONYMS_MAX_COUNT = 10
_SYNONYM_MAX_LEN = 255

# Known format subkeys per type, from the agent-metadata spec
_FORMAT_NUMERIC_SUBKEYS = frozenset(
    {"decimal_places", "hide_group_separator", "abbreviation", "currency_code"}
)
_FORMAT_DATE_SUBKEYS = frozenset({"date_format", "leading_zeros"})
_FORMAT_DATETIME_SUBKEYS = frozenset({"date_format", "time_format", "leading_zeros"})
_FORMAT_DECIMAL_PLACES_SUBKEYS = frozenset({"type", "places"})
_FORMAT_KNOWN_SUBKEYS: Dict[str, FrozenSet[str]] = {
    "number": _FORMAT_NUMERIC_SUBKEYS,
    "currency": _FORMAT_NUMERIC_SUBKEYS,
    "percentage": frozenset({"decimal_places", "hide_group_separator"}),
    "byte": frozenset({"decimal_places", "hide_group_separator"}),
    "date": _FORMAT_DATE_SUBKEYS,
    "date_time": _FORMAT_DATETIME_SUBKEYS,
}
_COUNT_DISTINCT = "COUNT_DISTINCT"
_JOIN_KEY_DESCRIPTION = (
    "Join key of a metric view join; not a dimension of the metric view."
)

# A join predicate, a top-level `source` or a measure expression can all fail to
# parse; sqlglot internals can also leak AttributeError/TypeError on malformed ASTs.
_PARSE_ERRORS = (
    sqlglot.errors.SqlglotError,
    RecursionError,
    ValueError,
    AttributeError,
    TypeError,
)


@dataclass(frozen=True)
class MetricViewColumnRef:
    """A physical column an expression reads, and the alias it was read through."""

    alias: str
    ref: TableReference
    column: str


@dataclass
class MetricViewExprParse:
    columns: List[MetricViewColumnRef] = field(default_factory=list)
    # Qualifiers not found in the alias map; "<source>" stands for unqualified
    # columns when the top-level source could not be resolved.
    unresolved_qualifiers: Set[str] = field(default_factory=set)
    error: Optional[Exception] = None
    empty_tree: bool = False
    aggregation_function: Optional[str] = None


def parse_metric_view_expr(
    expr: str, alias_map: Dict[str, TableReference]
) -> MetricViewExprParse:
    """Resolve the physical columns a dimension/measure expression reads.

    ``alias_map`` is keyed by lowercase qualifier (a join name, or the dotted
    path of a nested join); ``""`` is the top-level source (unqualified
    columns). MEASURE(name) refers to a peer measure, not a
    source column, so it is stripped before parsing.
    """
    stripped = MEASURE_REF_RE.sub("0", expr)
    try:
        tree = sqlglot.parse_one(stripped, dialect="databricks")
    except _PARSE_ERRORS as e:
        return MetricViewExprParse(error=e)
    if tree is None:
        return MetricViewExprParse(empty_tree=True)

    result = MetricViewExprParse(aggregation_function=_aggregation_function(tree))
    seen: Set[Tuple[str, str]] = set()
    for col in tree.find_all(sqlglot_exp.Column):
        # A nested join's column is referenced through its parent's path
        # (`instr.issuer.ctry_cd`); try the whole path, then its last segment.
        qualifier = ".".join(part.name for part in col.parts[:-1]).lower()
        ref = alias_map.get(qualifier)
        if ref is None and col.table and col.table.lower() != qualifier:
            qualifier = col.table.lower()
            ref = alias_map.get(qualifier)
        if ref is None:
            result.unresolved_qualifiers.add(qualifier or "<source>")
            continue
        key = (qualifier, col.name)
        if key in seen:
            continue
        seen.add(key)
        result.columns.append(
            MetricViewColumnRef(alias=qualifier, ref=ref, column=col.name)
        )
    return result


def _aggregation_function(tree: sqlglot_exp.Expr) -> Optional[str]:
    if not isinstance(tree, sqlglot_exp.AggFunc):
        return None
    if isinstance(tree, sqlglot_exp.Count) and isinstance(
        tree.this, sqlglot_exp.Distinct
    ):
        return _COUNT_DISTINCT
    return tree.sql_name()


# Keys of a metric view column's type_json `metadata` (Unity Catalog stores a
# field's agent metadata there, not in the YAML it returns).
_TYPE_JSON_METADATA = "metadata"
_COLUMN_COMMENT = "comment"
_COLUMN_SEMANTIC_METADATA = "semantic_metadata"
_COLUMN_WINDOW = "metric_view.window"
_TABLE_FILTER_PROPERTY = "metric_view.where"
_FORMAT_TYPE = "type"


@dataclass
class MetricViewColumnMetadataMerge:
    spec: Dict[str, Any]
    # Columns whose type_json or semantic_metadata could not be parsed.
    invalid_columns: List[str] = field(default_factory=list)


def merge_column_metadata(
    spec: Dict[str, Any], table: Table
) -> MetricViewColumnMetadataMerge:
    """Fill a metric view spec with the field metadata Unity Catalog keeps on columns.

    Unity Catalog normalizes the YAML it returns in ``view_definition``: a field's
    ``comment``, ``display_name``, ``synonyms`` and ``format`` are dropped from
    it and kept on the matching column (``comment`` and ``type_json.metadata``,
    with ``semantic_metadata`` a JSON string and ``format`` reshaped as
    ``{<type>: {...}}``). The top-level ``filter`` is also kept as the
    ``metric_view.where`` table property. A value in the YAML wins (older
    workspaces, hand-written specs); the column fills in what the YAML lacks.
    The input spec is not modified.
    """
    merged = copy.deepcopy(spec)
    result = MetricViewColumnMetadataMerge(spec=merged)
    filter_property = table.properties.get(_TABLE_FILTER_PROPERTY)
    if "filter" not in merged and filter_property:
        merged["filter"] = filter_property

    columns_by_name = {c.name.lower(): c for c in table.columns}
    for key in (_DIMENSIONS_KEY, _MEASURES_KEY):
        entries = merged.get(key)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                continue
            column = columns_by_name.get(entry["name"].lower())
            if column is None:
                continue
            metadata, valid = _column_metadata(column.type_json)
            semantic, semantic_valid = _semantic_metadata(
                metadata.get(_COLUMN_SEMANTIC_METADATA)
            )
            if not (valid and semantic_valid):
                result.invalid_columns.append(column.name)
            comment = column.comment or metadata.get(_COLUMN_COMMENT)
            if (
                not entry.get("comment")
                and not entry.get("description")
                and isinstance(comment, str)
                and comment.strip()
            ):
                entry["comment"] = comment
            for attr in ("display_name", "synonyms"):
                if attr not in entry and semantic.get(attr) is not None:
                    entry[attr] = semantic[attr]
            fmt = _normalize_format(semantic.get("format"))
            if "format" not in entry and fmt is not None:
                entry["format"] = fmt
            window = metadata.get(_COLUMN_WINDOW)
            if key == _MEASURES_KEY and "window" not in entry and window:
                entry["window"] = window
    return result


def _column_metadata(type_json: Optional[str]) -> Tuple[Dict[str, Any], bool]:
    if not type_json:
        return {}, True
    try:
        parsed = json.loads(type_json)
    except ValueError:
        return {}, False
    metadata = parsed.get(_TYPE_JSON_METADATA) if isinstance(parsed, dict) else None
    if metadata is None:
        return {}, True
    return (metadata, True) if isinstance(metadata, dict) else ({}, False)


def _semantic_metadata(raw: object) -> Tuple[Dict[str, Any], bool]:
    if raw is None:
        return {}, True
    if isinstance(raw, dict):
        return raw, True
    if not isinstance(raw, str):
        return {}, False
    try:
        parsed = json.loads(raw)
    except ValueError:
        return {}, False
    return (parsed, True) if isinstance(parsed, dict) else ({}, False)


def _normalize_format(fmt: object) -> Optional[Dict[str, Any]]:
    """`{"currency": {"currency_code": "USD"}}` -> `{"type": "currency", ...}`."""
    if not isinstance(fmt, dict) or not fmt:
        return None
    if _FORMAT_TYPE in fmt:
        return fmt
    if len(fmt) == 1:
        ((fmt_type, options),) = fmt.items()
        if isinstance(options, dict):
            return {_FORMAT_TYPE: fmt_type, **options}
        if options is None:
            return {_FORMAT_TYPE: fmt_type}
    return None


@dataclass
class MetricViewDimensionKeys:
    spec: Dict[str, Any]
    duplicate_names: List[str] = field(default_factory=list)


def normalize_dimension_keys(spec: Dict[str, Any]) -> MetricViewDimensionKeys:
    """Fold `fields:` (documented) and `dimensions:` (synonym) into `dimensions`.

    Every reader then only looks at `dimensions`. When both keys are present the
    lists are merged, `fields` first; a name declared in both keeps its first
    definition and is reported. The input spec is not modified.
    """
    fields = spec.get(_FIELDS_KEY)
    if fields is None:
        return MetricViewDimensionKeys(spec=spec)
    normalized = dict(spec)
    normalized.pop(_FIELDS_KEY)
    dimensions = spec.get(_DIMENSIONS_KEY)
    if not isinstance(fields, list):
        # Malformed; keep it where shape checks will see it.
        normalized.setdefault(_DIMENSIONS_KEY, fields)
        return MetricViewDimensionKeys(spec=normalized)
    merged: List[Any] = []
    seen: Set[str] = set()
    duplicates: List[str] = []
    for entry in fields + (dimensions if isinstance(dimensions, list) else []):
        name = entry.get("name") if isinstance(entry, dict) else None
        if isinstance(name, str):
            if name.lower() in seen:
                duplicates.append(name)
                continue
            seen.add(name.lower())
        merged.append(entry)
    normalized[_DIMENSIONS_KEY] = merged
    return MetricViewDimensionKeys(spec=normalized, duplicate_names=duplicates)


def join_custom_properties(spec: Dict[str, Any]) -> Dict[str, str]:
    """`metric_view.join.<path>.cardinality` / `.rely.<key>` for every join."""
    props: Dict[str, str] = {}

    def walk(raw_joins: object, parent: Optional[str]) -> None:
        if not isinstance(raw_joins, list):
            return
        for raw in raw_joins:
            if not isinstance(raw, dict) or not isinstance(raw.get("name"), str):
                continue
            path = raw["name"] if parent is None else f"{parent}.{raw['name']}"
            prefix = f"{JOIN_CUSTOM_PROPERTY_PREFIX}{path}"
            cardinality = raw.get(_JOIN_CARDINALITY)
            if isinstance(cardinality, str):
                props[f"{prefix}.{_JOIN_CARDINALITY}"] = cardinality
            rely = raw.get(_JOIN_RELY)
            if isinstance(rely, dict):
                for key, value in rely.items():
                    props[f"{prefix}.{_JOIN_RELY}.{key}"] = str(value).lower()
            walk(raw.get("joins"), path)

    walk(spec.get("joins"), None)
    return props


@dataclass(frozen=True)
class _BufferedWarning:
    message: str
    context: str
    title: Optional[str] = None
    exc: Optional[BaseException] = None


class MetricViewReportBuffer:
    """Counters and warnings raised while mapping one metric view.

    Committed to the report only once the view is emitted, so a view that falls
    back to the legacy `Metric View` dataset is not counted twice: the fallback
    path counts its own.
    """

    def __init__(self) -> None:
        self._counts: Dict[str, int] = {}
        self._warnings: List[_BufferedWarning] = []

    def count(self, name: str, n: int = 1) -> None:
        if n:
            self._counts[name] = self._counts.get(name, 0) + n

    def warning(
        self,
        *,
        message: str,
        context: str,
        title: Optional[str] = None,
        exc: Optional[BaseException] = None,
    ) -> None:
        self._warnings.append(
            _BufferedWarning(message=message, context=context, title=title, exc=exc)
        )

    def commit(self, report: UnityCatalogReport) -> None:
        for name, n in self._counts.items():
            current = getattr(report, name)
            assert isinstance(current, int), name
            setattr(report, name, current + n)
        for w in self._warnings:
            report.warning(
                title=w.title, message=w.message, context=w.context, exc=w.exc
            )
        self._counts.clear()
        self._warnings.clear()


def clean_display_name(
    raw: object, buffer: Optional[MetricViewReportBuffer] = None
) -> Optional[str]:
    """A field's display_name, truncated to the 255 characters Databricks allows.

    Pass ``buffer`` where the value is counted; the same field is cleaned once
    per path that reads it, and counted once.
    """
    if not isinstance(raw, str) or not raw.strip():
        return None
    if len(raw) > _DISPLAY_NAME_MAX_LEN and buffer is not None:
        buffer.count("num_metric_view_display_name_truncated")
    return raw[:_DISPLAY_NAME_MAX_LEN]


def clean_synonyms(
    raw: object, buffer: Optional[MetricViewReportBuffer] = None
) -> List[str]:
    """A field's synonyms: non-empty strings, each at most 255 characters, at
    most 10 (per the Databricks v1.1 spec). ``buffer`` as for clean_display_name."""
    if not isinstance(raw, list) or not raw:
        return []
    cleaned: List[str] = []
    for synonym in raw:
        if not isinstance(synonym, str) or not synonym.strip():
            if buffer is not None:
                buffer.count("num_metric_view_synonyms_dropped_invalid")
            continue
        if len(synonym) > _SYNONYM_MAX_LEN and buffer is not None:
            buffer.count("num_metric_view_synonyms_truncated")
        cleaned.append(synonym[:_SYNONYM_MAX_LEN])
    if len(cleaned) > _SYNONYMS_MAX_COUNT:
        if buffer is not None:
            buffer.count("num_metric_view_synonyms_overflow")
        cleaned = cleaned[:_SYNONYMS_MAX_COUNT]
    return cleaned


def entry_description(raw: Dict[str, Any]) -> Optional[str]:
    # v1.1 uses "comment"; fall back to "description" for older YAML.
    desc = raw.get("comment") or raw.get("description")
    return desc if isinstance(desc, str) and desc.strip() else None


def metric_view_description(
    spec: Dict[str, Any], table_comment: Optional[str]
) -> Optional[str]:
    # YAML top-level `comment` is the metric view's own documentation; UC's
    # table.comment may not be kept in sync. Prefer YAML when present.
    comment = spec.get("comment")
    if isinstance(comment, str) and comment.strip():
        return comment
    return table_comment


def metric_view_custom_properties(
    spec: Dict[str, Any], buffer: MetricViewReportBuffer
) -> Dict[str, str]:
    """The `metric_view.*` custom properties of a metric view spec: spec-level
    ones (filter, version, joins, materialization) and per-field ones."""
    props: Dict[str, str] = {}
    filter_expr = spec.get("filter")
    if isinstance(filter_expr, str) and filter_expr.strip():
        props["metric_view.filter"] = filter_expr

    version = spec.get("version")
    if version is not None:
        props["metric_view.spec_version"] = str(version)

    joins = spec.get("joins")
    if isinstance(joins, list) and joins:
        props["metric_view.joins"] = json.dumps(
            joins, separators=(",", ":"), default=str
        )

    props.update(join_custom_properties(spec))

    mat = spec.get("materialization")
    if isinstance(mat, dict):
        if isinstance(mat.get("schedule"), str):
            props["metric_view.materialization.schedule"] = mat["schedule"]
        if isinstance(mat.get("mode"), str):
            props["metric_view.materialization.mode"] = mat["mode"]
        mvs = mat.get("materialized_views")
        if isinstance(mvs, list):
            props["metric_view.materialization.materialized_views"] = json.dumps(
                mvs, separators=(",", ":"), default=str
            )

    props.update(_field_custom_properties(spec, buffer))
    return props


def _field_custom_properties(
    spec: Dict[str, Any], buffer: MetricViewReportBuffer
) -> Dict[str, str]:
    """Per-field agent metadata (display_name, synonyms, format, window) keyed
    metric_view.field.<name>.*."""
    props: Dict[str, str] = {}
    for section in (_DIMENSIONS_KEY, _MEASURES_KEY):
        entries = spec.get(section)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            name = entry.get("name")
            if not isinstance(name, str) or not name:
                continue
            # Lowercase keeps prop keys consistent with tag/description keys.
            prefix = f"{FIELD_CUSTOM_PROPERTY_PREFIX}{name.lower()}"
            display_name = clean_display_name(entry.get("display_name"), buffer)
            if display_name is not None:
                props[f"{prefix}.display_name"] = display_name
            synonyms = clean_synonyms(entry.get("synonyms"), buffer)
            if synonyms:
                props[f"{prefix}.synonyms"] = ",".join(synonyms)
            _add_format_props(entry, prefix, props, buffer)
            if section == _MEASURES_KEY:
                _add_window_props(entry, prefix, props)
    return props


def _add_format_props(
    entry: Dict[str, Any],
    prefix: str,
    props: Dict[str, str],
    buffer: MetricViewReportBuffer,
) -> None:
    fmt = entry.get("format")
    if not isinstance(fmt, dict):
        return
    fmt_type = fmt.get("type")
    if not isinstance(fmt_type, str):
        return
    props[f"{prefix}.format.type"] = fmt_type
    known = _FORMAT_KNOWN_SUBKEYS.get(fmt_type.lower(), frozenset())
    for subkey, val in fmt.items():
        if subkey == "type":
            continue
        if subkey not in known:
            buffer.count("num_metric_view_format_unknown_subkeys")
            continue
        if subkey == "decimal_places" and isinstance(val, dict):
            for dp_key, dp_val in val.items():
                if dp_key in _FORMAT_DECIMAL_PLACES_SUBKEYS:
                    props[f"{prefix}.format.decimal_places.{dp_key}"] = str(dp_val)
                else:
                    buffer.count("num_metric_view_format_unknown_subkeys")
        else:
            props[f"{prefix}.format.{subkey}"] = str(val)


def _add_window_props(
    entry: Dict[str, Any], prefix: str, props: Dict[str, str]
) -> None:
    window = entry.get("window")
    if not isinstance(window, list) or not window:
        return
    if len(window) == 1 and isinstance(window[0], dict):
        w = window[0]
        for wk in ("order", "range", "semiadditive"):
            wv = w.get(wk)
            if wv is not None:
                props[f"{prefix}.window.{wk}"] = str(wv)
    else:
        props[f"{prefix}.window"] = json.dumps(
            window, separators=(",", ":"), default=str
        )


def measure_refs(expr: str) -> List[str]:
    return [m.group(1) for m in MEASURE_REF_RE.finditer(expr)]


@dataclass
class _Join:
    name: str
    # Dotted path from the top-level source: `instr.issuer` for a join `issuer`
    # nested under `instr`. Databricks references nested join columns this way,
    # and two nested joins may share a name under different parents.
    path: str
    # Path of the parent join, or `source` for a top-level join.
    parent: str
    source: str
    ref: Optional[TableReference]
    on: Optional[str]
    using: Optional[List[str]]
    cardinality: Optional[str]


@dataclass
class _Entry:
    """A dimension or measure from the YAML spec, resolved against the aliases."""

    name: str
    expr: str
    is_measure: bool
    raw: Dict[str, Any]
    parsed: MetricViewExprParse
    # The alias whose logical dataset carries this field, or None for a measure
    # composed only of MEASURE() references (it becomes a metric, not a field).
    home_alias: Optional[str]
    measure_refs: List[str]


@dataclass
class MetricViewSemanticModelInput:
    """Everything the mapper needs about one metric view, resolved by the source."""

    table: Table
    spec: Dict[str, Any]
    # [platform_instance.][metastore.]catalog.schema
    model_path: str
    # Browse path entries of the metric view's schema (instance/catalog/schema).
    browse_path: List[BrowsePathEntryClass]
    schema_container_urn: str
    ownership: Optional[OwnershipClass]
    domains: Optional[DomainsClass]
    view_tags: Optional[GlobalTagsClass]
    # Lowercase column name -> tag URNs from Unity Catalog column tags.
    column_tags: Dict[str, List[str]]
    # Spec-level and per-field Databricks extensions (metric_view.*).
    spec_custom_properties: Dict[str, str]
    field_custom_properties: Dict[str, str]
    # Upstream dataset URNs for the root logical dataset when its `source` is not
    # a parseable table reference (subquery); resolved lazily from the REST API.
    fallback_root_upstreams: Callable[[], List[str]]


class UnityMetricViewSemanticModelMapper:
    """Maps a Unity Catalog metric view onto semanticModel, dataset and metric entities.

    One ``semanticModel`` per metric view. One logical dataset (subtype
    ``Semantic Model Dataset``) per table the view reads: the top-level
    ``source`` (alias ``source``) and every entry of the (possibly nested)
    ``joins`` hierarchy (alias = join name). Each join becomes a
    ``semanticModelInfo.relationships`` entry from its parent alias.

    Dimensions and measures become fields on the logical dataset whose alias
    their expression reads (the ``source`` dataset when they read none or more
    than one), with column-level lineage to the physical columns on that
    dataset's ``upstreamLineage``. Each measure is also a ``metric``, reading
    its field (``metricUpstreams``), and ``MEASURE(x)`` references become
    ``metricRelationships.derivedFrom`` edges.
    """

    def __init__(
        self,
        *,
        platform_instance: Optional[str],
        env: str,
        include_column_lineage: bool,
        gen_dataset_urn: Callable[[TableReference], str],
        resolve_source: Callable[[str, Table], Optional[TableReference]],
    ) -> None:
        self.platform_instance = platform_instance
        self.env = env
        self.include_column_lineage = include_column_lineage
        self.gen_dataset_urn = gen_dataset_urn
        self.resolve_source = resolve_source

    def gen_workunits(
        self, inp: MetricViewSemanticModelInput, buffer: MetricViewReportBuffer
    ) -> List[MetadataWorkUnit]:
        """Build every entity first, so a failure leaves nothing half-emitted.

        Counters and warnings go to ``buffer``; the caller commits them to the
        report only once the workunits are emitted."""
        table = inp.table
        context = table.ref.qualified_table_name
        model_urn = str(
            SemanticModelUrn(
                platform=DATABRICKS_PLATFORM, path=inp.model_path, id=table.name
            )
        )
        metric_path = f"{inp.model_path}.{table.name}"

        root_ref = self._resolve_root(inp)
        joins = self._collect_joins(inp, buffer)
        # Keyed by lowercase join path; a logical dataset's alias is its path.
        alias_refs: Dict[str, Optional[TableReference]] = {ROOT_ALIAS: root_ref}
        # Lowercase qualifier -> alias (path) it names.
        alias_names: Dict[str, str] = {ROOT_ALIAS: ROOT_ALIAS}
        for join in joins:
            alias_refs[join.path.lower()] = join.ref
            # A join named `source` takes the alias over: an explicit name wins.
            alias_names[join.path.lower()] = join.path
        for join in joins:
            # A bare nested join name is accepted too when it is unambiguous.
            alias_names.setdefault(join.name.lower(), join.path)
        alias_map: Dict[str, TableReference] = {}
        for qualifier, alias in alias_names.items():
            ref = alias_refs.get(alias.lower())
            if ref is not None:
                alias_map[qualifier] = ref
        if root_ref is not None:
            alias_map[""] = root_ref

        entries = self._collect_entries(inp, alias_map, alias_names, buffer)
        measures_by_lower = {e.name.lower(): e for e in entries if e.is_measure}

        fields_by_alias: Dict[str, Dict[str, SemanticFieldInput]] = {
            alias: {} for alias in alias_names.values()
        }
        for entry in entries:
            if entry.home_alias is None:
                continue
            fields_by_alias[entry.home_alias][entry.name] = self._field(entry, inp)

        relationships = self._relationships(
            joins, entries, fields_by_alias, context, buffer
        )

        datasets: Dict[str, SemanticModelDataset] = {}
        for alias, fields_by_path in fields_by_alias.items():
            datasets[alias] = self._logical_dataset(
                inp=inp,
                alias=alias,
                model_urn=model_urn,
                fields=list(fields_by_path.values()),
                entries=[e for e in entries if e.home_alias == alias],
                alias_ref=alias_refs.get(alias.lower()),
            )

        metrics: List[Metric] = []
        unresolved_refs: Set[str] = set()
        derived_edges = 0
        for entry in entries:
            if not entry.is_measure:
                continue
            derived_from: List[str] = []
            for ref_name in entry.measure_refs:
                target = measures_by_lower.get(ref_name.lower())
                if target is None:
                    unresolved_refs.add(ref_name)
                    continue
                if target.name == entry.name:
                    continue
                urn = str(
                    MetricUrn(
                        platform=DATABRICKS_PLATFORM, path=metric_path, id=target.name
                    )
                )
                if urn not in derived_from:
                    derived_from.append(urn)
            derived_edges += len(derived_from)
            metrics.append(
                self._metric(
                    inp=inp,
                    entry=entry,
                    model_urn=model_urn,
                    metric_path=metric_path,
                    dataset=(
                        datasets[entry.home_alias]
                        if entry.home_alias is not None
                        else None
                    ),
                    derived_from=sorted(derived_from),
                )
            )

        if unresolved_refs:
            buffer.count(
                "num_metric_view_unresolved_measure_refs", len(unresolved_refs)
            )
            buffer.warning(
                title="Unresolved MEASURE() reference in metric view",
                message=(
                    "A MEASURE() reference does not match any measure defined in "
                    "the same metric view, so no derivedFrom edge was emitted for it."
                ),
                context=f"{context}: refs={sorted(unresolved_refs)!r}",
            )

        model = SemanticModel(
            platform=DATABRICKS_PLATFORM,
            path=inp.model_path,
            id=table.name,
            platform_instance=self.platform_instance,
            name=table.name,
            description=metric_view_description(inp.spec, table.comment),
            created=table.created_at,
            last_modified=table.updated_at or table.created_at,
            native_definition=table.view_definition,
            datasets=list(datasets.values()),
            relationships=relationships or None,
            extra_aspects=self._governance_aspects(
                inp,
                browse_path=inp.browse_path,
                include_tags=True,
            ),
        )

        entities: List[Entity] = [model, *datasets.values(), *metrics]
        workunits = [wu for entity in entities for wu in entity.as_workunits()]

        buffer.count("num_metric_view_semantic_models_emitted", 1)
        buffer.count("num_metric_view_semantic_model_datasets_emitted", len(datasets))
        buffer.count("num_metric_view_metrics_emitted", len(metrics))
        buffer.count("num_metric_view_relationships_emitted", len(relationships))
        buffer.count("num_metric_view_derived_metric_edges", derived_edges)
        return workunits

    def _resolve_root(
        self, inp: MetricViewSemanticModelInput
    ) -> Optional[TableReference]:
        raw = inp.spec.get("source")
        if not isinstance(raw, str):
            return None
        return self.resolve_source(raw, inp.table)

    def _collect_joins(
        self, inp: MetricViewSemanticModelInput, buffer: MetricViewReportBuffer
    ) -> List[_Join]:
        joins: List[_Join] = []
        skipped = 0

        def walk(raw_joins: object, parent: str) -> None:
            # `parent` is the parent join's path, or `source`.
            nonlocal skipped
            if not isinstance(raw_joins, list):
                return
            for raw in raw_joins:
                if not isinstance(raw, dict):
                    skipped += 1
                    continue
                name = raw.get("name")
                source = raw.get("source")
                if not isinstance(name, str) or not name or not isinstance(source, str):
                    skipped += 1
                    continue
                ref = self.resolve_source(source, inp.table)
                if ref is None:
                    skipped += 1
                on = raw.get("on")
                using = raw.get("using")
                path = name if parent == ROOT_ALIAS else f"{parent}.{name}"
                joins.append(
                    _Join(
                        name=name,
                        path=path,
                        parent=parent,
                        source=source,
                        ref=ref,
                        on=on if isinstance(on, str) else None,
                        cardinality=(
                            raw[_JOIN_CARDINALITY]
                            if isinstance(raw.get(_JOIN_CARDINALITY), str)
                            else None
                        ),
                        using=(
                            [u for u in using if isinstance(u, str)]
                            if isinstance(using, list)
                            else None
                        ),
                    )
                )
                walk(raw.get("joins"), path)

        walk(inp.spec.get("joins"), ROOT_ALIAS)
        if skipped:
            buffer.count("num_metric_view_joins_skipped", skipped)
            buffer.warning(
                title="Metric view joins skipped",
                message="Metric view join entries with a malformed shape or an "
                "unparseable source have no upstream table. Lineage through them "
                "will be absent.",
                context=f"{inp.table.ref.qualified_table_name}: {skipped} join "
                "entry/entries affected",
            )
        return joins

    def _collect_entries(
        self,
        inp: MetricViewSemanticModelInput,
        alias_map: Dict[str, TableReference],
        alias_names: Dict[str, str],
        buffer: MetricViewReportBuffer,
    ) -> List[_Entry]:
        context = inp.table.ref.qualified_table_name
        entries: List[_Entry] = []
        seen: Set[str] = set()
        skipped = 0
        unresolved_qualifiers: Set[str] = set()
        # `source.col` reaches the top-level source too.
        expr_alias_map = dict(alias_map)
        if ROOT_ALIAS not in expr_alias_map and "" in alias_map:
            expr_alias_map[ROOT_ALIAS] = alias_map[""]
        for key in (_DIMENSIONS_KEY, _MEASURES_KEY):
            raw_entries = inp.spec.get(key)
            if not isinstance(raw_entries, list):
                continue
            for raw in raw_entries:
                if not isinstance(raw, dict):
                    skipped += 1
                    continue
                name = raw.get("name")
                expr = raw.get("expr")
                if not isinstance(name, str) or not name or not isinstance(expr, str):
                    skipped += 1
                    continue
                if name.lower() in seen:
                    buffer.warning(
                        title="Duplicate metric view field name",
                        message="Two dimensions/measures share a name; only the "
                        "first is emitted.",
                        context=f"{context}.{name}",
                    )
                    continue
                seen.add(name.lower())
                parsed = parse_metric_view_expr(expr, expr_alias_map)
                if parsed.error is not None:
                    buffer.count("num_metric_views_expr_parse_failures", 1)
                    buffer.warning(
                        message="Metric view expression failed to parse",
                        context=f"{context}:{name}: expr={expr!r}",
                        exc=parsed.error,
                    )
                if parsed.empty_tree:
                    buffer.count("num_metric_view_expr_empty_tree", 1)
                unresolved_qualifiers.update(parsed.unresolved_qualifiers)
                refs = measure_refs(expr) if key == _MEASURES_KEY else []
                entries.append(
                    _Entry(
                        name=name,
                        expr=expr,
                        is_measure=key == _MEASURES_KEY,
                        raw=raw,
                        parsed=parsed,
                        home_alias=self._home_alias(parsed, refs, alias_names),
                        measure_refs=refs,
                    )
                )
        if skipped:
            buffer.count("num_metric_view_skipped_dim_measure_entries", skipped)
            buffer.warning(
                message="Metric view dimension/measure entries skipped due to "
                "malformed shape (non-mapping entry, missing or non-string "
                "name/expr).",
                context=f"{context}: {skipped} dimension/measure entry/entries skipped",
            )
        if unresolved_qualifiers:
            buffer.count(
                "num_metric_view_unresolved_qualifiers", len(unresolved_qualifiers)
            )
            buffer.warning(
                message="Metric view expression references unknown qualifier not "
                "found in source/joins. Lineage rows for affected columns were "
                "skipped.",
                context=f"{context}: qualifiers={sorted(unresolved_qualifiers)!r}",
            )
        return entries

    @staticmethod
    def _home_alias(
        parsed: MetricViewExprParse, refs: List[str], alias_names: Dict[str, str]
    ) -> Optional[str]:
        aliases = {
            ROOT_ALIAS if c.alias in ("", ROOT_ALIAS) else c.alias
            for c in parsed.columns
        }
        if not aliases and refs:
            # Composed purely from other measures: a metric, not a field.
            return None
        if len(aliases) == 1:
            return alias_names.get(next(iter(aliases)), ROOT_ALIAS)
        return ROOT_ALIAS

    def _field(
        self, entry: _Entry, inp: MetricViewSemanticModelInput
    ) -> SemanticFieldInput:
        column = next(
            (c for c in inp.table.columns if c.name.lower() == entry.name.lower()),
            None,
        )
        native_type = column.type_text if column is not None else ""
        synonyms = clean_synonyms(entry.raw.get("synonyms"))
        return SemanticFieldInput(
            field_path=entry.name,
            type=native_type,
            semantic_type=(
                SemanticFieldTypeClass.MEASURE
                if entry.is_measure
                else SemanticFieldTypeClass.DIMENSION
            ),
            description=entry_description(entry.raw)
            or (column.comment if column is not None else None),
            tags=inp.column_tags.get(entry.name.lower()) or None,
            expression=DialectExpressionInput(
                expression=entry.expr, dialect=DialectClass.DATABRICKS
            ),
            aggregation_function=(
                entry.parsed.aggregation_function if entry.is_measure else None
            ),
            is_time_dimension=(
                not entry.is_measure
                and native_type.lower().startswith(_TIME_TYPE_PREFIXES)
            ),
            ai_context=AiContextInput(synonyms=synonyms) if synonyms else None,
        )

    def _relationships(
        self,
        joins: List[_Join],
        entries: List[_Entry],
        fields_by_alias: Dict[str, Dict[str, SemanticFieldInput]],
        context: str,
        buffer: MetricViewReportBuffer,
    ) -> List[SemanticModelRelationshipInput]:
        relationships: List[SemanticModelRelationshipInput] = []
        unresolved: List[str] = []
        unknown_cardinality: List[str] = []
        for join in joins:
            pairs = self._join_column_pairs(join)
            if not pairs:
                unresolved.append(join.path)
                continue
            from_columns = [
                self._ensure_join_field(join.parent, col, entries, fields_by_alias)
                for col, _ in pairs
            ]
            to_columns = [
                self._ensure_join_field(join.path, col, entries, fields_by_alias)
                for _, col in pairs
            ]
            cardinality: Optional[str] = ERModelRelationshipCardinalityClass.N_ONE
            if join.cardinality is not None:
                cardinality = _JOIN_CARDINALITIES.get(join.cardinality.lower())
                if cardinality is None:
                    unknown_cardinality.append(f"{join.path}: {join.cardinality}")
            relationships.append(
                SemanticModelRelationshipInput(
                    name=join.path,
                    from_alias=join.parent,
                    from_columns=from_columns,
                    to_alias=join.path,
                    to_columns=to_columns,
                    cardinality=cardinality,
                )
            )
        if unknown_cardinality:
            buffer.warning(
                title="Unrecognized metric view join cardinality",
                message="Expected `many_to_one` or `one_to_many`; the relationship "
                "is emitted without a cardinality.",
                context=f"{context}: {unknown_cardinality!r}",
            )
        if unresolved:
            buffer.count("num_metric_view_relationships_unresolved", len(unresolved))
            buffer.warning(
                title="Metric view join has no equi-join columns",
                message="The join's `on` predicate has no `a = b` column pair (or "
                "it failed to parse) and it has no `using` list, so it was not "
                "emitted as a semantic model relationship. Lineage through the "
                "join is unaffected.",
                context=f"{context}: joins={unresolved!r}",
            )
        return relationships

    @staticmethod
    def _join_column_pairs(join: _Join) -> List[Tuple[str, str]]:
        if join.using:
            return [(col, col) for col in join.using]
        if not join.on:
            return []
        try:
            tree = sqlglot.parse_one(join.on, dialect="databricks")
        except _PARSE_ERRORS:
            return []
        if tree is None:
            return []
        # Within a join's `on`, the join and its parent go by name or by path.
        to_qualifiers = {join.name.lower(), join.path.lower()}
        parent = join.parent.lower()
        from_qualifiers = (
            {parent, ""}
            if parent == ROOT_ALIAS
            else {parent, parent.rsplit(".", 1)[-1]}
        )
        pairs: List[Tuple[str, str]] = []
        for eq in tree.find_all(sqlglot_exp.EQ):
            left, right = eq.this, eq.expression
            if not (
                isinstance(left, sqlglot_exp.Column)
                and isinstance(right, sqlglot_exp.Column)
            ):
                continue
            left_q = _qualifier(left)
            right_q = _qualifier(right)
            if left_q in to_qualifiers or right_q in from_qualifiers - {""}:
                left, right = right, left
                left_q, right_q = right_q, left_q
            # Unqualified on both sides: Databricks writes `parent = join`.
            if left_q not in from_qualifiers or right_q not in to_qualifiers | {""}:
                continue
            pairs.append((left.name, right.name))
        return pairs

    @staticmethod
    def _ensure_join_field(
        alias: str,
        column: str,
        entries: List[_Entry],
        fields_by_alias: Dict[str, Dict[str, SemanticFieldInput]],
    ) -> str:
        """The field a join key lands on, adding it when no field exposes it.

        A dimension defined as exactly the key column is reused; otherwise a
        plain dimension named after the column is added, so the relationship's
        columns resolve on the logical dataset.
        """
        fields = fields_by_alias.setdefault(alias, {})
        for entry in entries:
            if entry.home_alias != alias or entry.is_measure:
                continue
            cols = entry.parsed.columns
            if (
                len(cols) == 1
                and cols[0].column.lower() == column.lower()
                and _strip_qualifier(entry.expr).lower() == column.lower()
            ):
                return entry.name
        for path in fields:
            if path.lower() == column.lower():
                return path
        fields[column] = SemanticFieldInput(
            field_path=column,
            type="",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            # DataHub has no "plain column" field kind, so say plainly that it is
            # not something the metric view can be grouped by.
            description=_JOIN_KEY_DESCRIPTION,
            expression=DialectExpressionInput(
                expression=f"{alias}.{column}", dialect=DialectClass.DATABRICKS
            ),
        )
        return column

    def _logical_dataset(
        self,
        *,
        inp: MetricViewSemanticModelInput,
        alias: str,
        model_urn: str,
        fields: List[SemanticFieldInput],
        entries: List[_Entry],
        alias_ref: Optional[TableReference],
    ) -> SemanticModelDataset:
        name = f"{inp.table.ref}.{alias}"
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=DATABRICKS_PLATFORM,
            name=name,
            platform_instance=self.platform_instance,
            env=self.env,
        )
        dataset = SemanticModelDataset(
            platform=DATABRICKS_PLATFORM,
            name=name,
            semantic_model=model_urn,
            alias=alias,
            schema=fields,
            platform_instance=self.platform_instance,
            env=self.env,
            extra_aspects=[
                ContainerClass(container=inp.schema_container_urn),
                *self._governance_aspects(
                    inp,
                    browse_path=[
                        *inp.browse_path,
                        BrowsePathEntryClass(id=inp.table.name, urn=model_urn),
                        BrowsePathEntryClass(id=alias, urn=dataset_urn),
                    ],
                    include_tags=False,
                ),
            ],
        )
        assert str(dataset.urn) == dataset_urn

        upstream_urns: List[str] = []
        if alias_ref is not None:
            upstream_urns.append(self.gen_dataset_urn(alias_ref))
        elif alias == ROOT_ALIAS:
            upstream_urns.extend(inp.fallback_root_upstreams())

        fine_grained: List[FineGrainedLineageClass] = []
        if self.include_column_lineage:
            for entry in entries:
                if not entry.parsed.columns:
                    continue
                upstream_fields = []
                for col in entry.parsed.columns:
                    upstream_dataset = self.gen_dataset_urn(col.ref)
                    if upstream_dataset not in upstream_urns:
                        upstream_urns.append(upstream_dataset)
                    upstream_fields.append(
                        make_schema_field_urn(upstream_dataset, col.column)
                    )
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=upstream_fields,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[make_schema_field_urn(dataset_urn, entry.name)],
                    )
                )
        if upstream_urns:
            dataset.set_upstreams(
                UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.VIEW)
                        for urn in upstream_urns
                    ],
                    fineGrainedLineages=fine_grained or None,
                )
            )

        custom_properties = self._dataset_custom_properties(inp, alias, entries)
        if custom_properties:
            dataset.set_custom_properties(custom_properties)
        return dataset

    @staticmethod
    def _dataset_custom_properties(
        inp: MetricViewSemanticModelInput, alias: str, entries: List[_Entry]
    ) -> Dict[str, str]:
        # Databricks-specific extensions: spec-level ones (filter, version, joins,
        # materialization) on the top-level source's dataset, per-field ones
        # (display_name, synonyms, format, window) on the dataset carrying the field.
        props: Dict[str, str] = {}
        if alias == ROOT_ALIAS:
            props.update(inp.spec_custom_properties)
        prefixes = tuple(
            f"{FIELD_CUSTOM_PROPERTY_PREFIX}{e.name.lower()}." for e in entries
        )
        if prefixes:
            props.update(
                {
                    k: v
                    for k, v in inp.field_custom_properties.items()
                    if k.startswith(prefixes)
                }
            )
        return props

    def _metric(
        self,
        *,
        inp: MetricViewSemanticModelInput,
        entry: _Entry,
        model_urn: str,
        metric_path: str,
        dataset: Optional[SemanticModelDataset],
        derived_from: List[str],
    ) -> Metric:
        column = next(
            (c for c in inp.table.columns if c.name.lower() == entry.name.lower()),
            None,
        )
        display_name = clean_display_name(entry.raw.get("display_name"))
        synonyms = clean_synonyms(entry.raw.get("synonyms"))
        upstream_datasets = [str(dataset.urn)] if dataset is not None else []
        metric = Metric(
            platform=DATABRICKS_PLATFORM,
            path=metric_path,
            id=entry.name,
            semantic_model=model_urn,
            platform_instance=self.platform_instance,
            name=display_name or entry.name,
            description=entry_description(entry.raw)
            or (column.comment if column is not None else None),
            created=inp.table.created_at,
            last_modified=inp.table.updated_at or inp.table.created_at,
            expression=DialectExpressionInput(
                expression=_with_window_note(entry.expr, entry.raw.get("window")),
                dialect=DialectClass.DATABRICKS,
            ),
            derived_from=derived_from,
            upstream_datasets=upstream_datasets,
            upstream_fields=(
                [str(SchemaFieldUrn(str(dataset.urn), entry.name))]
                if dataset is not None
                else []
            ),
            ai_context=AiContextInput(synonyms=synonyms) if synonyms else None,
            tags=inp.column_tags.get(entry.name.lower()) or None,
            extra_aspects=self._governance_aspects(
                inp,
                browse_path=[
                    *inp.browse_path,
                    BrowsePathEntryClass(id=inp.table.name, urn=model_urn),
                ],
                include_tags=False,
            ),
        )
        return metric

    @staticmethod
    def _governance_aspects(
        inp: MetricViewSemanticModelInput,
        *,
        browse_path: List[BrowsePathEntryClass],
        include_tags: bool,
    ) -> List[_Aspect]:
        aspects: List[_Aspect] = []
        if browse_path:
            aspects.append(BrowsePathsV2Class(path=browse_path))
        if inp.ownership is not None:
            aspects.append(inp.ownership)
        if inp.domains is not None:
            aspects.append(inp.domains)
        if include_tags and inp.view_tags is not None:
            aspects.append(inp.view_tags)
        return aspects


def _qualifier(column: sqlglot_exp.Column) -> str:
    return ".".join(part.name for part in column.parts[:-1]).lower()


def _strip_qualifier(expr: str) -> str:
    return expr.strip().rsplit(".", 1)[-1].strip("`")


def _with_window_note(expr: str, window: object) -> str:
    """Append a window measure's window as a trailing SQL comment.

    Without it a window measure's expression reads exactly like the plain
    aggregate over the same column (dbt does the same for cumulative metrics).
    """
    if not isinstance(window, list) or not window:
        return expr
    specs: List[str] = []
    for w in window:
        if not isinstance(w, dict):
            continue
        parts = [
            f"{key}={w[key]}"
            for key in ("order", "range", "semiadditive")
            if w.get(key) is not None
        ]
        if parts:
            specs.append(", ".join(parts))
    if not specs:
        return expr
    return f"{expr} -- window: {'; '.join(specs)}"
