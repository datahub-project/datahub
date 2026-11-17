import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
from sqlglot import expressions as sqlglot_exp

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy_types import Table, TableReference
from datahub.ingestion.source.unity.report import UnityCatalogReport
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ContainerClass,
    DatasetLineageTypeClass,
    DialectClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OwnerClass,
    SemanticFieldTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import SemanticModelUrn
from datahub.sdk.metric import Metric
from datahub.sdk.semantic_model import (
    AiContextInput,
    DialectExpressionInput,
    SemanticFieldInput,
    SemanticModel,
    SemanticModelDataset,
    SemanticModelRelationshipInput,
)

logger = logging.getLogger(__name__)

PLATFORM = "databricks"

# Databricks names the metric view's top-level `source` table `source` in join
# predicates, and unqualified columns in dimension/measure expressions resolve
# against it too.
SOURCE_ALIAS = "source"
_SOURCE_KEY = ""

MEASURE_REF_RE = re.compile(
    r"\bMEASURE\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)", re.IGNORECASE
)

_TIME_TYPE_PREFIXES = ("date", "timestamp")

_SQLGLOT_PARSE_ERRORS = (
    sqlglot.errors.SqlglotError,
    RecursionError,
    ValueError,
    AttributeError,
    TypeError,
)

SourceResolver = Callable[[str, Table], Optional[TableReference]]
ExpressionColumnParser = Callable[
    [str, Dict[str, TableReference], str],
    Tuple[List[Tuple[TableReference, str]], Set[str]],
]


@dataclass(frozen=True)
class MetricViewEntityContext:
    """Per-view metadata the source already resolves for its datasets."""

    schema_container_urn: str
    # Platform instance, metastore, catalog and schema entries, in that order.
    parent_browse_path: List[BrowsePathEntryClass]
    owners: Optional[List[OwnerClass]] = None
    tags: Optional[List[TagAssociationClass]] = None
    domain_urn: Optional[str] = None


@dataclass
class _LogicalTable:
    alias: str
    key: str
    parent_key: Optional[str]
    raw_source: Optional[str]
    ref: Optional[TableReference]
    join: Optional[Dict[str, Any]] = None
    fields: Dict[str, SemanticFieldInput] = field(default_factory=dict)
    # Field path -> the expression it was declared with, used to match join keys
    # onto an existing dimension rather than adding a duplicate field.
    field_exprs: Dict[str, str] = field(default_factory=dict)
    lineages: List[FineGrainedLineageClass] = field(default_factory=list)


class UnityMetricViewSemanticModelMapper:
    """Maps a Unity Catalog metric view onto semanticModel, dataset and metric entities.

    Mirrors the Snowflake semantic view mapping:

    - one ``semanticModel`` per metric view, holding the YAML as its native
      definition and the ``joins`` as relationships;
    - one logical dataset (subtype ``Semantic Model Dataset``) for the ``source``
      table and one per ``joins`` entry, each with upstream lineage to the
      physical table it reads;
    - dimensions as annotated fields on the logical dataset their expression
      reads from;
    - one ``metric`` per measure, upstream of the logical datasets it reads, with
      ``MEASURE(name)`` references as ``derivedFrom`` edges.
    """

    def __init__(
        self,
        *,
        config: UnityCatalogSourceConfig,
        report: UnityCatalogReport,
        platform_instance: Optional[str],
        dataset_urn_builder: Callable[[TableReference], str],
        resolve_source: SourceResolver,
        parse_expression_columns: ExpressionColumnParser,
    ) -> None:
        self.config = config
        self.report = report
        self.platform_instance = platform_instance
        self._dataset_urn = dataset_urn_builder
        self._resolve_source = resolve_source
        self._parse_expression_columns = parse_expression_columns
        # str(view ref) -> logical dataset URNs, so Genie agents that use a metric
        # view can point their lineage at datasets this run actually emitted.
        self._logical_dataset_urns: Dict[str, List[str]] = {}

    def logical_dataset_urns(self, view_ref: TableReference) -> Optional[List[str]]:
        return self._logical_dataset_urns.get(str(view_ref))

    def gen_workunits(
        self,
        table: Table,
        spec: Dict[str, Any],
        context: MetricViewEntityContext,
    ) -> Iterable[MetadataWorkUnit]:
        view_ref = table.ref
        model_path = self._model_path(view_ref)
        model_urn = str(
            SemanticModelUrn(platform=PLATFORM, path=model_path, id=view_ref.table)
        )
        view_name = view_ref.qualified_table_name

        logical_tables = self._logical_tables(table, spec)
        alias_map = {
            key: lt.ref for key, lt in logical_tables.items() if lt.ref is not None
        }
        source_ref = logical_tables[_SOURCE_KEY].ref
        if source_ref is not None:
            alias_map[SOURCE_ALIAS] = source_ref
        dataset_urns = {
            key: self._logical_dataset_urn(view_ref, lt.alias)
            for key, lt in logical_tables.items()
        }

        column_types = {c.name.lower(): c.type_text for c in table.columns}
        dimensions = _named_entries(spec, "dimensions")
        measures = _named_entries(spec, "measures")

        multi_alias_dimensions: List[str] = []
        for entry in dimensions:
            self._add_dimension(
                entry,
                logical_tables=logical_tables,
                alias_map=alias_map,
                dataset_urns=dataset_urns,
                column_types=column_types,
                view_name=view_name,
                multi_alias_dimensions=multi_alias_dimensions,
            )
        if multi_alias_dimensions:
            self.report.num_metric_view_dimensions_on_source_multi_alias += len(
                multi_alias_dimensions
            )
            self.report.info(
                title="Metric view dimension reads several joined tables",
                message="A dimension whose expression reads columns from more than "
                "one of the metric view's tables was placed on the `source` logical "
                "dataset, the grain the metric view is computed at. Its column-level "
                "lineage still points at every column it reads.",
                context=f"{view_name}: {sorted(multi_alias_dimensions)}",
            )

        relationships = self._relationships(
            logical_tables, dataset_urns=dataset_urns, view_name=view_name
        )

        model_browse_path = context.parent_browse_path + [
            BrowsePathEntryClass(id=view_ref.table, urn=model_urn)
        ]
        datasets = [
            self._logical_dataset(
                lt,
                view_ref=view_ref,
                model_urn=model_urn,
                context=context,
                browse_path=model_browse_path,
            )
            for lt in logical_tables.values()
        ]

        metrics = self._metrics(
            measures,
            spec=spec,
            table=table,
            logical_tables=logical_tables,
            dataset_urns=dataset_urns,
            model_urn=model_urn,
            model_path=model_path,
            context=context,
            browse_path=model_browse_path,
            view_name=view_name,
        )

        model = SemanticModel(
            platform=PLATFORM,
            path=model_path,
            id=view_ref.table,
            platform_instance=self.platform_instance,
            name=view_ref.table,
            description=_description(spec) or table.comment,
            created=table.created_at,
            last_modified=table.updated_at,
            native_definition=table.view_definition,
            datasets=datasets,
            relationships=relationships or None,
            owners=context.owners,
            tags=context.tags,
            domain=context.domain_urn,
            extra_aspects=[BrowsePathsV2Class(path=context.parent_browse_path)],
        )

        yield from model.as_workunits()
        for dataset in datasets:
            yield from dataset.as_workunits()
        for metric in metrics:
            yield from metric.as_workunits()

        self._logical_dataset_urns[str(view_ref)] = [str(ds.urn) for ds in datasets]
        self.report.semantic_models_emitted += 1
        self.report.semantic_model_datasets_emitted += len(datasets)
        self.report.metrics_emitted += len(metrics)

    def _model_path(self, view_ref: TableReference) -> str:
        # The semanticModel key has no platform_instance field, so the instance is
        # part of the path, as the Snowflake source does.
        parts = [
            self.platform_instance,
            view_ref.metastore,
            view_ref.catalog,
            view_ref.schema,
        ]
        return ".".join(part for part in parts if part)

    def _logical_dataset_urn(self, view_ref: TableReference, alias: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=self._logical_dataset_name(view_ref, alias),
            platform_instance=self.platform_instance,
            env=self.config.env,
        )

    @staticmethod
    def _logical_dataset_name(view_ref: TableReference, alias: str) -> str:
        # The dataset URN adds platform_instance itself, so it is not part of the
        # name. Matches the naming of the view's own dataset URN.
        return f"{view_ref}.{alias}"

    def _logical_tables(
        self, table: Table, spec: Dict[str, Any]
    ) -> Dict[str, _LogicalTable]:
        raw_source = spec.get("source")
        source_str = raw_source if isinstance(raw_source, str) else None
        tables: Dict[str, _LogicalTable] = {
            _SOURCE_KEY: _LogicalTable(
                alias=SOURCE_ALIAS,
                key=_SOURCE_KEY,
                parent_key=None,
                raw_source=source_str,
                ref=self._resolve_source(source_str, table) if source_str else None,
            )
        }
        self._add_joins(table, spec.get("joins"), _SOURCE_KEY, tables)
        return tables

    def _add_joins(
        self,
        table: Table,
        joins: object,
        parent_key: str,
        tables: Dict[str, _LogicalTable],
    ) -> None:
        if not isinstance(joins, list):
            return
        for join in joins:
            if not isinstance(join, dict):
                continue
            name = join.get("name")
            raw = join.get("source")
            if not isinstance(name, str) or not name:
                continue
            key = name.lower()
            if key in tables or key == SOURCE_ALIAS:
                # The legacy path already reports malformed joins; a repeated name
                # would merge two tables onto one logical dataset.
                continue
            raw_source = raw if isinstance(raw, str) else None
            tables[key] = _LogicalTable(
                alias=name,
                key=key,
                parent_key=parent_key,
                raw_source=raw_source,
                ref=self._resolve_source(raw_source, table) if raw_source else None,
                join=join,
            )
            self._add_joins(table, join.get("joins"), key, tables)

    def _add_dimension(
        self,
        entry: Dict[str, Any],
        *,
        logical_tables: Dict[str, _LogicalTable],
        alias_map: Dict[str, TableReference],
        dataset_urns: Dict[str, str],
        column_types: Dict[str, str],
        view_name: str,
        multi_alias_dimensions: List[str],
    ) -> None:
        name: str = entry["name"]
        expr = entry.get("expr")
        expr_str = expr if isinstance(expr, str) else None

        referenced = (
            self._referenced_tables(expr_str, logical_tables) if expr_str else set()
        )
        if len(referenced) == 1:
            key = next(iter(referenced))
        else:
            key = _SOURCE_KEY
            if len(referenced) > 1:
                multi_alias_dimensions.append(name)
        target = logical_tables[key]
        if name in target.fields:
            return

        native_type = column_types.get(name.lower(), "")
        target.fields[name] = SemanticFieldInput(
            field_path=name,
            type=native_type,
            semantic_type=SemanticFieldTypeClass.DIMENSION,
            description=_entry_description(entry),
            expression=(
                DialectExpressionInput(
                    expression=expr_str, dialect=DialectClass.DATABRICKS
                )
                if expr_str
                else None
            ),
            is_time_dimension=native_type.lower().startswith(_TIME_TYPE_PREFIXES),
            ai_context=_ai_context(entry),
        )
        if expr_str:
            target.field_exprs[name] = expr_str

        if self.config.include_column_lineage and expr_str:
            upstream_columns, _ = self._parse_expression_columns(
                expr_str, alias_map, f"{view_name}:{name}"
            )
            self._add_lineage(
                target,
                field_path=name,
                downstream_dataset_urn=dataset_urns[key],
                upstream_columns=upstream_columns,
            )

    def _add_lineage(
        self,
        target: _LogicalTable,
        *,
        field_path: str,
        downstream_dataset_urn: str,
        upstream_columns: List[Tuple[TableReference, str]],
    ) -> None:
        if not upstream_columns:
            return
        target.lineages.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[
                    make_schema_field_urn(self._dataset_urn(ref), column)
                    for ref, column in upstream_columns
                ],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(downstream_dataset_urn, field_path)],
            )
        )

    def _referenced_tables(
        self, expr: str, logical_tables: Dict[str, _LogicalTable]
    ) -> Set[str]:
        """Keys of the logical tables an expression reads columns from.

        Unqualified columns read `source`. MEASURE(name) references are other
        measures, not columns. Qualifiers that are not table names (e.g. a struct
        field access) are ignored.
        """
        qualifiers = _column_qualifiers(MEASURE_REF_RE.sub("0", expr))
        if qualifiers is None:
            return set()
        keys: Set[str] = set()
        for qualifier in qualifiers:
            key = _SOURCE_KEY if qualifier in (_SOURCE_KEY, SOURCE_ALIAS) else qualifier
            if key in logical_tables:
                keys.add(key)
        return keys

    def _relationships(
        self,
        logical_tables: Dict[str, _LogicalTable],
        *,
        dataset_urns: Dict[str, str],
        view_name: str,
    ) -> List[SemanticModelRelationshipInput]:
        relationships: List[SemanticModelRelationshipInput] = []
        skipped: List[str] = []
        for child in logical_tables.values():
            if child.join is None or child.parent_key is None:
                continue
            parent = logical_tables[child.parent_key]
            pairs = self._join_column_pairs(parent, child)
            label = f"{parent.alias} -> {child.alias}"
            if not pairs:
                skipped.append(label)
                continue
            relationships.append(
                SemanticModelRelationshipInput(
                    from_alias=parent.alias,
                    from_columns=[
                        self._join_key_field(parent, parent_col, dataset_urns)
                        for parent_col, _ in pairs
                    ],
                    to_alias=child.alias,
                    to_columns=[
                        self._join_key_field(child, child_col, dataset_urns)
                        for _, child_col in pairs
                    ],
                    name=f"{parent.alias}_to_{child.alias}",
                )
            )
        if skipped:
            self.report.num_metric_view_relationships_skipped += len(skipped)
            self.report.warning(
                title="Metric view join not emitted as a relationship",
                message="A join's `on` condition could not be read as equalities "
                "between columns qualified with the two tables' names (for example "
                "`source.customer_id = customer.id`), and it has no `using` list. "
                "Unqualified columns do not say which table they belong to, so the "
                "join was left out of the semantic model's relationships rather "
                "than guessed. Lineage is not affected.",
                context=f"{view_name}: {sorted(skipped)}",
            )
        return relationships

    @staticmethod
    def _join_column_pairs(
        parent: _LogicalTable, child: _LogicalTable
    ) -> Optional[List[Tuple[str, str]]]:
        assert child.join is not None
        using = child.join.get("using")
        if isinstance(using, list) and using:
            if all(isinstance(column, str) and column for column in using):
                return [(column, column) for column in using]
            return None

        on = child.join.get("on")
        if not isinstance(on, str) or not on.strip():
            return None
        try:
            tree = sqlglot.parse_one(on, dialect="databricks")
        except _SQLGLOT_PARSE_ERRORS:
            return None
        if tree is None:
            return None

        parent_qualifiers = {parent.key or SOURCE_ALIAS}
        child_qualifier = child.key
        pairs: List[Tuple[str, str]] = []
        for conjunct in tree.flatten() if isinstance(tree, sqlglot_exp.And) else [tree]:
            if not isinstance(conjunct, sqlglot_exp.EQ):
                return None
            left, right = conjunct.left, conjunct.right
            if not (
                isinstance(left, sqlglot_exp.Column)
                and isinstance(right, sqlglot_exp.Column)
            ):
                return None
            left_q, right_q = left.table.lower(), right.table.lower()
            if left_q in parent_qualifiers and right_q == child_qualifier:
                pairs.append((left.name, right.name))
            elif right_q in parent_qualifiers and left_q == child_qualifier:
                pairs.append((right.name, left.name))
            else:
                return None
        return pairs

    def _join_key_field(
        self,
        logical_table: _LogicalTable,
        column: str,
        dataset_urns: Dict[str, str],
    ) -> str:
        """Field path for a join key, adding the key as a field when no dimension covers it."""
        column_lower = column.lower()
        qualified = f"{logical_table.key or SOURCE_ALIAS}.{column_lower}"
        for field_path, expr in logical_table.field_exprs.items():
            if expr.strip().lower() in (column_lower, qualified):
                return field_path
        for field_path in logical_table.fields:
            if field_path.lower() == column_lower:
                return field_path

        # Join keys are columns of the logical table the YAML names in `on` /
        # `using`, but are rarely declared as dimensions. The relationship needs a
        # field on both sides, so add the key column itself.
        logical_table.fields[column] = SemanticFieldInput(
            field_path=column,
            type="",
            semantic_type=SemanticFieldTypeClass.DIMENSION,
        )
        logical_table.field_exprs[column] = column
        if self.config.include_column_lineage and logical_table.ref is not None:
            self._add_lineage(
                logical_table,
                field_path=column,
                downstream_dataset_urn=dataset_urns[logical_table.key],
                upstream_columns=[(logical_table.ref, column)],
            )
        return column

    def _logical_dataset(
        self,
        logical_table: _LogicalTable,
        *,
        view_ref: TableReference,
        model_urn: str,
        context: MetricViewEntityContext,
        browse_path: List[BrowsePathEntryClass],
    ) -> SemanticModelDataset:
        upstreams: Optional[UpstreamLineageClass] = None
        if logical_table.ref is not None:
            upstreams = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=self._dataset_urn(logical_table.ref),
                        type=DatasetLineageTypeClass.VIEW,
                    )
                ],
                fineGrainedLineages=logical_table.lineages or None,
            )
        # A subquery `source` has no table to point lineage at, so keep its SQL as
        # the logical dataset's view definition instead.
        view_definition = (
            logical_table.raw_source
            if logical_table.ref is None and logical_table.raw_source
            else None
        )
        name = self._logical_dataset_name(view_ref, logical_table.alias)
        return SemanticModelDataset(
            platform=PLATFORM,
            name=name,
            semantic_model=model_urn,
            alias=logical_table.alias,
            schema=list(logical_table.fields.values()),
            platform_instance=self.platform_instance,
            env=self.config.env,
            view_definition=view_definition,
            upstreams=upstreams,
            owners=context.owners,
            domain=context.domain_urn,
            extra_aspects=[
                ContainerClass(container=context.schema_container_urn),
                BrowsePathsV2Class(
                    path=browse_path
                    + [
                        BrowsePathEntryClass(
                            id=logical_table.alias,
                            urn=self._logical_dataset_urn(
                                view_ref, logical_table.alias
                            ),
                        )
                    ]
                ),
            ],
        )

    def _metrics(
        self,
        measures: List[Dict[str, Any]],
        *,
        spec: Dict[str, Any],
        table: Table,
        logical_tables: Dict[str, _LogicalTable],
        dataset_urns: Dict[str, str],
        model_urn: str,
        model_path: str,
        context: MetricViewEntityContext,
        browse_path: List[BrowsePathEntryClass],
        view_name: str,
    ) -> List[Metric]:
        view_filter = spec.get("filter")
        filter_str = (
            view_filter.strip()
            if isinstance(view_filter, str) and view_filter.strip()
            else None
        )
        metric_path = f"{model_path}.{table.ref.table}"

        metrics: Dict[str, Metric] = {}
        references: Dict[str, List[str]] = {}
        for entry in measures:
            name: str = entry["name"]
            key = name.lower()
            if key in metrics:
                continue
            expr = entry.get("expr")
            expr_str = expr if isinstance(expr, str) and expr.strip() else None
            measure_refs = MEASURE_REF_RE.findall(expr_str) if expr_str else []
            references[key] = measure_refs

            upstream_keys: Set[str] = set()
            if expr_str:
                upstream_keys = self._referenced_tables(expr_str, logical_tables)
                # A measure such as COUNT(1) reads no column but still aggregates
                # the rows of `source`; one built only from other measures reads
                # through them via derivedFrom instead.
                if not upstream_keys and not measure_refs:
                    upstream_keys = {_SOURCE_KEY}

            display_name = entry.get("display_name")
            metrics[key] = Metric(
                platform=PLATFORM,
                path=metric_path,
                id=name,
                semantic_model=model_urn,
                platform_instance=self.platform_instance,
                name=(
                    display_name
                    if isinstance(display_name, str) and display_name.strip()
                    else name
                ),
                description=_entry_description(entry),
                created=table.created_at,
                last_modified=table.updated_at,
                expression=(
                    DialectExpressionInput(
                        expression=_measure_expression(
                            expr_str, filter_str, entry.get("window")
                        ),
                        dialect=DialectClass.DATABRICKS,
                    )
                    if expr_str
                    else None
                ),
                upstream_datasets=sorted(dataset_urns[k] for k in upstream_keys),
                ai_context=_ai_context(entry),
                owners=context.owners,
                domain=context.domain_urn,
                extra_aspects=[BrowsePathsV2Class(path=browse_path)],
            )

        unresolved: Set[str] = set()
        for key, metric in metrics.items():
            derived_from: Dict[str, Metric] = {}
            for ref in references[key]:
                target = metrics.get(ref.lower())
                if target is None:
                    unresolved.add(ref)
                elif target is not metric:
                    derived_from[str(target.urn)] = target
            metric.set_derived_from(
                [derived_from[urn].urn for urn in sorted(derived_from)]
            )
        if unresolved:
            self.report.num_metric_view_unresolved_measure_refs += len(unresolved)
            self.report.warning(
                title="Unresolved MEASURE() reference in metric view",
                message="A MEASURE() reference does not match any measure defined in "
                "the same metric view, so no derivedFrom edge was emitted for it.",
                context=f"{view_name}: refs={sorted(unresolved)!r}",
            )
        return list(metrics.values())


def _named_entries(spec: Dict[str, Any], section: str) -> List[Dict[str, Any]]:
    entries = spec.get(section)
    if not isinstance(entries, list):
        return []
    return [
        entry
        for entry in entries
        if isinstance(entry, dict)
        and isinstance(entry.get("name"), str)
        and entry["name"]
    ]


def _column_qualifiers(expr: str) -> Optional[Set[str]]:
    try:
        tree = sqlglot.parse_one(expr, dialect="databricks")
    except _SQLGLOT_PARSE_ERRORS:
        return None
    if tree is None:
        return None
    return {
        (column.table or "").lower() for column in tree.find_all(sqlglot_exp.Column)
    }


def _description(spec: Dict[str, Any]) -> Optional[str]:
    comment = spec.get("comment")
    return comment if isinstance(comment, str) and comment.strip() else None


def _entry_description(entry: Dict[str, Any]) -> Optional[str]:
    # v1.1 uses `comment`; older YAML uses `description`.
    raw = entry.get("comment") or entry.get("description")
    return raw if isinstance(raw, str) and raw.strip() else None


def _ai_context(entry: Dict[str, Any]) -> Optional[AiContextInput]:
    synonyms = entry.get("synonyms")
    if not isinstance(synonyms, list):
        return None
    cleaned = [s.strip() for s in synonyms if isinstance(s, str) and s.strip()]
    return AiContextInput(synonyms=cleaned) if cleaned else None


def _measure_expression(expr: str, view_filter: Optional[str], window: object) -> str:
    # The view-level filter and a measure's window change what the measure
    # computes, but neither is part of `expr`. They are recorded as trailing
    # comments, as the dbt source does for filters and cumulative windows,
    # rather than rewritten into SQL the source never declared.
    parts = [expr]
    if view_filter:
        parts.append(f"/* filter: {_comment_safe(view_filter)} */")
    if isinstance(window, list) and window:
        rendered = json.dumps(window, separators=(",", ":"), default=str)
        parts.append(f"/* window: {_comment_safe(rendered)} */")
    return " ".join(parts)


def _comment_safe(text: str) -> str:
    return text.replace("*/", "* /")
