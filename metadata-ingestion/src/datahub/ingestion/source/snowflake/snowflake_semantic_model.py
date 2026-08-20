import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

import sqlglot
import sqlglot.errors
import sqlglot.expressions

from datahub.emitter.mce_builder import (
    make_dataplatform_instance_urn,
    make_tag_urn,
    make_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_structured_properties_to_entity_wu
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.snowflake.constants import SemanticViewColumnSubtype
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SemanticViewColumnMetadata,
    SnowflakeSemanticView,
    SnowflakeTag,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SNOWFLAKE_FIELD_TYPE_MAPPINGS,
    SnowflakeIdentifierBuilder,
)
from datahub.ingestion.source.sql.sql_utils import get_domain_wu
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    DateType,
    NullType,
    TimeType,
)
from datahub.metadata.schema_classes import (
    AiContextClass,
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DerivedMetricInputClass,
    DialectClass,
    DialectExpressionClass,
    DimensionClass,
    EdgeClass,
    ERModelRelationshipCardinalityClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    MySqlDDLClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SemanticFieldAnnotationClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
    SemanticModelRelationshipClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import (
    DataPlatformUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

_UNKNOWN_ACTOR_URN = "urn:li:corpuser:unknown"


@dataclass(frozen=True)
class _MetricKey:
    """Identity of a distinct semantic-view metric.

    Snowflake allows the same metric name on different logical tables, so a
    table-bound metric is identified by (name, logical table); a view-scoped
    (derived) metric has ``logical_table=None``.
    """

    name_key: str
    logical_table: Optional[str]


class SnowflakeSemanticModelMapper:
    """Maps a SnowflakeSemanticView onto semanticModel, dataset, and metric entities.

    Each logical table the view exposes becomes its own ``dataset`` entity
    (subtype ``Semantic Model Dataset``) carrying a ``semanticModelProperties``
    back-reference and a ``schemaMetadata`` projection of its dimension/fact
    columns; per-field semantic metadata is layered on each column's
    ``schemaField`` via ``semanticFieldAnnotation``. ``METRIC`` columns become
    first-class ``metric`` entities linked back to the model (``ModeledBy``,
    containment). The SemanticModel is a container of its datasets and metrics;
    lineage flows ``Metric -> Logical Dataset -> Physical Dataset`` via
    ``metricUpstreams.datasetUpstreams`` and each logical dataset's
    ``upstreamLineage``.
    """

    platform = "snowflake"

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        identifiers: SnowflakeIdentifierBuilder,
        domain_registry: Optional[DomainRegistry] = None,
    ) -> None:
        self.config = config
        self.report = report
        self.identifiers = identifiers
        self.domain_registry = domain_registry

    def gen_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
    ) -> Iterable[MetadataWorkUnit]:
        model_urn = self.identifiers.gen_semantic_model_urn(
            semantic_view.name, schema_name, db_name
        )
        distinct_metrics = self._distinct_metrics(semantic_view)
        logical_dataset_urns = self._build_logical_dataset_urns(
            semantic_view, schema_name, db_name
        )
        # Tables whose URN was already claimed by an earlier one, so they got no
        # dataset; their metrics would otherwise point at a model that omits them.
        collided_logical_tables = set(semantic_view.logical_to_physical_table) - set(
            logical_dataset_urns
        )
        # Indices for resolving derivedFrom references in derived-metric
        # expressions: a qualified `table.metric` resolves to a table-bound metric,
        # an unqualified `metric` to a view-scoped (derived) metric.
        # Excludes metrics on logical tables discarded for a URN collision: those
        # entities are never emitted, so resolving a reference against them would
        # produce a derivedFrom edge to a metric that does not exist.
        table_bound_metrics = {
            (key.logical_table, key.name_key): occ
            for key, occ in distinct_metrics.items()
            if key.logical_table is not None
            and key.logical_table not in collided_logical_tables
        }
        view_scoped_metrics = {
            key.name_key: occ
            for key, occ in distinct_metrics.items()
            if key.logical_table is None
        }
        shadowed_metric_names = self._shadowed_metric_names(semantic_view)
        lineages_by_dataset = self._route_lineages(
            fine_grained_lineages,
            logical_dataset_urns,
            model_urn=model_urn,
            semantic_view=semantic_view,
        )

        # Semantic model entity.
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=self._build_semantic_model_info(semantic_view, logical_dataset_urns),
        ).as_workunit()

        yield from self._gen_common_entity_aspects(
            entity_urn=model_urn,
            browse_path=self._browse_path_entries(db_name, schema_name),
        )

        yield from self._gen_view_tags(semantic_view, model_urn)
        yield from self._gen_domain_workunits(
            model_urn, semantic_view, schema_name, db_name
        )

        # One dataset entity per logical table.
        for logical_table, logical_dataset_urn in logical_dataset_urns.items():
            yield from self._gen_logical_dataset_workunits(
                semantic_view=semantic_view,
                logical_table=logical_table,
                logical_dataset_urn=logical_dataset_urn,
                model_urn=model_urn,
                schema_name=schema_name,
                db_name=db_name,
                fine_grained_lineages=lineages_by_dataset.get(logical_dataset_urn, []),
            )

        self._warn_unplaced_columns(semantic_view, logical_dataset_urns)

        # One metric entity per distinct metric. Snowflake allows the same metric
        # name on different logical tables (distinct, table-qualified metrics) plus
        # view-scoped derived metrics, so metrics are keyed by (logical table, name).
        for key, occurrence in distinct_metrics.items():
            # Only tables _build_logical_dataset_urns discarded for a URN
            # collision. A table simply absent from logical_to_physical_table is a
            # different case -- its metrics have always been emitted.
            if key.logical_table in collided_logical_tables:
                continue
            yield from self._gen_metric_workunits(
                occurrence=occurrence,
                logical_table=key.logical_table,
                table_bound_metrics=table_bound_metrics,
                view_scoped_metrics=view_scoped_metrics,
                shadowed_metric_names=shadowed_metric_names,
                model_urn=model_urn,
                logical_dataset_urns=logical_dataset_urns,
                semantic_view=semantic_view,
                schema_name=schema_name,
                db_name=db_name,
            )

    def _build_semantic_model_info(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_dataset_urns: "Dict[str, str]",
    ) -> SemanticModelInfoClass:
        # Membership lives on members (metricInfo.semanticModel /
        # semanticModelProperties.semanticModel), not on this aspect.
        return SemanticModelInfoClass(
            name=semantic_view.name,
            description=semantic_view.comment,
            created=self._audit_stamp(make_ts_millis(semantic_view.created)),
            lastModified=self._audit_stamp(make_ts_millis(semantic_view.last_altered)),
            nativeDefinition=(
                semantic_view.view_definition
                if self.config.include_view_definitions
                else None
            ),
            relationships=self._build_relationships(
                semantic_view, logical_dataset_urns
            ),
        )

    def _build_logical_dataset_urns(
        self,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> "Dict[str, str]":
        # Preserve declaration order so logical-dataset URN maps stay stable
        # across re-ingestions (dict iteration order == insertion order).
        urns: Dict[str, str] = {}
        claimed_by: Dict[str, str] = {}
        for logical_table in semantic_view.logical_to_physical_table:
            urn = self.identifiers.gen_semantic_model_dataset_urn(
                semantic_view.name, logical_table, schema_name, db_name
            )
            # Logical tables keep their stored casing, but the URN lowercases it
            # under convert_urns_to_lowercase, so `"orders"` and `"ORDERS"` can
            # still land on one dataset. Emitting both would write two tables'
            # schema, alias and lineage to it with the last one winning -- a
            # silent merge. Keep the first and say so instead.
            if urn in claimed_by:
                self.report.warning(
                    title="Semantic view logical tables share one dataset",
                    message="A semantic view declares logical tables that differ "
                    "only by case. DataHub lowercases the dataset name, so they "
                    "resolve to the same dataset and only the first is kept; the "
                    "rest are omitted along with their columns, metrics and column-level "
                    "lineage. Set "
                    "`convert_urns_to_lowercase: false` to keep them apart, or "
                    "rename them to differ by more than case.",
                    context=(
                        f"{semantic_view.name}: {logical_table!r} collides with "
                        f"{claimed_by[urn]!r}"
                    ),
                )
                continue
            claimed_by[urn] = logical_table
            urns[logical_table] = urn
        return urns

    def _warn_unplaced_columns(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_dataset_urns: "Dict[str, str]",
    ) -> None:
        # A non-metric column whose table_name doesn't match any logical table
        # in logical_to_physical_table can't be re-homed onto a logical dataset,
        # so it (and its semanticFieldAnnotation) is silently dropped. Surface
        # that to operators so the column isn't lost without a trace.
        # Judged per group, off the occurrences' own names rather than the dict
        # key, so a group filed under any other label reports identically.
        unplaced: Set[str] = set()
        for occurrences in semantic_view.column_occurrences.values():
            non_metric = [
                o for o in occurrences if o.subtype != SemanticViewColumnSubtype.METRIC
            ]
            # A group that is only metrics is not "unplaced" -- it is emitted as
            # metric entities, not as fields.
            if not non_metric:
                continue
            if not any(
                o.table_name and o.table_name in logical_dataset_urns
                for o in non_metric
            ):
                unplaced.add(non_metric[0].name)
        if unplaced:
            self.report.warning(
                title="Semantic view columns without a logical table",
                message=(
                    "Some dimensions/facts could not be associated with a"
                    " logical table and were omitted from the semantic model's"
                    " logical datasets."
                ),
                context=f"{semantic_view.name}: {sorted(unplaced)}",
            )

    def _build_relationships(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_dataset_urns: "Dict[str, str]",
    ) -> Optional[List[SemanticModelRelationshipClass]]:
        if not semantic_view.relationships:
            return None
        relationships: List[SemanticModelRelationshipClass] = []
        # A join names its tables by stored alias, not by URN, so a relationship
        # touching a logical table that was discarded for a URN collision points at
        # an alias no logical dataset carries -- the join silently never resolves.
        # Metrics and column lineage are already excluded for those tables; this is
        # the third place the collision reaches.
        dropped: List[str] = []
        for relationship in semantic_view.relationships:
            from_table = relationship.from_table
            absent = [
                table
                for table in (from_table, relationship.to_table)
                if table not in logical_dataset_urns
            ]
            if absent:
                dropped.append(
                    f"{relationship.name or 'unnamed'}: {', '.join(sorted(absent))}"
                )
                continue
            # Snowflake does not store cardinality; it infers one-to-one when the
            # from-side join columns uniquely identify a row - i.e. they are that
            # table's COMPLETE primary key or a COMPLETE declared unique key. A
            # subset of a composite key does not uniquely identify a row, so many
            # rows can share the value -> many-to-one.
            from_columns_key = {
                self.identifiers.column_identity_key(col)
                for col in relationship.from_columns
            }
            from_pk = semantic_view.primary_key_columns_by_table.get(from_table, set())
            from_unique_keys = semantic_view.unique_key_column_sets_by_table.get(
                from_table, []
            )
            is_one_to_one = bool(from_columns_key) and (
                from_columns_key == from_pk
                or any(from_columns_key == uk for uk in from_unique_keys)
            )
            cardinality = (
                ERModelRelationshipCardinalityClass.ONE_ONE
                if is_one_to_one
                else ERModelRelationshipCardinalityClass.N_ONE
            )
            relationships.append(
                SemanticModelRelationshipClass(
                    name=relationship.name,
                    # The stored name, matching each logical dataset's
                    # semanticModelProperties.alias, so join references resolve.
                    from_=from_table,
                    # A relationship key names the base-table column, while the
                    # logical dataset's field is named after the dimension defined
                    # over it, and the two can differ in casing. Resolve to the
                    # dimension name first, then apply the field-path rule.
                    fromColumns=[
                        self.identifiers.logical_dataset_field_path(
                            semantic_view.dimension_name_for_join_key(col, from_table),
                        )
                        for col in relationship.from_columns
                    ],
                    to=relationship.to_table,
                    toColumns=[
                        self.identifiers.logical_dataset_field_path(
                            semantic_view.dimension_name_for_join_key(
                                col, relationship.to_table
                            ),
                        )
                        for col in relationship.to_columns
                    ],
                    cardinality=cardinality,
                )
            )
        if dropped:
            self.report.warning(
                title="Semantic view relationship dropped for an absent table",
                message="A relationship referenced a logical table that has no "
                "dataset, so it was dropped rather than emitted as a join that "
                "cannot resolve. This happens when two logical tables differ only "
                "by case and collapse onto one dataset: the discarded table keeps "
                "its alias in the view's relationships, but nothing carries that "
                "alias.",
                context=f"{semantic_view.name}: {sorted(dropped)}",
            )
        self._warn_dangling_join_keys(semantic_view, relationships)
        return relationships

    def _warn_dangling_join_keys(
        self,
        semantic_view: SnowflakeSemanticView,
        relationships: List[SemanticModelRelationshipClass],
    ) -> None:
        # dimension_name_for_join_key falls through to the raw key when it cannot
        # resolve one, which anchors the relationship on a field path the logical
        # dataset never declares. Harmless-looking and invisible: the join simply
        # never resolves in the UI. Folding used to hide most mismatches; matching
        # on the stored name exactly means a disagreement now shows up here, so
        # say so rather than emit an edge that goes nowhere.
        declared: Dict[str, Set[str]] = {}
        for occurrences in semantic_view.column_occurrences.values():
            for occurrence in occurrences:
                # Metrics become their own entities, never fields on the logical
                # dataset, so a join key matching a metric name still dangles.
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if occurrence.table_name:
                    declared.setdefault(occurrence.table_name, set()).add(
                        self.identifiers.logical_dataset_field_path(occurrence.name)
                    )

        dangling: Set[str] = set()
        for relationship in relationships:
            for table, columns in (
                (relationship.from_, relationship.fromColumns),
                (relationship.to, relationship.toColumns),
            ):
                # Only judge tables we actually have columns for; a table with no
                # occurrences tells us nothing about whether the key is valid.
                known = declared.get(table)
                if not known:
                    continue
                for column in columns or []:
                    if column not in known:
                        dangling.add(f"{table}.{column}")

        if dangling:
            self.report.warning(
                title="Semantic view join key does not match any field",
                message="A relationship's join column could not be matched to a "
                "field on the logical dataset it references, so the join will not "
                "resolve. This happens when the dimension over the base-table "
                "column is renamed, or when Snowflake reports the column with "
                "different casing than the dimension that covers it.",
                context=f"{semantic_view.name}: {sorted(dangling)}",
            )

    def _route_lineages(
        self,
        fine_grained_lineages: List[FineGrainedLineageClass],
        logical_dataset_urns: "Dict[str, str]",
        model_urn: str,
        semantic_view: SnowflakeSemanticView,
    ) -> Dict[str, List[FineGrainedLineageClass]]:
        # Group non-metric FGLs by their downstream schemaField's parent dataset
        # (the logical dataset that owns the column). Metric FGLs are dropped
        # here: metric lineage is authored on the metric entity's
        # metricUpstreams aspect (Metric -> Logical Dataset), not as a
        # schemaField FGL on the logical dataset.
        #
        # A column that is a METRIC on a given logical table is emitted as a metric
        # entity, not a schemaField on that logical dataset, so an FGL onto it would
        # dangle. Keyed by (logical dataset, column) so a name that is a fact on one
        # table and a metric on another is handled correctly on each - dropping by
        # bare name would keep the dangling metric-side edge (or drop a valid fact
        # edge) whenever the name is shared across tables.
        metric_cols_by_urn: Dict[str, Set[str]] = {}
        # View-scoped (derived) metrics have no logical table, so the producer
        # anchors their FGL on the model URN. Their lineage flows via the metric
        # entity's derivedFrom, so drop those FGLs silently rather than warn.
        view_scoped_metric_names: Set[str] = set()
        for occurrences in semantic_view.column_occurrences.values():
            for occ in occurrences:
                if occ.subtype is not SemanticViewColumnSubtype.METRIC:
                    continue
                if occ.table_name:
                    dataset_urn = logical_dataset_urns.get(occ.table_name)
                    if dataset_urn:
                        metric_cols_by_urn.setdefault(dataset_urn, set()).add(
                            self.identifiers.logical_dataset_field_path(occ.name)
                        )
                else:
                    view_scoped_metric_names.add(
                        self.identifiers.logical_dataset_field_path(occ.name)
                    )

        by_dataset: Dict[str, List[FineGrainedLineageClass]] = {}
        for lineage in fine_grained_lineages:
            if not lineage.downstreams:
                continue
            # Compared as emitted field paths, not uppercased. The downstream is
            # already a logical-dataset field path, so folding both sides made a
            # dimension "col" match a metric "COL" and dropped the dimension's
            # lineage as if it were the metric's.
            downstream_field = self._downstream_field_name(lineage)
            parent_urn = SchemaFieldUrn.from_string(lineage.downstreams[0]).parent
            if downstream_field and downstream_field in metric_cols_by_urn.get(
                parent_urn, set()
            ):
                # Metric column on this logical table: lineage flows via the metric
                # entity, and the logical dataset has no schemaField for it.
                continue
            if parent_urn == model_urn:
                if downstream_field and downstream_field in view_scoped_metric_names:
                    # View-scoped (derived) metric: lineage flows via the metric
                    # entity's derivedFrom, not a schemaField FGL. Drop silently.
                    continue
                # The resolver only anchors a non-metric column on the model
                # URN when it has no logical-table association. A
                # dimension/fact without a logical table can't be re-homed onto
                # a logical dataset, so warn rather than silently collapse its
                # lineage onto the (now lineage-less) model.
                self.report.warning(
                    title=(
                        "Semantic view column lineage has no logical table association"
                    ),
                    message=(
                        "A non-metric column's fine-grained lineage could not be"
                        " associated with a logical table, so its column-level"
                        " lineage was dropped from the semantic model. Assign the"
                        " column to a logical table on the Snowflake side to"
                        " restore it."
                    ),
                    context=(f"{semantic_view.name}.{downstream_field or '<unknown>'}"),
                )
                continue
            by_dataset.setdefault(parent_urn, []).append(lineage)
        return by_dataset

    def _gen_domain_workunits(
        self,
        entity_urn: str,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        # Match the domain pattern against the view identifier so the model,
        # its metrics, and its logical datasets all land in the same domain.
        if self.domain_registry and self.config.domain:
            yield from get_domain_wu(
                dataset_name=self.identifiers.get_dataset_identifier(
                    semantic_view.name, schema_name, db_name
                ),
                entity_urn=entity_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
            )

    def _gen_logical_dataset_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table: str,
        logical_dataset_urn: str,
        model_urn: str,
        schema_name: str,
        db_name: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
    ) -> Iterable[MetadataWorkUnit]:
        physical_db, physical_schema, physical_table_name = (
            semantic_view.logical_to_physical_table[logical_table]
        )
        base_table_urn = self.identifiers.gen_dataset_urn(
            self.identifiers.get_dataset_identifier(
                physical_table_name, physical_schema, physical_db
            )
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=logical_dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=logical_dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.SEMANTIC_MODEL_DATASET]),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=logical_dataset_urn,
            aspect=SemanticModelPropertiesClass(
                alias=logical_table,
                semanticModel=model_urn,
            ),
        ).as_workunit()

        schema_fields = self._build_schema_fields(semantic_view, logical_table)
        # A logical table discarded for a URN collision still has columns, and the
        # producer anchored their lineage on the URN -- which is this one. Those
        # downstreams name field paths this dataset never declares, so the edge
        # resolves to nothing. Metrics are excluded upstream of here; lineage has
        # to be filtered against what actually got declared.
        declared_paths = {field.fieldPath for field in schema_fields}
        dangling = {
            path
            for path in (
                self._downstream_field_name(lineage)
                for lineage in fine_grained_lineages
            )
            if path is not None and path not in declared_paths
        }
        if dangling:
            fine_grained_lineages = [
                lineage
                for lineage in fine_grained_lineages
                if self._downstream_field_name(lineage) in declared_paths
            ]
            self.report.warning(
                title="Semantic view column lineage dropped for an absent field",
                message="Column-level lineage named a field the logical dataset "
                "does not declare, so it was dropped rather than emitted against "
                "nothing. This happens when two logical tables differ only by case "
                "and collapse onto one dataset: the discarded table's columns are "
                "not fields here, but its lineage still resolves to this URN.",
                context=f"{semantic_view.name}.{logical_table}: {sorted(dangling)}",
            )

        if schema_fields:
            yield MetadataChangeProposalWrapper(
                entityUrn=logical_dataset_urn,
                aspect=SchemaMetadataClass(
                    schemaName=logical_dataset_urn,
                    platform=str(DataPlatformUrn(self.platform)),
                    version=0,
                    hash="",
                    platformSchema=MySqlDDLClass(tableSchema=""),
                    fields=schema_fields,
                ),
            ).as_workunit()

            yield from self._gen_semantic_field_annotation_workunits(
                semantic_view=semantic_view,
                logical_table=logical_table,
                logical_dataset_urn=logical_dataset_urn,
            )

        # One table-level Upstream to the physical base table for this logical
        # table, plus the column-level FGLs routed to this logical dataset.
        yield MetadataChangeProposalWrapper(
            entityUrn=logical_dataset_urn,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=base_table_urn, type=DatasetLineageTypeClass.VIEW
                    )
                ],
                fineGrainedLineages=fine_grained_lineages or None,
            ),
        ).as_workunit()

        yield from self._gen_common_entity_aspects(
            entity_urn=logical_dataset_urn,
            browse_path=self._browse_path_entries(db_name, schema_name)
            + [
                BrowsePathEntryClass(
                    id=self.identifiers.snowflake_identifier(semantic_view.name),
                    urn=model_urn,
                ),
                BrowsePathEntryClass(
                    id=self.identifiers.snowflake_identifier(logical_table),
                    urn=logical_dataset_urn,
                ),
            ],
        )

        yield from self._gen_domain_workunits(
            logical_dataset_urn, semantic_view, schema_name, db_name
        )

        yield from self._gen_field_structured_property_workunits(
            semantic_view, logical_table, logical_dataset_urn
        )

        # Table-level synonyms have no aiContext home (aiContext is not
        # registered on dataset), so preserve them on the logical dataset's
        # datasetProperties.customProperties under the legacy key convention.
        table_synonyms = semantic_view.table_synonyms.get(logical_table, [])
        if table_synonyms:
            yield MetadataChangeProposalWrapper(
                entityUrn=logical_dataset_urn,
                aspect=DatasetPropertiesClass(
                    customProperties={
                        f"TABLE_SYNONYM_{logical_table}": ", ".join(table_synonyms)
                    }
                ),
            ).as_workunit()

    def _build_schema_fields(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table: str,
    ) -> List[SchemaFieldClass]:
        fields: List[SchemaFieldClass] = []
        seen_field_paths: Set[str] = set()
        for occurrences in semantic_view.column_occurrences.values():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if not (
                    occurrence.table_name and occurrence.table_name == logical_table
                ):
                    continue
                # Dedupe on the path actually emitted, not on the dict key. The
                # key is the stored name and the path may fold it, so whether two
                # keys land on one field depends on preserve_column_case -- a
                # decision made in _group_occurrences_by_case, far from here.
                # Keying the dedupe on the emitted value makes that irrelevant:
                # one field path, one field, however the bucketing changes.
                field_path = self.identifiers.logical_dataset_field_path(
                    occurrence.name
                )
                if field_path in seen_field_paths:
                    continue
                seen_field_paths.add(field_path)
                type_class = SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
                    _base_type(occurrence.data_type), NullType
                )
                fields.append(
                    SchemaFieldClass(
                        # Must match the anchor in
                        # snowflake_schema_gen.py::_generate_column_lineage_for_semantic_view
                        # so column-level lineage resolves.
                        fieldPath=field_path,
                        type=SchemaFieldDataTypeClass(type_class()),
                        nativeDataType=occurrence.data_type,
                        description=occurrence.comment,
                        nullable=True,
                        isPartOfKey=(
                            occurrence.identity_key
                            in semantic_view.primary_key_columns_by_table.get(
                                logical_table, set()
                            )
                        ),
                        globalTags=self._column_tags(occurrence.name, semantic_view),
                    )
                )
        return fields

    def _gen_semantic_field_annotation_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table: str,
        logical_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        seen_field_paths: Set[str] = set()
        for occurrences in semantic_view.column_occurrences.values():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if not (
                    occurrence.table_name and occurrence.table_name == logical_table
                ):
                    continue
                # Same dedupe rule as _build_schema_fields, for the same reason:
                # two annotations on one field URN is what a folded pair produces
                # if the key is trusted instead of the emitted path.
                field_path = self.identifiers.logical_dataset_field_path(
                    occurrence.name
                )
                if field_path in seen_field_paths:
                    continue
                seen_field_paths.add(field_path)
                field_type = (
                    SemanticFieldTypeClass.DIMENSION
                    if occurrence.subtype == SemanticViewColumnSubtype.DIMENSION
                    else SemanticFieldTypeClass.MEASURE
                )
                type_class = SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
                    _base_type(occurrence.data_type), NullType
                )
                field_urn = SchemaFieldUrn(logical_dataset_urn, field_path).urn()
                yield MetadataChangeProposalWrapper(
                    entityUrn=field_urn,
                    aspect=SemanticFieldAnnotationClass(
                        type=field_type,
                        expression=self._expression_for_field(
                            occurrence, logical_table
                        ),
                        dimension=(
                            DimensionClass(isTime=type_class in (DateType, TimeType))
                            if field_type == SemanticFieldTypeClass.DIMENSION
                            else None
                        ),
                    ),
                ).as_workunit()
                # Read synonyms from this table's own occurrence rather than the
                # view-merged map so they don't leak across same-named columns on
                # different logical tables. Emit aiContext only when non-empty.
                synonyms = occurrence.synonyms or []
                if synonyms:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=field_urn,
                        aspect=AiContextClass(synonyms=list(synonyms)),
                    ).as_workunit()

    def _gen_metric_workunits(
        self,
        occurrence: SemanticViewColumnMetadata,
        logical_table: Optional[str],
        table_bound_metrics: "Dict[Tuple[str, str], SemanticViewColumnMetadata]",
        view_scoped_metrics: Dict[str, SemanticViewColumnMetadata],
        shadowed_metric_names: Set[str],
        model_urn: str,
        logical_dataset_urns: "Dict[str, str]",
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        metric_urn = self.identifiers.gen_metric_urn(
            occurrence.name,
            semantic_view.name,
            schema_name,
            db_name,
            logical_table=logical_table,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=MetricInfoClass(
                name=occurrence.name,
                description=occurrence.comment,
                created=self._audit_stamp(make_ts_millis(semantic_view.created)),
                lastModified=self._audit_stamp(
                    make_ts_millis(semantic_view.last_altered)
                ),
                expression=self._metric_expression(occurrence),
                semanticModel=model_urn,
            ),
        ).as_workunit()

        # Read synonyms from this metric's own occurrence rather than the
        # view-merged map so they don't leak across same-named metrics on
        # different logical tables.
        metric_synonyms = occurrence.synonyms or []
        if metric_synonyms:
            yield MetadataChangeProposalWrapper(
                entityUrn=metric_urn,
                aspect=AiContextClass(synonyms=list(metric_synonyms)),
            ).as_workunit()

        yield from self._gen_common_entity_aspects(
            entity_urn=metric_urn,
            browse_path=self._browse_path_entries(db_name, schema_name)
            + [
                BrowsePathEntryClass(
                    id=self.identifiers.snowflake_identifier(semantic_view.name),
                    urn=model_urn,
                )
            ],
        )

        yield from self._emit_tags_for_entity(
            metric_urn, semantic_view.column_tags.get(occurrence.name, [])
        )

        yield from self._gen_domain_workunits(
            metric_urn, semantic_view, schema_name, db_name
        )

        # Parse once; both derivedFrom and datasetUpstreams walk the same AST.
        parsed = self._parse_metric_expression(occurrence, semantic_view)

        derived_from = self._derived_from_metrics(
            occurrence=occurrence,
            logical_table=logical_table,
            table_bound_metrics=table_bound_metrics,
            view_scoped_metrics=view_scoped_metrics,
            shadowed_metric_names=shadowed_metric_names,
            semantic_view=semantic_view,
            schema_name=schema_name,
            db_name=db_name,
            parsed=parsed,
        )
        # Always emit metricRelationships (even with empty derivedFrom) so
        # hasParentMetric is indexed as false - the /metrics sidebar lists root
        # metrics via hasParentMetric=false. These metrics have no parent, so
        # parentMetric is left unset.
        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=MetricRelationshipsClass(derivedFrom=derived_from),
        ).as_workunit()

        # Always emit metricUpstreams (even empty) so re-ingestion clears stale
        # server-side datasetUpstreams via whole-aspect UPSERT.
        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=MetricUpstreamsClass(
                datasetUpstreams=self._metric_dataset_upstreams(
                    logical_table=logical_table,
                    logical_dataset_urns=logical_dataset_urns,
                    table_bound_metrics=table_bound_metrics,
                    parsed=parsed,
                )
            ),
        ).as_workunit()

    def _parse_metric_expression(
        self,
        occurrence: SemanticViewColumnMetadata,
        semantic_view: SnowflakeSemanticView,
    ) -> Optional[sqlglot.expressions.Expr]:
        """Parse a metric expression once for derivedFrom and datasetUpstreams.

        Warns and increments the parse-failure counter on ``SqlglotError``;
        callers treat ``None`` as "no edges from this expression".
        """
        if not occurrence.expression:
            return None
        try:
            return sqlglot.parse_one(occurrence.expression, dialect="snowflake")
        except sqlglot.errors.SqlglotError as e:
            # Catch the SqlglotError base, not just ParseError: parse_one tokenizes
            # first and can raise TokenError (e.g. an unclosed quote), which would
            # otherwise escape and abort the remaining metrics for this view.
            self.report.warning(
                title="Could not parse semantic view metric expression",
                message=(
                    "A metric expression failed to parse, so its metric-to-metric "
                    "derivedFrom lineage and any expression-derived Metric → SMD "
                    "upstreams were skipped. The metric is still emitted."
                ),
                context=f"{semantic_view.name}.{occurrence.name}: {e}",
            )
            self.report.num_semantic_view_metric_expr_parse_failures += 1
            return None

    def _metric_dataset_upstreams(
        self,
        logical_table: Optional[str],
        logical_dataset_urns: "Dict[str, str]",
        table_bound_metrics: "Dict[Tuple[str, str], SemanticViewColumnMetadata]",
        parsed: Optional[sqlglot.expressions.Expr],
    ) -> List[EdgeClass]:
        """Resolve the Semantic Model Dataset URNs this metric reads from.

        Table-bound metrics have exactly one SMD upstream (their logical table).
        View-scoped/derived metrics may reference multiple logical tables via
        qualified column refs in their expression; those become SMD upstreams.
        Qualified refs that resolve to table-bound metrics are skipped — those
        are metric-to-metric edges via ``derivedFrom``. Metrics that only
        reference other metrics return an empty list — lineage reaches SMDs
        transitively via ``derivedFrom``.
        """
        if logical_table is not None:
            dataset_urn = logical_dataset_urns.get(logical_table)
            if dataset_urn:
                return [EdgeClass(destinationUrn=dataset_urn)]
            return []

        if parsed is None:
            return []

        # Qualified TABLE.col refs whose TABLE is a known logical dataset become
        # Metric → SMD edges, unless TABLE.NAME is itself a table-bound metric
        # (those are derivedFrom edges). Unqualified refs are metric-to-metric
        # or ambiguous, so they are skipped here.
        # Fold identifiers the same way _derived_from_metrics does: an unquoted
        # identifier folds to uppercase, a quoted one is already the stored
        # spelling. table_bound_metrics / logical_dataset_urns are keyed by that
        # stored spelling (and by column_identity_key for the name half).
        upstream_urns: Dict[str, EdgeClass] = {}
        for column in parsed.find_all(sqlglot.expressions.Column):
            if not column.table:
                continue
            stored_name = (
                column.name
                if getattr(column.this, "quoted", False)
                else column.name.upper()
            )
            name_key = self.identifiers.column_identity_key(stored_name)
            ref_table = (
                column.table
                if getattr(column.args.get("table"), "quoted", False)
                else column.table.upper()
            )
            if (ref_table, name_key) in table_bound_metrics:
                continue
            dataset_urn = logical_dataset_urns.get(ref_table)
            if dataset_urn:
                upstream_urns.setdefault(
                    dataset_urn, EdgeClass(destinationUrn=dataset_urn)
                )
        return [upstream_urns[urn] for urn in sorted(upstream_urns)]

    def _derived_from_metrics(
        self,
        occurrence: SemanticViewColumnMetadata,
        logical_table: Optional[str],
        table_bound_metrics: "Dict[Tuple[str, str], SemanticViewColumnMetadata]",
        view_scoped_metrics: Dict[str, SemanticViewColumnMetadata],
        shadowed_metric_names: Set[str],
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
        parsed: Optional[sqlglot.expressions.Expr],
    ) -> List[DerivedMetricInputClass]:
        # A derived metric references other metrics. Snowflake qualifies a
        # table-bound metric reference by its logical table (ORDERS.GROSS_REVENUE)
        # and leaves a view-scoped derived metric unqualified. So:
        #   - qualified TABLE.NAME resolves to a table-bound metric iff (TABLE, NAME)
        #     is a known metric; otherwise it is a fact/dimension column ref, skipped.
        #   - unqualified NAME resolves to a view-scoped metric; ambiguous names
        #     (also a column) are omitted - derivedFrom is isLineage:true, so a wrong
        #     edge is worse than a missing one. sqlglot skips string literals.
        if parsed is None:
            return []
        self_urn = self.identifiers.gen_metric_urn(
            occurrence.name,
            semantic_view.name,
            schema_name,
            db_name,
            logical_table=logical_table,
        )
        # Deduplicate by destination URN, keeping deterministic ordering.
        edges: Dict[str, DerivedMetricInputClass] = {}
        for column in parsed.find_all(sqlglot.expressions.Column):
            # Fold the way Snowflake resolves, exactly as
            # _extract_columns_from_expression does: an unquoted identifier folds
            # to uppercase, a quoted one is already the stored spelling. sqlglot
            # does not fold -- it reports what was written plus the quoted flag --
            # and SEMANTIC_METRICS.EXPRESSION hands back the DDL text with quotes
            # intact, so the flag survives to here.
            # There is exactly one spelling a reference can resolve to: verified
            # against Snowflake, an unquoted reference to a metric stored
            # `Order_Count` is rejected at CREATE SEMANTIC VIEW with "invalid
            # identifier 'ORDER_COUNT'". So no second candidate to fall back to.
            stored_name = (
                column.name
                if getattr(column.this, "quoted", False)
                else column.name.upper()
            )
            name_key = self.identifiers.column_identity_key(stored_name)
            if column.table:
                # Same fold as the metric name itself: an unquoted qualifier
                # resolves up, a quoted one is already the stored spelling.
                ref_table = (
                    column.table
                    if getattr(column.args.get("table"), "quoted", False)
                    else column.table.upper()
                )
                ref = table_bound_metrics.get((ref_table, name_key))
                if ref is None:
                    # Qualified fact/dimension column reference, not a metric.
                    continue
            else:
                # A name that is both a metric and a dimension/fact column of the
                # same view is genuinely ambiguous, so it gets no edge.
                if name_key in shadowed_metric_names:
                    continue
                ref = view_scoped_metrics.get(name_key)
                if ref is None:
                    continue
                ref_table = None
            dest_urn = self.identifiers.gen_metric_urn(
                # Use the referenced metric's original-case name so the URN matches
                # when convert_urns_to_lowercase=False.
                ref.name,
                semantic_view.name,
                schema_name,
                db_name,
                logical_table=ref_table,
            )
            if dest_urn == self_urn:
                continue
            edges.setdefault(dest_urn, DerivedMetricInputClass(destinationUrn=dest_urn))
        return [edges[urn] for urn in sorted(edges)]

    def _gen_common_entity_aspects(
        self, entity_urn: str, browse_path: List[BrowsePathEntryClass]
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=DataPlatformInstanceClass(
                    platform=str(DataPlatformUrn(self.platform)),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=BrowsePathsV2Class(path=browse_path)
        ).as_workunit()

    def _browse_path_entries(
        self, db_name: str, schema_name: str
    ) -> List[BrowsePathEntryClass]:
        entries: List[BrowsePathEntryClass] = []
        if self.config.platform_instance:
            entries.append(
                BrowsePathEntryClass(
                    id=self.config.platform_instance,
                    urn=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                )
            )
        entries.append(
            BrowsePathEntryClass(id=self.identifiers.snowflake_identifier(db_name))
        )
        entries.append(
            BrowsePathEntryClass(id=self.identifiers.snowflake_identifier(schema_name))
        )
        return entries

    def _gen_view_tags(
        self, semantic_view: SnowflakeSemanticView, model_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_tags_for_entity(model_urn, semantic_view.tags or [])

    def _tag_associations(self, tags: List[SnowflakeTag]) -> List[TagAssociationClass]:
        return [
            TagAssociationClass(
                tag=make_tag_urn(
                    self.identifiers.snowflake_identifier(tag.tag_identifier())
                )
            )
            for tag in tags
        ]

    def _structured_property_values(
        self, tags: List[SnowflakeTag]
    ) -> Dict[StructuredPropertyUrn, str]:
        return {
            StructuredPropertyUrn(
                self.identifiers.snowflake_identifier(
                    tag.structured_property_identifier()
                )
            ): tag.value
            for tag in tags
        }

    def _emit_tags_for_entity(
        self, entity_urn: str, tags: List[SnowflakeTag]
    ) -> Iterable[MetadataWorkUnit]:
        # Shared by the model (view-level tags) and each metric (column tags):
        # both need the same GlobalTags-vs-structured-properties branching.
        if not tags:
            return
        if self.config.extract_tags_as_structured_properties:
            yield from add_structured_properties_to_entity_wu(
                entity_urn,
                self._structured_property_values(tags),
                write_mode=self.config.structured_properties_write_mode,
            )
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=GlobalTagsClass(tags=self._tag_associations(tags)),
            ).as_workunit()

    def _column_tags(
        self, column_name: str, semantic_view: SnowflakeSemanticView
    ) -> Optional[GlobalTagsClass]:
        if (
            column_name not in semantic_view.column_tags
            or self.config.extract_tags_as_structured_properties
        ):
            return None
        return GlobalTagsClass(
            tags=self._tag_associations(semantic_view.column_tags[column_name])
        )

    def _gen_field_structured_property_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table: str,
        logical_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        # In structured-properties mode there is no field aspect to carry column
        # tags, so emit them as schemaField-level SP MCPs anchored on this
        # logical dataset's schemaField URNs. A column on multiple logical
        # tables emits SPs on each logical dataset's schemaField.
        if not self.config.extract_tags_as_structured_properties:
            return
        for _column_name, occurrences in semantic_view.column_occurrences.items():
            occurrence = next(
                (
                    o
                    for o in occurrences
                    if o.subtype != SemanticViewColumnSubtype.METRIC
                    and o.table_name
                    and o.table_name == logical_table
                    and o.name in semantic_view.column_tags
                ),
                None,
            )
            if occurrence is None:
                continue
            field_urn = SchemaFieldUrn(
                logical_dataset_urn,
                self.identifiers.logical_dataset_field_path(occurrence.name),
            ).urn()
            yield from add_structured_properties_to_entity_wu(
                field_urn,
                self._structured_property_values(
                    semantic_view.column_tags[occurrence.name]
                ),
                write_mode=self.config.structured_properties_write_mode,
            )

    def _expression_for_field(
        self,
        occurrence: SemanticViewColumnMetadata,
        logical_table: str,
    ) -> MetricExpressionClass:
        # SemanticFieldAnnotation.expression is required, so synthesize a
        # trivial qualified column reference when Snowflake reports no
        # expression for a dimension/fact (they are expression-backed).
        return MetricExpressionClass(
            dialects=[
                DialectExpressionClass(
                    dialect=DialectClass.SNOWFLAKE,
                    expression=(
                        occurrence.expression or f"{logical_table}.{occurrence.name}"
                    ),
                )
            ]
        )

    def _metric_expression(
        self, occurrence: SemanticViewColumnMetadata
    ) -> Optional[MetricExpressionClass]:
        # MetricInfo.expression is optional - omit it rather than fabricating a name.
        if not occurrence.expression:
            return None
        return MetricExpressionClass(
            dialects=[
                DialectExpressionClass(
                    dialect=DialectClass.SNOWFLAKE,
                    expression=occurrence.expression,
                )
            ]
        )

    def _distinct_metrics(
        self, semantic_view: SnowflakeSemanticView
    ) -> "Dict[_MetricKey, SemanticViewColumnMetadata]":
        # One distinct metric per (logical table, name). Snowflake allows the same
        # metric name on different logical tables (distinct, table-qualified
        # metrics); view-scoped derived metrics carry no logical table. Keying by
        # bare name would silently collapse table-bound siblings into one entity.
        grouped: "Dict[_MetricKey, List[SemanticViewColumnMetadata]]" = {}
        for occurrences in semantic_view.column_occurrences.values():
            for occurrence in occurrences:
                if occurrence.subtype != SemanticViewColumnSubtype.METRIC:
                    continue
                key = _MetricKey(
                    name_key=occurrence.identity_key,
                    # The stored name, matching logical_to_physical_table and the
                    # logical dataset URNs. Uppercasing here collapsed the metrics
                    # of two logical tables that differ only by case.
                    logical_table=occurrence.table_name or None,
                )
                grouped.setdefault(key, []).append(occurrence)

        metrics: "Dict[_MetricKey, SemanticViewColumnMetadata]" = {}
        for key, group in grouped.items():
            # A single (table, name) should have one definition. Snowflake's row
            # order isn't stable, so pick deterministically (smallest expression)
            # to keep metricInfo from flapping; warn only on a genuine duplicate.
            canonical = min(group, key=lambda o: o.expression or "")
            metrics[key] = canonical
            if len({o.expression for o in group}) > 1:
                self.report.warning(
                    title="Semantic view metric declared with conflicting expressions",
                    message="A metric is declared more than once on the same logical "
                    "table with different expressions. The occurrence with the "
                    "lexicographically smallest expression is used; the others are "
                    "dropped.",
                    context=f"{semantic_view.name}.{canonical.name}",
                )
        return metrics

    def _shadowed_metric_names(self, semantic_view: SnowflakeSemanticView) -> Set[str]:
        # A column name that is both a metric and a dimension/fact column of the
        # same view is ambiguous; used by _derived_from_metrics to avoid emitting
        # a wrong derivedFrom edge (isLineage:true, so a wrong edge is worse than
        # a missing one).
        # Read from each occurrence's own name, not the bucket key: the key is
        # only the first occurrence's spelling, which need not be the spelling
        # the shadowing column is actually stored under.
        # Keyed the same way the metric indices are, so the comparison is
        # like-for-like -- with casing preserved a dimension "col" must not
        # shadow a metric "COL", which are two different objects.
        return {
            occurrence.identity_key
            for occurrences in semantic_view.column_occurrences.values()
            for occurrence in occurrences
            if occurrence.subtype != SemanticViewColumnSubtype.METRIC
        }

    @staticmethod
    def _downstream_field_name(
        lineage: FineGrainedLineageClass,
    ) -> Optional[str]:
        if not lineage.downstreams:
            return None
        if len(lineage.downstreams) > 1:
            # Every semantic-view FGL has exactly one downstream per (column,
            # logical-table); log rather than crash if that assumption is violated.
            logger.debug(
                f"Semantic view fine-grained lineage has {len(lineage.downstreams)} "
                f"downstreams; only the first ({lineage.downstreams[0]}) is used "
                f"for metric-vs-model routing."
            )
        return SchemaFieldUrn.from_string(lineage.downstreams[0]).field_path

    @staticmethod
    def _audit_stamp(time_millis: Optional[int]) -> Optional[AuditStampClass]:
        if time_millis is None:
            return None
        return AuditStampClass(time=time_millis, actor=_UNKNOWN_ACTOR_URN)


def _base_type(data_type: str) -> str:
    # information_schema reports parameterized types (e.g. VARCHAR(16777216));
    # the type mapping is keyed on the bare type name.
    return data_type.split("(")[0].strip().upper()
