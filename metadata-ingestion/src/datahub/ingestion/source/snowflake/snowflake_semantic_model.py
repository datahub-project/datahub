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
    ERModelRelationshipCardinalityClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
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
    (derived) metric has ``logical_table_upper=None``.
    """

    name_upper: str
    logical_table_upper: Optional[str]


class SnowflakeSemanticModelMapper:
    """Maps a SnowflakeSemanticView onto semanticModel, dataset, and metric entities.

    Each logical table the view exposes becomes its own ``dataset`` entity
    (subtype ``Semantic Model Dataset``) carrying a ``semanticModelProperties``
    back-reference and a ``schemaMetadata`` projection of its dimension/fact
    columns; per-field semantic metadata is layered on each column's
    ``schemaField`` via ``semanticFieldAnnotation``. ``METRIC`` columns become
    first-class ``metric`` entities linked back to the model (``ModeledBy``).
    Lineage flows ``Metric -> SemanticModel -> Logical Dataset -> Physical Dataset``.
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
        # Indices for resolving derivedFrom references in derived-metric
        # expressions: a qualified `table.metric` resolves to a table-bound metric,
        # an unqualified `metric` to a view-scoped (derived) metric.
        table_bound_metrics = {
            (key.logical_table_upper, key.name_upper): occ
            for key, occ in distinct_metrics.items()
            if key.logical_table_upper is not None
        }
        view_scoped_metrics = {
            key.name_upper: occ
            for key, occ in distinct_metrics.items()
            if key.logical_table_upper is None
        }
        shadowed_metric_names = self._shadowed_metric_names(semantic_view)
        logical_dataset_urns = self._build_logical_dataset_urns(
            semantic_view, schema_name, db_name
        )
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
        for logical_table_upper, logical_dataset_urn in logical_dataset_urns.items():
            yield from self._gen_logical_dataset_workunits(
                semantic_view=semantic_view,
                logical_table_upper=logical_table_upper,
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
            yield from self._gen_metric_workunits(
                occurrence=occurrence,
                logical_table=key.logical_table_upper,
                table_bound_metrics=table_bound_metrics,
                view_scoped_metrics=view_scoped_metrics,
                shadowed_metric_names=shadowed_metric_names,
                model_urn=model_urn,
                semantic_view=semantic_view,
                schema_name=schema_name,
                db_name=db_name,
            )

    def _build_semantic_model_info(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_dataset_urns: "Dict[str, str]",
    ) -> SemanticModelInfoClass:
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
            datasets=list(logical_dataset_urns.values()),
            relationships=self._build_relationships(semantic_view),
        )

    def _build_logical_dataset_urns(
        self,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> "Dict[str, str]":
        # Preserve declaration order so SemanticModelInfo.datasets is stable
        # across re-ingestions (dict iteration order == insertion order).
        urns: Dict[str, str] = {}
        for logical_table_upper in semantic_view.logical_to_physical_table:
            urns[logical_table_upper] = self.identifiers.gen_semantic_model_dataset_urn(
                semantic_view.name, logical_table_upper, schema_name, db_name
            )
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
        placed: Set[str] = set()
        for col_name_upper, occurrences in semantic_view.column_occurrences.items():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if (
                    occurrence.table_name
                    and occurrence.table_name.upper() in logical_dataset_urns
                ):
                    placed.add(col_name_upper)
        unplaced = set(semantic_view.column_occurrences.keys()) - placed
        # Columns that are only metrics (no non-metric occurrence) are not
        # "unplaced" — they're emitted as metric entities, not fields.
        unplaced = {
            col
            for col in unplaced
            if any(
                o.subtype != SemanticViewColumnSubtype.METRIC
                for o in semantic_view.column_occurrences[col]
            )
        }
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
        self, semantic_view: SnowflakeSemanticView
    ) -> Optional[List[SemanticModelRelationshipClass]]:
        if not semantic_view.relationships:
            return None
        relationships: List[SemanticModelRelationshipClass] = []
        for relationship in semantic_view.relationships:
            from_table_upper = relationship.from_table.upper()
            # Snowflake does not store cardinality; it infers one-to-one when the
            # join columns are the primary key on both sides. The referenced (to)
            # side is always a primary key, so the join is one-to-one exactly when
            # the from-side join columns are that table's COMPLETE primary key -
            # a subset of a composite key does not uniquely identify a row, so many
            # rows can share the value -> many-to-one.
            from_columns_upper = {col.upper() for col in relationship.from_columns}
            from_pk = semantic_view.primary_key_columns_by_table.get(
                from_table_upper, set()
            )
            cardinality = (
                ERModelRelationshipCardinalityClass.ONE_ONE
                if from_columns_upper and from_columns_upper == from_pk
                else ERModelRelationshipCardinalityClass.N_ONE
            )
            relationships.append(
                SemanticModelRelationshipClass(
                    name=relationship.name,
                    # Uppercased to match each logical dataset's
                    # semanticModelProperties.alias, so join references resolve.
                    from_=from_table_upper,
                    # Join columns must match the logical-dataset schemaField paths,
                    # which are built as snowflake_identifier(name.upper()) and so
                    # get lowercased under convert_urns_to_lowercase=True. Apply the
                    # same normalization here or the join keys stay uppercase and
                    # never resolve against the lowercased field paths.
                    fromColumns=[
                        self.identifiers.snowflake_identifier(col.upper())
                        for col in relationship.from_columns
                    ],
                    to=relationship.to_table.upper(),
                    toColumns=[
                        self.identifiers.snowflake_identifier(col.upper())
                        for col in relationship.to_columns
                    ],
                    cardinality=cardinality,
                )
            )
        return relationships

    def _route_lineages(
        self,
        fine_grained_lineages: List[FineGrainedLineageClass],
        logical_dataset_urns: "Dict[str, str]",
        model_urn: str,
        semantic_view: SnowflakeSemanticView,
    ) -> Dict[str, List[FineGrainedLineageClass]]:
        # Group non-metric FGLs by their downstream schemaField's parent dataset
        # (the logical dataset that owns the column). Metric FGLs are dropped:
        # metric lineage flows Metric -> SemanticModel -> Logical Dataset, with
        # no metricUpstreams for semantic-model-backed metrics.
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
                    dataset_urn = logical_dataset_urns.get(occ.table_name.upper())
                    if dataset_urn:
                        metric_cols_by_urn.setdefault(dataset_urn, set()).add(
                            occ.name.upper()
                        )
                else:
                    view_scoped_metric_names.add(occ.name.upper())

        by_dataset: Dict[str, List[FineGrainedLineageClass]] = {}
        for lineage in fine_grained_lineages:
            if not lineage.downstreams:
                continue
            downstream_field = self._downstream_field_name(lineage)
            downstream_upper = downstream_field.upper() if downstream_field else None
            parent_urn = SchemaFieldUrn.from_string(lineage.downstreams[0]).parent
            if (
                downstream_upper
                and downstream_upper in metric_cols_by_urn.get(parent_urn, set())
            ):
                # Metric column on this logical table: lineage flows via the metric
                # entity, and the logical dataset has no schemaField for it.
                continue
            if parent_urn == model_urn:
                if downstream_upper and downstream_upper in view_scoped_metric_names:
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
        logical_table_upper: str,
        logical_dataset_urn: str,
        model_urn: str,
        schema_name: str,
        db_name: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
    ) -> Iterable[MetadataWorkUnit]:
        physical_db, physical_schema, physical_table_name = (
            semantic_view.logical_to_physical_table[logical_table_upper]
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
                alias=logical_table_upper,
                semanticModel=model_urn,
            ),
        ).as_workunit()

        schema_fields = self._build_schema_fields(semantic_view, logical_table_upper)
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
                logical_table_upper=logical_table_upper,
                logical_dataset_urn=logical_dataset_urn,
            )

        # Re-home the base-table lineage that previously hung off the model:
        # one table-level Upstream to the physical base table for this logical
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
                    id=self.identifiers.snowflake_identifier(logical_table_upper),
                    urn=logical_dataset_urn,
                ),
            ],
        )

        yield from self._gen_domain_workunits(
            logical_dataset_urn, semantic_view, schema_name, db_name
        )

        yield from self._gen_field_structured_property_workunits(
            semantic_view, logical_table_upper, logical_dataset_urn
        )

        # Table-level synonyms have no aiContext home (aiContext is not
        # registered on dataset), so preserve them on the logical dataset's
        # datasetProperties.customProperties under the legacy key convention.
        table_synonyms = semantic_view.table_synonyms.get(logical_table_upper, [])
        if table_synonyms:
            yield MetadataChangeProposalWrapper(
                entityUrn=logical_dataset_urn,
                aspect=DatasetPropertiesClass(
                    customProperties={
                        f"TABLE_SYNONYM_{logical_table_upper}": ", ".join(
                            table_synonyms
                        )
                    }
                ),
            ).as_workunit()

    def _build_schema_fields(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table_upper: str,
    ) -> List[SchemaFieldClass]:
        fields: List[SchemaFieldClass] = []
        seen_columns_upper: Set[str] = set()
        for col_name_upper, occurrences in semantic_view.column_occurrences.items():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if not (
                    occurrence.table_name
                    and occurrence.table_name.upper() == logical_table_upper
                ):
                    continue
                if col_name_upper in seen_columns_upper:
                    continue
                seen_columns_upper.add(col_name_upper)
                type_class = SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
                    _base_type(occurrence.data_type), NullType
                )
                fields.append(
                    SchemaFieldClass(
                        # Must match the col_name_upper anchor in
                        # snowflake_schema_gen.py::_generate_column_lineage_for_semantic_view
                        # so column-level lineage resolves.
                        fieldPath=self.identifiers.snowflake_identifier(
                            occurrence.name.upper()
                        ),
                        type=SchemaFieldDataTypeClass(type_class()),
                        nativeDataType=occurrence.data_type,
                        description=occurrence.comment,
                        nullable=True,
                        isPartOfKey=(
                            occurrence.name.upper()
                            in semantic_view.primary_key_columns_by_table.get(
                                logical_table_upper, set()
                            )
                        ),
                        globalTags=self._column_tags(occurrence.name, semantic_view),
                    )
                )
        return fields

    def _gen_semantic_field_annotation_workunits(
        self,
        semantic_view: SnowflakeSemanticView,
        logical_table_upper: str,
        logical_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        seen_columns_upper: Set[str] = set()
        for col_name_upper, occurrences in semantic_view.column_occurrences.items():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    continue
                if not (
                    occurrence.table_name
                    and occurrence.table_name.upper() == logical_table_upper
                ):
                    continue
                if col_name_upper in seen_columns_upper:
                    continue
                seen_columns_upper.add(col_name_upper)
                field_type = (
                    SemanticFieldTypeClass.DIMENSION
                    if occurrence.subtype == SemanticViewColumnSubtype.DIMENSION
                    else SemanticFieldTypeClass.MEASURE
                )
                type_class = SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
                    _base_type(occurrence.data_type), NullType
                )
                field_urn = SchemaFieldUrn(
                    logical_dataset_urn,
                    self.identifiers.snowflake_identifier(occurrence.name.upper()),
                ).urn()
                yield MetadataChangeProposalWrapper(
                    entityUrn=field_urn,
                    aspect=SemanticFieldAnnotationClass(
                        type=field_type,
                        expression=self._expression_for_field(
                            occurrence, logical_table_upper
                        ),
                        dimension=(
                            DimensionClass(isTime=type_class in (DateType, TimeType))
                            if field_type == SemanticFieldTypeClass.DIMENSION
                            else None
                        ),
                    ),
                ).as_workunit()
                # Column synonyms live on the first-class aiContext aspect of
                # the same schemaField (the model no longer embeds them on the
                # field). Read from this table's own occurrence rather than the
                # view-merged map so synonyms don't leak across same-named columns
                # on different logical tables. Emit only when non-empty.
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

        # Metric synonyms live on the first-class aiContext aspect of the
        # metric entity (the model no longer embeds them on metricInfo). Read from
        # this metric's own occurrence rather than the view-merged map so synonyms
        # don't leak across same-named metrics on different logical tables.
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

        derived_from = self._derived_from_metrics(
            occurrence=occurrence,
            logical_table=logical_table,
            table_bound_metrics=table_bound_metrics,
            view_scoped_metrics=view_scoped_metrics,
            shadowed_metric_names=shadowed_metric_names,
            semantic_view=semantic_view,
            schema_name=schema_name,
            db_name=db_name,
        )
        # Always emit metricRelationships (even with empty derivedFrom) so
        # hasParentMetric is indexed as false - the /metrics sidebar lists root
        # metrics via hasParentMetric=false. These metrics have no parent, so
        # parentMetric is left unset. metricUpstreams is intentionally not
        # emitted: lineage flows Metric -> SemanticModel (ModeledBy) -> Logical
        # Dataset (Contains) -> Physical Dataset (upstreamLineage).
        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=MetricRelationshipsClass(derivedFrom=derived_from),
        ).as_workunit()

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
    ) -> List[DerivedMetricInputClass]:
        # A derived metric references other metrics. Snowflake qualifies a
        # table-bound metric reference by its logical table (ORDERS.GROSS_REVENUE)
        # and leaves a view-scoped derived metric unqualified. So:
        #   - qualified TABLE.NAME resolves to a table-bound metric iff (TABLE, NAME)
        #     is a known metric; otherwise it is a fact/dimension column ref, skipped.
        #   - unqualified NAME resolves to a view-scoped metric; ambiguous names
        #     (also a column) are omitted - derivedFrom is isLineage:true, so a wrong
        #     edge is worse than a missing one. sqlglot skips string literals.
        if not occurrence.expression:
            return []
        try:
            parsed = sqlglot.parse_one(occurrence.expression, dialect="snowflake")
        except sqlglot.errors.SqlglotError as e:
            # Catch the SqlglotError base, not just ParseError: parse_one tokenizes
            # first and can raise TokenError (e.g. an unclosed quote), which would
            # otherwise escape and abort the remaining metrics for this view.
            # A metric whose expression won't parse loses its derivedFrom lineage.
            # Surface it (not just a debug log + counter) so operators can see which
            # metric was affected; the metric itself is still emitted without edges.
            self.report.warning(
                title="Could not parse semantic view metric expression",
                message=(
                    "A metric expression failed to parse, so its metric-to-metric "
                    "derivedFrom lineage was skipped. The metric is still emitted."
                ),
                context=f"{semantic_view.name}.{occurrence.name}: {e}",
            )
            self.report.num_semantic_view_metric_expr_parse_failures += 1
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
            name_upper = column.name.upper()
            if column.table:
                ref_table = column.table.upper()
                ref = table_bound_metrics.get((ref_table, name_upper))
                if ref is None:
                    # Qualified fact/dimension column reference, not a metric.
                    continue
            else:
                if name_upper in shadowed_metric_names:
                    continue
                ref = view_scoped_metrics.get(name_upper)
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
        logical_table_upper: str,
        logical_dataset_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        # In structured-properties mode there is no field aspect to carry column
        # tags, so emit them as schemaField-level SP MCPs anchored on this
        # logical dataset's schemaField URNs. A column on multiple logical
        # tables emits SPs on each logical dataset's schemaField.
        if not self.config.extract_tags_as_structured_properties:
            return
        for col_name_upper, occurrences in semantic_view.column_occurrences.items():
            occurrence = next(
                (
                    o
                    for o in occurrences
                    if o.subtype != SemanticViewColumnSubtype.METRIC
                    and o.table_name
                    and o.table_name.upper() == logical_table_upper
                    and o.name in semantic_view.column_tags
                ),
                None,
            )
            if occurrence is None:
                continue
            field_urn = SchemaFieldUrn(
                logical_dataset_urn,
                self.identifiers.snowflake_identifier(col_name_upper),
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
        logical_table_upper: str,
    ) -> MetricExpressionClass:
        # SemanticFieldAnnotation.expression is required, so synthesize a
        # trivial qualified column reference when Snowflake reports no
        # expression for a dimension/fact (they are expression-backed).
        return MetricExpressionClass(
            dialects=[
                DialectExpressionClass(
                    dialect=DialectClass.SNOWFLAKE,
                    expression=(
                        occurrence.expression
                        or f"{logical_table_upper}.{occurrence.name}"
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
                    name_upper=occurrence.name.upper(),
                    logical_table_upper=(
                        occurrence.table_name.upper() if occurrence.table_name else None
                    ),
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

    def _split_lineages_by_metric(
        self,
        fine_grained_lineages: List[FineGrainedLineageClass],
        metric_names_upper: Set[str],
        shadowed_metric_names: Set[str],
    ) -> Tuple[List[FineGrainedLineageClass], Dict[str, List[FineGrainedLineageClass]]]:
        # Retained for the unit test that exercises the routing heuristic in
        # isolation; production routing goes through _route_lineages, which
        # drops metric FGLs (no metricUpstreams) and groups the rest by their
        # downstream schemaField's parent logical dataset.
        model_lineages: List[FineGrainedLineageClass] = []
        metric_lineages: Dict[str, List[FineGrainedLineageClass]] = {}
        for lineage in fine_grained_lineages:
            downstream_field = self._downstream_field_name(lineage)
            downstream_upper = downstream_field.upper() if downstream_field else None
            if (
                downstream_upper
                and downstream_upper in metric_names_upper
                and downstream_upper not in shadowed_metric_names
            ):
                metric_lineages.setdefault(downstream_upper, []).append(lineage)
            else:
                model_lineages.append(lineage)
        return model_lineages, metric_lineages

    @staticmethod
    def _shadowed_metric_names(semantic_view: SnowflakeSemanticView) -> Set[str]:
        # A column name that is both a metric and a dimension/fact column of the
        # same view is ambiguous; used by _derived_from_metrics (avoid a wrong
        # derivedFrom edge) and _route_lineages (keep the column's own lineage on
        # its logical dataset rather than dropping it as a metric).
        return {
            col_upper
            for col_upper, occs in semantic_view.column_occurrences.items()
            if any(o.subtype != SemanticViewColumnSubtype.METRIC for o in occs)
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
