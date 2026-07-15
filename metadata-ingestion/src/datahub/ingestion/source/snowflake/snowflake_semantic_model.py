import logging
from typing import Dict, Iterable, List, Optional, Set

import sqlglot
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
    DerivedMetricInputClass,
    DialectClass,
    DialectExpressionClass,
    DimensionClass,
    EdgeClass,
    FineGrainedLineageClass,
    GlobalTagsClass,
    MetricExpressionClass,
    MetricInfoClass,
    MetricRelationshipsClass,
    MetricUpstreamsClass,
    ModelDatasetClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SemanticFieldClass,
    SemanticFieldTypeClass,
    SemanticModelInfoClass,
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


class SnowflakeSemanticModelMapper:
    """Maps a SnowflakeSemanticView onto semanticModel and metric entities.

    Dimensions and facts become SemanticFields grouped under the logical dataset
    (ModelDataset) they are defined on; METRIC columns become first-class metric
    entities linked back to the semanticModel via ModeledBy.
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
        metric_occurrences = self._metric_occurrences(semantic_view)
        metric_names_upper = set(metric_occurrences.keys())
        model_lineages, metric_lineages = self._split_lineages_by_metric(
            fine_grained_lineages, metric_names_upper
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=self._build_semantic_model_info(semantic_view),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.SEMANTIC_VIEW]),
        ).as_workunit()

        yield from self._gen_common_entity_aspects(
            entity_urn=model_urn,
            browse_path=self._browse_path_entries(db_name, schema_name),
        )

        yield from self._gen_view_tags(semantic_view, model_urn)
        yield from self._gen_field_structured_property_workunits(
            semantic_view, model_urn
        )

        if self.domain_registry and self.config.domain:
            yield from get_domain_wu(
                dataset_name=self.identifiers.get_dataset_identifier(
                    semantic_view.name, schema_name, db_name
                ),
                entity_urn=model_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
            )

        if semantic_view.resolved_upstream_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=model_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn, type=DatasetLineageTypeClass.VIEW
                        )
                        for upstream_urn in semantic_view.resolved_upstream_urns
                    ],
                    fineGrainedLineages=model_lineages or None,
                ),
            ).as_workunit()

        for metric_name_upper, occurrence in metric_occurrences.items():
            yield from self._gen_metric_workunits(
                occurrence=occurrence,
                metric_lineages=metric_lineages.get(metric_name_upper, []),
                metric_occurrences=metric_occurrences,
                model_urn=model_urn,
                semantic_view=semantic_view,
                schema_name=schema_name,
                db_name=db_name,
            )

    def _build_semantic_model_info(
        self, semantic_view: SnowflakeSemanticView
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
            datasets=self._build_model_datasets(semantic_view),
        )

    def _build_model_datasets(
        self, semantic_view: SnowflakeSemanticView
    ) -> List[ModelDatasetClass]:
        datasets: List[ModelDatasetClass] = []
        # Metrics are never placed on a logical table (they become metric entities,
        # not fields), so they must not count as "unplaced" below - seed them up
        # front rather than relying on the per-logical-table loop below, which does
        # not run at all when logical_to_physical_table is empty.
        placed_columns: Set[str] = set(self._metric_occurrences(semantic_view).keys())

        for (
            logical_name_upper,
            physical_table,
        ) in semantic_view.logical_to_physical_table.items():
            physical_db, physical_schema, physical_table_name = physical_table
            source_urn = self.identifiers.gen_dataset_urn(
                self.identifiers.get_dataset_identifier(
                    physical_table_name, physical_schema, physical_db
                )
            )

            fields: List[SemanticFieldClass] = []
            for (
                col_name_upper,
                occurrences,
            ) in semantic_view.column_occurrences.items():
                for occurrence in occurrences:
                    if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                        # Metrics become first-class metric entities, not fields.
                        continue
                    if (
                        occurrence.table_name
                        and occurrence.table_name.upper() == logical_name_upper
                    ):
                        fields.append(
                            self._build_semantic_field(occurrence, semantic_view)
                        )
                        placed_columns.add(col_name_upper)

            datasets.append(
                ModelDatasetClass(
                    name=logical_name_upper,
                    source=source_urn,
                    fields=fields or None,
                )
            )

        unplaced = set(semantic_view.column_occurrences.keys()) - placed_columns
        if unplaced:
            self.report.warning(
                title="Semantic view columns without a logical table",
                message="Some dimensions/facts could not be associated with a logical "
                "table and were omitted from the semantic model fields.",
                context=f"{semantic_view.name}: {sorted(unplaced)}",
            )

        return datasets

    def _build_semantic_field(
        self,
        occurrence: SemanticViewColumnMetadata,
        semantic_view: SnowflakeSemanticView,
    ) -> SemanticFieldClass:
        field_type = (
            SemanticFieldTypeClass.DIMENSION
            if occurrence.subtype == SemanticViewColumnSubtype.DIMENSION
            else SemanticFieldTypeClass.MEASURE
        )
        type_class = SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(
            _base_type(occurrence.data_type), NullType
        )

        return SemanticFieldClass(
            schemaField=SchemaFieldClass(
                # Intentionally uppercased. The column_occurrences key and the
                # fine-grained-lineage downstream field anchor built in
                # snowflake_schema_gen.py::_generate_column_lineage_for_semantic_view
                # both use `col_name_upper`, so the field path must match to keep the
                # SemanticField and its lineage anchor in sync. The cost is that a
                # quoted mixed-case Snowflake identifier ("myCol") renders as MYCOL in
                # semanticModelInfo when convert_urns_to_lowercase=False. Do NOT
                # "restore" the original case here - that would desync the field path
                # from its lineage anchor and break column-level lineage resolution.
                fieldPath=self.identifiers.snowflake_identifier(
                    occurrence.name.upper()
                ),
                type=SchemaFieldDataTypeClass(type_class()),
                nativeDataType=occurrence.data_type,
                description=occurrence.comment,
                nullable=True,
                isPartOfKey=occurrence.name.upper()
                in semantic_view.primary_key_columns,
                globalTags=self._column_tags(occurrence.name, semantic_view),
            ),
            type=field_type,
            expression=self._expression(occurrence),
            dimension=(
                DimensionClass(isTime=type_class in (DateType, TimeType))
                if field_type == SemanticFieldTypeClass.DIMENSION
                else None
            ),
            aiContext=(
                AiContextClass(synonyms=occurrence.synonyms)
                if occurrence.synonyms
                else None
            ),
        )

    def _gen_metric_workunits(
        self,
        occurrence: SemanticViewColumnMetadata,
        metric_lineages: List[FineGrainedLineageClass],
        metric_occurrences: Dict[str, SemanticViewColumnMetadata],
        model_urn: str,
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        metric_urn = self.identifiers.gen_metric_urn(
            occurrence.name, semantic_view.name, schema_name, db_name
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=metric_urn,
            aspect=MetricInfoClass(
                name=occurrence.name,
                description=occurrence.comment,
                expression=self._expression(occurrence),
                semanticModel=model_urn,
                aiContext=(
                    AiContextClass(synonyms=occurrence.synonyms)
                    if occurrence.synonyms
                    else None
                ),
            ),
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

        upstreams = self._build_metric_upstreams(metric_lineages)
        if upstreams:
            yield MetadataChangeProposalWrapper(
                entityUrn=metric_urn, aspect=upstreams
            ).as_workunit()

        derived_from = self._derived_from_metrics(
            occurrence, metric_occurrences, semantic_view, schema_name, db_name
        )
        if derived_from:
            yield MetadataChangeProposalWrapper(
                entityUrn=metric_urn,
                aspect=MetricRelationshipsClass(derivedFrom=derived_from),
            ).as_workunit()

    def _build_metric_upstreams(
        self, metric_lineages: List[FineGrainedLineageClass]
    ) -> Optional[MetricUpstreamsClass]:
        field_urns: List[str] = []
        dataset_urns: List[str] = []
        for lineage in metric_lineages:
            for upstream_field_urn in lineage.upstreams or []:
                if upstream_field_urn not in field_urns:
                    field_urns.append(upstream_field_urn)
                parent_urn = str(SchemaFieldUrn.from_string(upstream_field_urn).parent)
                if parent_urn not in dataset_urns:
                    dataset_urns.append(parent_urn)

        if not field_urns and not dataset_urns:
            return None
        return MetricUpstreamsClass(
            datasetUpstreams=[EdgeClass(destinationUrn=urn) for urn in dataset_urns]
            or None,
            fieldUpstreams=[EdgeClass(destinationUrn=urn) for urn in field_urns]
            or None,
        )

    def _derived_from_metrics(
        self,
        occurrence: SemanticViewColumnMetadata,
        metric_occurrences: Dict[str, SemanticViewColumnMetadata],
        semantic_view: SnowflakeSemanticView,
        schema_name: str,
        db_name: str,
    ) -> List[DerivedMetricInputClass]:
        # In Snowflake semantic view expressions, a reference to another metric is a
        # bare (unqualified) column name (e.g. REVENUE / ORDER_COUNT), while a
        # reference to a fact/dimension column on a logical table is qualified
        # (e.g. ORDERS.AMOUNT). Parsing with sqlglot instead of regex tokenization
        # avoids matching identifiers inside string literals and correctly handles
        # quoted identifiers; requiring no table qualifier avoids misreading a
        # qualified fact reference that happens to share a metric's name. Names that
        # are ambiguous - both a metric and a dimension/fact column of the same view -
        # are shadowed and omitted: derivedFrom is isLineage:true, so a wrong edge is
        # worse than a missing one.
        if not occurrence.expression:
            return []
        try:
            parsed = sqlglot.parse_one(occurrence.expression, dialect="snowflake")
        except Exception:
            return []
        referenced = {
            column.name.upper()
            for column in parsed.find_all(sqlglot.expressions.Column)
            if not column.table
        }
        shadowed = {
            col_upper
            for col_upper, occs in semantic_view.column_occurrences.items()
            if any(o.subtype != SemanticViewColumnSubtype.METRIC for o in occs)
        }
        derived: List[DerivedMetricInputClass] = []
        for name_upper in sorted(
            (referenced & metric_occurrences.keys())
            - {occurrence.name.upper()}
            - shadowed
        ):
            # Build the destination URN from the referenced metric's own occurrence
            # (preserving its original case), not from the upper-cased lookup key -
            # otherwise the two would diverge whenever convert_urns_to_lowercase=False.
            referenced_name = metric_occurrences[name_upper].name
            derived.append(
                DerivedMetricInputClass(
                    destinationUrn=self.identifiers.gen_metric_urn(
                        referenced_name, semantic_view.name, schema_name, db_name
                    )
                )
            )
        return derived

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

    def _emit_tags_for_entity(
        self, entity_urn: str, tags: List[SnowflakeTag]
    ) -> Iterable[MetadataWorkUnit]:
        # Shared by the semanticModel entity (view-level tags) and each metric
        # entity (that metric column's tags): both need the same GlobalTags-vs-
        # structured-properties branching.
        if not tags:
            return
        if self.config.extract_tags_as_structured_properties:
            yield from add_structured_properties_to_entity_wu(
                entity_urn,
                {
                    StructuredPropertyUrn(
                        self.identifiers.snowflake_identifier(
                            tag.structured_property_identifier()
                        )
                    ): tag.value
                    for tag in tags
                },
                write_mode=self.config.structured_properties_write_mode,
            )
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=GlobalTagsClass(
                    tags=[
                        TagAssociationClass(
                            tag=make_tag_urn(
                                self.identifiers.snowflake_identifier(
                                    tag.tag_identifier()
                                )
                            )
                        )
                        for tag in tags
                    ]
                ),
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
            tags=[
                TagAssociationClass(
                    tag=make_tag_urn(
                        self.identifiers.snowflake_identifier(tag.tag_identifier())
                    )
                )
                for tag in semantic_view.column_tags[column_name]
            ]
        )

    def _gen_field_structured_property_workunits(
        self, semantic_view: SnowflakeSemanticView, model_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # _column_tags only handles the normal (GlobalTags) tag mode, since it is
        # embedded directly into the SchemaFieldClass built in _build_semantic_field.
        # In extract_tags_as_structured_properties mode there is no aspect on
        # SchemaFieldClass to carry structured properties, so DIMENSION/FACT field
        # tags must be emitted as separate schemaField-level structured-properties
        # MCPs here, mirroring gen_column_tags_as_structured_properties in
        # snowflake_schema_gen.py for the legacy dataset-mode path.
        if not self.config.extract_tags_as_structured_properties:
            return
        for occurrences in semantic_view.column_occurrences.values():
            representative = next(
                (
                    o
                    for o in occurrences
                    if o.subtype != SemanticViewColumnSubtype.METRIC
                    and o.name in semantic_view.column_tags
                ),
                None,
            )
            if representative is None:
                continue
            field_urn = SchemaFieldUrn(
                model_urn,
                self.identifiers.snowflake_identifier(representative.name.upper()),
            ).urn()
            yield from add_structured_properties_to_entity_wu(
                field_urn,
                {
                    StructuredPropertyUrn(
                        self.identifiers.snowflake_identifier(
                            tag.structured_property_identifier()
                        )
                    ): tag.value
                    for tag in semantic_view.column_tags[representative.name]
                },
                write_mode=self.config.structured_properties_write_mode,
            )

    def _expression(
        self, occurrence: SemanticViewColumnMetadata
    ) -> MetricExpressionClass:
        return MetricExpressionClass(
            dialects=[
                DialectExpressionClass(
                    dialect=DialectClass.SNOWFLAKE,
                    expression=occurrence.expression or occurrence.name,
                )
            ]
        )

    def _metric_occurrences(
        self, semantic_view: SnowflakeSemanticView
    ) -> Dict[str, SemanticViewColumnMetadata]:
        metrics: Dict[str, SemanticViewColumnMetadata] = {}
        for col_name_upper, occurrences in semantic_view.column_occurrences.items():
            for occurrence in occurrences:
                if occurrence.subtype == SemanticViewColumnSubtype.METRIC:
                    # A metric may be declared on multiple logical tables; the first
                    # occurrence carries the expression used for the metric entity.
                    metrics.setdefault(col_name_upper, occurrence)
        return metrics

    def _split_lineages_by_metric(
        self,
        fine_grained_lineages: List[FineGrainedLineageClass],
        metric_names_upper: Set[str],
    ) -> (
        "tuple[List[FineGrainedLineageClass], Dict[str, List[FineGrainedLineageClass]]]"
    ):
        model_lineages: List[FineGrainedLineageClass] = []
        metric_lineages: Dict[str, List[FineGrainedLineageClass]] = {}
        for lineage in fine_grained_lineages:
            downstream_field = self._downstream_field_name(lineage)
            if downstream_field and downstream_field.upper() in metric_names_upper:
                metric_lineages.setdefault(downstream_field.upper(), []).append(lineage)
            else:
                model_lineages.append(lineage)
        return model_lineages, metric_lineages

    @staticmethod
    def _downstream_field_name(
        lineage: FineGrainedLineageClass,
    ) -> Optional[str]:
        if not lineage.downstreams:
            return None
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
