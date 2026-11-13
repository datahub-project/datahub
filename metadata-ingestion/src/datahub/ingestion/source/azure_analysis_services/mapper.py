import logging
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.emitter import mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.config import (
    AzureAnalysisServicesConfig,
)
from datahub.ingestion.source.azure_analysis_services.lineage import AasLineageExtractor
from datahub.ingestion.source.azure_analysis_services.models import (
    AasColumn,
    AasMeasure,
    AasTable,
    AasTabularModel,
)
from datahub.ingestion.source.azure_analysis_services.report import (
    AzureAnalysisServicesReport,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    DialectClass,
    DialectExpressionClass,
    DimensionClass,
    ERModelRelationshipCardinalityClass,
    FineGrainedLineageClass,
    ForeignKeyConstraintClass,
    MetricExpressionClass,
    NumberTypeClass,
    OtherSchemaClass,
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
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import SchemaFieldUrn, SemanticModelUrn
from datahub.sdk.metric import Metric

logger = logging.getLogger(__name__)

_SCHEMA_NAME = "aas"


class AasServerContainerKey(ContainerKey):
    server: str


class AasModelContainerKey(ContainerKey):
    server: str
    model: str


class AasMapper:
    def __init__(
        self,
        config: AzureAnalysisServicesConfig,
        report: AzureAnalysisServicesReport,
        ctx: PipelineContext,
        server_name: str,
        lineage_extractor: AasLineageExtractor,
    ) -> None:
        self.config = config
        self.report = report
        self.ctx = ctx
        self.server_name = server_name
        self.lineage_extractor = lineage_extractor
        # When the platform is overridden to ``powerbi`` the URNs deliberately
        # mirror the Power BI connector's naming so both connectors describe the
        # same logical entity and merge in the catalog.
        self._align_powerbi = self.config.platform == constants.PLATFORM_POWERBI

    # Container keys

    def _server_key(self) -> AasServerContainerKey:
        return AasServerContainerKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            server=self.server_name,
        )

    def _model_key(self, catalog: str) -> AasModelContainerKey:
        return AasModelContainerKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            server=self.server_name,
            model=catalog,
        )

    # URN naming

    def _table_dataset_name(self, catalog: str, table_name: str) -> str:
        if self._align_powerbi:
            # Power BI's ``form_full_table_name``: ``<dataset>.<table>`` with
            # spaces replaced by underscores. Matching it lets an AAS-backed
            # Power BI dataset stitch to the Power BI connector's tables.
            full_table_name = (
                f"{catalog.replace(' ', '_')}.{table_name.replace(' ', '_')}"
            )
            if self.config.include_workspace_name_in_dataset_urn:
                # Mirror Power BI's optional workspace prefix. The workspace name
                # is the ``<workspace>`` segment of the XMLA endpoint, exposed as
                # ``server_name``; Power BI lowercases it and swaps spaces.
                workspace_identifier = self.server_name.replace(" ", "_").lower()
                full_table_name = f"{workspace_identifier}.{full_table_name}"
            return full_table_name
        return f"{catalog}.{table_name}"

    def _dataset_urn(self, name: str) -> str:
        if self.config.convert_urns_to_lowercase:
            name = name.lower()
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.config.platform,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _table_dataset_urn(self, catalog: str, table_name: str) -> str:
        return self._dataset_urn(self._table_dataset_name(catalog, table_name))

    def _semantic_model_path_and_id(self, catalog: str) -> Tuple[str, str]:
        # The semanticModel key has no env or platform-instance field, so the
        # instance is embedded in the path the same way Snowflake does. The
        # displayed name stays the catalog; only the URN id and server segment
        # follow convert_urns_to_lowercase.
        server = self.server_name
        model_id = catalog
        if self.config.convert_urns_to_lowercase:
            server = server.lower()
            model_id = model_id.lower()
        path = server
        if self.config.platform_instance:
            path = f"{self.config.platform_instance}.{path}"
        return path, model_id

    def _semantic_model_urn(self, catalog: str) -> str:
        path, model_id = self._semantic_model_path_and_id(catalog)
        return str(
            SemanticModelUrn(
                platform=builder.make_data_platform_urn(self.config.platform),
                path=path,
                id=model_id,
            )
        )

    # Shared dataset aspects

    @staticmethod
    def _status_wu(dataset_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

    def _platform_instance_wus(self, dataset_urn: str) -> Iterable[MetadataWorkUnit]:
        if not self.config.platform_instance:
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.config.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.config.platform, self.config.platform_instance
                ),
            ),
        ).as_workunit()

    # Top-level entry

    def map_model(self, model: AasTabularModel) -> Iterable[MetadataWorkUnit]:
        server_key = self._server_key()
        yield from gen_containers(
            container_key=server_key,
            name=self.server_name,
            sub_types=[DatasetContainerSubTypes.ANALYSIS_SERVICES_SERVER],
        )

        model_key = self._model_key(model.catalog)
        # TMSCHEMA_MODEL.Name is "Model" in almost every tabular database, so the
        # catalog is the name users know the model by.
        yield from gen_containers(
            container_key=model_key,
            name=model.catalog,
            sub_types=[DatasetSubTypes.SEMANTIC_MODEL],
            parent_container_key=server_key,
            description=model.description,
            extra_properties=self._model_container_properties(model),
        )

        # Index only tables that pass table_pattern so intra-model FK/DAX lineage
        # never points at a filtered-out dataset that is not emitted.
        dataset_urn_by_table: Dict[str, str] = {}
        for table in model.tables:
            if not self.config.table_pattern.allowed(table.name):
                continue
            key = table.name.lower()
            if key in dataset_urn_by_table:
                self.report.warning(
                    title="Duplicate table name",
                    message="Two tables collide case-insensitively; lineage for one may be dropped.",
                    context=f"catalog={model.catalog}, table={table.name}",
                )
                continue
            dataset_urn_by_table[key] = self._table_dataset_urn(
                model.catalog, table.name
            )

        # Intra-model DAX column lineage is computed up front and merged into
        # each table's single UpstreamLineage aspect so it does not clobber the
        # upstream (M/Power Query) lineage written for the same entity.
        intra_model_fgl = self._intra_model_fgl_by_dataset(model, dataset_urn_by_table)

        emitted_keys: Set[str] = set()
        for table in model.tables:
            if not self.config.table_pattern.allowed(table.name):
                self.report.report_table_filtered(table.name)
                continue
            key = table.name.lower()
            if key in emitted_keys:
                # A case-collision duplicate was already emitted under the first
                # table's URN (warned about during indexing above); re-emitting it
                # would clobber that dataset's aspects with the duplicate's.
                continue
            emitted_keys.add(key)
            try:
                # Materialize inside the try so the except only guards the mapping
                # work. Yielding inside the try would also catch exceptions the
                # consumer throws back into this generator (a sink/pipeline error),
                # misreporting them as a table-mapping failure and dropping the
                # remaining work units for this table.
                workunits = list(
                    self._map_table(
                        model, table, model_key, dataset_urn_by_table, intra_model_fgl
                    )
                )
            except Exception as e:
                self.report.tables_skipped += 1
                self.report.warning(
                    title="Table mapping failed",
                    message="Skipped a table that could not be mapped to metadata.",
                    context=f"catalog={model.catalog}, table={table.name}",
                    exc=e,
                )
                continue
            yield from workunits

        # The catalog is the semantic model. Its summary lists the tables.
        yield from self._map_semantic_model(model)
        yield from self._map_metrics(model, dataset_urn_by_table)

    # Model container properties

    def _allowed_tables(self, model: AasTabularModel) -> List[AasTable]:
        # table_pattern gates what is emitted; container counts must reflect the
        # same filtered set so a denied table never leaks back in.
        return [t for t in model.tables if self.config.table_pattern.allowed(t.name)]

    def _model_container_properties(self, model: AasTabularModel) -> Dict[str, str]:
        props: Dict[str, str] = {
            constants.PROP_CATALOG: model.catalog,
            constants.PROP_TABLE_COUNT: str(len(self._allowed_tables(model))),
        }
        if model.culture:
            props[constants.PROP_CULTURE] = model.culture
        if self.config.extract_roles:
            for role in model.roles:
                props[f"{constants.PROP_ROLE_PREFIX}{role.name}"] = (
                    role.description or ""
                )
        return props

    # Per-table datasets

    def _map_table(
        self,
        model: AasTabularModel,
        table: AasTable,
        model_key: AasModelContainerKey,
        dataset_urn_by_table: Dict[str, str],
        intra_model_fgl: Dict[str, List[FineGrainedLineageClass]],
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = dataset_urn_by_table[table.name.lower()]
        self.report.tables_scanned += 1
        if table.is_calculated:
            self.report.calculated_tables_scanned += 1

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=table.name,
                description=table.description,
                customProperties=self._table_custom_properties(table),
            ),
        ).as_workunit()

        yield self._status_wu(dataset_urn)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=self._table_subtypes(table)),
        ).as_workunit()

        yield from self._platform_instance_wus(dataset_urn)

        view_properties = self._table_view_properties(table)
        if view_properties is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=view_properties
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=self._schema_metadata(
                model, table, dataset_urn, dataset_urn_by_table
            ),
        ).as_workunit()

        yield from add_dataset_to_container(model_key, dataset_urn)
        yield from self._dimension_annotations(table, dataset_urn)

        # Membership is stored on the table, not on the semantic model. The
        # alias is the table name the model's relationships use.
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SemanticModelPropertiesClass(
                alias=table.name,
                semanticModel=self._semantic_model_urn(model.catalog),
            ),
        ).as_workunit()

        upstream = self.lineage_extractor.extract_upstream_for_table(table, dataset_urn)
        intra_model = intra_model_fgl.get(dataset_urn, [])
        fine_grained = list(upstream.fine_grained) + intra_model
        upstreams = list(upstream.upstreams)
        # DAX column edges into other tables of the model need a matching
        # dataset-level edge.
        upstream_datasets = {u.dataset for u in upstreams}
        for edge in intra_model:
            for field_urn in edge.upstreams or []:
                upstream_dataset = self._dataset_urn_of_field(field_urn)
                if (
                    upstream_dataset
                    and upstream_dataset != dataset_urn
                    and upstream_dataset not in upstream_datasets
                ):
                    upstream_datasets.add(upstream_dataset)
                    upstreams.append(
                        UpstreamClass(
                            dataset=upstream_dataset,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )
                    )
        if upstreams or fine_grained:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=upstreams,
                    fineGrainedLineages=fine_grained or None,
                ),
            ).as_workunit()

    def _table_custom_properties(self, table: AasTable) -> Dict[str, str]:
        return {
            constants.PROP_IS_HIDDEN: str(table.is_hidden),
            constants.PROP_MEASURE_COUNT: str(len(table.measures)),
        }

    def _table_subtypes(self, table: AasTable) -> List[str]:
        # Each table is a logical dataset of the model's semantic model. A
        # calculated table additionally carries Calculated Table; a table backed
        # by an M/Power Query partition additionally carries View.
        subtypes: List[str] = [DatasetSubTypes.SEMANTIC_MODEL_DATASET]
        if table.is_calculated:
            subtypes.append(DatasetSubTypes.CALCULATED_TABLE)
        elif table.expression:
            subtypes.append(DatasetSubTypes.VIEW)
        return subtypes

    def _table_view_properties(self, table: AasTable) -> Optional[ViewPropertiesClass]:
        if table.is_calculated:
            for partition in table.partitions:
                if partition.query_definition:
                    return ViewPropertiesClass(
                        materialized=False,
                        viewLogic=partition.query_definition,
                        viewLanguage=constants.VIEW_LANGUAGE_DAX,
                    )
            return None
        view_logic = self._import_view_logic(table)
        if view_logic is not None:
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=view_logic,
                viewLanguage=constants.VIEW_LANGUAGE_M,
            )
        return None

    @staticmethod
    def _import_view_logic(table: AasTable) -> Optional[str]:
        # An import table can have several query partitions (e.g. incremental
        # refresh), each a complete M expression. A single partition is emitted
        # as-is. Multiple partitions must not be concatenated — two `let ... in`
        # blocks in a row is not valid M — so they are wrapped as fields of a
        # single M record literal, which is one valid M expression that preserves
        # every partition's logic.
        partitions = [(p.name, p.query_definition) for p in table.partitions]
        queries = [(name, q) for name, q in partitions if q]
        if not queries:
            return None
        if len(queries) == 1:
            return queries[0][1]
        fields: List[str] = []
        for idx, (name, query) in enumerate(queries):
            field_name = name or f"{constants.PARTITION_FIELD_NAME_PREFIX}{idx + 1}"
            # Quote the field with #"..." so arbitrary partition names are valid
            # M identifiers; escape embedded quotes per M's "" convention.
            escaped = field_name.replace('"', '""')
            fields.append(f'    #"{escaped}" = (\n{query}\n    )')
        return "[\n" + ",\n".join(fields) + "\n]"

    # Schema

    def _schema_metadata(
        self,
        model: AasTabularModel,
        table: AasTable,
        dataset_urn: str,
        dataset_urn_by_table: Dict[str, str],
    ) -> SchemaMetadataClass:
        fields: List[SchemaFieldClass] = []
        for column in table.columns:
            fields.append(self._field_for_column(column))
            self.report.columns_scanned += 1
        for measure in table.measures:
            fields.append(self._field_for_measure(measure))
            self.report.measures_scanned += 1

        return SchemaMetadataClass(
            schemaName=table.name,
            platform=builder.make_data_platform_urn(self.config.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
            foreignKeys=self._foreign_keys(
                model, table, dataset_urn, dataset_urn_by_table
            )
            or None,
        )

    @staticmethod
    def _describe_with_expression(
        description: Optional[str], expression: Optional[str]
    ) -> Optional[str]:
        # A DAX/M expression is the substance of a calculated field, so it goes
        # into the description; keep any human description ahead of it.
        if not expression:
            return description
        if description:
            return f"{description}\n\n{expression}"
        return expression

    def _field_for_column(self, column: AasColumn) -> SchemaFieldClass:
        native_type = (
            constants.NATIVE_TYPE_CALCULATED_COLUMN
            if column.is_calculated
            else column.dataType
        )
        # Only calculated columns carry a DAX expression worth surfacing.
        expression = column.expression if column.is_calculated else None
        return SchemaFieldClass(
            fieldPath=column.name,
            type=SchemaFieldDataTypeClass(type=column.datahubDataType),
            nativeDataType=native_type,
            description=self._describe_with_expression(column.description, expression),
            isPartOfKey=False,
        )

    def _field_for_measure(self, measure: AasMeasure) -> SchemaFieldClass:
        # Measures have no stored data type; they evaluate to a number.
        description = self._describe_with_expression(
            measure.description, measure.expression
        )
        return SchemaFieldClass(
            fieldPath=measure.name,
            type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
            nativeDataType=constants.NATIVE_TYPE_MEASURE,
            description=description,
            isPartOfKey=False,
        )

    def _foreign_keys(
        self,
        model: AasTabularModel,
        table: AasTable,
        dataset_urn: str,
        dataset_urn_by_table: Dict[str, str],
    ) -> List[ForeignKeyConstraintClass]:
        constraints: List[ForeignKeyConstraintClass] = []
        for rel in model.relationships:
            if rel.from_table.lower() != table.name.lower():
                continue
            foreign_dataset = dataset_urn_by_table.get(rel.to_table.lower())
            if not foreign_dataset:
                continue
            constraints.append(
                ForeignKeyConstraintClass(
                    name=f"{rel.from_table}.{rel.from_column}->{rel.to_table}.{rel.to_column}",
                    foreignDataset=foreign_dataset,
                    sourceFields=[
                        builder.make_schema_field_urn(dataset_urn, rel.from_column)
                    ],
                    foreignFields=[
                        builder.make_schema_field_urn(foreign_dataset, rel.to_column)
                    ],
                )
            )
        return constraints

    def _map_semantic_model(self, model: AasTabularModel) -> Iterable[MetadataWorkUnit]:
        model_urn = self._semantic_model_urn(model.catalog)
        # The key aspect is what makes the semantic model exist and be searchable;
        # without it only the info aspect lands and the entity never surfaces.
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SemanticModelUrn.from_string(model_urn).to_key_aspect(),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn,
            aspect=SemanticModelInfoClass(
                name=model.catalog,
                description=model.description,
                nativeDefinition=model.definition,
                relationships=self._semantic_model_relationships(model),
            ),
        ).as_workunit()
        yield MetadataChangeProposalWrapper(
            entityUrn=model_urn, aspect=StatusClass(removed=False)
        ).as_workunit()
        yield from self._platform_instance_wus(model_urn)

    @staticmethod
    def _dax_expression(expression: str) -> MetricExpressionClass:
        # The semantic model dialect enum has no DAX symbol, so DAX is OTHER.
        return MetricExpressionClass(
            dialects=[
                DialectExpressionClass(
                    dialect=DialectClass.OTHER, expression=expression
                )
            ]
        )

    @staticmethod
    def _is_exposed(table: AasTable, is_hidden: bool) -> bool:
        # A hidden table hides every column on it, the same as flagging the
        # column itself.
        return not (table.is_hidden or is_hidden)

    def _dimension_annotations(
        self, table: AasTable, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Every column that is not hidden is a dimension of the model. The
        # annotation is anchored on the column's schemaField entity.
        for column in table.columns:
            if not self._is_exposed(table, column.is_hidden):
                continue
            reference = column.expression or f"{table.name}[{column.name}]"
            yield MetadataChangeProposalWrapper(
                entityUrn=builder.make_schema_field_urn(dataset_urn, column.name),
                aspect=SemanticFieldAnnotationClass(
                    type=SemanticFieldTypeClass.DIMENSION,
                    expression=self._dax_expression(reference),
                    dimension=DimensionClass(
                        isTime=isinstance(column.datahub_data_type, DateTypeClass)
                    ),
                ),
            ).as_workunit()
            self.report.dimensions_emitted += 1

    @staticmethod
    def _urn_safe(value: str) -> str:
        # Measure names are free text; tuple URN keys cannot carry these.
        return value.replace(",", "_").replace("(", "_").replace(")", "_")

    def _measure_upstream_datasets(
        self,
        model: AasTabularModel,
        table: AasTable,
        measure: AasMeasure,
        dataset_urn_by_table: Dict[str, str],
    ) -> List[str]:
        # The measure's own table first, then every other table its DAX reads
        # (available when column-level lineage is on, from calc dependencies).
        upstreams: List[str] = [dataset_urn_by_table[table.name.lower()]]
        for dependency in model.calc_dependencies:
            if (
                dependency.object_type.upper() != "MEASURE"
                or dependency.table.lower() != table.name.lower()
                or dependency.object_name.lower() != measure.name.lower()
            ):
                continue
            referenced = dataset_urn_by_table.get(dependency.referenced_table.lower())
            if referenced and referenced not in upstreams:
                upstreams.append(referenced)
        return upstreams

    def _map_metrics(
        self, model: AasTabularModel, dataset_urn_by_table: Dict[str, str]
    ) -> Iterable[MetadataWorkUnit]:
        model_urn = self._semantic_model_urn(model.catalog)
        path, model_id = self._semantic_model_path_and_id(model.catalog)
        hidden_metrics = self.config.ingest_hidden_metrics
        seen_ids: Set[str] = set()
        for table in self._allowed_tables(model):
            if table.name.lower() not in dataset_urn_by_table:
                continue
            for measure in table.measures:
                hidden = not self._is_exposed(table, measure.is_hidden)
                if hidden and not hidden_metrics.enabled:
                    continue
                metric_id = self._urn_safe(measure.name)
                if self.config.convert_urns_to_lowercase:
                    metric_id = metric_id.lower()
                # DAX measure names are unique within a model, but two names
                # that differ only by case collapse once URNs are lowercased.
                if metric_id in seen_ids:
                    self.report.warning(
                        title="Duplicate measure name",
                        message="Two measures resolve to the same metric URN; only the first is emitted.",
                        context=f"catalog={model.catalog}, measure={measure.name}",
                    )
                    continue
                seen_ids.add(metric_id)
                metric = Metric(
                    platform=self.config.platform,
                    path=f"{path}.{model_id}",
                    id=metric_id,
                    semantic_model=model_urn,
                    platform_instance=self.config.platform_instance,
                    name=measure.name,
                    description=measure.description,
                    expression=(
                        self._dax_expression(measure.expression)
                        if measure.expression
                        else None
                    ),
                    upstream_datasets=self._measure_upstream_datasets(
                        model, table, measure, dataset_urn_by_table
                    ),
                    tags=(
                        [builder.make_tag_urn(hidden_metrics.hidden_metric_tag.strip())]
                        if hidden and hidden_metrics.tag_hidden_metrics
                        else None
                    ),
                )
                yield from metric.as_workunits()
                self.report.metrics_emitted += 1

    def _semantic_model_relationships(
        self, model: AasTabularModel
    ) -> Optional[List[SemanticModelRelationshipClass]]:
        # Join aliases must be the table names written on semanticModelProperties.
        # A relationship that names a filtered-out table is dropped so the
        # summary does not point at a dataset this run did not emit.
        alias_by_name = {t.name.lower(): t.name for t in self._allowed_tables(model)}
        relationships: List[SemanticModelRelationshipClass] = []
        for rel in model.relationships:
            if not rel.is_active:
                continue
            from_alias = alias_by_name.get(rel.from_table.lower())
            to_alias = alias_by_name.get(rel.to_table.lower())
            if not from_alias or not to_alias:
                continue
            # Tabular relationships point from the many side to the one side.
            relationships.append(
                SemanticModelRelationshipClass(
                    name=f"{from_alias}.{rel.from_column}->{to_alias}.{rel.to_column}",
                    from_=from_alias,
                    fromColumns=[rel.from_column],
                    to=to_alias,
                    toColumns=[rel.to_column],
                    cardinality=ERModelRelationshipCardinalityClass.N_ONE,
                )
            )
        return relationships or None

    # Intra-model DAX lineage

    def _intra_model_fgl_by_dataset(
        self, model: AasTabularModel, dataset_urn_by_table: Dict[str, str]
    ) -> Dict[str, List[FineGrainedLineageClass]]:
        # Group intra-model column edges by their downstream dataset so each
        # table can fold them into its single UpstreamLineage aspect.
        by_dataset: Dict[str, List[FineGrainedLineageClass]] = {}
        fine_grained = self.lineage_extractor.extract_intra_model_lineage(
            model, dataset_urn_by_table
        )
        for edge in fine_grained:
            for downstream in edge.downstreams or []:
                dataset_urn = self._dataset_urn_of_field(downstream)
                if dataset_urn:
                    by_dataset.setdefault(dataset_urn, []).append(edge)
        return by_dataset

    @staticmethod
    def _dataset_urn_of_field(field_urn: str) -> Optional[str]:
        # SchemaFieldUrn.parent is the parent dataset URN as a string.
        try:
            return SchemaFieldUrn.from_string(field_urn).parent
        except Exception:
            return None
