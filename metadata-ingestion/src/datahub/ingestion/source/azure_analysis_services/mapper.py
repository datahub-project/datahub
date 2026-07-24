import logging
from typing import Dict, Iterable, List, Optional

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
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    ForeignKeyConstraintClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)

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

    # --- Container keys ---------------------------------------------------

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

    # --- URN naming -------------------------------------------------------

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

    def _table_dataset_urn(self, catalog: str, table_name: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.config.platform,
            name=self._table_dataset_name(catalog, table_name),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _cube_dataset_name(self, catalog: str) -> str:
        return catalog

    def _cube_dataset_urn(self, catalog: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            platform=self.config.platform,
            name=self._cube_dataset_name(catalog),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    # --- Top-level entry --------------------------------------------------

    def map_model(self, model: AasTabularModel) -> Iterable[MetadataWorkUnit]:
        server_key = self._server_key()
        yield from gen_containers(
            container_key=server_key,
            name=self.server_name,
            sub_types=[DatasetContainerSubTypes.ANALYSIS_SERVICES_SERVER],
        )

        model_key = self._model_key(model.catalog)
        yield from gen_containers(
            container_key=model_key,
            name=model.name,
            sub_types=[DatasetSubTypes.SEMANTIC_MODEL],
            parent_container_key=server_key,
            description=model.description,
            extra_properties=self._model_container_properties(model),
        )

        dataset_urn_by_table: Dict[str, str] = {}
        for table in model.tables:
            dataset_urn_by_table[table.name.lower()] = self._table_dataset_urn(
                model.catalog, table.name
            )

        # Intra-model DAX column lineage is computed up front and merged into
        # each table's single UpstreamLineage aspect so it does not clobber the
        # upstream (M/Power Query) lineage written for the same entity.
        intra_model_fgl = self._intra_model_fgl_by_dataset(model, dataset_urn_by_table)

        for table in model.tables:
            if not self.config.table_pattern.allowed(table.name):
                self.report.report_table_filtered(table.name)
                continue
            try:
                yield from self._map_table(
                    model, table, model_key, dataset_urn_by_table, intra_model_fgl
                )
            except Exception as e:
                self.report.tables_skipped += 1
                self.report.warning(
                    title="Table mapping failed",
                    message="Skipped a table that could not be mapped to metadata.",
                    context=f"catalog={model.catalog}, table={table.name}",
                    exc=e,
                )

        yield from self._map_cube_dataset(model, model_key)

    # --- Model container properties ---------------------------------------

    def _model_container_properties(self, model: AasTabularModel) -> Dict[str, str]:
        props: Dict[str, str] = {
            constants.PROP_CATALOG: model.catalog,
            constants.PROP_TABLE_COUNT: str(len(model.tables)),
        }
        if model.culture:
            props[constants.PROP_CULTURE] = model.culture
        if self.config.extract_roles:
            for role in model.roles:
                props[f"{constants.PROP_ROLE_PREFIX}{role.name}"] = (
                    role.description or ""
                )
        return props

    # --- Per-table datasets -----------------------------------------------

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

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=self._table_subtypes(table)),
        ).as_workunit()

        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.config.platform),
                    instance=builder.make_dataplatform_instance_urn(
                        self.config.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

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

        upstream = self.lineage_extractor.extract_upstream_for_table(table, dataset_urn)
        fine_grained = list(upstream.fine_grained)
        fine_grained.extend(intra_model_fgl.get(dataset_urn, []))
        if upstream.upstreams or fine_grained:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=upstream.upstreams,
                    fineGrainedLineages=fine_grained or None,
                ),
            ).as_workunit()

    def _table_custom_properties(self, table: AasTable) -> Dict[str, str]:
        return {
            constants.PROP_IS_HIDDEN: str(table.is_hidden),
            constants.PROP_MEASURE_COUNT: str(len(table.measures)),
        }

    def _table_subtypes(self, table: AasTable) -> List[str]:
        # Every tabular-model table is a Table. A calculated table additionally
        # carries the Calculated Table subtype; a table backed by an M/Power
        # Query partition is additionally a View (its logic lives in
        # ViewProperties). Import tables get the single Table subtype.
        subtypes: List[str] = [DatasetSubTypes.TABLE]
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
        if table.expression:
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=table.expression,
                viewLanguage=constants.VIEW_LANGUAGE_M,
            )
        return None

    # --- Schema -----------------------------------------------------------

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

    def _field_for_column(self, column: AasColumn) -> SchemaFieldClass:
        native_type = (
            constants.NATIVE_TYPE_CALCULATED_COLUMN
            if column.is_calculated
            else column.dataType
        )
        description = column.description
        if column.is_calculated and column.expression:
            # A calculated column's DAX is the most useful thing to surface; keep
            # any human description ahead of it.
            description = (
                f"{description}\n\n{column.expression}"
                if description
                else column.expression
            )
        return SchemaFieldClass(
            fieldPath=column.name,
            type=SchemaFieldDataTypeClass(type=column.datahubDataType),
            nativeDataType=native_type,
            description=description,
            isPartOfKey=False,
        )

    def _field_for_measure(self, measure: AasMeasure) -> SchemaFieldClass:
        # Measures have no stored data type; they evaluate to a number. The DAX
        # expression is the substance, so it goes into the description.
        description = measure.description
        if measure.expression:
            description = (
                f"{description}\n\n{measure.expression}"
                if description
                else measure.expression
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

    # --- Model-level cube dataset -----------------------------------------

    def _map_cube_dataset(
        self, model: AasTabularModel, model_key: AasModelContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._cube_dataset_urn(model.catalog)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=DatasetPropertiesClass(
                name=model.name,
                description=model.description,
                customProperties=self._model_container_properties(model),
            ),
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=StatusClass(removed=False)
        ).as_workunit()

        # Dual subtypes mirror Tableau's cube-with-definition pattern: the model
        # is both a Cube and a Semantic Model.
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(
                typeNames=[DatasetSubTypes.CUBE, DatasetSubTypes.SEMANTIC_MODEL]
            ),
        ).as_workunit()

        if self.config.platform_instance:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.config.platform),
                    instance=builder.make_dataplatform_instance_urn(
                        self.config.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()

        if model.definition:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=False,
                    viewLogic=model.definition,
                    viewLanguage=constants.VIEW_LANGUAGE_TMSL,
                ),
            ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=self._cube_schema_metadata(model)
        ).as_workunit()

        yield from add_dataset_to_container(model_key, dataset_urn)

    def _cube_schema_metadata(self, model: AasTabularModel) -> SchemaMetadataClass:
        # The cube dataset exposes every measure in the model as a field so the
        # model's analytical surface is browsable in one place.
        fields: List[SchemaFieldClass] = []
        for table in model.tables:
            for measure in table.measures:
                field = self._field_for_measure(measure)
                field.fieldPath = f"{table.name}.{measure.name}"
                fields.append(field)
        return SchemaMetadataClass(
            schemaName=model.name,
            platform=builder.make_data_platform_urn(self.config.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    # --- Intra-model DAX lineage ------------------------------------------

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
        # A schemaField URN is ``urn:li:schemaField:(<datasetUrn>,<field>)``.
        prefix = "urn:li:schemaField:("
        if not field_urn.startswith(prefix):
            return None
        inner = field_urn[len(prefix) : field_urn.rfind(")")]
        return inner.rsplit(",", 1)[0]
