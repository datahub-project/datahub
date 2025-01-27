#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################
import logging
from datetime import datetime
from typing import Iterable, List, Optional, Tuple, Union

import more_itertools

import datahub.emitter.mce_builder as builder
import datahub.ingestion.source.powerbi.m_query.data_classes
import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    convert_dashboard_info_to_patch,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
)
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ContainerClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    EdgeClass,
    GlobalTagsClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import ChartUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

# Logger instance
logger = logging.getLogger(__name__)


class Mapper:
    """
    Transform PowerBi concepts Dashboard, Dataset and Tile to DataHub concepts Dashboard, Dataset and Chart
    """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """
        We can add EquableMetadataWorkUnit to set.
        This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
        """

        def __eq__(self, instance):
            return self.id == instance.id

        def __hash__(self):
            return id(self.id)

    def __init__(
        self,
        ctx: PipelineContext,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
        dataplatform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ):
        self.__ctx = ctx
        self.__config = config
        self.__reporter = reporter
        self.__dataplatform_instance_resolver = dataplatform_instance_resolver
        self.workspace_key: Optional[ContainerKey] = None

    @staticmethod
    def urn_to_lowercase(value: str, flag: bool) -> str:
        if flag is True:
            return lowercase_dataset_urn(value)

        return value

    def lineage_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(
            value, self.__config.convert_lineage_urns_to_lowercase
        )

    def assets_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(value, self.__config.convert_urns_to_lowercase)

    def new_mcp(
        self,
        entity_urn,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            changeType=change_type,
            entityUrn=entity_urn,
            aspect=aspect,
        )

    def _to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def extract_dataset_schema(
        self, table: powerbi_data_classes.Table, ds_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        schema_metadata = self.to_datahub_schema(table)
        schema_mcp = self.new_mcp(
            entity_urn=ds_urn,
            aspect=schema_metadata,
        )
        return [schema_mcp]

    def make_fine_grained_lineage_class(
        self,
        lineage: datahub.ingestion.source.powerbi.m_query.data_classes.Lineage,
        dataset_urn: str,
    ) -> List[FineGrainedLineage]:
        fine_grained_lineages: List[FineGrainedLineage] = []

        if (
            self.__config.extract_column_level_lineage is False
            or self.__config.extract_lineage is False
        ):
            return fine_grained_lineages

        if lineage is None:
            return fine_grained_lineages

        logger.info("Extracting column level lineage")

        cll: List[ColumnLineageInfo] = lineage.column_lineage

        for cll_info in cll:
            downstream = (
                [builder.make_schema_field_urn(dataset_urn, cll_info.downstream.column)]
                if cll_info.downstream is not None
                and cll_info.downstream.column is not None
                else []
            )

            upstreams = [
                builder.make_schema_field_urn(column_ref.table, column_ref.column)
                for column_ref in cll_info.upstreams
            ]

            fine_grained_lineages.append(
                FineGrainedLineage(
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        return fine_grained_lineages

    def extract_lineage(
        self, table: powerbi_data_classes.Table, ds_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []

        # table.dataset should always be set, but we check it just in case.
        parameters = table.dataset.parameters if table.dataset else {}

        upstream: List[UpstreamClass] = []
        cll_lineage: List[FineGrainedLineage] = []

        upstream_lineage: List[
            datahub.ingestion.source.powerbi.m_query.data_classes.Lineage
        ] = parser.get_upstream_tables(
            table=table,
            reporter=self.__reporter,
            platform_instance_resolver=self.__dataplatform_instance_resolver,
            ctx=self.__ctx,
            config=self.__config,
            parameters=parameters,
        )

        logger.debug(
            f"PowerBI virtual table {table.full_name} and it's upstream dataplatform tables = {upstream_lineage}"
        )

        for lineage in upstream_lineage:
            for upstream_dpt in lineage.upstreams:
                if (
                    upstream_dpt.data_platform_pair.powerbi_data_platform_name
                    not in self.__config.dataset_type_mapping.keys()
                ):
                    logger.debug(
                        f"Skipping upstream table for {ds_urn}. The platform {upstream_dpt.data_platform_pair.powerbi_data_platform_name} is not part of dataset_type_mapping",
                    )
                    continue

                upstream_table_class = UpstreamClass(
                    upstream_dpt.urn,
                    DatasetLineageTypeClass.TRANSFORMED,
                )

                upstream.append(upstream_table_class)

                # Add column level lineage if any
                cll_lineage.extend(
                    self.make_fine_grained_lineage_class(
                        lineage=lineage,
                        dataset_urn=ds_urn,
                    )
                )

        if len(upstream) > 0:
            upstream_lineage_class: UpstreamLineageClass = UpstreamLineageClass(
                upstreams=upstream,
                fineGrainedLineages=cll_lineage or None,
            )

            logger.debug(f"Dataset urn = {ds_urn} and its lineage = {upstream_lineage}")

            mcp = MetadataChangeProposalWrapper(
                entityType=Constant.DATASET,
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=ds_urn,
                aspect=upstream_lineage_class,
            )
            mcps.append(mcp)

        return mcps

    def create_datahub_owner_urn(self, user: str) -> str:
        """
        Create corpuser urn from PowerBI (configured by| modified by| created by) user
        """
        if self.__config.ownership.remove_email_suffix:
            return builder.make_user_urn(user.split("@")[0])
        return builder.make_user_urn(f"users.{user}")

    def to_datahub_schema_field(
        self,
        field: Union[powerbi_data_classes.Column, powerbi_data_classes.Measure],
    ) -> SchemaFieldClass:
        data_type = field.dataType
        if isinstance(field, powerbi_data_classes.Measure):
            description = (
                f"{field.expression} {field.description}"
                if field.description
                else field.expression
            )
        elif field.description:
            description = field.description
        else:
            description = None

        schema_field = SchemaFieldClass(
            fieldPath=f"{field.name}",
            type=SchemaFieldDataTypeClass(type=field.datahubDataType),
            nativeDataType=data_type,
            description=description,
        )
        return schema_field

    def to_datahub_schema(
        self,
        table: powerbi_data_classes.Table,
    ) -> SchemaMetadataClass:
        fields = []
        table_fields = (
            [self.to_datahub_schema_field(column) for column in table.columns]
            if table.columns
            else []
        )
        measure_fields = (
            [self.to_datahub_schema_field(measure) for measure in table.measures]
            if table.measures
            else []
        )
        fields.extend(table_fields)
        fields.extend(measure_fields)

        schema_metadata = SchemaMetadataClass(
            schemaName=table.name,
            platform=self.__config.platform_urn,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

        return schema_metadata

    def to_datahub_dataset(
        self,
        dataset: Optional[powerbi_data_classes.PowerBIDataset],
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
        In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.
        """

        dataset_mcps: List[MetadataChangeProposalWrapper] = []

        if dataset is None:
            return dataset_mcps

        logger.debug(f"Processing dataset {dataset.name}")

        if not any(
            [
                self.__config.filter_dataset_endorsements.allowed(tag)
                for tag in (dataset.tags or [""])
            ]
        ):
            logger.debug(
                "Returning empty dataset_mcps as no dataset tag matched with filter_dataset_endorsements"
            )
            return dataset_mcps

        logger.debug(
            f"Mapping dataset={dataset.name}(id={dataset.id}) to datahub dataset"
        )

        if self.__config.extract_datasets_to_containers:
            dataset_mcps.extend(self.generate_container_for_dataset(dataset))

        for table in dataset.tables:
            # Create a URN for dataset
            ds_urn = self.assets_urn_to_lowercase(
                builder.make_dataset_urn_with_platform_instance(
                    platform=self.__config.platform_name,
                    name=table.full_name,
                    platform_instance=self.__config.platform_instance,
                    env=self.__config.env,
                )
            )

            logger.debug(f"dataset_urn={ds_urn}")
            # Create datasetProperties mcp
            if table.expression:
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLogic=table.expression,
                    viewLanguage="m_query",
                )
                view_prop_mcp = self.new_mcp(
                    entity_urn=ds_urn,
                    aspect=view_properties,
                )
                dataset_mcps.extend([view_prop_mcp])
            ds_properties = DatasetPropertiesClass(
                name=table.name,
                description=dataset.description,
                externalUrl=dataset.webUrl,
                customProperties={
                    "datasetId": dataset.id,
                },
            )

            info_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=ds_properties,
            )

            # Remove status mcp
            status_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=StatusClass(removed=False),
            )
            if self.__config.extract_dataset_schema:
                dataset_mcps.extend(self.extract_dataset_schema(table, ds_urn))

            subtype_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        BIContainerSubTypes.POWERBI_DATASET_TABLE,
                    ]
                ),
            )
            # normally, the person who configures the dataset will be the most accurate person for ownership
            if (
                self.__config.extract_ownership
                and self.__config.ownership.dataset_configured_by_as_owner
                and dataset.configuredBy
            ):
                # Dashboard Ownership
                user_urn = self.create_datahub_owner_urn(dataset.configuredBy)
                owner_class = OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
                # Dashboard owner MCP
                ownership = OwnershipClass(owners=[owner_class])
                owner_mcp = self.new_mcp(
                    entity_urn=ds_urn,
                    aspect=ownership,
                )
                dataset_mcps.extend([owner_mcp])

            dataset_mcps.extend([info_mcp, status_mcp, subtype_mcp])

            if self.__config.extract_lineage is True:
                dataset_mcps.extend(self.extract_lineage(table, ds_urn))

            self.append_container_mcp(
                dataset_mcps,
                ds_urn,
                dataset,
            )

            self.append_tag_mcp(
                dataset_mcps,
                ds_urn,
                Constant.DATASET,
                dataset.tags,
            )
            self.extract_profile(dataset_mcps, workspace, dataset, table, ds_urn)

        return dataset_mcps

    def extract_profile(
        self,
        dataset_mcps: List[MetadataChangeProposalWrapper],
        workspace: powerbi_data_classes.Workspace,
        dataset: powerbi_data_classes.PowerBIDataset,
        table: powerbi_data_classes.Table,
        ds_urn: str,
    ) -> None:
        if not self.__config.profiling.enabled:
            # Profiling not enabled
            return

        if not self.__config.profile_pattern.allowed(
            f"{workspace.name}.{dataset.name}.{table.name}"
        ):
            logger.info(
                f"Table {table.name} in {dataset.name}, not allowed for profiling"
            )
            return
        logger.debug(f"Profiling table: {table.name}")

        profile = DatasetProfileClass(timestampMillis=builder.get_sys_time())
        profile.rowCount = table.row_count
        profile.fieldProfiles = []

        columns: List[
            Union[powerbi_data_classes.Column, powerbi_data_classes.Measure]
        ] = [*(table.columns or []), *(table.measures or [])]
        for column in columns:
            allowed_column = self.__config.profile_pattern.allowed(
                f"{workspace.name}.{dataset.name}.{table.name}.{column.name}"
            )
            if column.isHidden or not allowed_column:
                logger.info(f"Column {column.name} not allowed for profiling")
                continue
            measure_profile = column.measure_profile
            if measure_profile:
                field_profile = DatasetFieldProfileClass(column.name or "")
                field_profile.sampleValues = measure_profile.sample_values
                field_profile.min = measure_profile.min
                field_profile.max = measure_profile.max
                field_profile.uniqueCount = measure_profile.unique_count
                profile.fieldProfiles.append(field_profile)

        profile.columnCount = table.column_count

        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=ds_urn,
            aspectName="datasetProfile",
            aspect=profile,
        )
        dataset_mcps.append(mcp)

    @staticmethod
    def transform_tags(tags: List[str]) -> GlobalTagsClass:
        return GlobalTagsClass(
            tags=[
                TagAssociationClass(builder.make_tag_urn(tag_to_add))
                for tag_to_add in tags
            ]
        )

    def to_datahub_chart_mcp(
        self,
        tile: powerbi_data_classes.Tile,
        ds_mcps: List[MetadataChangeProposalWrapper],
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi tile to datahub chart
        """
        logger.info(f"Converting tile {tile.title}(id={tile.id}) to chart")
        # Create a URN for chart
        chart_urn = builder.make_chart_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=tile.get_urn_part(),
        )

        logger.info(f"{Constant.CHART_URN}={chart_urn}")

        ds_input: List[str] = self.to_urn_set(
            [x for x in ds_mcps if x.entityType == Constant.DATASET]
        )

        def tile_custom_properties(tile: powerbi_data_classes.Tile) -> dict:
            custom_properties: dict = {
                Constant.CREATED_FROM: tile.createdFrom.value,
            }

            if tile.dataset_id is not None:
                custom_properties[Constant.DATASET_ID] = tile.dataset_id

            if tile.dataset is not None and tile.dataset.webUrl is not None:
                custom_properties[Constant.DATASET_WEB_URL] = tile.dataset.webUrl

            if tile.report_id is not None:
                custom_properties[Constant.REPORT_ID] = tile.report_id

            if tile.report is not None and tile.report.webUrl is not None:
                custom_properties[Constant.REPORT_WEB_URL] = tile.report.webUrl

            return custom_properties

        # Create chartInfo mcp
        # Set chartUrl only if tile is created from Report
        chart_info_instance = ChartInfoClass(
            title=tile.title or "",
            description=tile.title or "",
            lastModified=ChangeAuditStamps(),
            inputs=ds_input,
            externalUrl=tile.report.webUrl if tile.report else None,
            customProperties=tile_custom_properties(tile),
        )

        info_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=StatusClass(removed=False),
        )

        # Subtype mcp
        subtype_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=SubTypesClass(
                typeNames=[BIAssetSubTypes.POWERBI_TILE],
            ),
        )

        # ChartKey aspect
        # Note: we previously would emit a ChartKey aspect with incorrect information.
        # Explicitly emitting this aspect isn't necessary, but we do it here to ensure that
        # the old, bad data gets overwritten.
        chart_key_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=ChartUrn.from_string(chart_urn).to_key_aspect(),
        )

        # Browse path
        browse_path = BrowsePathsClass(paths=[f"/powerbi/{workspace.name}"])
        browse_path_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=browse_path,
        )
        result_mcps = [
            info_mcp,
            status_mcp,
            subtype_mcp,
            chart_key_mcp,
            browse_path_mcp,
        ]

        self.append_container_mcp(
            result_mcps,
            chart_urn,
        )

        return result_mcps

    # written in this style to fix linter error
    def to_urn_set(self, mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
        return deduplicate_list(
            [
                mcp.entityUrn
                for mcp in mcps
                if mcp is not None and mcp.entityUrn is not None
            ]
        )

    def to_datahub_dashboard_mcp(
        self,
        dashboard: powerbi_data_classes.Dashboard,
        workspace: powerbi_data_classes.Workspace,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dashboard to Datahub dashboard
        """

        dashboard_urn = builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=dashboard.get_urn_part(),
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        def chart_custom_properties(dashboard: powerbi_data_classes.Dashboard) -> dict:
            return {
                Constant.CHART_COUNT: str(len(dashboard.tiles)),
                Constant.WORKSPACE_NAME: dashboard.workspace_name,
                Constant.WORKSPACE_ID: dashboard.workspace_id,
            }

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=dashboard.description,
            title=dashboard.displayName or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=dashboard.webUrl,
            customProperties={**chart_custom_properties(dashboard)},
        )

        info_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_key_cls,
        )

        # Dashboard Ownership
        owners = [
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            # Dashboard owner MCP
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=ownership,
            )

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{dashboard.workspace_name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=browse_path,
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
        ]

        if owner_mcp is not None:
            list_of_mcps.append(owner_mcp)

        self.append_container_mcp(
            list_of_mcps,
            dashboard_urn,
        )

        self.append_tag_mcp(
            list_of_mcps,
            dashboard_urn,
            Constant.DASHBOARD,
            dashboard.tags,
        )

        return list_of_mcps

    def append_container_mcp(
        self,
        list_of_mcps: List[MetadataChangeProposalWrapper],
        entity_urn: str,
        dataset: Optional[powerbi_data_classes.PowerBIDataset] = None,
    ) -> None:
        if self.__config.extract_datasets_to_containers and isinstance(
            dataset, powerbi_data_classes.PowerBIDataset
        ):
            container_key = dataset.get_dataset_key(self.__config.platform_name)
        elif self.__config.extract_workspaces_to_containers and self.workspace_key:
            container_key = self.workspace_key
        else:
            return None

        container_urn = builder.make_container_urn(
            guid=container_key.guid(),
        )
        mcp = MetadataChangeProposalWrapper(
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=entity_urn,
            aspect=ContainerClass(container=f"{container_urn}"),
        )
        list_of_mcps.append(mcp)

    def generate_container_for_workspace(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        self.workspace_key = workspace.get_workspace_key(
            platform_name=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            workspace_id_as_urn_part=self.__config.workspace_id_as_urn_part,
        )
        container_work_units = gen_containers(
            container_key=self.workspace_key,
            name=workspace.name,
            sub_types=[workspace.type],
            extra_properties={
                "workspace_id": workspace.id,
                "workspace_name": workspace.name,
                "workspace_type": workspace.type,
            },
        )
        return container_work_units

    def generate_container_for_dataset(
        self, dataset: powerbi_data_classes.PowerBIDataset
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dataset_key = dataset.get_dataset_key(self.__config.platform_name)
        container_work_units = gen_containers(
            container_key=dataset_key,
            name=dataset.name if dataset.name else dataset.id,
            parent_container_key=self.workspace_key,
            sub_types=[BIContainerSubTypes.POWERBI_DATASET],
        )

        # The if statement here is just to satisfy mypy
        return [
            wu.metadata
            for wu in container_work_units
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]

    def append_tag_mcp(
        self,
        list_of_mcps: List[MetadataChangeProposalWrapper],
        entity_urn: str,
        entity_type: str,
        tags: List[str],
    ) -> None:
        if self.__config.extract_endorsements_to_tags and tags:
            tags_mcp = self.new_mcp(
                entity_urn=entity_urn,
                aspect=self.transform_tags(tags),
            )
            list_of_mcps.append(tags_mcp)

    def to_datahub_user(
        self, user: powerbi_data_classes.User
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi user to datahub user
        """

        logger.debug(f"Mapping user {user.displayName}(id={user.id}) to datahub's user")

        # Create an URN for user
        user_id = user.get_urn_part(
            use_email=self.__config.ownership.use_powerbi_email,
            remove_email_suffix=self.__config.ownership.remove_email_suffix,
        )
        user_urn = builder.make_user_urn(user_id)
        user_key = CorpUserKeyClass(username=user.id)

        user_key_mcp = self.new_mcp(
            entity_urn=user_urn,
            aspect=user_key,
        )

        return [user_key_mcp]

    def to_datahub_users(
        self, users: List[powerbi_data_classes.User]
    ) -> List[MetadataChangeProposalWrapper]:
        user_mcps = []

        for user in users:
            if user:
                user_rights = [
                    user.datasetUserAccessRight,
                    user.reportUserAccessRight,
                    user.dashboardUserAccessRight,
                    user.groupUserAccessRight,
                ]
                if (
                    user.principalType == "User"
                    and self.__config.ownership.owner_criteria
                    and len(
                        set(user_rights) & set(self.__config.ownership.owner_criteria)
                    )
                    > 0
                ):
                    user_mcps.extend(self.to_datahub_user(user))
                elif self.__config.ownership.owner_criteria is None:
                    user_mcps.extend(self.to_datahub_user(user))
                else:
                    continue

        return user_mcps

    def to_datahub_chart(
        self,
        tiles: List[powerbi_data_classes.Tile],
        workspace: powerbi_data_classes.Workspace,
    ) -> Tuple[
        List[MetadataChangeProposalWrapper], List[MetadataChangeProposalWrapper]
    ]:
        ds_mcps = []
        chart_mcps = []

        # Return empty list if input list is empty
        if not tiles:
            return [], []

        logger.info(f"Converting tiles(count={len(tiles)}) to charts")

        for tile in tiles:
            if tile is None:
                continue
            # First convert the dataset to MCP, because dataset mcp is used in input attribute of chart mcp
            dataset_mcps = self.to_datahub_dataset(tile.dataset, workspace)
            # Now convert tile to chart MCP
            chart_mcp = self.to_datahub_chart_mcp(tile, dataset_mcps, workspace)

            ds_mcps.extend(dataset_mcps)
            chart_mcps.extend(chart_mcp)

        # Return dataset and chart MCPs

        return ds_mcps, chart_mcps

    def to_datahub_work_units(
        self,
        dashboard: powerbi_data_classes.Dashboard,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[EquableMetadataWorkUnit]:
        mcps = []

        logger.info(
            f"Converting dashboard={dashboard.displayName} to datahub dashboard"
        )

        # Convert user to CorpUser
        user_mcps: List[MetadataChangeProposalWrapper] = self.to_datahub_users(
            dashboard.users
        )
        # Convert tiles to charts
        ds_mcps, chart_mcps = self.to_datahub_chart(dashboard.tiles, workspace)
        # Lets convert dashboard to datahub dashboard
        dashboard_mcps: List[MetadataChangeProposalWrapper] = (
            self.to_datahub_dashboard_mcp(dashboard, workspace, chart_mcps, user_mcps)
        )

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        if self.__config.ownership.create_corp_user:
            mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(dashboard_mcps)

        # Convert MCP to work_units
        work_units = map(self._to_work_unit, mcps)
        # Return set of work_unit
        return deduplicate_list([wu for wu in work_units if wu is not None])

    def pages_to_chart(
        self,
        pages: List[powerbi_data_classes.Page],
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        chart_mcps = []

        # Return empty list if input list is empty
        if not pages:
            return []

        logger.debug(f"Converting pages(count={len(pages)}) to charts")

        def to_chart_mcps(
            page: powerbi_data_classes.Page,
            ds_mcps: List[MetadataChangeProposalWrapper],
        ) -> List[MetadataChangeProposalWrapper]:
            logger.debug(f"Converting page {page.displayName} to chart")
            # Create a URN for chart
            chart_urn = builder.make_chart_urn(
                platform=self.__config.platform_name,
                platform_instance=self.__config.platform_instance,
                name=page.get_urn_part(),
            )

            logger.debug(f"{Constant.CHART_URN}={chart_urn}")

            ds_input: List[str] = self.to_urn_set(
                [x for x in ds_mcps if x.entityType == Constant.DATASET]
            )

            # Create chartInfo mcp
            # Set chartUrl only if tile is created from Report
            chart_info_instance = ChartInfoClass(
                title=page.displayName or "",
                description=page.displayName or "",
                lastModified=ChangeAuditStamps(),
                inputs=ds_input,
                customProperties={Constant.ORDER: str(page.order)},
            )

            info_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=chart_info_instance,
            )

            # removed status mcp
            status_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=StatusClass(removed=False),
            )
            # Subtype mcp
            subtype_mcp = MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=SubTypesClass(
                    typeNames=[BIAssetSubTypes.POWERBI_PAGE],
                ),
            )

            # Browse path
            browse_path = BrowsePathsClass(
                paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}"]
            )
            browse_path_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=browse_path,
            )
            list_of_mcps = [info_mcp, status_mcp, subtype_mcp, browse_path_mcp]

            self.append_container_mcp(
                list_of_mcps,
                chart_urn,
            )

            return list_of_mcps

        for page in pages:
            if page is None:
                continue
            # Now convert tile to chart MCP
            chart_mcp = to_chart_mcps(page, ds_mcps)
            chart_mcps.extend(chart_mcp)

        return chart_mcps

    def report_to_dashboard(
        self,
        workspace: powerbi_data_classes.Workspace,
        report: powerbi_data_classes.Report,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
        dashboard_edges: List[EdgeClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi report to Datahub dashboard
        """

        dashboard_urn = builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=report.get_urn_part(),
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=report.description,
            title=report.name or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=report.webUrl,
            dashboards=dashboard_edges,
        )

        info_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(report.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_key_cls,
        )
        # Report Ownership
        owners = [
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            # Report owner MCP
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=ownership,
            )

        # Report browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=browse_path,
        )

        sub_type_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[report.type.value]),
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
            sub_type_mcp,
        ]

        if owner_mcp is not None:
            list_of_mcps.append(owner_mcp)

        self.append_container_mcp(
            list_of_mcps,
            dashboard_urn,
        )

        self.append_tag_mcp(
            list_of_mcps,
            dashboard_urn,
            Constant.DASHBOARD,
            report.tags,
        )

        return list_of_mcps

    def report_to_datahub_work_units(
        self,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
    ) -> Iterable[MetadataWorkUnit]:
        mcps: List[MetadataChangeProposalWrapper] = []

        logger.debug(f"Converting report={report.name} to datahub dashboard")
        # Convert user to CorpUser
        user_mcps = self.to_datahub_users(report.users)
        # Convert pages to charts. A report has a single dataset and the same dataset used in pages to create visualization
        ds_mcps = self.to_datahub_dataset(report.dataset, workspace)
        chart_mcps = self.pages_to_chart(report.pages, workspace, ds_mcps)

        # find all dashboards with a Tile referencing this report
        downstream_dashboards_edges = []
        for d in workspace.dashboards.values():
            if any(t.report_id == report.id for t in d.tiles):
                dashboard_urn = builder.make_dashboard_urn(
                    platform=self.__config.platform_name,
                    platform_instance=self.__config.platform_instance,
                    name=d.get_urn_part(),
                )
                edge = EdgeClass(
                    destinationUrn=dashboard_urn,
                    sourceUrn=None,
                    created=None,
                    lastModified=None,
                    properties=None,
                )
                downstream_dashboards_edges.append(edge)

        # Let's convert report to datahub dashboard
        report_mcps = self.report_to_dashboard(
            workspace, report, chart_mcps, user_mcps, downstream_dashboards_edges
        )

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        if self.__config.ownership.create_corp_user:
            mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(report_mcps)

        return map(self._to_work_unit, mcps)


@platform_name("PowerBI")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Disabled by default, configured using `extract_ownership`",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, configured using `extract_lineage`.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Disabled by default, configured using `extract_column_level_lineage`. ",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration profiling.enabled",
)
class PowerBiDashboardSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Power BI dashboards, tiles and datasets
    - Names, descriptions and URLs of dashboard and tile
    - Owners of dashboards
    """

    source_config: PowerBiDashboardSourceConfig
    reporter: PowerBiDashboardSourceReport
    dataplatform_instance_resolver: AbstractDataPlatformInstanceResolver
    accessed_dashboards: int = 0
    platform: str = "powerbi"

    def __init__(self, config: PowerBiDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.reporter = PowerBiDashboardSourceReport()
        self.dataplatform_instance_resolver = create_dataplatform_instance_resolver(
            self.source_config
        )
        try:
            self.powerbi_client = PowerBiAPI(
                config=self.source_config,
                reporter=self.reporter,
            )
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to PowerBI API Service. This exit will avoid raising
            # unwanted stacktrace on console

        self.mapper = Mapper(
            ctx, config, self.reporter, self.dataplatform_instance_resolver
        )

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.source_config, self.ctx
        )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            PowerBiAPI(
                PowerBiDashboardSourceConfig.parse_obj_allow_extras(config_dict),
                PowerBiDashboardSourceReport(),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_allowed_workspaces(self) -> List[powerbi_data_classes.Workspace]:
        all_workspaces = self.powerbi_client.get_workspaces()
        logger.info(f"Number of workspaces = {len(all_workspaces)}")
        self.reporter.all_workspace_count = len(all_workspaces)
        logger.debug(
            f"All workspaces: {[workspace.format_name_for_logger() for workspace in all_workspaces]}"
        )

        allowed_workspaces = []
        for workspace in all_workspaces:
            if not self.source_config.workspace_id_pattern.allowed(workspace.id):
                self.reporter.filtered_workspace_names.append(
                    f"{workspace.id} - {workspace.name}"
                )
                continue
            elif workspace.type not in self.source_config.workspace_type_filter:
                self.reporter.filtered_workspace_types.append(
                    f"{workspace.id} - {workspace.name} (type = {workspace.type})"
                )
                continue
            else:
                allowed_workspaces.append(workspace)

        logger.info(f"Number of allowed workspaces = {len(allowed_workspaces)}")
        logger.debug(
            f"Allowed workspaces: {[workspace.format_name_for_logger() for workspace in allowed_workspaces]}"
        )

        return allowed_workspaces

    def validate_dataset_type_mapping(self):
        powerbi_data_platforms: List[str] = [
            data_platform.value.powerbi_data_platform_name
            for data_platform in SupportedDataPlatform
        ]

        for key in self.source_config.dataset_type_mapping.keys():
            if key not in powerbi_data_platforms:
                raise ValueError(f"PowerBI DataPlatform {key} is not supported")

        logger.debug(
            f"Dataset lineage would get ingested for data-platform = {self.source_config.dataset_type_mapping}"
        )

    def extract_independent_datasets(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.extract_independent_datasets is False:
            if workspace.independent_datasets:
                self.reporter.info(
                    title="Skipped Independent Dataset",
                    message="Some datasets are not used in any visualizations. To ingest them, enable the `extract_independent_datasets` flag",
                    context=",".join(
                        [
                            dataset.name
                            for dataset in workspace.independent_datasets.values()
                            if dataset.name
                        ]
                    ),
                )
            return

        for dataset in workspace.independent_datasets.values():
            yield from auto_workunit(
                stream=self.mapper.to_datahub_dataset(
                    dataset=dataset,
                    workspace=workspace,
                )
            )

    def emit_app(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if workspace.app is None:
            return

        if not self.source_config.extract_app:
            self.reporter.info(
                title="App Ingestion Is Disabled",
                message="You are missing workspace app metadata. Please set flag `extract_app` to `true` in recipe to ingest workspace app.",
                context=f"workspace-name={workspace.name}, app-name = {workspace.app.name}",
            )
            return

        assets_within_app: List[EdgeClass] = [
            EdgeClass(
                destinationUrn=builder.make_dashboard_urn(
                    platform=self.source_config.platform_name,
                    platform_instance=self.source_config.platform_instance,
                    name=powerbi_data_classes.Dashboard.get_urn_part_by_id(
                        app_dashboard.original_dashboard_id
                    ),
                )
            )
            for app_dashboard in workspace.app.dashboards
        ]

        assets_within_app.extend(
            [
                EdgeClass(
                    destinationUrn=builder.make_dashboard_urn(
                        platform=self.source_config.platform_name,
                        platform_instance=self.source_config.platform_instance,
                        name=powerbi_data_classes.Report.get_urn_part_by_id(
                            app_report.original_report_id
                        ),
                    )
                )
                for app_report in workspace.app.reports
            ]
        )

        if assets_within_app:
            logger.debug(
                f"Emitting metadata-workunits for app {workspace.app.name}({workspace.app.id})"
            )

            app_urn: str = builder.make_dashboard_urn(
                platform=self.source_config.platform_name,
                platform_instance=self.source_config.platform_instance,
                name=powerbi_data_classes.App.get_urn_part_by_id(workspace.app.id),
            )

            dashboard_info: DashboardInfoClass = DashboardInfoClass(
                title=workspace.app.name,
                description=workspace.app.description
                if workspace.app.description
                else workspace.app.name,
                # lastModified=workspace.app.last_update,
                lastModified=ChangeAuditStamps(
                    lastModified=AuditStampClass(
                        actor="urn:li:corpuser:unknown",
                        time=int(
                            datetime.strptime(
                                workspace.app.last_update, "%Y-%m-%dT%H:%M:%S.%fZ"
                            ).timestamp()
                        ),
                    )
                    if workspace.app.last_update
                    else None
                ),
                dashboards=assets_within_app,
            )

            # Browse path
            browse_path: BrowsePathsClass = BrowsePathsClass(
                paths=[f"/powerbi/{workspace.name}"]
            )

            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=app_urn,
                aspects=(
                    dashboard_info,
                    browse_path,
                    StatusClass(removed=False),
                    SubTypesClass(typeNames=[BIAssetSubTypes.POWERBI_APP]),
                ),
            )

    def get_workspace_workunit(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.extract_workspaces_to_containers:
            workspace_workunits = self.mapper.generate_container_for_workspace(
                workspace
            )

            for workunit in workspace_workunits:
                # Return workunit to a Datahub Ingestion framework
                yield workunit

        yield from auto_workunit(self.emit_app(workspace=workspace))

        for dashboard in workspace.dashboards.values():
            try:
                # Fetch PowerBi users for dashboards
                dashboard.users = self.powerbi_client.get_dashboard_users(dashboard)
                # Increase dashboard and tiles count in report
                self.reporter.report_dashboards_scanned()
                self.reporter.report_charts_scanned(count=len(dashboard.tiles))
            except Exception as e:
                message = f"Error ({e}) occurred while loading dashboard {dashboard.displayName}(id={dashboard.id}) tiles."

                logger.exception(message, e)
                self.reporter.report_warning(dashboard.id, message)
            # Convert PowerBi Dashboard and child entities to Datahub work unit to ingest into Datahub
            workunits = self.mapper.to_datahub_work_units(dashboard, workspace)
            for workunit in workunits:
                wu = self._get_dashboard_patch_work_unit(workunit)
                if wu is not None:
                    yield wu

        for report in workspace.reports.values():
            for work_unit in self.mapper.report_to_datahub_work_units(
                report, workspace
            ):
                wu = self._get_dashboard_patch_work_unit(work_unit)
                if wu is not None:
                    yield wu

        yield from self.extract_independent_datasets(workspace)

    def _get_dashboard_patch_work_unit(
        self, work_unit: MetadataWorkUnit
    ) -> Optional[MetadataWorkUnit]:
        dashboard_info_aspect: Optional[DashboardInfoClass] = (
            work_unit.get_aspect_of_type(DashboardInfoClass)
        )

        if dashboard_info_aspect and self.source_config.patch_metadata:
            return convert_dashboard_info_to_patch(
                work_unit.get_urn(),
                dashboard_info_aspect,
                work_unit.metadata.systemMetadata,
            )
        else:
            return work_unit

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # As modified_workspaces is not idempotent, hence workunit processors are run later for each workspace_id
        # This will result in creating a checkpoint for each workspace_id
        if self.source_config.modified_since:
            return []  # Handle these in get_workunits_internal
        else:
            return [
                *super().get_workunit_processors(),
                self.stale_entity_removal_handler.workunit_processor,
            ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invokes this method
        """
        logger.info("PowerBi plugin execution is started")
        # Validate dataset type mapping
        self.validate_dataset_type_mapping()
        # Fetch PowerBi workspace for given workspace identifier

        allowed_workspaces = self.get_allowed_workspaces()

        batches = more_itertools.chunked(
            allowed_workspaces, self.source_config.scan_batch_size
        )
        for batch_workspaces in batches:
            for workspace in self.powerbi_client.fill_workspaces(
                batch_workspaces, self.reporter
            ):
                logger.info(f"Processing workspace id: {workspace.id}")

                if self.source_config.modified_since:
                    # As modified_workspaces is not idempotent, hence we checkpoint for each powerbi workspace
                    # Because job_id is used as a dictionary key, we have to set a new job_id
                    # Refer to https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stateful_ingestion_base.py#L390
                    self.stale_entity_removal_handler.set_job_id(workspace.id)
                    self.state_provider.register_stateful_ingestion_usecase_handler(
                        self.stale_entity_removal_handler
                    )

                    yield from self._apply_workunit_processors(
                        [
                            *super().get_workunit_processors(),
                            self.stale_entity_removal_handler.workunit_processor,
                        ],
                        self.get_workspace_workunit(workspace),
                    )
                else:
                    # Maintain backward compatibility
                    yield from self.get_workspace_workunit(workspace)

    def get_report(self) -> SourceReport:
        return self.reporter
