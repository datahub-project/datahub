#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################
import logging
from typing import Iterable, List, Optional, Tuple, Union

import datahub.emitter.mce_builder as builder
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser, resolver
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
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ChartKeyClass,
    ContainerClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
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
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.sqlglot_lineage import ColumnLineageInfo

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
            return value.lower()

        return value

    def lineage_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(
            value, self.__config.convert_lineage_urns_to_lowercase
        )

    def assets_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(value, self.__config.convert_urns_to_lowercase)

    def new_mcp(
        self,
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
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
            entity_type=Constant.DATASET,
            entity_urn=ds_urn,
            aspect_name=Constant.SCHEMA_METADATA,
            aspect=schema_metadata,
        )
        return [schema_mcp]

    def make_fine_grained_lineage_class(
        self, lineage: resolver.Lineage, dataset_urn: str
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

        upstream_lineage: List[resolver.Lineage] = parser.get_upstream_tables(
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
            ds_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.__config.platform_name,
                name=self.assets_urn_to_lowercase(table.full_name),
                platform_instance=self.__config.platform_instance,
                env=self.__config.env,
            )

            logger.debug(f"{Constant.Dataset_URN}={ds_urn}")
            # Create datasetProperties mcp
            if table.expression:
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLogic=table.expression,
                    viewLanguage="m_query",
                )
                view_prop_mcp = self.new_mcp(
                    entity_type=Constant.DATASET,
                    entity_urn=ds_urn,
                    aspect_name=Constant.VIEW_PROPERTIES,
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
                entity_type=Constant.DATASET,
                entity_urn=ds_urn,
                aspect_name=Constant.DATASET_PROPERTIES,
                aspect=ds_properties,
            )

            # Remove status mcp
            status_mcp = self.new_mcp(
                entity_type=Constant.DATASET,
                entity_urn=ds_urn,
                aspect_name=Constant.STATUS,
                aspect=StatusClass(removed=False),
            )
            if self.__config.extract_dataset_schema:
                dataset_mcps.extend(self.extract_dataset_schema(table, ds_urn))

            subtype_mcp = self.new_mcp(
                entity_type=Constant.DATASET,
                entity_urn=ds_urn,
                aspect_name=Constant.SUBTYPES,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.POWERBI_DATASET_TABLE,
                        DatasetSubTypes.VIEW,
                    ]
                ),
            )
            # normally the person who configure the dataset will be the most accurate person for ownership
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
                    entity_type=Constant.DATASET,
                    entity_urn=ds_urn,
                    aspect_name=Constant.OWNERSHIP,
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

        return dataset_mcps

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

            if tile.report is not None and tile.report.id is not None:
                custom_properties[Constant.REPORT_ID] = tile.report.id

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
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_INFO,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # ChartKey status
        chart_key_instance = ChartKeyClass(
            dashboardTool=self.__config.platform_name,
            chartId=Constant.CHART_ID.format(tile.id),
        )

        chart_key_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.CHART_KEY,
            aspect=chart_key_instance,
        )
        browse_path = BrowsePathsClass(paths=["/powerbi/{}".format(workspace.name)])
        browse_path_mcp = self.new_mcp(
            entity_type=Constant.CHART,
            entity_urn=chart_urn,
            aspect_name=Constant.BROWSERPATH,
            aspect=browse_path,
        )
        result_mcps = [info_mcp, status_mcp, chart_key_mcp, browse_path_mcp]

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
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_INFO,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_KEY,
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
                entity_type=Constant.DASHBOARD,
                entity_urn=dashboard_urn,
                aspect_name=Constant.OWNERSHIP,
                aspect=ownership,
            )

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{dashboard.workspace_name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.BROWSERPATH,
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
            sub_types=[BIContainerSubTypes.POWERBI_WORKSPACE],
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
                entity_type=entity_type,
                entity_urn=entity_urn,
                aspect_name=Constant.GLOBAL_TAGS,
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
            entity_type=Constant.CORP_USER,
            entity_urn=user_urn,
            aspect_name=Constant.CORP_USER_KEY,
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
        dashboard_mcps: List[
            MetadataChangeProposalWrapper
        ] = self.to_datahub_dashboard_mcp(dashboard, workspace, chart_mcps, user_mcps)

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
                entity_type=Constant.CHART,
                entity_urn=chart_urn,
                aspect_name=Constant.CHART_INFO,
                aspect=chart_info_instance,
            )

            # removed status mcp
            status_mcp = self.new_mcp(
                entity_type=Constant.CHART,
                entity_urn=chart_urn,
                aspect_name=Constant.STATUS,
                aspect=StatusClass(removed=False),
            )
            browse_path = BrowsePathsClass(paths=["/powerbi/{}".format(workspace.name)])
            browse_path_mcp = self.new_mcp(
                entity_type=Constant.CHART,
                entity_urn=chart_urn,
                aspect_name=Constant.BROWSERPATH,
                aspect=browse_path,
            )
            list_of_mcps = [info_mcp, status_mcp, browse_path_mcp]

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
        )

        info_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_INFO,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.STATUS,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(report.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.DASHBOARD_KEY,
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
                entity_type=Constant.DASHBOARD,
                entity_urn=dashboard_urn,
                aspect_name=Constant.OWNERSHIP,
                aspect=ownership,
            )

        # Report browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=Constant.BROWSERPATH,
            aspect=browse_path,
        )

        sub_type_mcp = self.new_mcp(
            entity_type=Constant.DASHBOARD,
            entity_urn=dashboard_urn,
            aspect_name=SubTypesClass.ASPECT_NAME,
            aspect=SubTypesClass(typeNames=[Constant.REPORT_TYPE_NAME]),
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

        logger.debug(f"Converting dashboard={report.name} to datahub dashboard")

        # Convert user to CorpUser
        user_mcps = self.to_datahub_users(report.users)
        # Convert pages to charts. A report has single dataset and same dataset used in pages to create visualization
        ds_mcps = self.to_datahub_dataset(report.dataset, workspace)
        chart_mcps = self.pages_to_chart(report.pages, workspace, ds_mcps)

        # Let's convert report to datahub dashboard
        report_mcps = self.report_to_dashboard(workspace, report, chart_mcps, user_mcps)

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        if self.__config.ownership.create_corp_user:
            mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(report_mcps)

        # Convert MCP to work_units
        work_units = map(self._to_work_unit, mcps)
        return work_units


@platform_name("PowerBI")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
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
class PowerBiDashboardSource(StatefulIngestionSourceBase):
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
        super(PowerBiDashboardSource, self).__init__(config, ctx)
        self.source_config = config
        self.reporter = PowerBiDashboardSourceReport()
        self.dataplatform_instance_resolver = create_dataplatform_instance_resolver(
            self.source_config
        )
        try:
            self.powerbi_client = PowerBiAPI(self.source_config)
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

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiDashboardSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_allowed_workspaces(self) -> List[powerbi_data_classes.Workspace]:
        all_workspaces = self.powerbi_client.get_workspaces()

        allowed_wrk = [
            workspace
            for workspace in all_workspaces
            if self.source_config.workspace_id_pattern.allowed(workspace.id)
        ]

        logger.info(f"Number of workspaces = {len(all_workspaces)}")
        self.reporter.report_number_of_workspaces(len(all_workspaces))
        logger.info(f"Number of allowed workspaces = {len(allowed_wrk)}")
        logger.debug(f"Workspaces = {all_workspaces}")

        return allowed_wrk

    def validate_dataset_type_mapping(self):
        powerbi_data_platforms: List[str] = [
            data_platform.value.powerbi_data_platform_name
            for data_platform in resolver.SupportedDataPlatform
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
        for dataset in workspace.independent_datasets:
            yield from auto_workunit(
                stream=self.mapper.to_datahub_dataset(
                    dataset=dataset,
                    workspace=workspace,
                )
            )

    def get_workspace_workunit(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.extract_workspaces_to_containers:
            workspace_workunits = self.mapper.generate_container_for_workspace(
                workspace
            )

            for workunit in workspace_workunits:
                # Return workunit to Datahub Ingestion framework
                yield workunit
        for dashboard in workspace.dashboards:
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
                # Return workunit to Datahub Ingestion framework
                yield workunit

        for report in workspace.reports:
            for work_unit in self.mapper.report_to_datahub_work_units(
                report, workspace
            ):
                yield work_unit

        yield from self.extract_independent_datasets(workspace)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # As modified_workspaces is not idempotent, hence workunit processors are run later for each workspace_id
        # This will result in creating checkpoint for each workspace_id
        if self.source_config.modified_since:
            return []  # Handle these in get_workunits_internal
        else:
            return [
                *super().get_workunit_processors(),
                self.stale_entity_removal_handler.workunit_processor,
            ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("PowerBi plugin execution is started")
        # Validate dataset type mapping
        self.validate_dataset_type_mapping()
        # Fetch PowerBi workspace for given workspace identifier

        allowed_workspaces = self.get_allowed_workspaces()
        workspaces_len = len(allowed_workspaces)

        batch_size = (
            self.source_config.scan_batch_size
        )  # 100 is the maximum allowed for powerbi scan
        num_batches = (workspaces_len + batch_size - 1) // batch_size
        batches = [
            allowed_workspaces[i * batch_size : (i + 1) * batch_size]
            for i in range(num_batches)
        ]
        for batch_workspaces in batches:
            for workspace in self.powerbi_client.fill_workspaces(
                batch_workspaces, self.reporter
            ):
                logger.info(f"Processing workspace id: {workspace.id}")

                if self.source_config.modified_since:
                    # As modified_workspaces is not idempotent, hence we checkpoint for each powerbi workspace
                    # Because job_id is used as dictionary key, we have to set a new job_id
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
