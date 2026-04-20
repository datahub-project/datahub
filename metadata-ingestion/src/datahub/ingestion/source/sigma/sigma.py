import logging
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    add_entity_to_container,
    add_owner_to_entity_wu,
    add_tags_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sigma.config import (
    PlatformDetail,
    SigmaSourceConfig,
    SigmaSourceReport,
    WorkspaceCounts,
)
from datahub.ingestion.source.sigma.data_classes import (
    Element,
    Page,
    SigmaDataModel,
    SigmaDataset,
    Workbook,
    WorkbookKey,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemalessClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.dataset_urn import DatasetUrn

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Sigma")
@config_class(SigmaSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[SourceCapabilityModifier.SIGMA_WORKSPACE],
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default.")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class SigmaSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Sigma Workspaces and Workbooks as Container.
    - Sigma Datasets
    - Pages as Dashboard and its Elements as Charts
    """

    config: SigmaSourceConfig
    reporter: SigmaSourceReport
    platform: str = "sigma"

    def __init__(self, config: SigmaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = SigmaSourceReport()
        self.dataset_upstream_urn_mapping: Dict[str, List[str]] = {}
        try:
            self.sigma_api = SigmaAPI(self.config, self.reporter)
        except Exception as e:
            raise ConfigurationError("Unable to connect sigma API") from e

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            SigmaAPI(
                SigmaSourceConfig.parse_obj_allow_extras(config_dict),
                SigmaSourceReport(),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = SigmaSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _gen_workbook_key(self, workbook_id: str) -> WorkbookKey:
        return WorkbookKey(
            workbookId=workbook_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_workspace_key(self, workspace_id: str) -> WorkspaceKey:
        return WorkspaceKey(
            workspaceId=workspace_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _get_allowed_workspaces(self) -> List[Workspace]:
        all_workspaces = self.sigma_api.workspaces.values()
        logger.info(f"Number of workspaces = {len(all_workspaces)}")

        allowed_workspaces = []
        for workspace in all_workspaces:
            if self.config.workspace_pattern.allowed(workspace.name):
                allowed_workspaces.append(workspace)
            else:
                self.reporter.workspaces.dropped(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        logger.info(f"Number of allowed workspaces = {len(allowed_workspaces)}")

        return allowed_workspaces

    def _gen_workspace_workunit(
        self, workspace: Workspace
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workspace to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workspace.createdBy)
        yield from gen_containers(
            container_key=self._gen_workspace_key(workspace.workspaceId),
            name=workspace.name,
            sub_types=[BIContainerSubTypes.SIGMA_WORKSPACE],
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            created=int(workspace.createdAt.timestamp() * 1000),
            last_modified=int(workspace.updatedAt.timestamp() * 1000),
        )

    def _gen_sigma_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: SigmaDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=dataset.url,
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
            customProperties={"datasetId": dataset.datasetId},
            tags=[dataset.badge] if dataset.badge else None,
        )
        if dataset.path:
            dataset_properties.customProperties["path"] = dataset.path
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_entity_browsepath_aspect(
        self,
        entity_urn: str,
        parent_entity_urn: str,
        paths: List[str],
    ) -> MetadataWorkUnit:
        entries = [
            BrowsePathEntryClass(id=parent_entity_urn, urn=parent_entity_urn)
        ] + [BrowsePathEntryClass(id=path) for path in paths]
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(entries),
        ).as_workunit()

    def _gen_dataset_workunit(
        self, dataset: SigmaDataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_sigma_dataset_urn(dataset.get_urn_part())

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        if dataset.workspaceId:
            self.reporter.workspaces.increment_datasets_count(dataset.workspaceId)
            yield from add_entity_to_container(
                container_key=self._gen_workspace_key(dataset.workspaceId),
                entity_type="dataset",
                entity_urn=dataset_urn,
            )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self.sigma_api.get_user_name(dataset.createdBy)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.SIGMA_DATASET]),
        ).as_workunit()

        if dataset.path and dataset.workspaceId:
            paths = dataset.path.split("/")[1:]
            if len(paths) > 0:
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dataset_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(dataset.workspaceId)
                    ),
                    paths=paths,
                )

        if dataset.badge:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(builder.make_tag_urn(dataset.badge))]
                ),
            ).as_workunit()

    def _gen_data_model_schema_aspect(
        self, data_model: SigmaDataModel
    ) -> Optional[MetadataWorkUnit]:
        if not data_model.columns:
            return None
        fields = [
            SchemaFieldClass(
                fieldPath=col.name,
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="String",
                description=col.label if col.label else None,
            )
            for col in data_model.columns
        ]
        return MetadataChangeProposalWrapper(
            entityUrn=self._gen_sigma_dataset_urn(data_model.get_urn_part()),
            aspect=SchemaMetadataClass(
                schemaName=data_model.name,
                platform=builder.make_data_platform_urn(self.platform),
                version=0,
                hash="",
                platformSchema=SchemalessClass(),
                fields=fields,
            ),
        ).as_workunit()

    def _gen_data_model_upstream_lineage(
        self, data_model: SigmaDataModel
    ) -> Optional[MetadataWorkUnit]:
        upstreams = []
        unresolved_dataset_ids = set()
        skipped_warehouse_tables = False
        for source in data_model.sources:
            if source.type == "dataset" and source.datasetId:
                # /sources gives the UUID; existing dataset entities use the URL-based
                # ID — look it up in the mapping populated during dataset ingestion.
                urn_part = self.sigma_api.dataset_id_to_urn_part.get(source.datasetId)
                if urn_part is None:
                    unresolved_dataset_ids.add(source.datasetId)
                    continue
                upstream_urn = self._gen_sigma_dataset_urn(urn_part)
                upstreams.append(
                    Upstream(dataset=upstream_urn, type=DatasetLineageType.COPY)
                )
            elif source.type == "dataModel" and source.dataModelId:
                upstream_urn = self._gen_sigma_dataset_urn(source.dataModelId)
                upstreams.append(
                    Upstream(dataset=upstream_urn, type=DatasetLineageType.COPY)
                )
            elif source.type == "table":
                # Warehouse table sources from /sources provide only Sigma internal
                # inodeId — not enough to generate a warehouse URN without SQL parsing.
                logger.debug(
                    f"Skipping warehouse table upstream for data model '{data_model.name}': "
                    f"inodeId-only reference cannot be resolved to a warehouse URN without SQL parsing."
                )
                skipped_warehouse_tables = True
            else:
                logger.debug(
                    f"Unknown source type '{source.type}' for data model '{data_model.name}', skipping."
                )
        if unresolved_dataset_ids:
            self.reporter.warning(
                message=f"Data model '{data_model.name}' references datasetId(s) not in ingested datasets; upstream edges skipped.",
                title="Unresolved Sigma dataset upstream",
                context=data_model.dataModelId,
            )
        if skipped_warehouse_tables:
            self.reporter.warning(
                message=f"Data model '{data_model.name}' has warehouse table source(s); upstream edges skipped (inodeId cannot be resolved to a warehouse URN without SQL parsing).",
                title="Warehouse table upstream skipped",
                context=data_model.dataModelId,
            )
            self.reporter.skipped_warehouse_table_upstreams += 1
        if not upstreams:
            return None
        return MetadataChangeProposalWrapper(
            entityUrn=self._gen_sigma_dataset_urn(data_model.get_urn_part()),
            aspect=UpstreamLineage(upstreams=upstreams),
        ).as_workunit()

    def _gen_data_model_workunit(
        self, data_model: SigmaDataModel
    ) -> Iterable[MetadataWorkUnit]:
        data_model_urn = self._gen_sigma_dataset_urn(data_model.get_urn_part())

        yield self._gen_entity_status_aspect(data_model_urn)

        dm_properties = DatasetProperties(
            name=data_model.name,
            description=data_model.description,
            qualifiedName=data_model.name,
            externalUrl=data_model.url,
            created=TimeStamp(time=int(data_model.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(data_model.updatedAt.timestamp() * 1000)),
            customProperties={"dataModelId": data_model.dataModelId},
        )
        if data_model.path:
            dm_properties.customProperties["path"] = data_model.path
        yield MetadataChangeProposalWrapper(
            entityUrn=data_model_urn, aspect=dm_properties
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=data_model_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.SIGMA_DATA_MODEL]),
        ).as_workunit()

        schema_wu = self._gen_data_model_schema_aspect(data_model)
        if schema_wu:
            yield schema_wu

        upstream_wu = self._gen_data_model_upstream_lineage(data_model)
        if upstream_wu:
            yield upstream_wu

        if data_model.workspaceId:
            self.reporter.workspaces.increment_data_models_count(data_model.workspaceId)
            yield from add_entity_to_container(
                container_key=self._gen_workspace_key(data_model.workspaceId),
                entity_type="dataset",
                entity_urn=data_model_urn,
            )

        dpi_aspect = self._gen_dataplatform_instance_aspect(data_model_urn)
        if dpi_aspect:
            yield dpi_aspect

        if data_model.createdBy:
            owner_username = self.sigma_api.get_user_name(data_model.createdBy)
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(data_model_urn, owner_username)

        if data_model.path and data_model.workspaceId:
            paths = data_model.path.split("/")[1:]
            if paths:
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=data_model_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(data_model.workspaceId)
                    ),
                    paths=paths,
                )

        if data_model.badge:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_model_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(builder.make_tag_urn(data_model.badge))]
                ),
            ).as_workunit()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(self, page: Page) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())
        dashboard_info_cls = DashboardInfoClass(
            title=page.name,
            description="",
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=element.get_urn_part(),
                )
                for element in page.elements
            ],
            lastModified=ChangeAuditStampsClass(),
            customProperties={"ElementsCount": str(len(page.elements))},
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _get_element_data_source_platform_details(
        self, full_path: str
    ) -> Optional[PlatformDetail]:
        data_source_platform_details: Optional[PlatformDetail] = None
        while full_path != "":
            if full_path in self.config.chart_sources_platform_mapping:
                data_source_platform_details = (
                    self.config.chart_sources_platform_mapping[full_path]
                )
                break
            else:
                full_path = "/".join(full_path.split("/")[:-1])
        if (
            not data_source_platform_details
            and "*" in self.config.chart_sources_platform_mapping
        ):
            data_source_platform_details = self.config.chart_sources_platform_mapping[
                "*"
            ]

        return data_source_platform_details

    def _get_element_input_details(
        self, element: Element, workbook: Workbook
    ) -> Dict[str, List[str]]:
        """
        Returns dict with keys as the all element input dataset urn and values as their all upstream dataset urns
        """
        inputs: Dict[str, List[str]] = {}
        sql_parser_in_tables: List[str] = []

        data_source_platform_details = self._get_element_data_source_platform_details(
            f"{workbook.path}/{workbook.name}/{element.name}"
        )

        if element.query and data_source_platform_details:
            try:
                sql_parser_in_tables = create_lineage_sql_parsed_result(
                    query=element.query.strip(),
                    default_db=data_source_platform_details.default_db,
                    default_schema=data_source_platform_details.default_schema,
                    platform=data_source_platform_details.data_source_platform,
                    env=data_source_platform_details.env,
                    platform_instance=data_source_platform_details.platform_instance,
                    generate_column_lineage=False,
                ).in_tables
            except Exception:
                logger.debug(f"Unable to parse query of element {element.name}")

        # Add sigma dataset as input of element if present
        # and its matched sql parser in_table as its upsteam dataset
        for source_id, source_name in element.upstream_sources.items():
            source_id = source_id.split("-")[-1]
            for in_table_urn in list(sql_parser_in_tables):
                if (
                    DatasetUrn.from_string(in_table_urn).name.split(".")[-1]
                    in source_name.lower()
                ):
                    dataset_urn = self._gen_sigma_dataset_urn(source_id)
                    if dataset_urn not in inputs:
                        inputs[dataset_urn] = [in_table_urn]
                    else:
                        inputs[dataset_urn].append(in_table_urn)
                    sql_parser_in_tables.remove(in_table_urn)

        # Add remaining sql parser in_tables as direct input of element
        for in_table_urn in sql_parser_in_tables:
            inputs[in_table_urn] = []

        return inputs

    def _gen_elements_workunit(
        self,
        elements: List[Element],
        workbook: Workbook,
        all_input_fields: List[InputFieldClass],
        paths: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma page element to Datahub Chart
        """
        for element in elements:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.get_urn_part(),
            )

            custom_properties = {
                "VizualizationType": str(element.vizualizationType),
                "type": str(element.type) if element.type else "Unknown",
            }

            yield self._gen_entity_status_aspect(chart_urn)

            inputs: Dict[str, List[str]] = self._get_element_input_details(
                element, workbook
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=element.name,
                    description="",
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    externalUrl=element.url,
                    inputs=list(inputs.keys()),
                ),
            ).as_workunit()

            if workbook.workspaceId:
                self.reporter.workspaces.increment_elements_count(workbook.workspaceId)

                yield self._gen_entity_browsepath_aspect(
                    entity_urn=chart_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            # Add sigma dataset's upstream dataset urn mapping
            for dataset_urn, upstream_dataset_urns in inputs.items():
                if (
                    upstream_dataset_urns
                    and dataset_urn not in self.dataset_upstream_urn_mapping
                ):
                    self.dataset_upstream_urn_mapping[dataset_urn] = (
                        upstream_dataset_urns
                    )

            element_input_fields = [
                InputFieldClass(
                    schemaFieldUrn=builder.make_schema_field_urn(chart_urn, column),
                    schemaField=SchemaFieldClass(
                        fieldPath=column,
                        type=SchemaFieldDataTypeClass(StringTypeClass()),
                        nativeDataType="String",  # Make type default as Sting
                    ),
                )
                for column in element.columns
            ]

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(fields=element_input_fields),
            ).as_workunit()

            all_input_fields.extend(element_input_fields)

    def _gen_pages_workunit(
        self, workbook: Workbook, paths: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workbook page to Datahub dashboard
        """
        for page in workbook.pages:
            dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(page)

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            all_input_fields: List[InputFieldClass] = []

            if workbook.workspaceId:
                self.reporter.workspaces.increment_pages_count(workbook.workspaceId)
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dashboard_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            yield from self._gen_elements_workunit(
                page.elements, workbook, all_input_fields, paths
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=InputFieldsClass(fields=all_input_fields),
            ).as_workunit()

    def _gen_workbook_workunit(self, workbook: Workbook) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma Workbook to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workbook.ownerId)

        dashboard_urn = self._gen_dashboard_urn(workbook.workbookId)

        yield self._gen_entity_status_aspect(dashboard_urn)

        lastModified = AuditStampClass(
            time=int(workbook.updatedAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )
        created = AuditStampClass(
            time=int(workbook.createdAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )

        dashboard_info_cls = DashboardInfoClass(
            title=workbook.name,
            description=workbook.description if workbook.description else "",
            dashboards=[
                EdgeClass(
                    destinationUrn=self._gen_dashboard_urn(page.get_urn_part()),
                    sourceUrn=dashboard_urn,
                )
                for page in workbook.pages
            ],
            externalUrl=workbook.url,
            lastModified=ChangeAuditStampsClass(
                created=created, lastModified=lastModified
            ),
            customProperties={
                "path": workbook.path,
                "latestVersion": str(workbook.latestVersion),
            },
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

        # Set subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[BIContainerSubTypes.SIGMA_WORKBOOK]),
        ).as_workunit()

        # Ownership
        owner_urn = (
            builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None
        )
        if owner_urn:
            yield from add_owner_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                owner_urn=owner_urn,
            )

        # Tags
        tags = [workbook.badge] if workbook.badge else None
        if tags:
            yield from add_tags_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                tags=sorted(tags),
            )

        paths = workbook.path.split("/")[1:]
        if workbook.workspaceId:
            self.reporter.workspaces.increment_workbooks_count(workbook.workspaceId)

            yield self._gen_entity_browsepath_aspect(
                entity_urn=dashboard_urn,
                parent_entity_urn=builder.make_container_urn(
                    self._gen_workspace_key(workbook.workspaceId)
                ),
                paths=paths + [workbook.name],
            )

            if len(paths) == 0:
                yield from add_entity_to_container(
                    container_key=self._gen_workspace_key(workbook.workspaceId),
                    entity_type="dashboard",
                    entity_urn=dashboard_urn,
                )

        yield from self._gen_pages_workunit(workbook, paths)

    def _gen_sigma_dataset_upstream_lineage_workunit(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        for (
            dataset_urn,
            upstream_dataset_urns,
        ) in self.dataset_upstream_urn_mapping.items():
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                        for upstream_dataset_urn in upstream_dataset_urns
                    ],
                ),
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Sigma plugin execution is started")
        self.sigma_api.fill_workspaces()

        for dataset in self.sigma_api.get_sigma_datasets():
            yield from self._gen_dataset_workunit(dataset)

        if self.config.ingest_data_models:
            for data_model in self.sigma_api.get_data_models():
                yield from self._gen_data_model_workunit(data_model)

        for workbook in self.sigma_api.get_sigma_workbooks():
            yield from self._gen_workbook_workunit(workbook)

        for workspace in self._get_allowed_workspaces():
            self.reporter.workspaces.processed(
                f"{workspace.name} ({workspace.workspaceId})"
            )
            yield from self._gen_workspace_workunit(workspace)
            if self.reporter.workspaces.workspace_counts.get(
                workspace.workspaceId, WorkspaceCounts()
            ).is_empty():
                logger.warning(
                    f"Workspace {workspace.name} ({workspace.workspaceId}) is empty. If this is not expected, add the user associated with the Client ID/Secret to each workspace with missing metadata"
                )
                self.reporter.empty_workspaces.append(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        yield from self._gen_sigma_dataset_upstream_lineage_workunit()

    def get_report(self) -> SourceReport:
        return self.reporter
