import logging
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_entity_to_container, gen_containers
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
)
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import (
    Element,
    Page,
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
from datahub.metadata.com.linkedin.pegasus2avro.dataset import DatasetProperties
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    TagAssociationClass,
)

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Sigma")
@config_class(SigmaSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
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
        super(SigmaSource, self).__init__(config, ctx)
        self.config = config
        self.reporter = SigmaSourceReport()
        try:
            self.sigma_api = SigmaAPI(self.config)
        except Exception as e:
            logger.warning(e)
            exit(1)  # Exit pipeline as we are not able to connect to sigma api service.

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            SigmaAPI(SigmaSourceConfig.parse_obj_allow_extras(config_dict))
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = SigmaSourceConfig.parse_obj(config_dict)
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
        allowed_workspaces = [
            workspace
            for workspace in all_workspaces
            if self.config.workspace_pattern.allowed(workspace.name)
        ]
        logger.info(f"Number of workspaces = {len(all_workspaces)}")
        self.reporter.report_number_of_workspaces(len(all_workspaces))
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
            owner_urn=builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None,
            created=int(workspace.createdAt.timestamp() * 1000),
            last_modified=int(workspace.updatedAt.timestamp() * 1000),
        )

    def _get_sigma_dataset_identifier(self, dataset: SigmaDataset) -> str:
        return f"{dataset.datasetId}".lower()

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
            tags=[dataset.badge] if dataset.badge else None,
        )
        dataset_properties.customProperties.update({"path": dataset.path})
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
        if self.config.platform_instance:
            urn = builder.make_dataplatform_instance_urn(
                self.platform, self.config.platform_instance
            )
        entries = [BrowsePathEntryClass(id=urn, urn=urn)] + entries
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(entries),
        ).as_workunit()

    def _gen_dataset_workunit(
        self, dataset: SigmaDataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_identifier = self._get_sigma_dataset_identifier(dataset)
        dataset_urn = self._gen_sigma_dataset_urn(dataset_identifier)

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_dataset_properties(dataset_urn, dataset)

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
        dashboard_urn = self._gen_dashboard_urn(page.pageId)
        dashboard_info_cls = DashboardInfoClass(
            title=page.name,
            description="",
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=element.elementId,
                )
                for element in page.elements
            ],
            lastModified=ChangeAuditStampsClass(),
            customProperties={"ElementsCount": str(len(page.elements))},
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _gen_elements_workunit(
        self, elements: List[Element], workbook_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma page element to Datahub Chart
        """

        for element in elements:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.elementId,
            )

            custom_properties = {
                "VizualizationType": str(element.vizualizationType),
                "Columns": str(element.columns),
            }

            yield self._gen_entity_status_aspect(chart_urn)

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=element.name,
                    description=element.type,
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    externalUrl=element.url,
                    inputs=[
                        self._gen_sigma_dataset_urn(dataset_id.lower())
                        for dataset_id in element.upstream_datasets
                    ],
                ),
            ).as_workunit()

            yield from add_entity_to_container(
                container_key=self._gen_workbook_key(workbook_id),
                entity_type="chart",
                entity_urn=chart_urn,
            )

    def _gen_pages_workunit(self, workbook: Workbook) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workbook page to Datahub dashboard
        """
        for page in workbook.pages:
            dashboard_urn = self._gen_dashboard_urn(page.pageId)

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(page)

            yield from add_entity_to_container(
                container_key=self._gen_workbook_key(workbook.workbookId),
                entity_type="dashboard",
                entity_urn=dashboard_urn,
            )

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield from self._gen_elements_workunit(page.elements, workbook.workbookId)

    def _gen_workbook_workunit(self, workbook: Workbook) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma Workbook to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workbook.createdBy)
        yield from gen_containers(
            container_key=self._gen_workbook_key(workbook.workbookId),
            name=workbook.name,
            sub_types=[BIContainerSubTypes.SIGMA_WORKBOOK],
            parent_container_key=self._gen_workspace_key(workbook.workspaceId),
            extra_properties={
                "path": workbook.path,
                "latestVersion": str(workbook.latestVersion),
            },
            owner_urn=builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None,
            external_url=workbook.url,
            tags=[workbook.badge] if workbook.badge else None,
            created=int(workbook.createdAt.timestamp() * 1000),
            last_modified=int(workbook.updatedAt.timestamp() * 1000),
        )

        paths = workbook.path.split("/")[1:]
        if len(paths) > 0:
            yield self._gen_entity_browsepath_aspect(
                entity_urn=builder.make_container_urn(
                    self._gen_workbook_key(workbook.workbookId),
                ),
                parent_entity_urn=builder.make_container_urn(
                    self._gen_workspace_key(workbook.workspaceId)
                ),
                paths=paths,
            )

        yield from self._gen_pages_workunit(workbook)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Sigma plugin execution is started")
        entities = self.sigma_api.get_sigma_entities()
        for entity in entities:
            if isinstance(entity, Workbook):
                yield from self._gen_workbook_workunit(entity)
            elif isinstance(entity, SigmaDataset):
                yield from self._gen_dataset_workunit(entity)
        for workspace in self._get_allowed_workspaces():
            yield from self._gen_workspace_workunit(workspace)

    def get_report(self) -> SourceReport:
        return self.reporter
