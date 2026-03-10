"""Microsoft Fabric Data Factory ingestion source for DataHub.

This connector extracts metadata from Microsoft Fabric Data Factory items:
- Workspaces as Containers
- Data Pipelines as DataFlows with Activities as DataJobs
- Copy Jobs as DataFlows with dataset-level lineage
- Dataflow Gen2 as DataFlows (metadata only)
"""

import logging
from typing import Iterable, Optional, Union

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.auth import FabricAuthHelper
from datahub.ingestion.source.fabric.common.models import (
    FabricWorkspace,
    build_workspace_container,
)
from datahub.ingestion.source.fabric.data_factory.client import (
    FabricDataFactoryClient,
)
from datahub.ingestion.source.fabric.data_factory.config import (
    FabricDataFactorySourceConfig,
)
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactoryClientReport,
    FabricDataFactorySourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM = "fabric-data-factory"


@platform_name("Fabric Data Factory")
@config_class(FabricDataFactorySourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class FabricDataFactorySource(StatefulIngestionSourceBase):
    """Extracts metadata from Microsoft Fabric Data Factory."""

    config: FabricDataFactorySourceConfig
    report: FabricDataFactorySourceReport
    platform: str = PLATFORM

    def __init__(self, config: FabricDataFactorySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report = FabricDataFactorySourceReport()

        auth_helper = FabricAuthHelper(config.credential)
        self.client_report = FabricDataFactoryClientReport()
        self.client = FabricDataFactoryClient(
            auth_helper,
            timeout=config.api_timeout,
            report=self.client_report,
        )
        self.report.client_report = self.client_report

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "FabricDataFactorySource":
        config = FabricDataFactorySourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(
        self,
    ) -> list[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> FabricDataFactorySourceReport:
        return self.report

    def get_workunits_internal(
        self,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Generate workunits for all Fabric Data Factory resources."""
        logger.info("Starting Fabric Data Factory ingestion")

        try:
            workspaces = list(self.client.list_workspaces())

            for workspace in workspaces:
                self.report.report_api_call()

                if not self.config.workspace_pattern.allowed(workspace.name):
                    self.report.report_workspace_filtered(workspace.name)
                    continue

                self.report.report_workspace_scanned()
                logger.info(f"Processing workspace: {workspace.name} ({workspace.id})")

                try:
                    yield from self._create_workspace_container(workspace)

                    # TODO: Process pipelines, copy jobs, dataflows
                    # yield from self._process_pipelines(workspace)
                    # yield from self._process_copyjobs(workspace)
                    # yield from self._process_dataflows(workspace)

                except Exception as e:
                    self.report.report_warning(
                        title="Failed to Process Workspace",
                        message="Error processing workspace. Skipping to next.",
                        context=f"workspace={workspace.name}",
                        exc=e,
                    )

        except Exception as e:
            self.report.report_failure(
                title="Failed to List Workspaces",
                message="Unable to retrieve workspaces from Fabric.",
                context="",
                exc=e,
            )
        finally:
            self.client.close()

    def _create_workspace_container(
        self, workspace: FabricWorkspace
    ) -> Iterable[Entity]:
        yield from build_workspace_container(
            workspace=workspace,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
