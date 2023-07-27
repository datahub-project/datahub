import logging
from typing import Iterable, List, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.airbyte.config import (
    AirbyteSourceConfig,
    AirbyteSourceReport,
)
from datahub.ingestion.source.airbyte.rest_api_wrapper.airbyte_api import AirbyteAPI
from datahub.ingestion.source.airbyte.rest_api_wrapper.data_classes import Workspace
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Airbyte")
@config_class(AirbyteSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class AirbyteSource(StatefulIngestionSourceBase):
    """
    This plugin extracts airbyte workspace, connections, sources, destinations and jobs.
    This plugin is in beta and has only been tested on PostgreSQL.
    """

    config: AirbyteSourceConfig
    report: AirbyteSourceReport
    platform: str = "airbyte"

    def __init__(self, config: AirbyteSourceConfig, ctx: PipelineContext):
        super(AirbyteSource, self).__init__(config, ctx)
        self.config = config
        self.report = AirbyteSourceReport()
        try:
            self.airbyte_client = AirbyteAPI(self.config)
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to Airbyte API Service. This exit will avoid raising
            # unwanted stacktrace on console

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = AirbyteSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workspace_workunit(
        self, workspace: Workspace
    ) -> Iterable[MetadataWorkUnit]:
        pass

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Airbyte plugin execution is started")

        # Fetch Airbyte workspace
        workspaces = self.airbyte_client.get_workspaces()
        for workspace in workspaces:
            logger.info(f"Processing workspace id: {workspace.workspace_id}")
            yield from self.get_workspace_workunit(workspace)

    def get_report(self) -> SourceReport:
        return self.report
