import logging
from typing import Iterable, List, Optional

# from datahub.api.entities.datajob import DataFlow, DataJob
# from datahub.api.entities.dataprocess.dataprocess_instance import (
#     DataProcessInstance,
#     InstanceRunResult,
# )
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
from datahub.ingestion.source.fivetran.config import (
    FivetranSourceConfig,
    FivetranSourceReport,
)

# from datahub.utilities.urns.data_flow_urn import DataFlowUrn
# from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.ingestion.source.fivetran.fivetran_schema import (
    Connector,
    FivetranLogDataDictionary,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

# Logger instance
logger = logging.getLogger(__name__)


@platform_name("Fivetran")
@config_class(FivetranSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class FivetranSource(StatefulIngestionSourceBase):
    """
    This plugin extracts fivetran users, connectors, destinations and sync history.
    This plugin is in beta and has only been tested on Snowflake connector.
    """

    config: FivetranSourceConfig
    report: FivetranSourceReport
    platform: str = "fivetran"

    def __init__(self, config: FivetranSourceConfig, ctx: PipelineContext):
        super(FivetranSource, self).__init__(config, ctx)
        self.config = config
        self.report = FivetranSourceReport()

        self.audit_log = FivetranLogDataDictionary(self.config)

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.config, self.ctx
        )

    def get_connector_workunit(
        self, connector: Connector
    ) -> Iterable[MetadataWorkUnit]:
        pass

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = FivetranSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invoke this method
        """
        logger.info("Fivetran plugin execution is started")
        connectors = self.audit_log.get_connectors_list()
        for connector in connectors:
            logger.info(f"Processing connector id: {connector.connector_id}")
            yield from self.get_connector_workunit(connector)

    def get_report(self) -> SourceReport:
        return self.report
