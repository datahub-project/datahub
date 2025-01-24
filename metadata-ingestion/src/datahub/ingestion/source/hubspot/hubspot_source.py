from dataclasses import dataclass
from typing import Iterable, List, Optional

from pydantic import Field, SecretStr

from datahub.configuration.source_common import EnvConfigMixin
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)


class HubSpotConfig(EnvConfigMixin, StatefulIngestionConfigBase):
    access_token: SecretStr = Field(
        description="Access token for HubSpot API",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class HubSpotReport(SourceReport, StaleEntityRemovalSourceReport):
    dashboards_found: int = 0


@platform_name("HubSpot", id="hubspot")
@config_class(HubSpotConfig)
@support_status(SupportStatus.TESTING)
class HubSpotSource(StatefulIngestionSourceBase):
    PLATFORM = "hubspot"

    def __init__(self, ctx: PipelineContext, config: HubSpotConfig):
        self.ctx = ctx
        self.config = config
        self.report = HubSpotReport()

    @classmethod
    def create(cls, config_dict, ctx):
        config = HubSpotConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]
