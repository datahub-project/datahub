import logging
from typing import Iterable

from pydantic import Field

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)


class DataHubMockDataConfig(ConfigModel):
    """
    Configuration for the DataHub Mock Data source.
    This source is a skeleton that doesn't produce any workunits.
    """

    enabled: bool = Field(
        default=True,
        description="Whether this source is enabled",
    )

    description: str = Field(
        default="Mock data source for testing purposes",
        description="Description of this mock data source",
    )


@platform_name("DataHubMockData")
@config_class(DataHubMockDataConfig)
@support_status(SupportStatus.TESTING)
class DataHubMockDataSource(Source):
    """
    This source is for generating mock data for testing purposes.
    Expect breaking changes as we iterate on the mock data source.
    """

    def __init__(self, ctx: PipelineContext, config: DataHubMockDataConfig):
        self.ctx = ctx
        self.config = config
        self.report = SourceReport()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        logger.info(
            f"DataHubMockDataSource is running with description: {self.config.description}"
        )

        yield from []

    def get_report(self) -> SourceReport:
        return self.report
