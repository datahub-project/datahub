from typing import Iterable

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
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource
from datahub.utilities.sample_data import download_sample_data


class DemoDataConfig(ConfigModel):
    # The demo data source does not accept any configuration.
    pass


@platform_name("Demo Data")
@config_class(DemoDataConfig)
@support_status(SupportStatus.UNKNOWN)
class DemoDataSource(Source):
    """
    This source loads sample data into DataHub. It is intended for demo and testing purposes only.
    """

    def __init__(self, ctx: PipelineContext, config: DemoDataConfig):
        file_config = FileSourceConfig(path=str(download_sample_data()))
        self.file_source = GenericFileSource(ctx, file_config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.file_source.get_workunits()

    def get_report(self) -> SourceReport:
        return self.file_source.get_report()
