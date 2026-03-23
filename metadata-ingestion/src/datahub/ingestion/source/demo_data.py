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
from datahub.ingestion.source.datapack import DataPackSource, DataPackSourceConfig


class DemoDataConfig(ConfigModel):
    # The demo data source does not accept any configuration.
    pass


@platform_name("Demo Data")
@config_class(DemoDataConfig)
@support_status(SupportStatus.UNKNOWN)
class DemoDataSource(Source):
    """
    This source loads sample data into DataHub. It is intended for demo and testing purposes only.

    This is a thin wrapper around the DataPack source that loads the "bootstrap" pack.
    For more control, use the 'datapack' source type directly.
    """

    def __init__(self, ctx: PipelineContext, config: DemoDataConfig):
        datapack_config = DataPackSourceConfig(
            pack_name="bootstrap",
            no_time_shift=True,  # Backward compat: demo-data never time-shifted
        )
        self.datapack_source = DataPackSource(ctx, datapack_config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.datapack_source.get_workunits()

    def get_report(self) -> SourceReport:
        return self.datapack_source.get_report()
