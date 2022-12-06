import pathlib

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource
from datahub.utilities.sample_data import download_sample_data


class DummySourceConfig(FileSourceConfig):
    path = pathlib.Path("")


@platform_name("DummySource")
@config_class(ConfigModel)
@support_status(SupportStatus.UNKNOWN)
class DummySource(GenericFileSource):
    """
    This is a dummy plugin only for testing and fast demonstration of managed ingestion.
    """

    def __init__(self, ctx: PipelineContext, config: DummySourceConfig):
        config.path = pathlib.Path(download_sample_data())
        super().__init__(ctx, config)
