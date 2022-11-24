import pathlib

from datahub.cli.docker_cli import download_sample_data
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.file import FileSourceConfig, GenericFileSource


@platform_name("SampleData")
@config_class(ConfigModel)
@support_status(SupportStatus.UNKNOWN)
class SampleDataSource(GenericFileSource):
    """
    This plugin is only for demonstration purposes.
    """

    def __init__(self, ctx: PipelineContext, config: FileSourceConfig):
        config.path = pathlib.Path(download_sample_data())
        super().__init__(ctx, config)
