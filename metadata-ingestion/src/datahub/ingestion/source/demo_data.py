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


class DemoDataConfig(ConfigModel):
    # The demo data source does not accept any configuration.
    pass


@platform_name("Demo Data")
@config_class(ConfigModel)
@support_status(SupportStatus.UNKNOWN)
class DemoDataSource(GenericFileSource):
    """
    This source loads sample data into DataHub. It is intended for demo and testing purposes only.
    """

    def __init__(self, ctx: PipelineContext, config: DemoDataConfig):
        file_config = FileSourceConfig(filename=download_sample_data())
        super().__init__(ctx, file_config)

    @classmethod
    def create(cls, config_dict, ctx):
        config = DemoDataConfig.parse_obj(config_dict or {})
        return cls(ctx, config)
