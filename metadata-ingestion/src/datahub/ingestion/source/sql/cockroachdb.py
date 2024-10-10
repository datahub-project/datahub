from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


class CockroachDBConfig(PostgresConfig):
    scheme = Field(default="cockroachdb+psycopg2", description="database scheme")
    schema_pattern = Field(
        default=AllowDenyPattern(deny=["information_schema", "crdb_internal"])
    )


@platform_name("CockroachDB")
@config_class(CockroachDBConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class CockroachDBSource(PostgresSource):
    config: CockroachDBConfig

    def __init__(self, config: CockroachDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_platform(self):
        return "cockroachdb"

    @classmethod
    def create(cls, config_dict, ctx):
        config = CockroachDBConfig.parse_obj(config_dict)
        return cls(config, ctx)
