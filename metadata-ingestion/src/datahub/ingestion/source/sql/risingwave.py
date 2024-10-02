import logging
from typing import Optional

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
from src.datahub.ingestion.source.sql.postgres import (
    PostgresConfig,
    PostgresSource,
)

logger: logging.Logger = logging.getLogger(__name__)


class RisingWaveConfig(PostgresConfig):
    initial_database: Optional[str] = Field(
        default="dev",
        description=(
            "Initial database used to query for the list of databases, when ingesting multiple databases. "
            "Note: this is not used if `database` or `sqlalchemy_uri` are provided."
        ),
    )
    schema_pattern = Field(
        default=AllowDenyPattern(deny=["information_schema", "rw_catalog"])
    )


# TODO: override some stuff here to make it RisingWave specific
# e.g. use rw_catalog for columns, etc.
@platform_name("RisingWave")
@config_class(RisingWaveConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DOMAINS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING, "Optionally enabled via configuration"
)
@capability(
    SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration"
)
class RisingWaveSource(PostgresSource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views, and tables
    - Column types associated with each table
    - Also supports PostGIS extensions
    - Table, row, and column statistics via optional SQL profiling
    """

    config: RisingWaveConfig

    def __init__(self, config: RisingWaveConfig, ctx: PipelineContext):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "risingwave"

    @classmethod
    def create(cls, config_dict, ctx):
        config = RisingWaveConfig.model_validate(config_dict)
        return cls(config, ctx)
