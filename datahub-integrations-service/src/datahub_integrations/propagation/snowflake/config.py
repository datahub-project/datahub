from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeConnectionConfig,
)


class SnowflakeConnectionConfigPermissive(
    SnowflakeConnectionConfig, PermissiveConfigModel
):
    pass
