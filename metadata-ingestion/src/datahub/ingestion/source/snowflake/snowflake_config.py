import logging
from typing import Dict, Optional, cast

from pydantic import Field, root_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.sql.sql_common import SQLAlchemyStatefulIngestionConfig
from datahub.ingestion.source_config.sql.snowflake import (
    SnowflakeConfig,
    SnowflakeProvisionRoleConfig,
)
from datahub.ingestion.source_config.usage.snowflake_usage import (
    SnowflakeStatefulIngestionConfig,
    SnowflakeUsageConfig,
)


class SnowflakeV2StatefulIngestionConfig(
    SQLAlchemyStatefulIngestionConfig, SnowflakeStatefulIngestionConfig
):
    pass


logger = logging.Logger(__name__)


class SnowflakeV2Config(SnowflakeConfig, SnowflakeUsageConfig):
    convert_urns_to_lowercase: bool = Field(
        default=True,
    )

    include_usage_stats: bool = Field(
        default=True,
        description="If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
    )

    check_role_grants: bool = Field(
        default=False,
        description="Not supported",
    )

    provision_role: Optional[SnowflakeProvisionRoleConfig] = Field(
        default=None, description="Not supported"
    )

    @root_validator(pre=False)
    def validate_unsupported_configs(cls, values: Dict) -> Dict:

        value = values.get("provision_role")
        if value is not None and value.enabled:
            raise ValueError(
                "Provision role is currently not supported. Set `provision_role.enabled` to False."
            )

        value = values.get("profiling")
        if value is not None and value.enabled and not value.profile_table_level_only:
            raise ValueError(
                "Only table level profiling is supported. Set `profiling.profile_table_level_only` to True.",
            )

        value = values.get("check_role_grants")
        if value is not None and value:
            raise ValueError(
                "Check role grants is not supported. Set `check_role_grants` to False.",
            )

        value = values.get("include_read_operational_stats")
        if value is not None and value:
            raise ValueError(
                "include_read_operational_stats is not supported. Set `include_read_operational_stats` to False.",
            )

        # Always exclude reporting metadata for INFORMATION_SCHEMA schema
        schema_pattern = values.get("schema_pattern")
        if schema_pattern is not None and schema_pattern:
            logger.debug("Adding deny for INFORMATION_SCHEMA to schema_pattern.")
            cast(AllowDenyPattern, schema_pattern).deny.append(r"^INFORMATION_SCHEMA$")

        return values
