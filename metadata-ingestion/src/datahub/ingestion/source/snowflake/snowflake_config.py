from typing import Dict, Optional

from pydantic import Field, root_validator

from datahub.ingestion.source.sql.sql_common import SQLAlchemyStatefulIngestionConfig
from datahub.ingestion.source_config.sql.snowflake import (
    SnowflakeConfig,
    SnowflakeProvisionRoleConfig,
)


class SnowflakeV2Config(SnowflakeConfig):
    _convert_urns_to_lowercase: bool = Field(
        default=True,
        exclude=True,
        description="Not supported",
    )

    check_role_grants: bool = Field(
        default=False,
        exclude=True,
        description="Not supported",
    )

    provision_role: Optional[SnowflakeProvisionRoleConfig] = Field(
        default=None, exclude=True, description="Not supported"
    )

    stateful_ingestion: Optional[SQLAlchemyStatefulIngestionConfig] = Field(
        default=None, exclude=True, description="Not supported"
    )

    @root_validator(pre=False)
    def validate_unsupported_configs(cls, values: Dict) -> Dict:
        value = values.get("stateful_ingestion")
        if value is not None and value.enabled:
            raise ValueError(
                "Stateful ingestion is currently not supported. Set `stateful_ingestion.enabled` to False"
            )

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
        return values
