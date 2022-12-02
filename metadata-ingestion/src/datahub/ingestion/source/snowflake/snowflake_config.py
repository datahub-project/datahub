import logging
from typing import Dict, Optional, cast

from pydantic import Field, SecretStr, root_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.glossary.classifier import ClassificationConfig
from datahub.ingestion.source_config.sql.snowflake import (
    BaseSnowflakeConfig,
    SnowflakeConfig,
    SnowflakeProvisionRoleConfig,
)
from datahub.ingestion.source_config.usage.snowflake_usage import SnowflakeUsageConfig

logger = logging.Logger(__name__)


class SnowflakeV2Config(SnowflakeConfig, SnowflakeUsageConfig):
    convert_urns_to_lowercase: bool = Field(
        default=True,
    )

    include_usage_stats: bool = Field(
        default=True,
        description="If enabled, populates the snowflake usage statistics. Requires appropriate grants given to the role.",
    )

    include_technical_schema: bool = Field(
        default=True,
        description="If enabled, populates the snowflake technical schema and descriptions.",
    )

    check_role_grants: bool = Field(
        default=False,
        description="Not supported",
    )

    provision_role: Optional[SnowflakeProvisionRoleConfig] = Field(
        default=None, description="Not supported"
    )

    classification: Optional[ClassificationConfig] = Field(
        default=None,
        description="For details, refer [Classification](../../../../metadata-ingestion/docs/dev_guides/classification.md).",
    )

    include_external_url: bool = Field(
        default=True,
        description="Whether to populate Snowsight url for Snowflake Objects",
    )

    @root_validator(pre=False)
    def validate_unsupported_configs(cls, values: Dict) -> Dict:

        value = values.get("provision_role")
        if value is not None and value.enabled:
            raise ValueError(
                "Provision role is currently not supported. Set `provision_role.enabled` to False."
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

        include_technical_schema = values.get("include_technical_schema")
        include_profiles = (
            values.get("profiling") is not None and values["profiling"].enabled
        )
        delete_detection_enabled = (
            values.get("stateful_ingestion") is not None
            and values["stateful_ingestion"].enabled
            and values["stateful_ingestion"].remove_stale_metadata
        )
        include_table_lineage = values.get("include_table_lineage")

        # TODO: Allow lineage extraction and profiling irrespective of basic schema extraction,
        # as it seems possible with some refractor
        if not include_technical_schema and any(
            [include_profiles, delete_detection_enabled, include_table_lineage]
        ):
            raise ValueError(
                "Can not perform Deletion Detection, Lineage Extraction, Profiling without extracting snowflake technical schema.  Set `include_technical_schema` to True or disable Deletion Detection, Lineage Extraction, Profiling."
            )

        return values

    def get_sql_alchemy_url(
        self,
        database: str = None,
        username: Optional[str] = None,
        password: Optional[SecretStr] = None,
        role: Optional[str] = None,
    ) -> str:
        return BaseSnowflakeConfig.get_sql_alchemy_url(
            self, database=database, username=username, password=password, role=role
        )
