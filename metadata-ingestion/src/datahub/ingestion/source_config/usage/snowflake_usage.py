import logging
from typing import Dict, Optional

import pydantic

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    StatefulRedundantRunSkipConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.sql.snowflake import BaseSnowflakeConfig

logger = logging.getLogger(__name__)


class SnowflakeStatefulIngestionConfig(StatefulRedundantRunSkipConfig):
    """
    Specialization of basic StatefulIngestionConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the SnowflakeUsageConfig.
    """

    pass


class SnowflakeUsageConfig(
    BaseSnowflakeConfig, BaseUsageConfig, StatefulIngestionConfigBase
):
    options: dict = pydantic.Field(
        default_factory=dict,
        description="Any options specified here will be passed to SQLAlchemy's create_engine as kwargs. See https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine for details.",
    )

    database_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern(
            deny=[r"^UTIL_DB$", r"^SNOWFLAKE$", r"^SNOWFLAKE_SAMPLE_DATA$"]
        ),
        description="List of regex patterns for databases to include/exclude in usage ingestion.",
    )
    email_domain: Optional[str] = pydantic.Field(
        description="Email domain of your organisation so users can be displayed on UI appropriately."
    )
    schema_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for schemas to include/exclude in usage ingestion.",
    )
    table_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for tables to include in ingestion.",
    )
    view_pattern: AllowDenyPattern = pydantic.Field(
        default=AllowDenyPattern.allow_all(),
        description="List of regex patterns for views to include in ingestion.",
    )
    apply_view_usage_to_tables: bool = pydantic.Field(
        default=False,
        description="Allow/deny patterns for views in snowflake dataset names.",
    )
    stateful_ingestion: Optional[SnowflakeStatefulIngestionConfig] = pydantic.Field(
        default=None, description="Stateful ingestion related configs"
    )

    def get_options(self) -> dict:
        options_connect_args: Dict = super().get_sql_alchemy_connect_args()
        options_connect_args.update(self.options.get("connect_args", {}))
        self.options["connect_args"] = options_connect_args
        return self.options

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(
            database="snowflake",
            username=self.username,
            password=self.password,
            role=self.role,
        )
