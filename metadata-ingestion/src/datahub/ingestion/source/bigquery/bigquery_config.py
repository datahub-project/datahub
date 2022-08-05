from datetime import timedelta
from typing import Dict, Optional

import pydantic
from pydantic import root_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.ingestion.source_config.sql.bigquery import BigQueryConfig


class BigQueryUsageConfig(BaseUsageConfig):
    query_log_delay: Optional[pydantic.PositiveInt] = pydantic.Field(
        default=None,
        description="To account for the possibility that the query event arrives after the read event in the audit logs, we wait for at least query_log_delay additional events to be processed before attempting to resolve BigQuery job information from the logs. If query_log_delay is None, it gets treated as an unlimited delay, which prioritizes correctness at the expense of memory usage.",
    )

    max_query_duration: timedelta = pydantic.Field(
        default=timedelta(minutes=15),
        description="Correction to pad start_time and end_time with. For handling the case where the read happens within our time range but the query completion event is delayed and happens after the configured end time.",
    )


class BigQueryV2Config(BigQueryConfig):
    project_id_pattern: AllowDenyPattern = AllowDenyPattern()
    usage: BigQueryUsageConfig = pydantic.Field(
        default=BigQueryUsageConfig(), description="Usage related configs"
    )
    include_usage_statistics: bool = pydantic.Field(
        default=True,
        description="Generate usage statistic",
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
        return values

    def get_allow_pattern_string(self) -> str:
        return "|".join(self.table_pattern.allow) if self.table_pattern else ""

    def get_deny_pattern_string(self) -> str:
        return "|".join(self.table_pattern.deny) if self.table_pattern else ""
