import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

import pydantic
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import UsageStatsWorkUnit
from datahub.ingestion.source.snowflake import SnowflakeConfig
from datahub.ingestion.source.usage_common import BaseUsageConfig

logger = logging.getLogger(__name__)

SNOWFLAKE_USAGE_SQL_TEMPLATE = """
SELECT
    -- access_history.query_id, -- only for debugging purposes
    access_history.query_start_time,
    access_history.user_name,
    -- access_history.direct_objects_accessed, -- might be useful in the future
    access_history.base_objects_accessed,
    query_history.query_text,
    query_history.query_type,
    -- query_history.execution_status, -- not really necessary, but should equal "SUCCESS"
    -- query_history.warehouse_name,
    query_history.role_name
FROM
    snowflake.account_usage.access_history access_history
LEFT JOIN
    snowflake.account_usage.query_history query_history
    ON access_history.query_id = query_history.query_id
WHERE   query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
ORDER BY query_start_time DESC
;
""".strip()


@dataclass
class SnowflakeJoinedAccessEvent:
    query_start_time: datetime
    user_name: str
    base_objects_accessed: list
    query_text: str
    query_type: str
    role_name: str


class SnowflakeUsageConfig(SnowflakeConfig, BaseUsageConfig):
    @pydantic.validator("role", always=True)
    def role_accountadmin(cls, v):
        if not v or v.lower() != "accountadmin":
            # This isn't an error, since the privileges can be delegated to other
            # roles as well: https://docs.snowflake.com/en/sql-reference/account-usage.html#enabling-account-usage-for-other-roles
            logger.info(
                'snowflake usage tables are only accessible by role "accountadmin" by default; you set %s',
                v,
            )
        return v


@dataclass
class SnowflakeUsageSource(Source):
    config: SnowflakeUsageConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[UsageStatsWorkUnit]:
        access_events = self._get_snowflake_history()
        breakpoint()

    def _make_usage_query(self) -> str:
        return SNOWFLAKE_USAGE_SQL_TEMPLATE.format(
            start_time_millis=int(self.config.start_time.timestamp() * 1000),
            end_time_millis=int(self.config.end_time.timestamp() * 1000),
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        return engine

    def _get_snowflake_history(self) -> Iterable[SnowflakeJoinedAccessEvent]:
        query = self._make_usage_query()
        engine = self._make_sql_engine()

        results = engine.execute(query)
        for row in results:
            event = SnowflakeJoinedAccessEvent()
            yield event

    def get_report(self):
        return self.report

    def close(self):
        pass
