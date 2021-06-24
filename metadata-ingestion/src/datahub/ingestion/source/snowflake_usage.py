import dataclasses
import json
import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional

import pydantic
import pydantic.dataclasses
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
    query_history.query_text,
    query_history.query_type,
    access_history.base_objects_accessed,
    -- access_history.direct_objects_accessed, -- might be useful in the future
    -- query_history.execution_status, -- not really necessary, but should equal "SUCCESS"
    -- query_history.warehouse_name,
    access_history.user_name,
    users.first_name,
    users.last_name,
    users.display_name,
    users.email,
    query_history.role_name
FROM
    snowflake.account_usage.access_history access_history
LEFT JOIN
    snowflake.account_usage.query_history query_history
    ON access_history.query_id = query_history.query_id
LEFT JOIN
    snowflake.account_usage.users users
    ON access_history.user_name = users.name
WHERE   ARRAY_SIZE(base_objects_accessed) > 0
    AND query_start_time >= to_timestamp_ltz({start_time_millis}, 3)
    AND query_start_time < to_timestamp_ltz({end_time_millis}, 3)
ORDER BY query_start_time DESC
;
""".strip()


@pydantic.dataclasses.dataclass
class SnowflakeColumnReference:
    columnId: int
    columnName: str


@pydantic.dataclasses.dataclass
class SnowflakeObjectAccessEntry:
    columns: List[SnowflakeColumnReference]
    objectDomain: str
    objectId: int
    objectName: str


@pydantic.dataclasses.dataclass
class SnowflakeJoinedAccessEvent:
    query_start_time: datetime
    query_text: str
    query_type: str
    base_objects_accessed: List[SnowflakeObjectAccessEntry]

    user_name: str
    first_name: Optional[str]
    last_name: Optional[str]
    display_name: Optional[str]
    email: str
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


@dataclasses.dataclass
class SnowflakeUsageSource(Source):
    config: SnowflakeUsageConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[UsageStatsWorkUnit]:
        access_events = self._get_snowflake_history()

        access_events = list(access_events)
        breakpoint()
        yield from []

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
            # Make some minor type conversions.
            event_dict = row._asdict()
            event_dict["base_objects_accessed"] = json.loads(
                event_dict["base_objects_accessed"]
            )
            event_dict["query_start_time"] = (
                event_dict["query_start_time"]
            ).astimezone(tz=timezone.utc)

            event = SnowflakeJoinedAccessEvent(**event_dict)
            yield event

    def get_report(self):
        return self.report

    def close(self):
        pass
