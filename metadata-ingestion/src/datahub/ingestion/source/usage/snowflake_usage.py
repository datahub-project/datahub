import collections
import dataclasses
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import pydantic
import pydantic.dataclasses
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.snowflake import BaseSnowflakeConfig
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
    get_time_bucket,
)

logger = logging.getLogger(__name__)

SnowflakeTableRef = str
AggregatedDataset = GenericAggregatedDataset[SnowflakeTableRef]

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


class PermissiveModel(BaseModel):
    class Config:
        extra = "allow"


class SnowflakeObjectAccessEntry(PermissiveModel):
    columns: List[SnowflakeColumnReference]
    objectDomain: str
    objectId: int
    objectName: str


class SnowflakeJoinedAccessEvent(PermissiveModel):
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


class SnowflakeUsageConfig(BaseSnowflakeConfig, BaseUsageConfig):
    env: str = builder.DEFAULT_ENV
    options: dict = {}

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

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(database="snowflake")


@dataclasses.dataclass
class SnowflakeUsageSource(Source):
    config: SnowflakeUsageConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        access_events = self._get_snowflake_history()
        aggregated_info = self._aggregate_access_events(access_events)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

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
            if hasattr(row, "_asdict"):
                # Compat with SQLAlchemy 1.3 and 1.4
                # See https://docs.sqlalchemy.org/en/14/changelog/migration_14.html#rowproxy-is-no-longer-a-proxy-is-now-called-row-and-behaves-like-an-enhanced-named-tuple.
                event_dict = row._asdict()
            else:
                event_dict = dict(row)

            # no use processing events that don't have a query text
            if event_dict["query_text"] is None:
                continue

            event_dict["base_objects_accessed"] = json.loads(
                event_dict["base_objects_accessed"]
            )
            event_dict["query_start_time"] = (
                event_dict["query_start_time"]
            ).astimezone(tz=timezone.utc)

            try:  # big hammer try block to ensure we don't fail on parsing events
                event = SnowflakeJoinedAccessEvent(**event_dict)
                yield event
            except Exception:
                self.report.report_warning(
                    "usage", f"Failed to parse usage line {event_dict}"
                )

    def _aggregate_access_events(
        self, events: Iterable[SnowflakeJoinedAccessEvent]
    ) -> Dict[datetime, Dict[SnowflakeTableRef, AggregatedDataset]]:
        datasets: Dict[
            datetime, Dict[SnowflakeTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(
                event.query_start_time, self.config.bucket_duration
            )

            for object in event.base_objects_accessed:
                resource = object.objectName

                agg_bucket = datasets[floored_ts].setdefault(
                    resource,
                    AggregatedDataset(bucket_start_time=floored_ts, resource=resource),
                )
                agg_bucket.add_read_entry(
                    event.email,
                    event.query_text,
                    [colRef.columnName.lower() for colRef in object.columns],
                )

        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn(
                "snowflake", resource.lower(), self.config.env
            ),
            self.config.top_n_queries,
        )

    def get_report(self):
        return self.report

    def close(self):
        pass
