import collections
import dataclasses
import logging
from datetime import datetime
from typing import Dict, Iterable, List

from dateutil import parser
from pydantic.fields import Field
from pydantic.main import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.configuration.time_window_config import get_time_bucket
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.clickhouse import ClickHouseConfig
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
)

logger = logging.getLogger(__name__)

clickhouse_datetime_format = "%Y-%m-%d %H:%M:%S"

clickhouse_usage_sql_comment = """\
SELECT user                                                                       AS usename
     , query
     , substring(full_table_name, 1, position(full_table_name, '.') - 1)          AS schema_
     , substring(full_table_name, position(full_table_name, '.') + 1)             AS table
     , arrayMap(x -> substr(x, length(full_table_name) + 2),
                arrayFilter(x -> startsWith(x, full_table_name || '.'), columns)) AS columns
     , query_start_time                                                           AS starttime
     , event_time                                                                 AS endtime
  FROM {query_log_table}
 ARRAY JOIN tables AS full_table_name
 WHERE is_initial_query
   AND type = 'QueryFinish'
   AND query_kind = 'Select'
   AND full_table_name NOT LIKE 'system.%%'
   AND full_table_name NOT LIKE '_table_function.%%'
   AND table NOT LIKE '`.inner%%'
   AND event_time >= '{start_time}'
   AND event_time < '{end_time}'
 ORDER BY event_time DESC"""

ClickHouseTableRef = str
AggregatedDataset = GenericAggregatedDataset[ClickHouseTableRef]


class ClickHouseJoinedAccessEvent(BaseModel):
    usename: str = None  # type:ignore
    query: str = None  # type: ignore
    schema_: str = None  # type:ignore
    table: str = None  # type:ignore
    columns: List[str]
    starttime: datetime
    endtime: datetime


class ClickHouseUsageConfig(
    ClickHouseConfig, BaseUsageConfig, EnvBasedSourceConfigBase
):
    email_domain: str = Field(description="")
    options: dict = Field(default={}, description="")
    query_log_table: str = Field(default="system.query_log", exclude=True)

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url()


@platform_name("ClickHouse")
@config_class(ClickHouseUsageConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@dataclasses.dataclass
class ClickHouseUsageSource(Source):
    """
    This plugin has the below functionalities -
    1. For a specific dataset this plugin ingests the following statistics -
       1. top n queries.
       2. top users.
       3. usage of each column in the dataset.
    2. Aggregation of these statistics into buckets, by day or hour granularity.

    Usage information is computed by querying the system.query_log table. In case you have a cluster or need to apply additional transformation/filters you can create a view and put to the `query_log_table` setting.

    :::note

    This source only does usage statistics. To get the tables, views, and schemas in your ClickHouse warehouse, ingest using the `clickhouse` source described above.

    :::

    """

    config: ClickHouseUsageConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = ClickHouseUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Gets ClickHouse usage stats as work units"""
        access_events = self._get_clickhouse_history()
        # If the query results is empty, we don't want to proceed
        if not access_events:
            return []

        joined_access_event = self._get_joined_access_event(access_events)
        aggregated_info = self._aggregate_access_events(joined_access_event)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

    def _make_usage_query(self) -> str:
        return clickhouse_usage_sql_comment.format(
            query_log_table=self.config.query_log_table,
            start_time=self.config.start_time.strftime(clickhouse_datetime_format),
            end_time=self.config.end_time.strftime(clickhouse_datetime_format),
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url = {url}")
        engine = create_engine(url, **self.config.options)
        return engine

    def _get_clickhouse_history(self):
        query = self._make_usage_query()
        engine = self._make_sql_engine()
        results = engine.execute(query)
        events = []
        for row in results:
            # minor type conversion
            if hasattr(row, "_asdict"):
                event_dict = row._asdict()
            else:
                event_dict = dict(row)

            # stripping extra spaces caused by above _asdict() conversion
            for k, v in event_dict.items():
                if isinstance(v, str):
                    event_dict[k] = v.strip()

            if not self.config.schema_pattern.allowed(
                event_dict.get("schema_")
            ) or not self.config.table_pattern.allowed(event_dict.get("table")):
                continue

            if event_dict.get("starttime", None):
                event_dict["starttime"] = event_dict.get("starttime").__str__()
            if event_dict.get("endtime", None):
                event_dict["endtime"] = event_dict.get("endtime").__str__()
            # when the http protocol is used, the columns field is returned as a string
            if isinstance(event_dict.get("columns"), str):
                event_dict["columns"] = (
                    event_dict.get("columns").replace("'", "").strip("][").split(",")
                )

            logger.debug(f"event_dict: {event_dict}")
            events.append(event_dict)

        if events:
            return events

        # SQL results can be empty. If results is empty, the SQL connection closes.
        # Then, we don't want to proceed ingestion.
        logging.info("SQL Result is empty")
        return None

    def _convert_str_to_datetime(self, v):
        if isinstance(v, str):
            isodate = parser.parse(v)  # compatible with Python 3.6+
            return isodate.strftime(clickhouse_datetime_format)

    def _get_joined_access_event(self, events):
        joined_access_events = []
        for event_dict in events:

            event_dict["starttime"] = self._convert_str_to_datetime(
                event_dict.get("starttime")
            )
            event_dict["endtime"] = self._convert_str_to_datetime(
                event_dict.get("endtime")
            )

            if not (event_dict.get("schema_", None) and event_dict.get("table", None)):
                logging.info("An access event parameter(s) is missing. Skipping ....")
                continue

            if not event_dict.get("usename") or event_dict["usename"] == "":
                logging.info("The username parameter is missing. Skipping ....")
                continue

            joined_access_events.append(ClickHouseJoinedAccessEvent(**event_dict))

        return joined_access_events

    def _aggregate_access_events(
        self, events: List[ClickHouseJoinedAccessEvent]
    ) -> Dict[datetime, Dict[ClickHouseTableRef, AggregatedDataset]]:
        datasets: Dict[
            datetime, Dict[ClickHouseTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(event.starttime, self.config.bucket_duration)

            resource = (
                f'{self.config.platform_instance+"." if self.config.platform_instance else ""}'
                f"{event.schema_}.{event.table}"
            )

            agg_bucket = datasets[floored_ts].setdefault(
                resource,
                AggregatedDataset(bucket_start_time=floored_ts, resource=resource),
            )

            # current limitation in user stats UI, we need to provide email to show users
            user_email = f"{event.usename if event.usename else 'unknown'}"
            if "@" not in user_email:
                user_email += f"@{self.config.email_domain}"
            logger.info(f"user_email: {user_email}")
            agg_bucket.add_read_entry(
                user_email,
                event.query,
                event.columns,
            )
        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn(
                "clickhouse", resource, self.config.env
            ),
            self.config.top_n_queries,
            self.config.format_sql_queries,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
