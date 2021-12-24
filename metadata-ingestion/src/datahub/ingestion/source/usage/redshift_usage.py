import collections
import dataclasses
import logging
from datetime import datetime
from typing import Dict, Iterable, List

from dateutil import parser
from pydantic import Field
from pydantic.main import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import get_time_bucket
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.redshift import RedshiftConfig
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
)

logger = logging.getLogger(__name__)

redshift_datetime_format = "%Y-%m-%d %H:%M:%S"

# add this join to the sql comment for more metrics on completed queries
# LEFT JOIN svl_query_metrics_summary sqms ON ss.query = sqms.query
# Reference: https://docs.aws.amazon.com/redshift/latest/dg/r_SVL_QUERY_METRICS_SUMMARY.html

# this sql query joins stl_scan over table info,
# querytext, and user info to get usage stats
# using non-LEFT joins here to limit the results to
# queries run by the user on user-defined tables.
redshift_usage_sql_comment = """
SELECT DISTINCT ss.userid,
       ss.query,
       sui.usename,
       ss.tbl,
       sq.querytxt,
       sti.database,
       sti.schema,
       sti.table,
       sq.starttime,
       sq.endtime,
       sq.aborted
FROM stl_scan ss
  JOIN svv_table_info sti ON ss.tbl = sti.table_id
  JOIN stl_query sq ON ss.query = sq.query
  JOIN svl_user_info sui ON sq.userid = sui.usesysid
WHERE ss.starttime >= '{start_time}'
AND ss.starttime < '{end_time}'
AND sq.aborted = 0
ORDER BY ss.endtime DESC;
""".strip()


RedshiftTableRef = str
AggregatedDataset = GenericAggregatedDataset[RedshiftTableRef]


class RedshiftJoinedAccessEvent(BaseModel):
    userid: int
    usename: str = None  # type:ignore
    query: int
    tbl: int
    text: str = Field(None, alias="querytxt")
    database: str = None  # type:ignore
    schema_: str = Field(None, alias="schema")
    table: str = None  # type:ignore
    starttime: datetime
    endtime: datetime


class RedshiftUsageConfig(RedshiftConfig, BaseUsageConfig):
    env: str = builder.DEFAULT_ENV
    email_domain: str
    options: dict = {}

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url()


@dataclasses.dataclass
class RedshiftUsageSource(Source):
    config: RedshiftUsageConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Gets Redshift usage stats as work units"""
        access_events = self._get_redshift_history()
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
        return redshift_usage_sql_comment.format(
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url = {url}")
        engine = create_engine(url, **self.config.options)
        return engine

    def _get_redshift_history(self):
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

            if event_dict.get("starttime", None):
                event_dict["starttime"] = event_dict.get("starttime").__str__()
            if event_dict.get("endtime", None):
                event_dict["endtime"] = event_dict.get("endtime").__str__()

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
            return isodate.strftime(redshift_datetime_format)

    def _get_joined_access_event(self, events):
        joined_access_events = []
        for event_dict in events:

            event_dict["starttime"] = self._convert_str_to_datetime(
                event_dict.get("starttime")
            )
            event_dict["endtime"] = self._convert_str_to_datetime(
                event_dict.get("endtime")
            )

            if not (
                event_dict.get("database", None)
                and event_dict.get("schema", None)
                and event_dict.get("table", None)
            ):
                logging.info("An access event parameter(s) is missing. Skipping ....")
                continue

            if not event_dict.get("usename") or event_dict["usename"] == "":
                logging.info("The username parameter is missing. Skipping ....")
                continue

            joined_access_events.append(RedshiftJoinedAccessEvent(**event_dict))
        return joined_access_events

    def _aggregate_access_events(
        self, events: List[RedshiftJoinedAccessEvent]
    ) -> Dict[datetime, Dict[RedshiftTableRef, AggregatedDataset]]:
        datasets: Dict[
            datetime, Dict[RedshiftTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(event.starttime, self.config.bucket_duration)

            resource = f"{event.database}.{event.schema_}.{event.table}"

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
                event.text,
                [],  # TODO: not currently supported by redshift; find column level changes
            )
        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn(
                "redshift", resource.lower(), self.config.env
            ),
            self.config.top_n_queries,
        )

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
