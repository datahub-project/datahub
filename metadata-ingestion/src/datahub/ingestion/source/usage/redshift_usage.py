import collections
import dataclasses
import logging
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Union

from pydantic import Field
from pydantic.main import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.result import ResultProxy, RowProxy

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import get_time_bucket
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.redshift import RedshiftConfig
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    OperationClass,
    OperationTypeClass,
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
SELECT DISTINCT ss.userid as userid,
       ss.query as query,
       sui.usename as username,
       ss.tbl as tbl,
       sq.querytxt as querytxt,
       sti.database as database,
       sti.schema as schema,
       sti.table as table,
       sq.starttime as starttime,
       sq.endtime as endtime
FROM stl_scan ss
  JOIN svv_table_info sti ON ss.tbl = sti.table_id
  JOIN stl_query sq ON ss.query = sq.query
  JOIN svl_user_info sui ON sq.userid = sui.usesysid
WHERE ss.starttime >= '{start_time}'
AND ss.starttime < '{end_time}'
AND sti.database = '{database}'
AND sq.aborted = 0
ORDER BY ss.endtime DESC;
""".strip()


RedshiftTableRef = str
AggregatedDataset = GenericAggregatedDataset[RedshiftTableRef]
AggregatedAccessEvents = Dict[datetime, Dict[RedshiftTableRef, AggregatedDataset]]


class RedshiftAccessEvent(BaseModel):
    userid: int
    username: str
    query: int
    tbl: int
    text: str = Field(None, alias="querytxt")
    database: str
    schema_: str = Field(alias="schema")
    table: str
    starttime: datetime
    endtime: datetime


class RedshiftUsageConfig(RedshiftConfig, BaseUsageConfig):
    env: str = builder.DEFAULT_ENV
    email_domain: str
    options: Dict = {}

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url()


@dataclasses.dataclass
class RedshiftUsageSourceReport(SourceReport):
    filtered: Set[str] = dataclasses.field(default_factory=set)

    def report_dropped(self, key: str) -> None:
        self.filtered.add(key)


class RedshiftUsageSource(Source):
    def __init__(self, config: RedshiftUsageConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config: RedshiftUsageConfig = config
        self.report: RedshiftUsageSourceReport = RedshiftUsageSourceReport()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "RedshiftUsageSource":
        config = RedshiftUsageConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Gets Redshift usage stats as work units"""
        engine: Engine = self._make_sql_engine()
        if self.config.include_operational_stats:
            # Generate operation aspect workunits
            yield from self._gen_operation_aspect_workunits(engine)

        # Generate aggregate events
        access_events_iterable: Iterable[
            RedshiftAccessEvent
        ] = self._gen_access_events_from_history_query(
            self._make_usage_query(redshift_usage_sql_comment), engine
        )

        aggregated_info: AggregatedAccessEvents = self._aggregate_access_events(
            access_events_iterable
        )
        # Generate usage workunits from aggregated events.
        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu: MetadataWorkUnit = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

    def _gen_operation_aspect_workunits_by_type(
        self, operation_type: Union[str, "OperationTypeClass"], engine: Engine
    ) -> Iterable[MetadataWorkUnit]:
        # Determine the table to query
        op_to_table_map: Dict[str, str] = {
            OperationTypeClass.INSERT: "stl_insert",
            OperationTypeClass.DELETE: "stl_delete",
        }
        table_name: Optional[str] = op_to_table_map.get(str(operation_type))
        assert table_name is not None
        # Get the access events for the table corresponding to the operation.
        access_events_iterable: Iterable[
            RedshiftAccessEvent
        ] = self._gen_access_events_from_history_query(
            self._make_redshift_operation_aspect_query(table_name), engine
        )
        # Generate operation aspect work units from the access events
        yield from self._gen_operation_aspect_workunits_by_type_from_access_events(
            access_events_iterable, operation_type
        )

    def _gen_operation_aspect_workunits(
        self, engine: Engine
    ) -> Iterable[MetadataWorkUnit]:
        yield from self._gen_operation_aspect_workunits_by_type(
            OperationTypeClass.INSERT, engine
        )
        yield from self._gen_operation_aspect_workunits_by_type(
            OperationTypeClass.DELETE, engine
        )

    def _make_usage_query(self, query: str) -> str:
        return query.format(
            start_time=self.config.start_time.strftime(redshift_datetime_format),
            end_time=self.config.end_time.strftime(redshift_datetime_format),
            database=self.config.database,
        )

    def _make_redshift_operation_aspect_query(self, table_name: str) -> str:
        return f"""
        SELECT DISTINCT ss.userid as userid,
               ss.query as query,
               ss.rows as rows,
               sui.usename as username,
               ss.tbl as tbl,
               sq.querytxt as querytxt,
               sti.database as database,
               sti.schema as schema,
               sti.table as table,
               sq.starttime as starttime,
               sq.endtime as endtime
        FROM {table_name} ss
        JOIN svv_table_info sti ON ss.tbl = sti.table_id
        JOIN stl_query sq ON ss.query = sq.query
        JOIN svl_user_info sui ON sq.userid = sui.usesysid
        WHERE ss.starttime >= '{self.config.start_time.strftime(redshift_datetime_format)}'
        AND ss.starttime < '{self.config.end_time.strftime(redshift_datetime_format)}'
        AND ss.rows > 0
        AND sq.aborted = 0
        ORDER BY ss.endtime DESC;
        """.strip()

    def _make_sql_engine(self) -> Engine:
        url: str = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url = {url}")
        return create_engine(url, **self.config.options)

    def _should_process_row(self, row: RowProxy) -> bool:
        # Check for mandatory proerties being present first.
        missing_props: List[str] = [
            prop
            for prop in ["database", "schema", "table", "username"]
            if not row[prop]
        ]
        if missing_props:
            logging.info(
                f"Access event parameter(s):[{','.join(missing_props)}] missing. Skipping ...."
            )
            return False
        # Check schema/table allow/deny patterns
        full_table_name: str = f"{row['database']}.{row['schema']}.{row['table']}"
        if not self.config.schema_pattern.allowed(row["schema"]):
            logger.debug(f"Filtering out {full_table_name} due to schema_pattern.")
            self.report.report_dropped(full_table_name)
            return False
        if not self.config.table_pattern.allowed(row["table"]):
            logger.debug(f"Filtering out {full_table_name} due to table_pattern.")
            self.report.report_dropped(full_table_name)
            return False
        # Passed all checks.
        return True

    def _gen_access_events_from_history_query(
        self, query: str, engine: Engine
    ) -> Iterable[RedshiftAccessEvent]:
        results: ResultProxy = engine.execute(query)
        for row in results:  # type: RowProxy
            if not self._should_process_row(row):
                continue
            access_event = RedshiftAccessEvent(**dict(row.items()))
            # Replace database name with the alias name if one is provided in the config.
            if self.config.database_alias:
                access_event.database = self.config.database_alias
            yield access_event

    def _gen_operation_aspect_workunits_by_type_from_access_events(
        self,
        events_iterable: Iterable[RedshiftAccessEvent],
        operation_type: Union[str, "OperationTypeClass"],
    ) -> Iterable[MetadataWorkUnit]:
        for event in events_iterable:
            if not (
                event.database
                and event.username
                and event.schema_
                and event.table
                and event.endtime
            ):
                continue

            resource: str = f"{event.database}.{event.schema_}.{event.table}"
            last_updated_timestamp: int = int(event.endtime.timestamp() * 1000)
            user_email: str = event.username

            operation_aspect = OperationClass(
                timestampMillis=last_updated_timestamp,
                lastUpdatedTimestamp=last_updated_timestamp,
                actor=builder.make_user_urn(user_email.split("@")[0]),
                operationType=operation_type,
            )
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                aspectName="operation",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=builder.make_dataset_urn_with_platform_instance(
                    "redshift",
                    resource.lower(),
                    self.config.platform_instance,
                    self.config.env,
                ),
                aspect=operation_aspect,
            )
            wu = MetadataWorkUnit(
                id=f"operation-aspect-{event.table}-{event.endtime.isoformat()}",
                mcp=mcp,
            )
            self.report.report_workunit(wu)
            yield wu

    def _aggregate_access_events(
        self, events_iterable: Iterable[RedshiftAccessEvent]
    ) -> AggregatedAccessEvents:
        datasets: AggregatedAccessEvents = collections.defaultdict(dict)
        for event in events_iterable:
            floored_ts: datetime = get_time_bucket(
                event.starttime, self.config.bucket_duration
            )
            resource: str = f"{event.database}.{event.schema_}.{event.table}"
            # Get a reference to the bucket value(or initialize not yet in dict) and update it.
            agg_bucket: AggregatedDataset = datasets[floored_ts].setdefault(
                resource,
                AggregatedDataset(
                    bucket_start_time=floored_ts,
                    resource=resource,
                    user_email_pattern=self.config.user_email_pattern,
                ),
            )
            # current limitation in user stats UI, we need to provide email to show users
            user_email: str = f"{event.username if event.username else 'unknown'}"
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
            lambda resource: builder.make_dataset_urn_with_platform_instance(
                "redshift",
                resource.lower(),
                self.config.platform_instance,
                self.config.env,
            ),
            self.config.top_n_queries,
            self.config.format_sql_queries,
        )

    def get_report(self) -> RedshiftUsageSourceReport:
        return self.report

    def close(self) -> None:
        pass
