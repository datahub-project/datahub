import collections
import dataclasses
import json
import logging
from datetime import datetime
from email.utils import parseaddr
from typing import Dict, Iterable, List, Optional

from dateutil import parser
from pydantic.fields import Field
from pydantic.main import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.configuration.time_window_config import get_time_bucket
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.trino import TrinoConfig
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
)

logger = logging.getLogger(__name__)

trino_datetime_format = "%Y-%m-%d %H:%M:%S.%f %Z"

# Qeurying Starburst completed queries table
# https://docs.starburst.io/latest/security/event-logger.html#completed-queries
trino_usage_sql_comment = """
SELECT DISTINCT usr,
        query,
        "catalog",
        "schema",
        query_type,
        accessed_metadata,
        create_time,
        end_time
FROM {audit_catalog}.{audit_schema}.completed_queries
WHERE 1 = 1
AND query_type  = 'SELECT'
AND create_time >= timestamp '{start_time}'
AND end_time < timestamp '{end_time}'
AND query_state  = 'FINISHED'
ORDER BY end_time desc
""".strip()

TrinoTableRef = str
AggregatedDataset = GenericAggregatedDataset[TrinoTableRef]


class TrinoConnectorInfo(BaseModel):
    partitionIds: List[str]
    truncated: Optional[bool]


class TrinoAccessedMetadata(BaseModel):
    catalog_name: Optional[str] = Field(None, alias="catalogName")
    schema_name: Optional[str] = Field(None, alias="schema")
    table: Optional[str] = None
    columns: List[str]
    connector_info: Optional[TrinoConnectorInfo] = Field(None, alias="connectorInfo")


class TrinoJoinedAccessEvent(BaseModel):
    usr: Optional[str] = None
    query: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    query_type: Optional[str] = None
    table: Optional[str] = None
    accessed_metadata: List[TrinoAccessedMetadata]
    starttime: datetime = Field(alias="create_time")
    endtime: Optional[datetime] = Field(alias="end_time")


class EnvBasedSourceBaseConfig:
    pass


class TrinoUsageConfig(TrinoConfig, BaseUsageConfig, EnvBasedSourceBaseConfig):
    email_domain: str = Field(
        description="The email domain which will be appended to the users "
    )
    audit_catalog: str = Field(
        description="The catalog name where the audit table can be found "
    )
    audit_schema: str = Field(
        description="The schema name where the audit table can be found"
    )
    options: dict = Field(default={}, description="")
    database: str = Field(description="The name of the catalog from getting the usage")

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url()


@dataclasses.dataclass
class TrinoUsageReport(SourceReport):
    num_joined_access_events_skipped: int = 0


@platform_name("Trino")
@config_class(TrinoUsageConfig)
@support_status(SupportStatus.CERTIFIED)
@dataclasses.dataclass
class TrinoUsageSource(Source):
    """
    If you are using Starburst Trino you can collect usage stats the following way.

    #### Prerequsities
    1. You need to setup Event Logger which saves audit logs into a Postgres db and setup this db as a catalog in Trino

    2. Install starbust-trino-usage plugin
    Run pip install 'acryl-datahub[starburst-trino-usage]'.

    """

    config: TrinoUsageConfig
    report: TrinoUsageReport = dataclasses.field(default_factory=TrinoUsageReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = TrinoUsageConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        access_events = self._get_trino_history()
        # If the query results is empty, we don't want to proceed
        if not access_events:
            return []

        joined_access_event = self._get_joined_access_event(access_events)
        aggregated_info = self._aggregate_access_events(joined_access_event)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                yield self._make_usage_stat(aggregate)

    def _make_usage_query(self) -> str:
        return trino_usage_sql_comment.format(
            audit_catalog=self.config.audit_catalog,
            audit_schema=self.config.audit_schema,
            start_time=self.config.start_time.strftime(trino_datetime_format),
            end_time=self.config.end_time.strftime(trino_datetime_format),
        )

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url = {url}")
        engine = create_engine(url, **self.config.options)
        return engine

    def _get_trino_history(self):
        query = self._make_usage_query()
        engine = self._make_sql_engine()
        results = engine.execute(query)
        events = []
        for row in results:
            event_dict = row._asdict()

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
            return isodate
        if isinstance(v, datetime):
            return v

    def _get_joined_access_event(self, events):
        joined_access_events = []
        for event_dict in events:
            if event_dict.get("create_time"):
                event_dict["create_time"] = self._convert_str_to_datetime(
                    event_dict["create_time"]
                )
            else:
                self.report.num_joined_access_events_skipped += 1
                logging.info("The create_time parameter is missing. Skipping ....")
                continue

            event_dict["end_time"] = self._convert_str_to_datetime(
                event_dict.get("end_time")
            )

            if not event_dict["accessed_metadata"]:
                self.report.num_joined_access_events_skipped += 1
                logging.info("Field accessed_metadata is empty. Skipping ....")
                continue

            event_dict["accessed_metadata"] = json.loads(
                event_dict["accessed_metadata"]
            )

            if not event_dict.get("usr"):
                self.report.num_joined_access_events_skipped += 1
                logging.info("The username parameter is missing. Skipping ....")
                continue

            try:
                joined_access_events.append(TrinoJoinedAccessEvent(**event_dict))
            except Exception as e:
                self.report.num_joined_access_events_skipped += 1
                logger.info(f"Error while parsing TrinoJoinedAccessEvent: {e}")

        return joined_access_events

    def _aggregate_access_events(
        self, events: List[TrinoJoinedAccessEvent]
    ) -> Dict[datetime, Dict[TrinoTableRef, AggregatedDataset]]:
        datasets: Dict[
            datetime, Dict[TrinoTableRef, AggregatedDataset]
        ] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(event.starttime, self.config.bucket_duration)
            for metadata in event.accessed_metadata:
                # Skipping queries starting with $system@
                if metadata.catalog_name and metadata.catalog_name.startswith(
                    "$system@"
                ):
                    logging.debug(
                        f"Skipping system query for {metadata.catalog_name}..."
                    )
                    continue

                # Filtering down queries to the selected catalog
                if metadata.catalog_name != self.config.database:
                    continue

                resource = (
                    f"{metadata.catalog_name}.{metadata.schema_name}.{metadata.table}"
                )

                agg_bucket = datasets[floored_ts].setdefault(
                    resource,
                    AggregatedDataset(
                        bucket_start_time=floored_ts,
                        resource=resource,
                    ),
                )

                # add @unknown.com to username
                # current limitation in user stats UI, we need to provide email to show users
                if event.usr and "@" in parseaddr(event.usr)[1]:
                    username = event.usr
                else:
                    username = f"{event.usr if event.usr else 'unknown'}@{self.config.email_domain}"

                agg_bucket.add_read_entry(
                    username,
                    event.query,
                    metadata.columns,
                    user_email_pattern=self.config.user_email_pattern,
                )
        return datasets

    def _make_usage_stat(self, agg: AggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn_with_platform_instance(
                "trino",
                resource.lower(),
                self.config.platform_instance,
                self.config.env,
            ),
            self.config.top_n_queries,
            self.config.format_sql_queries,
            self.config.include_top_n_queries,
            self.config.queries_character_limit,
        )

    def get_report(self) -> SourceReport:
        return self.report
