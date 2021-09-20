import collections
import dataclasses
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional

from pydantic import BaseModel
from sql_metadata import Parser
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.ingestion.source.usage.sql_usage_common import sql_compatibility_change
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
    get_time_bucket,
)

logger = logging.getLogger(__name__)


class SqlFromTableEvent(BaseModel):
    query_start_time: datetime
    query_text: str

    class Config:
        extra = "allow"


class UsageFromSqlTableConfig(BaseUsageConfig, SQLAlchemyGenericConfig):
    usage_query: str
    usage_platform_name: str
    usage_table_prefix: Optional[str]

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url()


@dataclasses.dataclass
class UsageFromSqlTableSource(Source):
    config: UsageFromSqlTableConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = UsageFromSqlTableConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        events = self._get_history()
        aggregated_info = self._aggregate_events(events)

        for time_bucket in aggregated_info.values():
            for aggregate in time_bucket.values():
                wu = self._make_usage_stat(aggregate)
                self.report.report_workunit(wu)
                yield wu

    def _make_sql_engine(self) -> Engine:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        return engine

    def _get_history(self):
        engine = self._make_sql_engine()
        results = engine.execute(self.config.usage_query)

        for row in results:
            event_dict = sql_compatibility_change(row)

            if event_dict["query_text"] is None:
                continue

            query_id = event_dict["query_id"]
            query_text = event_dict["query_text"]
            try:
                parser = Parser(query_text)

                tables = []
                for table in parser.tables:
                    table_parts = table.split(".")
                    if (
                        len(table_parts) == 1
                        and self.config.usage_table_prefix is not None
                    ):
                        table = f"{self.config.usage_table_prefix}.{table}"
                    tables.append(table)
                event_dict["tables"] = tables

                event_dict["columns"] = parser.columns
            except Exception:
                self.report.report_warning(
                    "usage", f"Failed to parse sql query id = {query_id}"
                )
                continue

            event_dict["query_start_time"] = (
                event_dict["query_start_time"]
            ).astimezone(tz=timezone.utc)

            yield event_dict

    def _aggregate_events(self, events: Iterable[Dict]) -> Dict[datetime, Dict]:
        datasets: Dict[datetime, Dict] = collections.defaultdict(dict)

        for event in events:
            floored_ts = get_time_bucket(
                event["query_start_time"], self.config.bucket_duration
            )

            for table in event["tables"]:
                agg_bucket = datasets[floored_ts].setdefault(
                    table,
                    GenericAggregatedDataset(
                        bucket_start_time=floored_ts, resource=table
                    ),
                )
                agg_bucket.add_read_entry(
                    event["query_email"],
                    event["query_text"],
                    event["columns"],
                )

        return datasets

    def _make_usage_stat(self, agg: GenericAggregatedDataset) -> MetadataWorkUnit:
        return agg.make_usage_workunit(
            self.config.bucket_duration,
            lambda resource: builder.make_dataset_urn(
                self.config.usage_platform_name, resource.lower(), self.config.env
            ),
            self.config.top_n_queries,
        )

    def get_report(self):
        return self.report

    def close(self):
        pass
