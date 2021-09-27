import collections
import dataclasses
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from sql_metadata import Parser

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.sql.sql_generic import SQLAlchemyGenericConfig
from datahub.ingestion.source.usage.sql_usage_common import SqlUsageSource
from datahub.ingestion.source.usage.usage_common import (
    BaseUsageConfig,
    GenericAggregatedDataset,
    get_time_bucket,
)

logger = logging.getLogger(__name__)


class UsageFromSqlTableConfig(BaseUsageConfig, SQLAlchemyGenericConfig):
    usage_query: str
    table_default_prefix: Optional[str]
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclasses.dataclass
class UsageFromSqlTableSource(SqlUsageSource):
    config: UsageFromSqlTableConfig
    report: SourceReport = dataclasses.field(default_factory=SourceReport)

    @classmethod
    def create(cls, config_dict, ctx):
        config = UsageFromSqlTableConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def _get_columns(self, parser, query_id):
        columns = set()

        if parser.columns_dict is None:
            return columns

        for column_list in parser.columns_dict.values():
            for column in column_list:
                if type(column) == list:
                    logger.warn(f"query_id={query_id} {column} of list type")
                else:
                    columns.add(column)

        return columns

    def _get_tables(self, parser, query_id):
        tables = []
        total_count = 0

        for table in parser.tables:
            total_count += 1
            table_parts = table.split(".")
            if len(table_parts) == 1 and self.get_table_default_prefix() is not None:
                table = f"{self.get_table_default_prefix()}.{table}"

            if not self.config.table_pattern.allowed(table):
                continue
            tables.append(table.lower())

        if total_count == 0:
            logger.warn(f"query_id={query_id} no tables found")

        return tables


    def parse_query(self, event_dict):
        query_id = event_dict["query_id"]
        query_text = event_dict["query_text"]
        parser = Parser(query_text)

        event_dict["tables"] = self._get_tables(parser, query_id)
        event_dict["columns"] = list(self._get_columns(parser, query_id))

    def process_row(self, row):
        event_dict = self.sql_compatibility_change(row)

        if event_dict["query_text"] is None:
            return None

        query_id = None
        try:
            query_id = event_dict["query_id"]
            self.parse_query(event_dict)
        except Exception:
            logger.warn(f"query={query_id} {traceback.format_exc()}")
            self.get_report().report_warning(
                "usage", f"Failed to parse sql query_id={query_id}"
            )
            return None

        event_dict["query_start_time"] = (event_dict["query_start_time"]).astimezone(
            tz=timezone.utc
        )

        return event_dict

    def get_history(self) -> Iterable:
        query = self.config.usage_query
        engine = self.make_sql_engine()

        results = engine.execute(query)

        for row in results:
            result = self.process_row(row)
            if result is None:
                continue
            yield result

    def aggregate_events(
        self, events: Iterable
    ) -> Dict[datetime, Dict[Any, GenericAggregatedDataset]]:
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

    def get_config(self):
        return self.config

    def get_platform(self) -> str:
        return self.config.platform

    def get_report(self) -> SourceReport:
        return self.report

    def get_table_default_prefix(self) -> Optional[str]:
        return self.get_config().table_default_prefix
