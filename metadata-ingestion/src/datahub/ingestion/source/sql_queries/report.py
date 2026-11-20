from dataclasses import dataclass
from typing import Optional

from datahub.ingestion.api.source import SourceReport
from datahub.sql_parsing.schema_resolver import SchemaResolverReport
from datahub.sql_parsing.sql_parsing_aggregator import SqlAggregatorReport


@dataclass
class SqlQueriesSourceReport(SourceReport):
    num_entries_processed: int = 0
    num_entries_failed: int = 0
    # Records read from a backend that had no SQL in the mapped query field.
    num_entries_skipped_no_query: int = 0
    num_queries_aggregator_failures: int = 0
    num_queries_processed: int = 0
    num_invalid_table_entries: int = 0
    num_temp_table_matches: int = 0

    sql_aggregator: Optional[SqlAggregatorReport] = None
    schema_resolver_report: Optional[SchemaResolverReport] = None
