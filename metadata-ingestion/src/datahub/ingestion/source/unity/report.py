from dataclasses import dataclass

from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class UnityCatalogReport(StaleEntityRemovalSourceReport):
    metastores: EntityFilterReport = EntityFilterReport.field(type="metastore")
    catalogs: EntityFilterReport = EntityFilterReport.field(type="catalog")
    schemas: EntityFilterReport = EntityFilterReport.field(type="schema")
    tables: EntityFilterReport = EntityFilterReport.field(type="table/view")

    num_queries: int = 0
    num_queries_dropped_parse_failure: int = 0
    num_queries_dropped_missing_table: int = 0  # Can be due to pattern filter
    num_queries_dropped_duplicate_table: int = 0
    num_queries_parsed_by_spark_plan: int = 0

    num_operational_stats_workunits_emitted: int = 0
    num_usage_workunits_emitted: int = 0
