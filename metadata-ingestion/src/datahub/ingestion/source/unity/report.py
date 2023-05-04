from dataclasses import dataclass, field

from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class UnityCatalogReport(StaleEntityRemovalSourceReport):
    metastores: EntityFilterReport = EntityFilterReport.field(type="metastore")
    catalogs: EntityFilterReport = EntityFilterReport.field(type="catalog")
    schemas: EntityFilterReport = EntityFilterReport.field(type="schema")
    tables: EntityFilterReport = EntityFilterReport.field(type="table/view")
    table_profiles: EntityFilterReport = EntityFilterReport.field(type="table profile")

    num_queries: int = 0
    num_queries_dropped_parse_failure: int = 0
    num_queries_dropped_missing_table: int = 0  # Can be due to pattern filter
    num_queries_dropped_duplicate_table: int = 0
    num_queries_parsed_by_spark_plan: int = 0

    num_operational_stats_workunits_emitted: int = 0
    num_usage_workunits_emitted: int = 0

    profile_table_timeouts: LossyList[str] = field(default_factory=LossyList)
    profile_table_empty: LossyList[str] = field(default_factory=LossyList)
    num_profile_table_failures: int = 0
    num_profile_failed_int_casts: int = 0
    num_profile_workunits_emitted: int = 0
