from dataclasses import dataclass, field
from typing import Optional, Tuple

from datahub.ingestion.api.report import EntityFilterReport, Report
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.utilities.lossy_collections import LossyDict, LossyList
from datahub.utilities.perf_timer import PerfTimer


@dataclass
class UnityCatalogUsagePerfReport(Report):
    get_queries_timer: PerfTimer = field(default_factory=PerfTimer)
    sql_parsing_timer: PerfTimer = field(default_factory=PerfTimer)
    spark_sql_parsing_timer: PerfTimer = field(default_factory=PerfTimer)
    aggregator_add_event_timer: PerfTimer = field(default_factory=PerfTimer)
    gen_operation_timer: PerfTimer = field(default_factory=PerfTimer)
    query_fingerprinting_timer: PerfTimer = field(default_factory=PerfTimer)


@dataclass
class UnityCatalogReport(IngestionStageReport, SQLSourceReport):
    metastores: EntityFilterReport = EntityFilterReport.field(type="metastore")
    catalogs: EntityFilterReport = EntityFilterReport.field(type="catalog")
    schemas: EntityFilterReport = EntityFilterReport.field(type="schema")
    tables: EntityFilterReport = EntityFilterReport.field(type="table/view")
    table_profiles: EntityFilterReport = EntityFilterReport.field(type="table profile")
    notebooks: EntityFilterReport = EntityFilterReport.field(type="notebook")

    hive_metastore_catalog_found: Optional[bool] = None

    num_column_lineage_skipped_column_count: int = 0
    num_external_upstreams_lacking_permissions: int = 0
    num_external_upstreams_unsupported: int = 0

    num_queries: int = 0
    num_unique_queries: int = 0
    num_queries_dropped_parse_failure: int = 0
    num_queries_missing_table: int = 0  # Can be due to pattern filter
    num_queries_duplicate_table: int = 0
    num_queries_parsed_by_spark_plan: int = 0
    usage_perf_report: UnityCatalogUsagePerfReport = field(
        default_factory=UnityCatalogUsagePerfReport
    )

    # Distinguish from Operations emitted for created / updated timestamps
    num_operational_stats_workunits_emitted: int = 0

    profile_table_timeouts: LossyList[str] = field(default_factory=LossyList)
    profile_table_empty: LossyList[str] = field(default_factory=LossyList)
    profile_table_errors: LossyDict[str, LossyList[Tuple[str, str]]] = field(
        default_factory=LossyDict
    )
    num_profile_missing_size_in_bytes: int = 0
    num_profile_missing_row_count: int = 0
    num_profile_failed_unsupported_column_type: int = 0
    num_profile_failed_int_casts: int = 0

    num_catalogs_missing_name: int = 0
    num_schemas_missing_name: int = 0
    num_tables_missing_name: int = 0
    num_columns_missing_name: int = 0
    num_queries_missing_info: int = 0
