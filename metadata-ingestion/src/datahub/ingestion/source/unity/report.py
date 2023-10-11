from dataclasses import dataclass, field
from typing import Tuple

from datahub.ingestion.api.report import EntityFilterReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class UnityCatalogReport(IngestionStageReport, StaleEntityRemovalSourceReport):
    metastores: EntityFilterReport = EntityFilterReport.field(type="metastore")
    catalogs: EntityFilterReport = EntityFilterReport.field(type="catalog")
    schemas: EntityFilterReport = EntityFilterReport.field(type="schema")
    tables: EntityFilterReport = EntityFilterReport.field(type="table/view")
    table_profiles: EntityFilterReport = EntityFilterReport.field(type="table profile")
    notebooks: EntityFilterReport = EntityFilterReport.field(type="notebook")

    num_column_lineage_skipped_column_count: int = 0

    num_queries: int = 0
    num_queries_dropped_parse_failure: int = 0
    num_queries_missing_table: int = 0  # Can be due to pattern filter
    num_queries_duplicate_table: int = 0
    num_queries_parsed_by_spark_plan: int = 0

    # Distinguish from Operations emitted for created / updated timestamps
    num_operational_stats_workunits_emitted: int = 0

    profile_table_timeouts: LossyList[str] = field(default_factory=LossyList)
    profile_table_empty: LossyList[str] = field(default_factory=LossyList)
    profile_table_errors: LossyDict[str, LossyList[Tuple[str, str]]] = field(
        default_factory=LossyDict
    )
    num_profile_failed_unsupported_column_type: int = 0
    num_profile_failed_int_casts: int = 0
