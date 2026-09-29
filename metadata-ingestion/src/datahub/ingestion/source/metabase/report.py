from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class MetabaseReport(StaleEntityRemovalSourceReport):
    # Entity counters
    dashboards_scanned: int = 0
    dashboards_dropped: int = 0
    charts_scanned: int = 0
    charts_dropped: int = 0
    models_scanned: int = 0
    models_dropped: int = 0

    # Lineage counters
    native_sql_parse_failures: int = 0
    mbql_field_refs_by_name_dropped: int = 0
    query_builder_cll_dropped: int = 0
