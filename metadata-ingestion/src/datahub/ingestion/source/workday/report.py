from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class WorkdayReport(StaleEntityRemovalSourceReport):
    tables_scanned: int = 0
    datasets_scanned: int = 0
    data_sources_scanned: int = 0
    reports_scanned: int = 0
    fields_scanned: int = 0
    lineage_edges_emitted: int = 0
    external_upstreams_resolved: int = 0
    tokens_issued: int = 0
    unknown_field_types: int = 0
    api_errors: int = 0

    filtered_tables: LossyList[str] = field(default_factory=LossyList)
    filtered_datasets: LossyList[str] = field(default_factory=LossyList)
    filtered_data_sources: LossyList[str] = field(default_factory=LossyList)
    filtered_reports: LossyList[str] = field(default_factory=LossyList)
    malformed_objects_skipped: LossyList[str] = field(default_factory=LossyList)
    unknown_field_type_samples: LossyList[str] = field(default_factory=LossyList)

    def report_table_scanned(self) -> None:
        self.tables_scanned += 1

    def report_dataset_scanned(self) -> None:
        self.datasets_scanned += 1

    def report_data_source_scanned(self) -> None:
        self.data_sources_scanned += 1

    def report_report_scanned(self) -> None:
        self.reports_scanned += 1

    def report_field_scanned(self) -> None:
        self.fields_scanned += 1

    def report_lineage_edges(self, count: int) -> None:
        self.lineage_edges_emitted += count

    def report_external_upstream_resolved(self) -> None:
        self.external_upstreams_resolved += 1

    def report_token_issued(self) -> None:
        self.tokens_issued += 1

    def report_unknown_field_type(self, descriptor: str) -> None:
        self.unknown_field_types += 1
        self.unknown_field_type_samples.append(descriptor)

    def report_api_error(self) -> None:
        self.api_errors += 1

    def report_malformed_object(self, context: str) -> None:
        self.malformed_objects_skipped.append(context)
