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
    business_objects_scanned: int = 0
    custom_reports_scanned: int = 0
    buckets_scanned: int = 0
    fields_scanned: int = 0
    lineage_edges_emitted: int = 0
    cll_edges_emitted: int = 0
    related_object_edges_emitted: int = 0
    transform_logic_captured: int = 0
    external_upstreams_resolved: int = 0
    tables_hydrated: int = 0
    datasets_hydrated: int = 0
    tokens_issued: int = 0
    unknown_field_types: int = 0
    api_errors: int = 0

    filtered_tables: LossyList[str] = field(default_factory=LossyList)
    filtered_datasets: LossyList[str] = field(default_factory=LossyList)
    filtered_data_sources: LossyList[str] = field(default_factory=LossyList)
    filtered_reports: LossyList[str] = field(default_factory=LossyList)
    filtered_business_objects: LossyList[str] = field(default_factory=LossyList)
    filtered_custom_reports: LossyList[str] = field(default_factory=LossyList)
    filtered_buckets: LossyList[str] = field(default_factory=LossyList)
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

    def report_business_object_scanned(self) -> None:
        self.business_objects_scanned += 1

    def report_custom_report_scanned(self) -> None:
        self.custom_reports_scanned += 1

    def report_bucket_scanned(self) -> None:
        self.buckets_scanned += 1

    def report_field_scanned(self) -> None:
        self.fields_scanned += 1

    def report_lineage_edges(self, count: int) -> None:
        self.lineage_edges_emitted += count

    def report_cll_edges(self, count: int) -> None:
        self.cll_edges_emitted += count

    def report_related_object_edges(self, count: int) -> None:
        self.related_object_edges_emitted += count

    def report_transform_logic_captured(self) -> None:
        self.transform_logic_captured += 1

    def report_table_hydrated(self) -> None:
        self.tables_hydrated += 1

    def report_dataset_hydrated(self) -> None:
        self.datasets_hydrated += 1

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
