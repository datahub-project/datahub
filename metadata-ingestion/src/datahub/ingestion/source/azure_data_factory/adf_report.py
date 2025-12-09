"""Custom report class for Azure Data Factory connector."""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class AzureDataFactorySourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for Azure Data Factory source.

    Tracks metrics specific to ADF ingestion including counts of
    factories, pipelines, activities, and lineage extraction results.
    """

    # Entity counts
    factories_scanned: int = 0
    pipelines_scanned: int = 0
    activities_scanned: int = 0
    datasets_scanned: int = 0
    linked_services_scanned: int = 0
    data_flows_scanned: int = 0
    triggers_scanned: int = 0

    # Filtered entities
    filtered_factories: LossyList[str] = field(default_factory=LossyList)
    filtered_pipelines: LossyList[str] = field(default_factory=LossyList)

    # Lineage metrics
    lineage_edges_extracted: int = 0
    lineage_extraction_failures: int = 0
    datasets_with_lineage: int = 0
    datasets_without_platform_mapping: LossyList[str] = field(default_factory=LossyList)

    # Execution history metrics
    pipeline_runs_scanned: int = 0
    activity_runs_scanned: int = 0

    # API metrics
    api_calls: int = 0
    api_errors: int = 0

    def report_factory_scanned(self) -> None:
        """Increment factories scanned counter."""
        self.factories_scanned += 1

    def report_factory_filtered(self, factory_name: str) -> None:
        """Record a filtered factory."""
        self.filtered_factories.append(factory_name)

    def report_pipeline_scanned(self) -> None:
        """Increment pipelines scanned counter."""
        self.pipelines_scanned += 1

    def report_pipeline_filtered(self, pipeline_name: str) -> None:
        """Record a filtered pipeline."""
        self.filtered_pipelines.append(pipeline_name)

    def report_activity_scanned(self) -> None:
        """Increment activities scanned counter."""
        self.activities_scanned += 1

    def report_dataset_scanned(self) -> None:
        """Increment datasets scanned counter."""
        self.datasets_scanned += 1

    def report_linked_service_scanned(self) -> None:
        """Increment linked services scanned counter."""
        self.linked_services_scanned += 1

    def report_data_flow_scanned(self) -> None:
        """Increment data flows scanned counter."""
        self.data_flows_scanned += 1

    def report_trigger_scanned(self) -> None:
        """Increment triggers scanned counter."""
        self.triggers_scanned += 1

    def report_lineage_extracted(self) -> None:
        """Increment lineage edges counter."""
        self.lineage_edges_extracted += 1
        self.datasets_with_lineage += 1

    def report_lineage_failed(self, entity_name: str, error: str) -> None:
        """Record a lineage extraction failure."""
        self.lineage_extraction_failures += 1
        self.report_warning(
            title="Lineage Extraction Failed",
            message="Unable to extract lineage for this entity.",
            context=f"entity={entity_name}, error={error}",
        )

    def report_unmapped_platform(
        self, dataset_name: str, linked_service_type: str
    ) -> None:
        """Record a dataset with unmapped platform."""
        self.datasets_without_platform_mapping.append(
            f"{dataset_name} (type={linked_service_type})"
        )

    def report_pipeline_run_scanned(self) -> None:
        """Increment pipeline runs scanned counter."""
        self.pipeline_runs_scanned += 1

    def report_activity_run_scanned(self) -> None:
        """Increment activity runs scanned counter."""
        self.activity_runs_scanned += 1

    def report_api_call(self) -> None:
        """Track an API call."""
        self.api_calls += 1

    def report_api_error(self, endpoint: str, error: str) -> None:
        """Record an API error."""
        self.api_errors += 1
        self.report_warning(
            title="API Error",
            message="Failed to call Azure Data Factory API.",
            context=f"endpoint={endpoint}, error={error}",
        )
