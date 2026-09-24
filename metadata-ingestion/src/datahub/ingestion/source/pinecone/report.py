"""Reporting for Pinecone source."""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class PineconeSourceReport(StaleEntityRemovalSourceReport):
    """Report for Pinecone source ingestion."""

    indexes_scanned: int = 0
    indexes_filtered: int = 0
    namespaces_scanned: int = 0
    namespaces_filtered: int = 0
    datasets_generated: int = 0

    indexes_failed: LossyList[str] = field(default_factory=LossyList)
    namespaces_failed: LossyList[str] = field(default_factory=LossyList)
    schema_inference_failed: LossyList[str] = field(default_factory=LossyList)

    def report_index_scanned(self, index_name: str) -> None:
        self.indexes_scanned += 1

    def report_index_filtered(self, index_name: str) -> None:
        self.indexes_filtered += 1

    def report_index_failed(self, index_name: str, error: str) -> None:
        self.indexes_failed.append(f"{index_name}: {error}")

    def report_namespace_scanned(self, index_name: str, namespace: str) -> None:
        self.namespaces_scanned += 1

    def report_namespace_filtered(self, index_name: str, namespace: str) -> None:
        self.namespaces_filtered += 1

    def report_namespace_failed(
        self, index_name: str, namespace: str, error: str
    ) -> None:
        self.namespaces_failed.append(f"{index_name}/{namespace}: {error}")

    def report_dataset_generated(self) -> None:
        self.datasets_generated += 1

    def report_schema_inference_failed(self, dataset_name: str, error: str) -> None:
        self.schema_inference_failed.append(f"{dataset_name}: {error}")
