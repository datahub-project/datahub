"""Reporting for Pinecone source."""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class PineconeSourceReport(StaleEntityRemovalSourceReport):
    """Report for Pinecone source ingestion."""

    # Counts
    indexes_scanned: int = 0
    indexes_filtered: int = 0
    namespaces_scanned: int = 0
    namespaces_filtered: int = 0
    datasets_generated: int = 0

    # Errors
    indexes_failed: LossyList[str] = field(default_factory=LossyList)
    namespaces_failed: LossyDict[str, LossyList[str]] = field(
        default_factory=lambda: LossyDict()
    )
    schema_inference_failed: LossyDict[str, str] = field(default_factory=LossyDict)

    # Warnings
    warnings: LossyDict[str, LossyList[str]] = field(default_factory=lambda: LossyDict())

    def report_index_scanned(self, index_name: str) -> None:
        """Record that an index was scanned."""
        self.indexes_scanned += 1

    def report_index_filtered(self, index_name: str) -> None:
        """Record that an index was filtered out."""
        self.indexes_filtered += 1

    def report_index_failed(self, index_name: str, error: str) -> None:
        """Record that an index failed to process."""
        self.indexes_failed.append(f"{index_name}: {error}")

    def report_namespace_scanned(self, index_name: str, namespace: str) -> None:
        """Record that a namespace was scanned."""
        self.namespaces_scanned += 1

    def report_namespace_filtered(self, index_name: str, namespace: str) -> None:
        """Record that a namespace was filtered out."""
        self.namespaces_filtered += 1

    def report_namespace_failed(self, index_name: str, namespace: str, error: str) -> None:
        """Record that a namespace failed to process."""
        if index_name not in self.namespaces_failed:
            self.namespaces_failed[index_name] = LossyList()
        self.namespaces_failed[index_name].append(f"{namespace}: {error}")

    def report_dataset_generated(self) -> None:
        """Record that a dataset was generated."""
        self.datasets_generated += 1

    def report_schema_inference_failed(self, dataset_name: str, error: str) -> None:
        """Record that schema inference failed for a dataset."""
        self.schema_inference_failed[dataset_name] = error

    def report_warning(self, key: str, message: str) -> None:
        """Record a warning message."""
        if key not in self.warnings:
            self.warnings[key] = LossyList()
        self.warnings[key].append(message)
