"""Reporting for the github-documents ingestion source."""

from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)


@dataclass
class GitHubDocumentsSourceReport(StaleEntityRemovalSourceReport):
    files_processed: int = 0
    folders_processed: int = 0
    files_skipped: int = 0
