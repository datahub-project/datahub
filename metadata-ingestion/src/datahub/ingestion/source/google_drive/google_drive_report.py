from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class GoogleDriveSourceReport(StaleEntityRemovalSourceReport):
    roots_scanned: int = 0
    roots_failed: int = 0
    folders_discovered: int = 0
    folders_filtered: int = 0
    folders_emitted: int = 0
    folders_skipped_unchanged: int = 0
    folders_failed: int = 0
    documents_discovered: int = 0
    documents_filtered: int = 0
    documents_emitted: int = 0
    documents_skipped_unchanged: int = 0
    documents_skipped_empty: int = 0
    documents_skipped_too_long: int = 0
    documents_export_failed: int = 0
    documents_unparseable: int = 0
    documents_html_fallback: int = 0
    documents_resurrected: int = 0
    documents_failed: int = 0
    documents_embedding_deferred: int = 0
    # Stored documents that could not be read back from DataHub, so they were
    # re-exported instead of being checked for changes.
    change_detection_lookup_failures: int = 0
    change_detection_batch_reads: int = 0
    # Batched reads that failed and were retried one document at a time.
    change_detection_batch_fallbacks: int = 0
    shortcuts_skipped: int = 0
    failed_documents: LossyList[str] = field(default_factory=LossyList)

    num_documents_with_embeddings: int = 0
    num_embedding_failures: int = 0
    num_documents_truncated_oversized: int = 0
    num_documents_dropped_oversized: int = 0
    embedding_failures: LossyList[str] = field(default_factory=LossyList)
    num_documents_limit_reached: bool = False

    api_calls: Dict[str, int] = field(default_factory=dict)

    def report_api_call(self, name: str) -> None:
        self.api_calls[name] = self.api_calls.get(name, 0) + 1
