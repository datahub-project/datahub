from dataclasses import dataclass, field
from typing import List, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class QuipSourceReport(StaleEntityRemovalSourceReport):
    folders_scanned: int = 0
    folder_documents_created: int = 0

    threads_scanned: int = 0
    threads_processed: int = 0
    threads_skipped: int = 0
    threads_failed: int = 0
    failed_threads: List[Tuple[str, str]] = field(default_factory=list)

    total_text_extracted_bytes: int = 0

    # Mirrored from the chunking sub-component.
    num_documents_with_embeddings: int = 0
    num_embedding_failures: int = 0
    embedding_failures: LossyList[str] = field(default_factory=LossyList)
    num_documents_limit_reached: bool = False

    def report_folder_scanned(self) -> None:
        self.folders_scanned += 1

    def report_folder_document_created(self) -> None:
        self.folder_documents_created += 1

    def report_thread_scanned(self) -> None:
        self.threads_scanned += 1

    def report_thread_processed(self) -> None:
        self.threads_processed += 1

    def report_thread_skipped(self, thread_id: str, reason: str) -> None:
        self.threads_skipped += 1
        self.report_warning(thread_id, f"Skipped thread: {reason}")

    def report_thread_failed(self, thread_id: str, error: str) -> None:
        self.threads_failed += 1
        self.failed_threads.append((thread_id, error))
        self.report_failure(thread_id, f"Failed to process thread: {error}")

    def report_text_extracted(self, num_bytes: int) -> None:
        self.total_text_extracted_bytes += num_bytes
