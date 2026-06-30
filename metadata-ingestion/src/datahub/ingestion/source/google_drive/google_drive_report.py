"""Source report for the Google Drive ingestion source."""

from dataclasses import dataclass, field

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class GoogleDriveSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for GoogleDriveSource."""

    # Discovery statistics
    folders_discovered: int = 0
    files_discovered: int = 0

    # Document processing
    docs_processed: int = 0
    docs_skipped_too_short: int = 0
    docs_failed: int = 0

    # Folder entities
    folders_ingested: int = 0

    # Totals
    total_text_bytes: int = 0

    # Limit tracking
    num_documents_limit_reached: bool = False

    # Error tracking
    processing_errors: LossyList[str] = field(default_factory=LossyList)

    def report_folder_discovered(self) -> None:
        self.folders_discovered += 1

    def report_file_discovered(self) -> None:
        self.files_discovered += 1

    def report_doc_processed(self, text_length: int) -> None:
        self.docs_processed += 1
        self.total_text_bytes += text_length

    def report_doc_skipped_too_short(
        self, file_id: str, length: int, minimum: int
    ) -> None:
        self.docs_skipped_too_short += 1
        # Use a stable title/message as the aggregation key and put the
        # per-document detail in `context`, so identical skips group together.
        self.report_warning(
            title="Document skipped: text too short",
            message="Document text length is below the configured minimum.",
            context=f"file_id={file_id}, length={length}, minimum={minimum}",
        )

    def report_doc_failed(self, file_id: str, error: str) -> None:
        self.docs_failed += 1
        self.processing_errors.append(f"{file_id}: {error}")
        self.report_failure(
            title="Failed to ingest document",
            message="Failed to ingest a Google Drive document.",
            context=f"file_id={file_id}: {error}",
        )

    def report_folder_ingested(self) -> None:
        self.folders_ingested += 1
