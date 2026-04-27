from dataclasses import dataclass, field
from typing import List, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class SharePointSourceReport(StaleEntityRemovalSourceReport):
    """Tracks per-run metrics for the SharePoint source in both modes."""

    # Site-level metrics (both modes)
    sites_scanned: int = 0
    sites_processed: int = 0
    sites_failed: int = 0
    failed_sites: List[str] = field(default_factory=list)

    # Data lake mode
    libraries_scanned: int = 0
    libraries_processed: int = 0
    files_scanned: int = 0
    files_processed: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    failed_files: LossyList[str] = field(default_factory=LossyList)
    total_bytes_scanned: int = 0
    files_without_schema: int = 0

    # Document mode
    pages_scanned: int = 0
    pages_processed: int = 0
    pages_failed: int = 0
    failed_pages: LossyList[Tuple[str, str]] = field(default_factory=LossyList)
    doc_files_scanned: int = 0
    doc_files_processed: int = 0
    doc_files_failed: int = 0
    total_text_extracted_bytes: int = 0

    # Embedding statistics (mirrored from DocumentChunkingSource)
    num_documents_with_embeddings: int = 0
    num_embedding_failures: int = 0
    embedding_failures: LossyList[str] = field(default_factory=LossyList)
    num_documents_limit_reached: bool = False

    def report_site_scanned(self) -> None:
        self.sites_scanned += 1

    def report_site_processed(self) -> None:
        self.sites_processed += 1

    def report_site_failed(self, site_path: str, error: str) -> None:
        self.sites_failed += 1
        self.failed_sites.append(site_path)
        self.report_warning(site_path, f"Failed to process site: {error}")

    def report_library_scanned(self) -> None:
        self.libraries_scanned += 1

    def report_library_processed(self) -> None:
        self.libraries_processed += 1

    def report_file_scanned(self, size_bytes: int = 0) -> None:
        self.files_scanned += 1
        self.total_bytes_scanned += size_bytes

    def report_file_processed(self) -> None:
        self.files_processed += 1

    def report_file_skipped(self, path: str, reason: str) -> None:
        self.files_skipped += 1
        self.report_warning(path, f"Skipped: {reason}")

    def report_file_failed(self, path: str, error: str) -> None:
        self.files_failed += 1
        self.failed_files.append(path)
        self.report_failure(path, f"Failed to process file: {error}")

    def report_page_scanned(self) -> None:
        self.pages_scanned += 1

    def report_page_processed(self) -> None:
        self.pages_processed += 1

    def report_page_failed(self, page_id: str, site_path: str, error: str) -> None:
        self.pages_failed += 1
        self.failed_pages.append((site_path, page_id))
        self.report_failure(page_id, f"Failed to process page: {error}")

    def report_text_extracted(self, num_bytes: int) -> None:
        self.total_text_extracted_bytes += num_bytes
