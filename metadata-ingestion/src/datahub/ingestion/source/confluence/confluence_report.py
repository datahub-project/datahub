"""Report classes for Confluence source."""

from dataclasses import dataclass, field
from typing import List, Tuple

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class ConfluenceSourceReport(StaleEntityRemovalSourceReport):
    """Ingestion report for Confluence source."""

    # Space-level metrics
    spaces_scanned: int = 0
    spaces_processed: int = 0
    spaces_failed: int = 0
    failed_spaces: List[str] = field(default_factory=list)

    # Page-level metrics
    pages_scanned: int = 0
    pages_processed: int = 0
    pages_skipped: int = 0
    pages_failed: int = 0
    failed_pages: List[Tuple[str, str]] = field(default_factory=list)

    # Content metrics
    total_text_extracted_bytes: int = 0
    total_chunks_generated: int = 0
    total_embeddings_generated: int = 0

    # Embedding statistics (from chunking source)
    num_documents_with_embeddings: int = 0
    num_embedding_failures: int = 0
    embedding_failures: LossyList[str] = field(default_factory=LossyList)
    num_documents_limit_reached: bool = False

    # Performance metrics
    avg_page_processing_time_seconds: float = 0.0
    _total_processing_time: float = field(default=0.0, repr=False)
    _page_count_for_timing: int = field(default=0, repr=False)

    def report_space_scanned(self, space_key: str) -> None:
        """Record that a space was scanned."""
        self.spaces_scanned += 1

    def report_space_processed(self, space_key: str) -> None:
        """Record successful space processing."""
        self.spaces_processed += 1

    def report_space_failed(self, space_key: str, error: str) -> None:
        """Record space processing failure."""
        self.spaces_failed += 1
        self.failed_spaces.append(space_key)
        self.report_warning(space_key, f"Failed to process space: {error}")

    def report_page_scanned(self) -> None:
        """Record that a page was scanned."""
        self.pages_scanned += 1

    def report_page_processed(self, page_id: str, processing_time: float = 0.0) -> None:
        """Record successful page processing."""
        self.pages_processed += 1
        if processing_time > 0:
            self._total_processing_time += processing_time
            self._page_count_for_timing += 1
            self.avg_page_processing_time_seconds = (
                self._total_processing_time / self._page_count_for_timing
            )

    def report_page_skipped(self, page_id: str, reason: str) -> None:
        """Record that a page was skipped."""
        self.pages_skipped += 1
        self.report_warning(page_id, f"Skipped page: {reason}")

    def report_page_failed(self, page_id: str, space_key: str, error: str) -> None:
        """Record page processing failure."""
        self.pages_failed += 1
        self.failed_pages.append((space_key, page_id))
        self.report_failure(page_id, f"Failed to process page: {error}")

    def report_text_extracted(self, num_bytes: int) -> None:
        """Record text extraction metrics."""
        self.total_text_extracted_bytes += num_bytes

    def report_chunks_generated(self, num_chunks: int) -> None:
        """Record chunk generation metrics."""
        self.total_chunks_generated += num_chunks

    def report_embeddings_generated(self, num_embeddings: int) -> None:
        """Record embedding generation metrics."""
        self.total_embeddings_generated += num_embeddings
