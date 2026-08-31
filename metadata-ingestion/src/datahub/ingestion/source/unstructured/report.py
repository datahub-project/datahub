"""Reporting for UnstructuredSource."""

from dataclasses import dataclass, field
from typing import Dict

from datahub.ingestion.api.source import SourceReport
from datahub.utilities.lossy_collections import LossyDict, LossyList


@dataclass
class UnstructuredSourceReport(SourceReport):
    """Report for UnstructuredSource (Part I: Text Extraction)."""

    # Files processed
    num_files_scanned: int = 0
    num_files_processed: int = 0
    num_files_skipped: int = 0
    num_files_failed: int = 0

    # Documents created
    num_documents_created: int = 0
    num_folder_documents_created: int = 0

    # Text extraction
    total_text_extracted_bytes: int = 0
    total_elements_extracted: int = 0

    # Errors
    processing_errors: LossyList[str] = field(default_factory=LossyList)
    skipped_files: LossyDict[str, str] = field(
        default_factory=LossyDict
    )  # file_path -> reason

    # Statistics by file type
    files_by_type: Dict[str, int] = field(default_factory=dict)

    # Partitioning statistics
    partitioning_strategy_used: Dict[str, int] = field(
        default_factory=dict
    )  # strategy -> count

    def report_file_scanned(self) -> None:
        self.num_files_scanned += 1

    def report_file_processed(
        self, file_type: str, num_elements: int, text_bytes: int
    ) -> None:
        self.num_files_processed += 1
        self.total_elements_extracted += num_elements
        self.total_text_extracted_bytes += text_bytes
        self.files_by_type[file_type] = self.files_by_type.get(file_type, 0) + 1

    def report_file_skipped(self, file_path: str, reason: str) -> None:
        self.num_files_skipped += 1
        self.skipped_files[file_path] = reason

    def report_file_failed(self, error_msg: str) -> None:
        self.num_files_failed += 1
        self.processing_errors.append(error_msg)

    def report_document_created(self, is_folder: bool = False) -> None:
        self.num_documents_created += 1
        if is_folder:
            self.num_folder_documents_created += 1

    def report_partitioning_strategy(self, strategy: str) -> None:
        self.partitioning_strategy_used[strategy] = (
            self.partitioning_strategy_used.get(strategy, 0) + 1
        )
