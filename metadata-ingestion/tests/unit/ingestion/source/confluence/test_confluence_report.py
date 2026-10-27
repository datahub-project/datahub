"""Unit tests for ConfluenceSourceReport metric tracking."""

from datahub.ingestion.source.confluence.confluence_report import (
    ConfluenceSourceReport,
)


def test_space_metrics_track_scanned_processed_and_failed() -> None:
    report = ConfluenceSourceReport()

    report.report_space_scanned("TEAM")
    report.report_space_scanned("DOCS")
    report.report_space_processed("TEAM")
    report.report_space_failed("DOCS", "permission denied")

    assert report.spaces_scanned == 2
    assert report.spaces_processed == 1
    assert report.spaces_failed == 1
    # A failed space is recorded so operators can see which spaces to investigate.
    assert report.failed_spaces == ["DOCS"]
    assert any("permission denied" in str(warning) for warning in report.warnings)


def test_page_skipped_and_failed_are_recorded() -> None:
    report = ConfluenceSourceReport()

    report.report_page_skipped("123", "Filtered by pages.deny")
    report.report_page_failed("456", "DOCS", "boom")

    assert report.pages_skipped == 1
    assert report.pages_failed == 1
    # Failed pages keep both the space and page id for triage.
    assert report.failed_pages == [("DOCS", "456")]
    assert any("boom" in str(failure) for failure in report.failures)


def test_avg_page_processing_time_only_counts_timed_pages() -> None:
    report = ConfluenceSourceReport()

    # Pages reported without a processing time must not skew the average.
    report.report_page_processed("1")
    report.report_page_processed("2", processing_time=2.0)
    report.report_page_processed("3", processing_time=4.0)

    assert report.pages_processed == 3
    assert report.avg_page_processing_time_seconds == 3.0


def test_content_metrics_accumulate() -> None:
    report = ConfluenceSourceReport()

    report.report_text_extracted(100)
    report.report_text_extracted(50)
    report.report_chunks_generated(3)
    report.report_chunks_generated(2)
    report.report_embeddings_generated(4)

    assert report.total_text_extracted_bytes == 150
    assert report.total_chunks_generated == 5
    assert report.total_embeddings_generated == 4
