"""Tests for configurable report sample sizes via environment variables."""

import pytest

from datahub.ingestion.api.sink import SinkReport
from datahub.ingestion.api.source import SourceReport, StructuredLogLevel


class TestReportSampleSizeFromEnvVars:
    """Tests that report sample sizes are configurable via environment variables."""

    def test_sink_report_sample_sizes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test SinkReport reads sample sizes from env vars."""
        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "100")
        monkeypatch.setenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", "75")

        report = SinkReport()
        assert report.failures.max_elements == 100
        assert report.warnings.max_elements == 75

    def test_source_report_sample_sizes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test SourceReport reads sample sizes from env vars via StructuredLogs."""
        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "100")
        monkeypatch.setenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", "75")

        report = SourceReport()
        assert (
            report._structured_logs._entries[StructuredLogLevel.ERROR].max_elements
            == 100
        )
        assert (
            report._structured_logs._entries[StructuredLogLevel.WARN].max_elements == 75
        )

    def test_context_list_respects_sample_size(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that the per-entry context LossyList respects the sample size env var.

        When multiple datasets share the same error message they are grouped into one
        StructuredLogEntry. The context list within that entry must honour
        DATAHUB_REPORT_FAILURE_SAMPLE_SIZE so all affected datasets are visible.
        """
        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "50")

        report = SourceReport()
        # Log the same error message 30 times with different context values.
        for i in range(30):
            report.report_failure(
                message="Error processing table", context=f"table_{i}"
            )

        failures = report._structured_logs._entries[StructuredLogLevel.ERROR]
        entry = next(iter(failures.values()))
        assert entry.context.max_elements == 50, (
            "context LossyList max_elements should match DATAHUB_REPORT_FAILURE_SAMPLE_SIZE"
        )
        assert list(entry.context) == [f"table_{i}" for i in range(30)], (
            "all 30 context items should be retained (30 < max_elements=50, no sampling)"
        )

    def test_context_list_truncates_at_configured_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression test: context LossyList must sample when entries exceed max_elements.

        Before the fix, max_elements was hardcoded to 10 inside report_log(), so this
        test would fail: logging 20 entries with max_elements=5 would not trigger sampling.
        """
        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "5")

        report = SourceReport()
        for i in range(20):
            report.report_failure(
                message="Error processing table", context=f"table_{i}"
            )

        failures = report._structured_logs._entries[StructuredLogLevel.ERROR]
        entry = next(iter(failures.values()))
        assert entry.context.max_elements == 5
        assert entry.context.sampled, (
            "context list should be sampled when 20 > max_elements=5"
        )
