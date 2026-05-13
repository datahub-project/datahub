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
