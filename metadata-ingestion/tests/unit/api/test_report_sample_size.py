"""Tests for configurable report sample sizes via environment variables."""

import logging

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


class TestDetailedSinkLogging:
    """Tests for detailed sink logging with configurable log levels.

    Note: Base SinkReport doesn't have logging - it's added in specific sinks like datahub_rest.py
    These tests focus on SourceReport which does have configurable logging.
    """

    def test_source_report_logs_at_debug_by_default(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test SourceReport logs at DEBUG level by default."""
        monkeypatch.delenv("DATAHUB_SINK_ERROR_LOG_LEVEL", raising=False)
        monkeypatch.delenv("DATAHUB_SINK_WARNING_LOG_LEVEL", raising=False)
        report = SourceReport()

        with caplog.at_level(logging.DEBUG):
            report.report_warning(title="Test Warning", message="warning message")
            report.report_failure(title="Test Failure", message="failure message")

        assert "Test Warning: warning message" in caplog.text
        assert "Test Failure: failure message" in caplog.text

    def test_source_report_not_visible_at_info_by_default(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test SourceReport logs not visible at INFO level by default."""
        monkeypatch.delenv("DATAHUB_SINK_ERROR_LOG_LEVEL", raising=False)
        monkeypatch.delenv("DATAHUB_SINK_WARNING_LOG_LEVEL", raising=False)
        report = SourceReport()

        with caplog.at_level(logging.INFO):
            report.report_warning(title="Test Warning", message="warning message")
            # report_failure with log=True (default) will log at ERROR level
            # We need to call with log=False to test our DEBUG logging
            report.report_failure(
                title="Test Failure", message="failure message", log=False
            )

        assert "Test Warning: warning message" not in caplog.text
        assert "Test Failure: failure message" not in caplog.text

    def test_source_errors_at_error_level(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test SourceReport errors logged at ERROR level when configured."""
        monkeypatch.setenv("DATAHUB_SINK_ERROR_LOG_LEVEL", "ERROR")
        report = SourceReport()

        with caplog.at_level(logging.ERROR):
            report.report_failure(title="Test Failure", message="failure message")

        assert "Test Failure: failure message" in caplog.text

    def test_source_warnings_at_warning_level(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test SourceReport warnings logged at WARNING level when configured.

        Note: Need to reload the module to pick up the new env var value since
        log levels are computed at module load time.
        """
        import importlib
        import sys

        monkeypatch.setenv("DATAHUB_SINK_WARNING_LOG_LEVEL", "WARNING")

        # Reload modules to pick up new env var
        if "datahub.configuration.env_vars" in sys.modules:
            importlib.reload(sys.modules["datahub.configuration.env_vars"])
        if "datahub.ingestion.api.source" in sys.modules:
            importlib.reload(sys.modules["datahub.ingestion.api.source"])

        from datahub.ingestion.api.source import SourceReport as ReloadedSourceReport

        report = ReloadedSourceReport()

        with caplog.at_level(logging.WARNING):
            report.report_warning(title="Test Warning", message="warning message")

        assert "Test Warning: warning message" in caplog.text
