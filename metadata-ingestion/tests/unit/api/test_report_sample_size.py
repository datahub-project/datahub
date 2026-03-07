"""Tests for configurable report sample sizes via environment variables."""

from unittest import mock

from datahub.ingestion.api.sink import SinkReport
from datahub.ingestion.api.source import (
    SourceReport,
    StructuredLogLevel,
    StructuredLogs,
)


class TestReportSampleSizeFromEnvVars:
    """Tests that report sample sizes are configurable via environment variables."""

    def test_sink_report_default_sample_sizes(self) -> None:
        """Test SinkReport uses default sample size of 10."""
        report = SinkReport()
        assert report.failures.max_elements == 10
        assert report.warnings.max_elements == 10

    def test_sink_report_both_custom_sample_sizes(self) -> None:
        """Test SinkReport reads both sample sizes from env vars."""
        with (
            mock.patch(
                "datahub.ingestion.api.sink.get_report_failure_sample_size",
                return_value=100,
            ),
            mock.patch(
                "datahub.ingestion.api.sink.get_report_warning_sample_size",
                return_value=75,
            ),
        ):
            report = SinkReport()
            assert report.failures.max_elements == 100
            assert report.warnings.max_elements == 75

    def test_structured_logs_default_sample_sizes(self) -> None:
        """Test StructuredLogs uses default sample size of 10."""
        logs = StructuredLogs()

        assert logs._entries[StructuredLogLevel.ERROR].max_elements == 10
        assert logs._entries[StructuredLogLevel.WARN].max_elements == 10

    def test_source_report_inherits_sample_sizes(self) -> None:
        """Test SourceReport uses env vars through StructuredLogs."""
        with (
            mock.patch(
                "datahub.ingestion.api.source.get_report_failure_sample_size",
                return_value=100,
            ),
            mock.patch(
                "datahub.ingestion.api.source.get_report_warning_sample_size",
                return_value=75,
            ),
        ):
            report = SourceReport()

            assert (
                report._structured_logs._entries[StructuredLogLevel.ERROR].max_elements
                == 100
            )
            assert (
                report._structured_logs._entries[StructuredLogLevel.WARN].max_elements
                == 75
            )
