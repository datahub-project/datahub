"""Tests for report configuration features: configurable sample sizes."""

import logging

import pytest

from datahub.ingestion.api.sink import SinkReport
from datahub.ingestion.api.source import (
    SourceReport,
    StructuredLogLevel,
    StructuredLogs,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig, ReportConfig


class TestReportConfig:
    """Tests for ReportConfig class."""

    def test_report_config_defaults(self):
        """Test that ReportConfig has correct default values."""
        config = ReportConfig()
        assert config.failure_sample_size == 10
        assert config.warning_sample_size == 10

    def test_report_config_custom_values(self):
        """Test that ReportConfig accepts custom values."""
        config = ReportConfig(
            failure_sample_size=50,
            warning_sample_size=25,
        )
        assert config.failure_sample_size == 50
        assert config.warning_sample_size == 25

    def test_report_config_minimum_sample_size(self):
        """Test that sample size must be at least 1."""
        with pytest.raises(ValueError):
            ReportConfig(failure_sample_size=0)
        with pytest.raises(ValueError):
            ReportConfig(warning_sample_size=0)
        with pytest.raises(ValueError):
            ReportConfig(failure_sample_size=-1)

    def test_pipeline_config_includes_report(self):
        """Test that PipelineConfig includes the report field with defaults."""
        config = PipelineConfig(
            source={"type": "file", "config": {"path": "test.json"}}
        )
        assert config.report is not None
        assert isinstance(config.report, ReportConfig)
        assert config.report.failure_sample_size == 10

    def test_pipeline_config_with_custom_report(self):
        """Test that PipelineConfig accepts custom report configuration."""
        config = PipelineConfig(
            source={"type": "file", "config": {"path": "test.json"}},
            report=ReportConfig(
                failure_sample_size=100,
                warning_sample_size=50,
            ),
        )
        assert config.report.failure_sample_size == 100
        assert config.report.warning_sample_size == 50


class TestStructuredLogs:
    """Tests for StructuredLogs configurable sample sizes."""

    def test_structured_logs_default_sample_sizes(self):
        """Test that StructuredLogs uses default sample sizes."""
        logs = StructuredLogs()
        assert logs.failure_sample_size == 10
        assert logs.warning_sample_size == 10

    def test_structured_logs_custom_sample_sizes(self):
        """Test that StructuredLogs accepts custom sample sizes."""
        logs = StructuredLogs(
            failure_sample_size=50,
            warning_sample_size=25,
        )
        assert logs.failure_sample_size == 50
        assert logs.warning_sample_size == 25
        # Verify the underlying LossyDicts have correct max_elements
        assert logs._entries[StructuredLogLevel.ERROR].max_elements == 50
        assert logs._entries[StructuredLogLevel.WARN].max_elements == 25

    def test_structured_logs_samples_failures_correctly(self):
        """Test that failures are sampled according to failure_sample_size."""
        logs = StructuredLogs(failure_sample_size=5)

        # Add more failures than the sample size
        for i in range(20):
            logs.report_log(
                StructuredLogLevel.ERROR,
                message=f"failure_{i}",
                context=f"context_{i}",
            )

        failures = logs.failures
        # Should have sampled entries (5) plus indication of sampling
        assert failures.total_elements == 20
        assert failures.sampled is True

    def test_structured_logs_samples_warnings_correctly(self):
        """Test that warnings are sampled according to warning_sample_size."""
        logs = StructuredLogs(warning_sample_size=3)

        # Add more warnings than the sample size
        for i in range(15):
            logs.report_log(
                StructuredLogLevel.WARN,
                message=f"warning_{i}",
                context=f"context_{i}",
            )

        warnings = logs.warnings
        assert warnings.total_elements == 15
        assert warnings.sampled is True

    def test_structured_logs_log_param_controls_logging(self, caplog):
        """Test that log parameter controls console logging."""
        logs = StructuredLogs()

        with caplog.at_level(logging.ERROR):
            # log=False should not log
            logs.report_log(
                StructuredLogLevel.ERROR,
                message="silent failure",
                context="silent context",
                log=False,
            )
            # log=True should log
            logs.report_log(
                StructuredLogLevel.ERROR,
                message="logged failure",
                context="logged context",
                log=True,
            )

        assert "silent failure" not in caplog.text
        assert "logged failure" in caplog.text


class TestSinkReport:
    """Tests for SinkReport configurable sample sizes."""

    def test_sink_report_default_sample_sizes(self):
        """Test that SinkReport uses default sample sizes."""
        report = SinkReport()
        assert report.failure_sample_size == 10
        assert report.warning_sample_size == 10
        # Verify LossyLists have correct max_elements
        assert report.failures.max_elements == 10
        assert report.warnings.max_elements == 10

    def test_sink_report_configure_report(self):
        """Test that configure_report updates settings correctly."""
        report = SinkReport()
        report.configure_report(
            failure_sample_size=50,
            warning_sample_size=25,
        )
        assert report.failure_sample_size == 50
        assert report.warning_sample_size == 25
        # Verify LossyLists were reinitialized
        assert report.failures.max_elements == 50
        assert report.warnings.max_elements == 25

    def test_sink_report_samples_failures_correctly(self):
        """Test that failures are sampled according to failure_sample_size."""
        report = SinkReport()
        report.configure_report(failure_sample_size=5)

        # Add more failures than the sample size
        for i in range(20):
            report.report_failure({"error": f"failure_{i}"})

        assert report.failures.total_elements == 20
        assert report.failures.sampled is True

    def test_sink_report_samples_warnings_correctly(self):
        """Test that warnings are sampled according to warning_sample_size."""
        report = SinkReport()
        report.configure_report(warning_sample_size=3)

        # Add more warnings than the sample size
        for i in range(15):
            report.report_warning({"warning": f"warning_{i}"})

        assert report.warnings.total_elements == 15
        assert report.warnings.sampled is True


class TestSourceReport:
    """Tests for SourceReport configurable sample sizes."""

    def test_source_report_default_sample_sizes(self):
        """Test that SourceReport uses default sample sizes."""
        report = SourceReport()
        assert report.failure_sample_size == 10
        assert report.warning_sample_size == 10

    def test_source_report_configure_report(self):
        """Test that configure_report updates settings correctly."""
        report = SourceReport()
        report.configure_report(
            failure_sample_size=100,
            warning_sample_size=50,
        )
        assert report.failure_sample_size == 100
        assert report.warning_sample_size == 50
        # Verify StructuredLogs was updated with correct settings
        assert report._structured_logs.failure_sample_size == 100
        assert report._structured_logs.warning_sample_size == 50

    def test_source_report_samples_failures_correctly(self):
        """Test that failures are sampled according to failure_sample_size."""
        report = SourceReport()
        report.configure_report(failure_sample_size=5)

        # Add more failures than the sample size
        for i in range(20):
            report.report_failure(message=f"failure_{i}", context=f"context_{i}")

        failures = report.failures
        assert failures.total_elements == 20
        assert failures.sampled is True

    def test_source_report_samples_warnings_correctly(self):
        """Test that warnings are sampled according to warning_sample_size."""
        report = SourceReport()
        report.configure_report(warning_sample_size=3)

        # Add more warnings than the sample size
        for i in range(15):
            report.report_warning(message=f"warning_{i}", context=f"context_{i}")

        warnings = report.warnings
        assert warnings.total_elements == 15
        assert warnings.sampled is True

    def test_source_report_failure_logs_by_default(self, caplog):
        """Test that report_failure logs by default."""
        report = SourceReport()

        with caplog.at_level(logging.ERROR):
            report.report_failure(message="test failure", context="test context")

        assert "test failure" in caplog.text


class TestReportConfigIntegration:
    """Integration tests for report configuration through the pipeline."""

    def test_source_report_large_sample_size(self):
        """Test that a large sample size captures more entries."""
        report = SourceReport()
        report.configure_report(failure_sample_size=100)

        # Add exactly 50 failures
        for i in range(50):
            report.report_failure(message=f"failure_{i}", context=f"context_{i}")

        failures = report.failures
        # With sample size of 100, all 50 should be captured without sampling
        assert failures.total_elements == 50
        assert failures.sampled is False

    def test_sink_report_large_sample_size(self):
        """Test that a large sample size captures more entries."""
        report = SinkReport()
        report.configure_report(failure_sample_size=100)

        # Add exactly 50 failures
        for i in range(50):
            report.report_failure({"error": f"failure_{i}"})

        # With sample size of 100, all 50 should be captured without sampling
        assert report.failures.total_elements == 50
        assert report.failures.sampled is False

    def test_configure_report_called_early(self):
        """Test that configure_report can be called before any failures."""
        report = SinkReport()

        # Configure first
        report.configure_report(failure_sample_size=5)

        # Then add failures
        for i in range(10):
            report.report_failure({"error": f"failure_{i}"})

        # Should sample correctly
        assert report.failures.total_elements == 10
        assert report.failures.sampled is True

    def test_configure_report_preserves_existing_entries(self):
        """Test that configure_report doesn't wipe out existing entries."""
        report = SourceReport()

        # Add some failures first
        report.report_failure(message="early failure", context="early context")

        # Now configure
        report.configure_report(failure_sample_size=50)

        # Verify the early failure is still there
        assert report.failures.total_elements == 1
        assert any("early failure" in str(f) for f in report.failures)


class TestEnvVarFunctions:
    """Tests for environment variable getter functions."""

    def test_get_report_failure_sample_size_default(self, monkeypatch):
        """Test get_report_failure_sample_size returns 10 when unset."""
        from datahub.configuration import env_vars

        monkeypatch.delenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", raising=False)
        assert env_vars.get_report_failure_sample_size() == 10

    def test_get_report_failure_sample_size_custom(self, monkeypatch):
        """Test get_report_failure_sample_size returns custom value."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "50")
        assert env_vars.get_report_failure_sample_size() == 50

    def test_get_report_warning_sample_size_default(self, monkeypatch):
        """Test get_report_warning_sample_size returns 10 when unset."""
        from datahub.configuration import env_vars

        monkeypatch.delenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", raising=False)
        assert env_vars.get_report_warning_sample_size() == 10

    def test_get_report_warning_sample_size_custom(self, monkeypatch):
        """Test get_report_warning_sample_size returns custom value."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", "25")
        assert env_vars.get_report_warning_sample_size() == 25
