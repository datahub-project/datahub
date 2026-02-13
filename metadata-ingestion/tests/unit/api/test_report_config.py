"""Tests for report configuration features: configurable sample sizes and verbose logging."""

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
        assert config.log_failure_summaries_to_console is None
        assert config.log_warning_summaries_to_console is None

    def test_report_config_custom_values(self):
        """Test that ReportConfig accepts custom values."""
        config = ReportConfig(
            failure_sample_size=50,
            warning_sample_size=25,
            log_failure_summaries_to_console=True,
            log_warning_summaries_to_console=True,
        )
        assert config.failure_sample_size == 50
        assert config.warning_sample_size == 25
        assert config.log_failure_summaries_to_console is True
        assert config.log_warning_summaries_to_console is True

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
                log_failure_summaries_to_console=True,
                log_warning_summaries_to_console=False,
            ),
        )
        assert config.report.failure_sample_size == 100
        assert config.report.warning_sample_size == 50
        assert config.report.log_failure_summaries_to_console is True
        assert config.report.log_warning_summaries_to_console is False


class TestStructuredLogs:
    """Tests for StructuredLogs configurable sample sizes and verbose logging."""

    def test_structured_logs_default_sample_sizes(self):
        """Test that StructuredLogs uses default sample sizes."""
        logs = StructuredLogs()
        assert logs.failure_sample_size == 10
        assert logs.warning_sample_size == 10
        assert logs.log_failure_summaries_to_console is None
        assert logs.log_warning_summaries_to_console is None

    def test_structured_logs_custom_sample_sizes(self):
        """Test that StructuredLogs accepts custom sample sizes."""
        logs = StructuredLogs(
            failure_sample_size=50,
            warning_sample_size=25,
            log_failure_summaries_to_console=True,
            log_warning_summaries_to_console=True,
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

    def test_structured_logs_verbose_failure_logging(self, caplog):
        """Test that log_failure_summaries_to_console logs every failure."""
        logs = StructuredLogs(log_failure_summaries_to_console=True)

        with caplog.at_level(logging.ERROR):
            logs.report_log(
                StructuredLogLevel.ERROR,
                message="test failure",
                context="test context",
            )

        assert "test failure" in caplog.text
        assert "test context" in caplog.text

    def test_structured_logs_verbose_warning_logging(self, caplog):
        """Test that log_warning_summaries_to_console logs every warning."""
        logs = StructuredLogs(log_warning_summaries_to_console=True)

        with caplog.at_level(logging.WARNING):
            logs.report_log(
                StructuredLogLevel.WARN,
                message="test warning",
                context="test context",
            )

        assert "test warning" in caplog.text

    def test_structured_logs_none_uses_caller_choice(self, caplog):
        """Test that log_failure_summaries_to_console=None uses caller's log param."""
        logs = StructuredLogs()  # log_failure_summaries_to_console=None by default

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

    def test_structured_logs_true_forces_logging(self, caplog):
        """Test that log_failure_summaries_to_console=True forces logging even when log=False."""
        logs = StructuredLogs(log_failure_summaries_to_console=True)

        with caplog.at_level(logging.ERROR):
            logs.report_log(
                StructuredLogLevel.ERROR,
                message="forced log failure",
                context="context",
                log=False,  # This should be overridden
            )

        assert "forced log failure" in caplog.text

    def test_structured_logs_false_suppresses_logging(self, caplog):
        """Test that log_failure_summaries_to_console=False suppresses logging even when log=True."""
        logs = StructuredLogs(log_failure_summaries_to_console=False)

        with caplog.at_level(logging.ERROR):
            logs.report_log(
                StructuredLogLevel.ERROR,
                message="suppressed failure",
                context="context",
                log=True,  # This should be overridden/suppressed
            )

        assert "suppressed failure" not in caplog.text


class TestSinkReport:
    """Tests for SinkReport configurable sample sizes and verbose logging."""

    def test_sink_report_default_sample_sizes(self):
        """Test that SinkReport uses default sample sizes."""
        report = SinkReport()
        assert report.failure_sample_size == 10
        assert report.warning_sample_size == 10
        assert report.log_failure_summaries_to_console is None
        assert report.log_warning_summaries_to_console is None
        # Verify LossyLists have correct max_elements
        assert report.failures.max_elements == 10
        assert report.warnings.max_elements == 10

    def test_sink_report_configure_report(self):
        """Test that configure_report updates settings correctly."""
        report = SinkReport()
        report.configure_report(
            failure_sample_size=50,
            warning_sample_size=25,
            log_failure_summaries_to_console=True,
            log_warning_summaries_to_console=True,
        )
        assert report.failure_sample_size == 50
        assert report.warning_sample_size == 25
        assert report.log_failure_summaries_to_console is True
        assert report.log_warning_summaries_to_console is True
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

    def test_sink_report_verbose_failure_logging(self, caplog):
        """Test that log_failure_summaries_to_console logs every failure."""
        report = SinkReport()
        report.configure_report(log_failure_summaries_to_console=True)

        with caplog.at_level(logging.ERROR):
            report.report_failure({"error": "test sink failure", "urn": "urn:li:test"})

        assert "test sink failure" in caplog.text

    def test_sink_report_verbose_warning_logging(self, caplog):
        """Test that log_warning_summaries_to_console logs every warning."""
        report = SinkReport()
        report.configure_report(log_warning_summaries_to_console=True)

        with caplog.at_level(logging.WARNING):
            report.report_warning({"warning": "test sink warning"})

        assert "test sink warning" in caplog.text

    def test_sink_report_none_uses_caller_choice(self, caplog):
        """Test that log_failure_summaries_to_console=None uses caller's log param."""
        report = SinkReport()  # log_failure_summaries_to_console=None by default

        with caplog.at_level(logging.ERROR):
            report.report_failure({"error": "silent failure"}, log=False)
            report.report_failure({"error": "logged failure"}, log=True)

        assert "silent failure" not in caplog.text
        assert "logged failure" in caplog.text

    def test_sink_report_true_forces_logging(self, caplog):
        """Test that log_failure_summaries_to_console=True forces logging even when log=False."""
        report = SinkReport()
        report.configure_report(log_failure_summaries_to_console=True)

        with caplog.at_level(logging.ERROR):
            report.report_failure({"error": "forced log"}, log=False)

        assert "forced log" in caplog.text

    def test_sink_report_false_suppresses_logging(self, caplog):
        """Test that log_failure_summaries_to_console=False suppresses logging even when log=True."""
        report = SinkReport()
        report.configure_report(log_failure_summaries_to_console=False)

        with caplog.at_level(logging.ERROR):
            report.report_failure({"error": "suppressed failure"}, log=True)

        assert "suppressed failure" not in caplog.text

    def test_sink_report_warning_log_param(self, caplog):
        """Test log parameter on report_warning."""
        report = SinkReport()

        with caplog.at_level(logging.WARNING):
            report.report_warning({"warning": "explicit warning"}, log=True)

        assert "explicit warning" in caplog.text


class TestSourceReport:
    """Tests for SourceReport configurable sample sizes and verbose logging."""

    def test_source_report_default_sample_sizes(self):
        """Test that SourceReport uses default sample sizes."""
        report = SourceReport()
        assert report.failure_sample_size == 10
        assert report.warning_sample_size == 10
        assert report.log_failure_summaries_to_console is None
        assert report.log_warning_summaries_to_console is None

    def test_source_report_configure_report(self):
        """Test that configure_report updates settings correctly."""
        report = SourceReport()
        report.configure_report(
            failure_sample_size=100,
            warning_sample_size=50,
            log_failure_summaries_to_console=True,
            log_warning_summaries_to_console=True,
        )
        assert report.failure_sample_size == 100
        assert report.warning_sample_size == 50
        assert report.log_failure_summaries_to_console is True
        assert report.log_warning_summaries_to_console is True
        # Verify StructuredLogs was reinitialized with correct settings
        assert report._structured_logs.failure_sample_size == 100
        assert report._structured_logs.warning_sample_size == 50
        assert report._structured_logs.log_failure_summaries_to_console is True
        assert report._structured_logs.log_warning_summaries_to_console is True

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

    def test_source_report_verbose_failure_logging(self, caplog):
        """Test that log_failure_summaries_to_console logs every failure through StructuredLogs."""
        report = SourceReport()
        report.configure_report(log_failure_summaries_to_console=True)

        with caplog.at_level(logging.ERROR):
            # Use report_failure with log=False to test that verbose flag overrides
            report.report_failure(
                message="verbose test failure", context="test context"
            )

        assert "verbose test failure" in caplog.text

    def test_source_report_verbose_warning_logging(self, caplog):
        """Test that log_warning_summaries_to_console logs every warning through StructuredLogs."""
        report = SourceReport()
        report.configure_report(log_warning_summaries_to_console=True)

        with caplog.at_level(logging.WARNING):
            report.report_warning(
                message="verbose test warning", context="test context"
            )

        assert "verbose test warning" in caplog.text


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

    def test_verbose_logging_with_sampling(self, caplog):
        """Test that verbose logging works even when sampling."""
        report = SinkReport()
        report.configure_report(
            failure_sample_size=2, log_failure_summaries_to_console=True
        )

        with caplog.at_level(logging.ERROR):
            # Add more failures than sample size
            for i in range(5):
                report.report_failure({"error": f"failure_{i}"})

        # All failures should be logged even though only 2 are sampled
        for i in range(5):
            assert f"failure_{i}" in caplog.text

        # But only 2 are in the report sample
        assert report.failures.total_elements == 5
        assert report.failures.sampled is True


class TestEnvVarFunctions:
    """Tests for environment variable getter functions."""

    def test_get_report_log_failure_summaries_to_console_true(self, monkeypatch):
        """Test get_report_log_failure_summaries_to_console returns True when env var is 'true'."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE", "true")
        assert env_vars.get_report_log_failure_summaries_to_console() is True

    def test_get_report_log_failure_summaries_to_console_false(self, monkeypatch):
        """Test get_report_log_failure_summaries_to_console returns False when env var is 'false'."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE", "false")
        assert env_vars.get_report_log_failure_summaries_to_console() is False

    def test_get_report_log_failure_summaries_to_console_unset(self, monkeypatch):
        """Test get_report_log_failure_summaries_to_console returns None when env var is unset."""
        from datahub.configuration import env_vars

        monkeypatch.delenv(
            "DATAHUB_REPORT_LOG_FAILURE_SUMMARIES_TO_CONSOLE", raising=False
        )
        assert env_vars.get_report_log_failure_summaries_to_console() is None

    def test_get_report_log_warning_summaries_to_console_true(self, monkeypatch):
        """Test get_report_log_warning_summaries_to_console returns True when env var is 'true'."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE", "true")
        assert env_vars.get_report_log_warning_summaries_to_console() is True

    def test_get_report_log_warning_summaries_to_console_false(self, monkeypatch):
        """Test get_report_log_warning_summaries_to_console returns False when env var is 'false'."""
        from datahub.configuration import env_vars

        monkeypatch.setenv("DATAHUB_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE", "false")
        assert env_vars.get_report_log_warning_summaries_to_console() is False

    def test_get_report_log_warning_summaries_to_console_unset(self, monkeypatch):
        """Test get_report_log_warning_summaries_to_console returns None when env var is unset."""
        from datahub.configuration import env_vars

        monkeypatch.delenv(
            "DATAHUB_REPORT_LOG_WARNING_SUMMARIES_TO_CONSOLE", raising=False
        )
        assert env_vars.get_report_log_warning_summaries_to_console() is None

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
