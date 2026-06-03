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

    def test_per_entry_context_respects_sample_size(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When many items are grouped under one error key (same title+message),
        every context line must be retained up to the configured sample size,
        not silently truncated to the LossyList default of 10.
        """
        monkeypatch.setenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "50")

        report = SourceReport()
        for i in range(24):
            report.failure(
                message="Error processing table",
                context=f"table_{i}",
                log=False,
            )

        failures = report.failures
        assert len(failures) == 1
        (entry,) = list(failures)
        assert entry.context.max_elements == 50
        assert not entry.context.sampled
        assert len(list(entry.context)) == 24

    def test_set_sample_sizes_resizes_existing_context(self) -> None:
        """set_sample_sizes must grow context lists on already-recorded entries.

        Sources can call report_warning/report_failure during __init__, before
        the pipeline applies the configured sample size. Those entries' nested
        context lists must be resized too, otherwise they remain capped at the
        default of 10.
        """
        report = SourceReport()
        for i in range(5):
            report.failure(
                message="Error processing table",
                context=f"table_{i}",
                log=False,
            )

        report.set_sample_sizes(failure_size=200, warning_size=200, info_size=200)

        for i in range(5, 30):
            report.failure(
                message="Error processing table",
                context=f"table_{i}",
                log=False,
            )

        (entry,) = list(report.failures)
        assert entry.context.max_elements == 200
        assert not entry.context.sampled
        assert len(list(entry.context)) == 30

    def test_set_sample_sizes_shrinks_existing_context(self) -> None:
        """set_sample_sizes must prune context lists when shrinking.

        If a source logs many context items at the default sample size, then
        the pipeline applies a smaller configured limit, excess contexts must
        be sampled down to the new limit.
        """
        report = SourceReport()
        for i in range(20):
            report.failure(
                message="Error processing table",
                context=f"table_{i}",
                log=False,
            )

        report.set_sample_sizes(failure_size=5, warning_size=5, info_size=5)

        (entry,) = list(report.failures)
        assert entry.context.max_elements == 5
        assert entry.context.sampled
        retained = list(entry.context)
        assert len(retained) == 5
        # All retained items must come from the original set.
        original = {f"table_{i}" for i in range(20)}
        assert set(retained) <= original
