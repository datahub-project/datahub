import json
from unittest.mock import Mock

from datahub.ingestion.reporting.file_reporter import FileReporter, FileReporterConfig
from datahub.masking.secret_registry import SecretRegistry


def _write_report(tmp_path, report):
    out_file = tmp_path / "report.json"
    reporter = FileReporter(FileReporterConfig(filename=str(out_file)))
    reporter.on_completion(status="SUCCESS", report=report, ctx=Mock())
    return out_file.read_text()


def test_report_is_written_masked(tmp_path):
    SecretRegistry.reset_instance()
    SecretRegistry.get_instance().register_secret("DB_PASS", "hunter2secret")

    content = _write_report(
        tmp_path,
        {"source": {"failures": ["connect failed for pw=hunter2secret"]}},
    )

    assert "hunter2secret" not in content
    assert "***REDACTED:DB_PASS***" in content
    SecretRegistry.reset_instance()


def test_report_without_secrets_written_verbatim(tmp_path):
    SecretRegistry.reset_instance()
    report = {"source": {"events_produced": 200}, "cli": {"version": "1.0"}}

    content = _write_report(tmp_path, report)

    assert json.loads(content) == report
    SecretRegistry.reset_instance()
