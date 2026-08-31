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


def test_secret_with_quotes_masked_despite_json_escaping(tmp_path):
    SecretRegistry.reset_instance()
    secret = 'pä"ssword123'
    SecretRegistry.get_instance().register_secret("DB_PASS", secret)

    content = _write_report(tmp_path, {"failures": [f"auth failed for {secret}"]})

    assert secret not in content
    assert json.dumps(secret)[1:-1] not in content
    assert "***REDACTED:DB_PASS***" in content
    assert json.loads(content) == {
        "failures": ["auth failed for ***REDACTED:DB_PASS***"]
    }
    SecretRegistry.reset_instance()


def test_flag_disables_report_masking(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAHUB_DISABLE_SECRET_MASKING", "true")
    SecretRegistry.reset_instance()
    SecretRegistry.get_instance().register_secret("DB_PASS", "hunter2secret")

    content = _write_report(tmp_path, {"failures": ["pw=hunter2secret"]})

    assert json.loads(content) == {"failures": ["pw=hunter2secret"]}
    SecretRegistry.reset_instance()
