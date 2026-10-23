import json

from datahub.ingestion.api.source import SourceReport


def test_report_to_string_unsampled():
    source_report: SourceReport = SourceReport()
    source_report.warning("key1", "problem 1", log=False)
    source_report.failure("key2", "reason 2")
    str = source_report.as_string()
    assert "'warnings': [{'message': 'key1', 'context': ['problem 1']}]" in str
    assert "'failures': [{'message': 'key2', 'context': ['reason 2']}]" in str


def test_report_to_string_sampled():
    source_report = SourceReport()
    for i in range(0, 100):
        source_report.warning(f"key{i}", "Test message", log=False)

    str = source_report.as_string()
    assert "sampled of 100 total elements" in str
    str = source_report.as_json()
    print(str)
    report_as_dict = json.loads(str)
    assert len(report_as_dict["warnings"]) == 11


def test_report_caps_exception_text():
    source_report = SourceReport()
    source_report.warning(
        "key1", "ctx", exc=ValueError("x" * 5000 + "secret_tail"), log=False
    )

    context = source_report.warnings[0].context[0]
    assert "ValueError" in context
    assert "secret_tail" not in context
