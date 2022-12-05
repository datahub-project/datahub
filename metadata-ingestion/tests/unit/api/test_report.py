import json

from datahub.ingestion.api.source import SourceReport


def test_report_to_string_unsampled():
    source_report: SourceReport = SourceReport()
    source_report.report_warning("key1", "problem 1")
    source_report.report_failure("key2", "reason 2")
    str = source_report.as_string()
    assert "'warnings': {'key1': ['problem 1']}" in str
    assert "'failures': {'key2': ['reason 2']}" in str


def test_report_to_string_sampled():
    source_report = SourceReport()
    for i in range(0, 100):
        source_report.report_warning(f"key{i}", "Test message")

    str = source_report.as_string()
    assert "'sampled': '10 sampled of at most 100 entries.'" in str
    str = source_report.as_json()
    print(str)
    report_as_dict = json.loads(str)
    assert len(report_as_dict["warnings"]) == 11
