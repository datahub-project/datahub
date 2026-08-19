import json
from datetime import datetime, timezone

from datahub.ingestion.source.datahub.quarantine import QuarantineWriter


def _row() -> dict:
    return {
        "urn": "urn:li:dashboard:(my_tool,my dashboard (copy))",
        "aspect": "dashboardInfo",
        "version": 0,
        "createdon": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "metadata": '{"title": "x"}',
        "systemmetadata": "{}",
    }


def test_creates_no_file_when_nothing_is_written(tmp_path):
    """A clean run must leave no artifact behind."""
    path = tmp_path / "parse-errors.jsonl"
    writer = QuarantineWriter(str(path))
    writer.close()

    assert not path.exists()
    assert writer.records_written == 0


def test_writes_one_ndjson_line_per_row(tmp_path):
    path = tmp_path / "parse-errors.jsonl"
    writer = QuarantineWriter(str(path))
    writer.write(_row(), "DashboardUrn dashboard_id contains reserved characters")
    writer.write(_row(), "KeyError: 'someRemovedAspect'")
    writer.close()

    lines = path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    assert writer.records_written == 2

    first = json.loads(lines[0])
    assert first["error"] == "DashboardUrn dashboard_id contains reserved characters"
    assert first["row"]["urn"] == "urn:li:dashboard:(my_tool,my dashboard (copy))"
    assert first["row"]["aspect"] == "dashboardInfo"
    # createdon is a datetime and must survive serialisation rather than blowing up.
    assert first["row"]["createdon"].startswith("2026-01-01")


def test_disables_itself_when_the_file_cannot_be_written(tmp_path):
    """A diagnostic artifact must never fail the run."""
    path = tmp_path / "no-such-dir" / "parse-errors.jsonl"
    writer = QuarantineWriter(str(path))

    writer.write(_row(), "some error")  # must not raise
    writer.write(_row(), "another error")

    assert writer.records_written == 0
    writer.close()
