import time

from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport


def test_ingestion_stage_context_records_duration():
    report = IngestionStageReport()
    with report.new_stage(stage="Test Stage"):
        pass
    assert len(report.ingestion_stage_durations) == 1
    assert "Test Stage" in next(iter(report.ingestion_stage_durations.keys()))


def test_ingestion_stage_context_handles_exceptions():
    report = IngestionStageReport()
    try:
        with report.new_stage(stage="Test Stage"):
            raise ValueError("Test Exception")
    except ValueError:
        pass
    assert len(report.ingestion_stage_durations) == 1
    assert "Test Stage" in next(iter(report.ingestion_stage_durations))


def test_ingestion_stage_context_report_handles_multiple_stages():
    report = IngestionStageReport()
    with report.new_stage(stage="Test Stage 1"):
        time.sleep(0.1)
    with report.new_stage(stage="Test Stage 2"):
        time.sleep(0.1)
    with report.new_stage(stage="Test Stage 3"):
        time.sleep(0.1)
    assert len(report.ingestion_stage_durations) == 3
    assert all(
        isinstance(duration, float) and duration > 0.0
        for duration in report.ingestion_stage_durations.values()
    )

    sorted_stages = list(sorted(report.ingestion_stage_durations.keys()))
    assert "Test Stage 1" in sorted_stages[0]
    assert "Test Stage 2" in sorted_stages[1]
    assert "Test Stage 3" in sorted_stages[2]
