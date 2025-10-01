import time

import pytest

from datahub.ingestion.source_report.ingestion_stage import (
    IngestionHighStage,
    IngestionStageReport,
)


def test_ingestion_stage_context_records_duration():
    report = IngestionStageReport()
    with report.new_stage(stage="Test Stage"):
        pass
    assert len(report.ingestion_stage_durations) == 1
    key = next(iter(report.ingestion_stage_durations.keys()))
    assert key[0] == IngestionHighStage._UNDEFINED.value
    assert "Test Stage" in key[1]


def test_ingestion_stage_context_handles_exceptions():
    report = IngestionStageReport()
    try:
        with report.new_stage(stage="Test Stage"):
            raise ValueError("Test Exception")
    except ValueError:
        pass
    assert len(report.ingestion_stage_durations) == 1
    key = next(iter(report.ingestion_stage_durations.keys()))
    assert "Test Stage" in key[1]


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
    assert "Test Stage 1" in sorted_stages[0][1]
    assert "Test Stage 2" in sorted_stages[1][1]
    assert "Test Stage 3" in sorted_stages[2][1]


def test_ingestion_stage_context_report_handles_nested_stages():
    report = IngestionStageReport()
    with report.new_stage(stage="Outer"):
        with report.new_stage(stage="Inner1"):
            time.sleep(0.1)
        with report.new_stage(stage="Inner2"):
            time.sleep(0.1)

    assert len(report.ingestion_stage_durations) == 3
    assert all(
        isinstance(duration, float) and duration > 0.0
        for duration in report.ingestion_stage_durations.values()
    )
    sorted_stages = list(sorted(report.ingestion_stage_durations.keys()))
    assert "Inner1" in sorted_stages[0][1]
    assert "Inner2" in sorted_stages[1][1]
    assert "Outer" in sorted_stages[2][1]

    # Check that outer stage duration >= sum of inner stage durations
    outer_key = [k for k in report.ingestion_stage_durations if "Outer" in k[1]][0]
    inner1_key = [k for k in report.ingestion_stage_durations if "Inner1" in k[1]][0]
    inner2_key = [k for k in report.ingestion_stage_durations if "Inner2" in k[1]][0]

    outer_duration = report.ingestion_stage_durations[outer_key]
    inner1_duration = report.ingestion_stage_durations[inner1_key]
    inner2_duration = report.ingestion_stage_durations[inner2_key]

    inner_sum = inner1_duration + inner2_duration
    # generous tolerance to avoid flakiness in test
    assert outer_duration == pytest.approx(inner_sum, abs=0.05)


def test_ingestion_high_stage_context_records_duration():
    report = IngestionStageReport()
    with report.new_high_stage(stage=IngestionHighStage.PROFILING):
        time.sleep(0.1)
    assert len(report.ingestion_high_stage_seconds) == 1
    assert IngestionHighStage.PROFILING.value in report.ingestion_high_stage_seconds
    assert report.ingestion_high_stage_seconds[IngestionHighStage.PROFILING.value] > 0


def test_ingestion_stage_with_high_stage():
    report = IngestionStageReport()
    with report.new_stage(stage="Test Stage", high_stage=IngestionHighStage.PROFILING):
        time.sleep(0.1)
    assert len(report.ingestion_stage_durations) == 1
    key = next(iter(report.ingestion_stage_durations.keys()))
    assert key[0] == IngestionHighStage.PROFILING.value
    assert "Test Stage" in key[1]
    assert report.ingestion_high_stage_seconds[IngestionHighStage.PROFILING.value] > 0
