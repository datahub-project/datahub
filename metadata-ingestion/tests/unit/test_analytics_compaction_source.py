from unittest.mock import MagicMock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.analytics_compaction.analytics_compaction_source import (
    DataHubAnalyticsCompactionSource,
)


def test_soft_skip_on_503():
    ctx = PipelineContext(run_id="test")
    graph = MagicMock()
    graph._gms_server = "http://localhost:8080"
    response = MagicMock()
    response.status_code = 503
    graph._session.post.return_value = response
    ctx.graph = graph

    source = DataHubAnalyticsCompactionSource.create({}, ctx)
    assert list(source.get_workunits_internal()) == []
    assert source.report.skipped_unavailable is True


def test_records_compact_result():
    ctx = PipelineContext(run_id="test")
    graph = MagicMock()
    graph._gms_server = "http://localhost:8080"
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "lockNotAcquired": False,
        "moreWorkRemaining": True,
        "hoursSealed": 2,
        "daysCompacted": 1,
        "monthsCompacted": 0,
        "implementation": "pgAnalytics",
    }
    graph._session.post.return_value = response
    ctx.graph = graph

    source = DataHubAnalyticsCompactionSource.create({}, ctx)
    assert list(source.get_workunits_internal()) == []
    assert source.report.hours_sealed == 2
    assert source.report.days_compacted == 1
    assert source.report.more_work_remaining is True
    assert source.report.implementation == "pgAnalytics"
