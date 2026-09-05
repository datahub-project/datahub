from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import OperationalError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.analytics_compaction.analytics_compaction_source import (
    COMPACT_PATH,
    DataHubAnalyticsCompactionSource,
)


def _source_with_graph(response=None, graph=None):
    ctx = PipelineContext(run_id="test")
    if graph is None:
        graph = MagicMock()
        graph.config.server = "http://localhost:8080"
        graph.session.post.return_value = response
    ctx.graph = graph
    return DataHubAnalyticsCompactionSource.create({}, ctx), graph


def test_soft_skip_on_503():
    response = MagicMock()
    response.status_code = 503
    source, _ = _source_with_graph(response)
    assert list(source.get_workunits_internal()) == []
    assert source.report.skipped_unavailable is True


def test_records_compact_result():
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
    source, _ = _source_with_graph(response)
    assert list(source.get_workunits_internal()) == []
    assert source.report.hours_sealed == 2
    assert source.report.days_compacted == 1
    assert source.report.more_work_remaining is True
    assert source.report.implementation == "pgAnalytics"


def test_records_lock_not_acquired():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "lockNotAcquired": True,
        "moreWorkRemaining": False,
        "hoursSealed": 0,
        "daysCompacted": 0,
        "monthsCompacted": 0,
        "implementation": "pgAnalytics",
    }
    source, _ = _source_with_graph(response)
    assert list(source.get_workunits_internal()) == []
    assert source.report.lock_not_acquired is True


def test_sends_config_overrides_in_payload():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {
        "lockNotAcquired": False,
        "moreWorkRemaining": False,
        "hoursSealed": 0,
        "daysCompacted": 0,
        "monthsCompacted": 0,
    }
    ctx = PipelineContext(run_id="test")
    graph = MagicMock()
    graph.config.server = "http://localhost:8080/"
    graph.session.post.return_value = response
    ctx.graph = graph
    source = DataHubAnalyticsCompactionSource.create(
        {
            "max_hours_to_seal": 10,
            "max_days_to_compact": 5,
            "max_months_to_compact": 2,
            "max_wall_clock_millis": 60000,
        },
        ctx,
    )
    assert list(source.get_workunits_internal()) == []
    graph.session.post.assert_called_once_with(
        f"http://localhost:8080{COMPACT_PATH}",
        json={
            "maxHoursToSeal": 10,
            "maxDaysToCompact": 5,
            "maxMonthsToCompact": 2,
            "maxWallClockMillis": 60000,
        },
    )


def test_missing_graph_raises():
    ctx = PipelineContext(run_id="test")
    ctx.graph = None
    source = DataHubAnalyticsCompactionSource.create({}, ctx)
    with pytest.raises(OperationalError, match="graph client is required"):
        list(source.get_workunits_internal())


def test_http_error_raises():
    response = MagicMock()
    response.status_code = 500
    response.text = "boom"
    source, _ = _source_with_graph(response)
    with pytest.raises(OperationalError, match="status=500"):
        list(source.get_workunits_internal())


def test_transport_error_raises():
    ctx = PipelineContext(run_id="test")
    graph = MagicMock()
    graph.config.server = "http://localhost:8080"
    graph.session.post.side_effect = RuntimeError("connection refused")
    ctx.graph = graph
    source = DataHubAnalyticsCompactionSource.create({}, ctx)
    with pytest.raises(OperationalError, match="compact request failed"):
        list(source.get_workunits_internal())


def test_get_report_returns_source_report():
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"lockNotAcquired": False}
    source, _ = _source_with_graph(response)
    list(source.get_workunits_internal())
    assert source.get_report() is source.report
    assert source.report.event_not_produced_warn is False
