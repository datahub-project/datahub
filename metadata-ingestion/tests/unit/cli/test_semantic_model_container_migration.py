"""Unit tests for datahub.cli.semantic_model_container_migration."""

from typing import Dict, List, Optional, Type
from unittest.mock import MagicMock

from datahub.cli.semantic_model_container_migration import (
    migrate_semantic_model,
    run_migration,
)
from datahub.metadata.schema_classes import (
    EdgeClass,
    MetricUpstreamsClass,
    SemanticModelInfoClass,
    _Aspect,
)

_SM_URN = "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics,orders_model)"
_DS1 = "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.orders_ds,PROD)"
_DS2 = (
    "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.customers_ds,PROD)"
)
_METRIC = "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,revenue)"
_METRIC2 = "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,orders)"
_METRIC3 = "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,customers)"


def _graph_mock(
    *,
    exists: bool = True,
    info: Optional[SemanticModelInfoClass] = None,
    metric_upstreams: Optional[Dict[str, MetricUpstreamsClass]] = None,
    metric_urns: Optional[List[str]] = None,
) -> MagicMock:
    graph = MagicMock()
    graph.exists.return_value = exists
    metric_upstreams = metric_upstreams or {}

    def get_aspect(urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
        if aspect_type is SemanticModelInfoClass and urn == _SM_URN:
            return info
        if aspect_type is MetricUpstreamsClass:
            return metric_upstreams.get(urn)
        return None

    graph.get_aspect.side_effect = get_aspect
    graph.get_urns_by_filter.return_value = iter(metric_urns or [])
    return graph


class TestReportSemanticModelOldShape:
    def test_reports_metrics_missing_upstreams(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1, _DS2]),
            metric_urns=[_METRIC, _METRIC2, _METRIC3],
            metric_upstreams={
                _METRIC: MetricUpstreamsClass(
                    datasetUpstreams=[EdgeClass(destinationUrn=_DS1)]
                ),
            },
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.datasets_seen == [_DS1, _DS2]
        assert result.metrics_seen == [_METRIC, _METRIC2, _METRIC3]
        assert result.metrics_missing_upstreams == [_METRIC2, _METRIC3]
        graph.emit_mcp.assert_not_called()


class TestReportAllMetricsOnNewShape:
    def test_new_shape_ready_when_all_metrics_have_upstreams(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[_METRIC],
            metric_upstreams={
                _METRIC: MetricUpstreamsClass(
                    datasetUpstreams=[EdgeClass(destinationUrn=_DS1)]
                ),
            },
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.metrics_missing_upstreams == []
        graph.emit_mcp.assert_not_called()


class TestReportEmptyDatasets:
    def test_empty_datasets_is_skipped(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[]),
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.skipped_empty_datasets is True
        assert result.datasets_seen == []
        assert result.metrics_missing_upstreams == []
        assert result.metrics_seen == []
        assert any("empty or missing" in n for n in result.notes)
        graph.get_urns_by_filter.assert_not_called()


class TestMetricFailureIsolation:
    def test_metric_failure_does_not_skip_remaining(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[_METRIC, _METRIC2, _METRIC3],
            metric_upstreams={},
        )

        def get_aspect(urn: str, aspect_type: Type[_Aspect]) -> Optional[_Aspect]:
            if aspect_type is SemanticModelInfoClass and urn == _SM_URN:
                return SemanticModelInfoClass(name="orders_model", datasets=[_DS1])
            if aspect_type is MetricUpstreamsClass:
                if urn == _METRIC2:
                    raise RuntimeError("graph read failed")
                return None
            return None

        graph.get_aspect.side_effect = get_aspect

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.metrics_seen == [_METRIC, _METRIC2, _METRIC3]
        assert _METRIC in result.metrics_missing_upstreams
        assert _METRIC3 in result.metrics_missing_upstreams
        assert _METRIC2 not in result.metrics_missing_upstreams
        assert result.metric_errors == [(_METRIC2, "graph read failed")]


class TestDryRun:
    def test_dry_run_is_report_only(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=True)

        assert result.metrics_missing_upstreams == [_METRIC]
        graph.emit_mcp.assert_not_called()


class TestRunMigration:
    def test_aggregates_results(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[],
        )
        report = run_migration(graph, [_SM_URN], dry_run=True)
        assert len(report.results) == 1
        assert report.dry_run is True
        text = repr(report)
        assert "Semantic Model Container Migration Report" in text
        assert "[Dry Run]" in text
        assert "on the old shape" in text
        assert "empty semanticModelInfo.datasets) = 0" in text

    def test_report_separates_empty_datasets_skips(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[]),
            metric_urns=[],
        )
        report = run_migration(graph, [_SM_URN], dry_run=True)
        text = repr(report)
        assert "on the old shape" in text and "= 0" in text
        assert "empty semanticModelInfo.datasets) = 1" in text
        assert f"skipped: {_SM_URN}" in text

    def test_report_lists_needs_reingest(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[_METRIC],
        )
        report = run_migration(graph, [_SM_URN], dry_run=False)
        text = repr(report)
        assert "re-ingest required) = 1" in text
        assert "metrics missing metricUpstreams" in text
        assert _METRIC in text
