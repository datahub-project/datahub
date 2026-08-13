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


class TestMigrateSemanticModelHappyPath:
    def test_backfills_metric_upstreams(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1, _DS2]),
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.datasets_seen == [_DS1, _DS2]
        assert result.upstreams_written == [_METRIC]
        assert graph.emit_mcp.call_count == 1

        mcp = graph.emit_mcp.call_args_list[0].args[0]
        assert mcp.entityUrn == _METRIC
        upstreams = mcp.aspect
        assert isinstance(upstreams, MetricUpstreamsClass)
        assert [e.destinationUrn for e in upstreams.datasetUpstreams or []] == [
            _DS1,
            _DS2,
        ]


class TestMigrateSemanticModelIdempotent:
    def test_rerun_skips_existing_upstreams(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_upstreams={
                _METRIC: MetricUpstreamsClass(
                    datasetUpstreams=[EdgeClass(destinationUrn=_DS1)]
                )
            },
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.upstreams_written == []
        assert any(
            "datasetUpstreams already set" in s for s in result.upstreams_skipped
        )
        graph.emit_mcp.assert_not_called()


class TestMigrateSemanticModelRespectsExistingUpstreams:
    def test_does_not_overwrite_existing_dataset_upstreams(self):
        other_ds = (
            "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.other,PROD)"
        )
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_upstreams={
                _METRIC: MetricUpstreamsClass(
                    datasetUpstreams=[EdgeClass(destinationUrn=other_ds)]
                )
            },
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.upstreams_written == []
        assert any(
            "datasetUpstreams already set" in s for s in result.upstreams_skipped
        )
        graph.emit_mcp.assert_not_called()


class TestMigrateSemanticModelEmptyDatasets:
    def test_empty_datasets_is_skipped_like_subtype_filter(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[]),
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=False)

        assert result.error is None
        assert result.skipped_empty_datasets is True
        assert result.datasets_seen == []
        assert result.upstreams_written == []
        assert result.metrics_seen == []
        assert any("empty or missing" in n for n in result.notes)
        graph.emit_mcp.assert_not_called()
        graph.get_urns_by_filter.assert_not_called()


class TestDryRun:
    def test_dry_run_does_not_emit(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[_DS1]),
            metric_urns=[_METRIC],
        )

        result = migrate_semantic_model(graph, _SM_URN, dry_run=True)

        assert result.upstreams_written == [_METRIC]
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
        assert "with stored datasets (old shape) = 1" in text
        assert "skipped (empty semanticModelInfo.datasets) = 0" in text

    def test_report_separates_empty_datasets_skips(self):
        graph = _graph_mock(
            info=SemanticModelInfoClass(name="orders_model", datasets=[]),
            metric_urns=[],
        )
        report = run_migration(graph, [_SM_URN], dry_run=True)
        text = repr(report)
        assert "with stored datasets (old shape) = 0" in text
        assert "skipped (empty semanticModelInfo.datasets) = 1" in text
        assert f"skipped: {_SM_URN}" in text
