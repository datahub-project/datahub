"""upstreamMetrics on chart, dashboard, and dataset.

Omit leaves the aspect off the entity. A list fully replaces it. ``[]`` emits
an empty list, which is distinct from omit and clears stored edges on upsert.
"""

from typing import Callable, Union

import pytest

import datahub.metadata.schema_classes as models
from datahub.errors import SdkUsageError
from datahub.metadata.urns import DatasetUrn, MetricUrn
from datahub.sdk.chart import Chart
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity
from datahub.sdk.metric import Metric

METRIC_A = "urn:li:metric:(urn:li:dataPlatform:snowflake,finance,total_revenue)"
METRIC_B = "urn:li:metric:(urn:li:dataPlatform:snowflake,finance,margin)"
DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.orders,PROD)"
MODEL = "urn:li:semanticModel:(urn:li:dataPlatform:snowflake,analytics,orders_model)"

Consumer = Union[Chart, Dashboard, Dataset]
EntityFactory = Callable[[], Consumer]


def _chart() -> Chart:
    return Chart(platform="looker", name="rev_by_region")


def _dashboard() -> Dashboard:
    return Dashboard(platform="looker", name="exec_overview")


def _dataset() -> Dataset:
    return Dataset(platform="snowflake", name="db.sales.orders")


def _metric_entity() -> Metric:
    return Metric(
        platform="snowflake",
        path="finance",
        id="total_revenue",
        semantic_model=MODEL,
    )


FACTORIES = [_chart, _dashboard, _dataset]


def _metric_aspect(entity: Entity) -> models.UpstreamMetricsClass:
    aspects = [
        mcp.aspect
        for mcp in entity.as_mcps()
        if mcp.aspectName == "upstreamMetrics" and mcp.aspect is not None
    ]
    assert len(aspects) == 1
    aspect = aspects[0]
    assert isinstance(aspect, models.UpstreamMetricsClass)
    return aspect


@pytest.mark.parametrize("factory", FACTORIES)
def test_omit_does_not_emit_upstream_metrics(factory: EntityFactory) -> None:
    entity: Consumer = factory()
    assert entity.upstream_metrics is None
    assert "upstreamMetrics" not in {mcp.aspectName for mcp in entity.as_mcps()}


@pytest.mark.parametrize("factory", FACTORIES)
def test_list_replaces_and_edges_carry_destination_only(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics([METRIC_A, MetricUrn.from_string(METRIC_B)])

    aspect = _metric_aspect(entity)
    assert [edge.destinationUrn for edge in aspect.metrics] == [METRIC_A, METRIC_B]
    for edge in aspect.metrics:
        assert edge.sourceUrn is None
        assert edge.created is None
        assert edge.lastModified is None
        assert edge.properties is None
    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_A, METRIC_B]


@pytest.mark.parametrize("factory", FACTORIES)
def test_empty_list_clears_and_is_distinct_from_omit(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics([METRIC_A])
    entity.set_upstream_metrics([])

    assert entity.upstream_metrics == []
    assert _metric_aspect(entity).metrics == []


@pytest.mark.parametrize("factory", FACTORIES)
def test_second_call_replaces_rather_than_appends(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics([METRIC_A])
    entity.set_upstream_metrics([METRIC_B])

    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_B]


@pytest.mark.parametrize("factory", FACTORIES)
def test_dedupe_preserves_first_seen_order(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics(
        [METRIC_B, METRIC_A, MetricUrn.from_string(METRIC_A), _metric_entity()]
    )

    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_B, METRIC_A]


@pytest.mark.parametrize("factory", FACTORIES)
def test_metric_entity_is_written(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics([_metric_entity()])

    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_A]


@pytest.mark.parametrize("factory", FACTORIES)
def test_scalar_metric_urn_is_one_edge(factory: EntityFactory) -> None:
    entity = factory()
    entity.set_upstream_metrics(METRIC_A)

    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_A]


@pytest.mark.parametrize(
    "bad",
    [
        DATASET,
        DatasetUrn.from_string(DATASET),
        _dataset(),
        "urn:li:metric:not-a-metric-urn",
    ],
)
@pytest.mark.parametrize("factory", FACTORIES)
def test_non_metric_urn_raises_and_keeps_prior_list(
    factory: EntityFactory, bad: object
) -> None:
    entity = factory()
    entity.set_upstream_metrics([METRIC_A])

    with pytest.raises(SdkUsageError, match="metric URN"):
        entity.set_upstream_metrics([METRIC_B, bad])  # type: ignore[list-item]

    assert entity.upstream_metrics is not None
    assert [str(urn) for urn in entity.upstream_metrics] == [METRIC_A]


@pytest.mark.parametrize("factory", FACTORIES)
def test_failed_write_on_empty_entity_stays_absent(factory: EntityFactory) -> None:
    entity = factory()
    with pytest.raises(SdkUsageError, match="metric URN"):
        entity.set_upstream_metrics([DATASET])

    assert entity.upstream_metrics is None
    assert "upstreamMetrics" not in {mcp.aspectName for mcp in entity.as_mcps()}


@pytest.mark.parametrize("factory", FACTORIES)
def test_constructor_kwarg_matches_setter(factory: EntityFactory) -> None:
    via_setter = factory()
    via_setter.set_upstream_metrics([METRIC_A, METRIC_B])

    if isinstance(via_setter, Chart):
        via_constructor: Entity = Chart(
            platform="looker",
            name="rev_by_region",
            upstream_metrics=[METRIC_A, METRIC_B],
        )
    elif isinstance(via_setter, Dashboard):
        via_constructor = Dashboard(
            platform="looker",
            name="exec_overview",
            upstream_metrics=[METRIC_A, METRIC_B],
        )
    else:
        via_constructor = Dataset(
            platform="snowflake",
            name="db.sales.orders",
            upstream_metrics=[METRIC_A, METRIC_B],
        )

    assert _metric_aspect(via_constructor) == _metric_aspect(via_setter)


def test_chart_dataset_and_metric_inputs_do_not_cross() -> None:
    dataset_then_metric = _chart()
    dataset_then_metric.set_input_datasets([DATASET])
    dataset_then_metric.set_upstream_metrics([METRIC_A])

    metric_then_dataset = _chart()
    metric_then_dataset.set_upstream_metrics([METRIC_A])
    metric_then_dataset.set_input_datasets([DATASET])

    for chart in (dataset_then_metric, metric_then_dataset):
        assert [str(urn) for urn in chart.input_datasets] == [DATASET]
        assert chart.upstream_metrics is not None
        assert [str(urn) for urn in chart.upstream_metrics] == [METRIC_A]


def test_dashboard_dataset_and_metric_inputs_do_not_cross() -> None:
    dataset_then_metric = _dashboard()
    dataset_then_metric.set_input_datasets([DATASET])
    dataset_then_metric.set_upstream_metrics([METRIC_A])

    metric_then_dataset = _dashboard()
    metric_then_dataset.set_upstream_metrics([METRIC_A])
    metric_then_dataset.set_input_datasets([DATASET])

    for dashboard in (dataset_then_metric, metric_then_dataset):
        assert [str(urn) for urn in dashboard.input_datasets] == [DATASET]
        assert dashboard.upstream_metrics is not None
        assert [str(urn) for urn in dashboard.upstream_metrics] == [METRIC_A]


def test_dataset_upstreams_and_metrics_do_not_cross() -> None:
    dataset_then_metric = _dataset()
    dataset_then_metric.set_upstreams([DATASET])
    dataset_then_metric.set_upstream_metrics([METRIC_A])

    metric_then_dataset = _dataset()
    metric_then_dataset.set_upstream_metrics([METRIC_A])
    metric_then_dataset.set_upstreams([DATASET])

    for dataset in (dataset_then_metric, metric_then_dataset):
        assert dataset.upstreams is not None
        assert [upstream.dataset for upstream in dataset.upstreams.upstreams] == [
            DATASET
        ]
        assert dataset.upstream_metrics is not None
        assert [str(urn) for urn in dataset.upstream_metrics] == [METRIC_A]


def test_chart_rejects_metric_urn_as_dataset_input() -> None:
    chart = _chart()
    chart.set_input_datasets([DATASET])

    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        chart.set_input_datasets([DATASET, METRIC_A])
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        chart.add_input_dataset(METRIC_A)

    assert [str(urn) for urn in chart.input_datasets] == [DATASET]
    assert chart.upstream_metrics is None


def test_dashboard_rejects_metric_urn_as_dataset_input() -> None:
    dashboard = _dashboard()
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dashboard.set_input_datasets([METRIC_A])
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dashboard.add_input_dataset(METRIC_A)
    assert dashboard.upstream_metrics is None


def test_dataset_rejects_metric_urn_in_each_upstream_shape() -> None:
    dataset = _dataset()
    dataset.set_upstreams([DATASET])

    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dataset.set_upstreams([DATASET, METRIC_A])
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dataset.set_upstreams({METRIC_A: {"amount": ["amount"]}})
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dataset.set_upstreams(
            models.UpstreamLineageClass(
                upstreams=[
                    models.UpstreamClass(
                        dataset=METRIC_A,
                        type=models.DatasetLineageTypeClass.TRANSFORMED,
                    )
                ]
            )
        )
    metric_upstream = models.UpstreamClass(
        dataset=METRIC_A,
        type=models.DatasetLineageTypeClass.TRANSFORMED,
    )
    metric_upstream.dataset = MetricUrn.from_string(METRIC_A)  # type: ignore[assignment]
    with pytest.raises(SdkUsageError, match="set_upstream_metrics"):
        dataset.set_upstreams([metric_upstream])

    assert dataset.upstreams is not None
    assert [upstream.dataset for upstream in dataset.upstreams.upstreams] == [DATASET]
    assert dataset.upstream_metrics is None


def test_dataset_lineage_class_without_metrics_is_unchanged() -> None:
    dataset = _dataset()
    dataset.set_upstreams(
        models.UpstreamLineageClass(
            upstreams=[
                models.UpstreamClass(
                    dataset=DATASET,
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                )
            ]
        )
    )

    assert dataset.upstream_metrics is None
    assert dataset.upstreams is not None
    assert [upstream.dataset for upstream in dataset.upstreams.upstreams] == [DATASET]
