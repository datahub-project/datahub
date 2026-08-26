from typing import Dict, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
from datahub.ingestion.source.cube.cube_semantic_model import (
    CubeSemanticModelMapper,
    parse_cube_join_sql,
)
from datahub.ingestion.source.cube.models import (
    CubeEntity,
    CubeJoin,
    CubeMember,
)
from datahub.metadata.schema_classes import (
    ERModelRelationshipCardinalityClass,
    MetricInfoClass,
    SemanticModelInfoClass,
    SemanticModelPropertiesClass,
)


def _config(**overrides: object) -> CubeSourceConfig:
    base: Dict[str, object] = {
        "api_url": "https://demo.cubecloud.dev/cubejs-api",
        "api_token": "t",
        "emit_semantic_model_entities": True,
    }
    base.update(overrides)
    return CubeSourceConfig.model_validate(base)


def _mapper(**overrides: object) -> CubeSemanticModelMapper:
    config = _config(**overrides)
    return CubeSemanticModelMapper(
        config=config,
        path="demo",
        cube_dataset_urn_fn=lambda name: (
            f"urn:li:dataset:(urn:li:dataPlatform:cube,{name},PROD)"
        ),
        container_urn="urn:li:container:cube_demo",
        report=CubeSourceReport(),
    )


def _orders_cube() -> CubeEntity:
    return CubeEntity(
        name="orders",
        description="Customer orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.customer_id = {customers}.id",
            )
        ],
        measures=[
            CubeMember(name="count", is_measure=True, agg_type="count"),
            CubeMember(name="total_amount", is_measure=True, agg_type="sum"),
        ],
        dimensions=[
            CubeMember(name="id", is_measure=False, is_primary_key=True),
            CubeMember(name="status", is_measure=False),
            CubeMember(name="created_at", is_measure=False, is_temporal=True),
            CubeMember(name="customer_id", is_measure=False, is_hidden=True),
        ],
    )


def _customers_cube() -> CubeEntity:
    return CubeEntity(
        name="customers",
        description="Registered customers",
        dimensions=[
            CubeMember(name="id", is_measure=False, is_primary_key=True),
            CubeMember(name="name", is_measure=False),
            CubeMember(name="city", is_measure=False),
        ],
    )


def _orders_view() -> CubeEntity:
    return CubeEntity(
        name="orders_view",
        is_view=True,
        description="Query-ready orders",
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            ),
            CubeMember(
                name="total_amount",
                is_measure=True,
                agg_type="sum",
                member_references=["orders.total_amount"],
            ),
        ],
        dimensions=[
            CubeMember(
                name="status",
                is_measure=False,
                member_references=["orders.status"],
            ),
            CubeMember(
                name="created_at",
                is_measure=False,
                is_temporal=True,
                member_references=["orders.created_at"],
            ),
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            ),
            CubeMember(
                name="city",
                is_measure=False,
                member_references=["customers.city"],
            ),
        ],
    )


def _aspects_by_urn(wus: List[MetadataWorkUnit]) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for wu in wus:
        urn = wu.get_urn()
        aspect = wu.metadata.aspect  # type: ignore[union-attr]
        if aspect is None:
            continue
        out.setdefault(urn, {})[type(aspect).__name__] = aspect
    return out


def test_parse_cube_join_sql() -> None:
    assert parse_cube_join_sql(None) == []
    assert parse_cube_join_sql("{CUBE}.customer_id = {customers}.id") == [
        ("customer_id", "customers", "id")
    ]
    assert parse_cube_join_sql("{customers}.id = {CUBE}.customer_id") == [
        ("customer_id", "customers", "id")
    ]
    assert parse_cube_join_sql("{CUBE}.a = {other}.b AND {CUBE}.c = {other}.d") == [
        ("a", "other", "b"),
        ("c", "other", "d"),
    ]
    assert parse_cube_join_sql("{other}.b = {CUBE}.a AND {CUBE}.c = {other}.d") == [
        ("a", "other", "b"),
        ("c", "other", "d"),
    ]


def test_view_emits_semantic_model_metrics_and_logical_datasets() -> None:
    mapper = _mapper()
    cubes = {"orders": _orders_cube(), "customers": _customers_cube()}
    aspects = _aspects_by_urn(list(mapper.emit(_orders_view(), cubes)))

    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    assert model_urn in aspects
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.name == "orders_view"
    assert info.datasets in (None, [])
    assert info.relationships
    rel = info.relationships[0]
    assert rel.from_ == "orders"
    assert rel.to == "customers"
    assert rel.fromColumns == ["customer_id"]
    assert rel.toColumns == ["id"]
    assert rel.cardinality == ERModelRelationshipCardinalityClass.N_ONE

    view_dataset = "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view,PROD)"
    assert view_dataset not in aspects

    orders_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.orders,PROD)"
    )
    customers_logical = (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view.customers,PROD)"
    )
    assert orders_logical in aspects
    assert customers_logical in aspects
    assert aspects[orders_logical]["SubTypesClass"].typeNames == [  # type: ignore[attr-defined]
        DatasetSubTypes.SEMANTIC_MODEL_DATASET
    ]
    props = aspects[orders_logical]["SemanticModelPropertiesClass"]
    assert isinstance(props, SemanticModelPropertiesClass)
    assert props.alias == "orders"
    assert props.semanticModel == model_urn

    count_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    amount_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,total_amount)"
    )
    assert count_metric in aspects
    assert amount_metric in aspects
    metric_info = aspects[count_metric]["MetricInfoClass"]
    assert isinstance(metric_info, MetricInfoClass)
    assert metric_info.semanticModel == model_urn

    assert mapper.view_chart_inputs["orders_view"] == [
        orders_logical,
        customers_logical,
    ]
    assert mapper.report.semantic_models_emitted == 1
    assert mapper.report.metrics_emitted == 2
    assert mapper.report.semantic_model_datasets_emitted == 2


def test_core_view_without_cube_references_uses_alias_member() -> None:
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, {"orders": _orders_cube()})))
    assert any(urn.startswith("urn:li:semanticModel:") for urn in aspects)
    assert any(urn.startswith("urn:li:metric:") for urn in aspects)
    assert (
        "urn:li:dataset:(urn:li:dataPlatform:cube,demo.orders_view,PROD)" not in aspects
    )


def test_empty_view_does_not_emit_semantic_model() -> None:
    view = CubeEntity(name="empty_view", is_view=True)
    mapper = _mapper()
    wus = list(mapper.emit(view, {}))
    assert wus == []
    assert mapper.view_chart_inputs["empty_view"] == []
    assert mapper.report.semantic_models_emitted == 0


def test_same_named_measures_from_different_cubes_get_distinct_metric_urns() -> None:
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            ),
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                title="Customer count",
                member_references=["customers.count"],
            ),
        ],
    )
    cubes = {
        "orders": _orders_cube(),
        "customers": CubeEntity(
            name="customers",
            measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
            dimensions=[CubeMember(name="id", is_measure=False, is_primary_key=True)],
        ),
    }
    mapper = _mapper()
    aspects = _aspects_by_urn(list(mapper.emit(view, cubes)))
    orders_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.orders,count)"
    )
    customers_metric = (
        "urn:li:metric:(urn:li:dataPlatform:cube,demo.orders_view.customers,count)"
    )
    assert orders_metric in aspects
    assert customers_metric in aspects
    customer_info = aspects[customers_metric]["MetricInfoClass"]
    assert isinstance(customer_info, MetricInfoClass)
    assert customer_info.name == "Customer count"


def test_reversed_join_sql_emits_relationship() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{customers}.id = {CUBE}.customer_id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
        dimensions=[CubeMember(name="customer_id", is_measure=False)],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert info.relationships
    rel = info.relationships[0]
    assert rel.from_ == "orders"
    assert rel.to == "customers"
    assert rel.fromColumns == ["customer_id"]
    assert rel.toColumns == ["id"]


def test_unparsed_join_sql_is_warned_and_skipped() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[
            CubeJoin(
                name="customers",
                relationship="many_to_one",
                sql="{CUBE}.customer_id > {customers}.id",
            )
        ],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships
    assert mapper.report.warnings


def test_join_without_sql_is_skipped() -> None:
    cube = CubeEntity(
        name="orders",
        joins=[CubeJoin(name="customers", relationship="many_to_one")],
        measures=[CubeMember(name="count", is_measure=True, agg_type="count")],
    )
    view = CubeEntity(
        name="orders_view",
        is_view=True,
        cube_references=["orders", "customers"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                agg_type="count",
                member_references=["orders.count"],
            )
        ],
        dimensions=[
            CubeMember(
                name="name",
                is_measure=False,
                member_references=["customers.name"],
            )
        ],
    )
    mapper = _mapper()
    aspects = _aspects_by_urn(
        list(mapper.emit(view, {"orders": cube, "customers": _customers_cube()}))
    )
    model_urn = "urn:li:semanticModel:(urn:li:dataPlatform:cube,demo,orders_view)"
    info = aspects[model_urn]["SemanticModelInfoClass"]
    assert isinstance(info, SemanticModelInfoClass)
    assert not info.relationships
