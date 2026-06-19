from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.cube.config import CubeSourceConfig
from datahub.ingestion.source.cube.cube_lineage import CubeLineageBuilder
from datahub.ingestion.source.cube.models import (
    CubeColumnReference,
    CubeEntity,
    CubeMember,
)


def _builder(warehouse_platform=None, warehouse_database=None, **cfg_overrides):
    base = {"api_url": "https://demo.cubecloud.dev/cubejs-api", "api_token": "t"}
    base.update(cfg_overrides)
    config = CubeSourceConfig.model_validate(base)
    return CubeLineageBuilder(
        config=config,
        ctx=PipelineContext(run_id="test"),
        warehouse_platform=warehouse_platform,
        warehouse_database=warehouse_database,
    )


def test_warehouse_table_and_column_lineage() -> None:
    entity = CubeEntity(
        name="orders",
        table_references=[CubeColumnReference(schema_name="public", table="orders")],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                column_references=[
                    CubeColumnReference(
                        schema_name="public", table="orders", column="id"
                    )
                ],
            )
        ],
    )
    lineage = _builder(
        warehouse_platform="postgres", warehouse_database="analytics"
    ).build(entity)

    assert lineage is not None
    upstream_datasets = {u.dataset for u in lineage.upstreams}
    assert any(
        "postgres" in u and "analytics.public.orders" in u for u in upstream_datasets
    )
    assert lineage.fineGrainedLineages is not None
    assert len(lineage.fineGrainedLineages) == 1


def test_view_to_cube_lineage_from_member_references() -> None:
    entity = CubeEntity(
        name="orders",
        is_view=True,
        cube_references=["base_orders"],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                member_references=["base_orders.count"],
            )
        ],
    )
    lineage = _builder().build(entity)

    assert lineage is not None
    upstream_datasets = {u.dataset for u in lineage.upstreams}
    assert any(
        "dataPlatform:cube" in u and "base_orders" in u for u in upstream_datasets
    )
    assert lineage.fineGrainedLineages is not None
    upstream_fields = lineage.fineGrainedLineages[0].upstreams
    assert any("base_orders" in f and f.endswith("count)") for f in upstream_fields)


def test_ingest_lineage_disabled_returns_none() -> None:
    entity = CubeEntity(name="orders", cube_references=["base_orders"])
    assert _builder(ingest_lineage=False).build(entity) is None


def test_column_lineage_disabled_keeps_coarse_only() -> None:
    entity = CubeEntity(
        name="orders",
        is_view=True,
        cube_references=["base_orders"],
        measures=[
            CubeMember(
                name="count", is_measure=True, member_references=["base_orders.count"]
            )
        ],
    )
    lineage = _builder(include_column_lineage=False).build(entity)

    assert lineage is not None
    assert lineage.fineGrainedLineages is None
    assert len(lineage.upstreams) == 1


def _uppercase_entity() -> CubeEntity:
    return CubeEntity(
        name="orders",
        table_references=[CubeColumnReference(schema_name="PUBLIC", table="ORDERS")],
        measures=[
            CubeMember(
                name="count",
                is_measure=True,
                column_references=[
                    CubeColumnReference(
                        schema_name="PUBLIC", table="ORDERS", column="ID"
                    )
                ],
            )
        ],
    )


def test_warehouse_urns_lowercased_by_default() -> None:
    lineage = _builder(warehouse_platform="snowflake").build(_uppercase_entity())

    assert lineage is not None
    assert any("public.orders" in u.dataset for u in lineage.upstreams)
    assert lineage.fineGrainedLineages is not None
    assert any(f.endswith("id)") for f in lineage.fineGrainedLineages[0].upstreams)


def test_warehouse_urns_preserve_case_when_disabled() -> None:
    lineage = _builder(
        warehouse_platform="snowflake", convert_lineage_urns_to_lowercase=False
    ).build(_uppercase_entity())

    assert lineage is not None
    assert any("PUBLIC.ORDERS" in u.dataset for u in lineage.upstreams)


def test_single_warehouse_table_urn_for_one_to_one_cube() -> None:
    entity = CubeEntity(
        name="orders",
        table_references=[CubeColumnReference(schema_name="public", table="orders")],
    )
    urn = _builder(
        warehouse_platform="postgres", warehouse_database="analytics"
    ).single_warehouse_table_urn(entity)

    assert urn is not None
    assert "analytics.public.orders" in urn


def test_single_warehouse_table_urn_none_for_view() -> None:
    entity = CubeEntity(name="orders_view", is_view=True, cube_references=["orders"])
    assert _builder().single_warehouse_table_urn(entity) is None


def test_single_warehouse_table_urn_none_for_multi_table_join() -> None:
    entity = CubeEntity(
        name="orders",
        table_references=[
            CubeColumnReference(schema_name="public", table="orders"),
            CubeColumnReference(schema_name="public", table="customers"),
        ],
    )
    urn = _builder(
        warehouse_platform="postgres", warehouse_database="analytics"
    ).single_warehouse_table_urn(entity)

    assert urn is None
