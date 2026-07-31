from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.cube.config import CubeSourceConfig, CubeSourceReport
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
        report=CubeSourceReport(),
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


def test_include_lineage_disabled_returns_none() -> None:
    entity = CubeEntity(name="orders", cube_references=["base_orders"])
    assert _builder(include_lineage=False).build(entity) is None


def test_sql_parse_failure_is_counted_and_reported() -> None:
    # An unparseable cube SQL must be counted in the report, not silently dropped.
    entity = CubeEntity(name="orders", sql="THIS IS NOT VALID SQL ((")
    builder = _builder(warehouse_platform="postgres", parse_sql_for_lineage=True)
    builder.build(entity)
    assert builder.report.sql_parsing_failures >= 1
    assert len(builder.report.warnings) >= 1


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


class _FakeResolver:
    def __init__(self, urn: str, schema_info: dict) -> None:
        self._urn = urn
        self._schema_info = schema_info

    def resolve_table(self, table: object) -> tuple:
        return self._urn, self._schema_info

    def resolve_urn(self, urn: str) -> tuple:
        return self._urn, self._schema_info


def test_warehouse_columns_snapped_to_gms_casing() -> None:
    # With a schema in GMS, casing is reconciled to it ("amount" -> "Amount")
    # rather than blindly lowercased.
    canonical_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.public.orders,PROD)"
    )
    fake = _FakeResolver(canonical_urn, {"Amount": "NUMBER", "Id": "NUMBER"})
    entity = CubeEntity(
        name="orders",
        table_references=[CubeColumnReference(schema_name="public", table="orders")],
        measures=[
            CubeMember(
                name="total_amount",
                is_measure=True,
                column_references=[
                    CubeColumnReference(
                        schema_name="public", table="orders", column="amount"
                    )
                ],
            )
        ],
    )
    builder = _builder(warehouse_platform="snowflake", warehouse_database="analytics")
    builder.ctx.graph = object()  # type: ignore[assignment]
    with patch(
        "datahub.ingestion.source.cube.cube_lineage.create_and_cache_schema_resolver",
        return_value=fake,
    ):
        lineage = builder.build(entity)

    assert lineage is not None
    assert any(u.dataset == canonical_urn for u in lineage.upstreams)
    assert lineage.fineGrainedLineages is not None
    upstreams = lineage.fineGrainedLineages[0].upstreams
    assert any(f.endswith(",Amount)") for f in upstreams)


def test_core_column_lineage_name_matched_to_warehouse_schema() -> None:
    # Cube Core members carry no column references. When the warehouse schema is
    # in DataHub, members are matched by name to real columns; members that do
    # not correspond to a column (e.g. count, total_amount) are not linked.
    canonical_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.public.orders,PROD)"
    )
    fake = _FakeResolver(
        canonical_urn, {"id": "NUMBER", "status": "STRING", "created_at": "TIME"}
    )
    entity = CubeEntity(
        name="orders",
        sql="SELECT * FROM public.orders",
        measures=[
            CubeMember(name="count", is_measure=True),
            CubeMember(name="total_amount", is_measure=True),
        ],
        dimensions=[
            CubeMember(name="id", is_measure=False),
            CubeMember(name="status", is_measure=False),
        ],
    )
    builder = _builder(
        warehouse_platform="postgres",
        warehouse_database="analytics",
        parse_sql_for_lineage=True,
    )
    builder.ctx.graph = object()  # type: ignore[assignment]
    with (
        patch(
            "datahub.ingestion.source.cube.cube_lineage.create_and_cache_schema_resolver",
            return_value=fake,
        ),
        patch.object(builder, "_parse_sql_tables", return_value=[canonical_urn]),
    ):
        lineage = builder.build(entity)

    assert lineage is not None
    assert lineage.fineGrainedLineages is not None
    linked = {f for fg in lineage.fineGrainedLineages for f in fg.upstreams}
    assert any(f.endswith(",id)") for f in linked)
    assert any(f.endswith(",status)") for f in linked)
    assert not any("total_amount" in f or "count" in f for f in linked)
    # Only the two name-matched dimensions produce column lineage.
    assert len(lineage.fineGrainedLineages) == 2
