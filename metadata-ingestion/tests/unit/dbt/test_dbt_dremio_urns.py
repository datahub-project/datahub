"""The Dremio source names every dataset `dremio.<path>`. dbt must use the same
names for its target-platform URNs, or siblings and lineage point at entities
that never exist."""

from typing import List, Optional

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import DBTColumn, DBTNode
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource


def _node(
    dbt_name: str,
    database: Optional[str],
    schema: str,
    name: str,
    node_type: str = "model",
    materialization: Optional[str] = "table",
    upstream_nodes: Optional[List[str]] = None,
    compiled_code: Optional[str] = None,
    columns: Optional[List[DBTColumn]] = None,
) -> DBTNode:
    return DBTNode(
        database=database,
        schema=schema,
        name=name,
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="dremio",
        dbt_name=dbt_name,
        dbt_file_path=None,
        dbt_package_name="my_project",
        node_type=node_type,
        max_loaded_at=None,
        materialization=materialization,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=upstream_nodes or [],
        compiled_code=compiled_code,
        columns=columns or [],
    )


def _urn(name: str, platform: str = "dremio") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},DEV)"


@pytest.mark.parametrize(
    "database, expected_name",
    [
        ("my_source_s3", "dremio.my_source_s3.materialize.orders"),
        ("My_Source_S3", "dremio.my_source_s3.materialize.orders"),
        ("dremio", "dremio.materialize.orders"),
        ("DREMIO", "dremio.materialize.orders"),
        (None, "dremio.materialize.orders"),
    ],
)
def test_dremio_target_urn_matches_dremio_source(
    database: Optional[str], expected_name: str
) -> None:
    node = _node("model.my_project.orders", database, "materialize", "orders")
    node.convert_urns_to_lowercase = True
    assert node.get_urn("dremio", "DEV", None) == _urn(expected_name)


def test_dremio_target_urn_keeps_case_without_lowercasing() -> None:
    node = _node("model.my_project.orders", "My_Space", "materialize", "Orders")
    assert node.get_urn("dremio", "DEV", None) == _urn(
        "dremio.My_Space.materialize.Orders"
    )


@pytest.mark.parametrize("platform", ["snowflake", "postgres", "dbt"])
def test_non_dremio_urns_unchanged(platform: str) -> None:
    node = _node("model.my_project.orders", "my_source_s3", "materialize", "orders")
    assert node.get_urn(platform, "DEV", None) == _urn(
        "my_source_s3.materialize.orders", platform
    )


def _make_dremio_source() -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-dremio")
    return DBTCoreSource(
        DBTCoreConfig(
            manifest_path="temp/",
            catalog_path="temp/",
            target_platform="dremio",
            env="DEV",
            enable_meta_mapping=False,
        ),
        ctx,
    )


def _column(name: str) -> DBTColumn:
    return DBTColumn(name=name, comment="", description="", index=0, data_type="int")


def test_dremio_cll_upstream_urns_carry_dremio_prefix() -> None:
    upstream = _node(
        "source.my_project.my_source_s3.orders",
        "my_source_s3",
        "materialize",
        "orders",
        node_type="source",
        materialization=None,
        columns=[_column("order_id")],
    )
    model = _node(
        "model.my_project.order_ids",
        "my_space",
        "marts",
        "order_ids",
        upstream_nodes=[upstream.dbt_name],
        compiled_code='SELECT order_id FROM "my_source_s3"."materialize"."orders"',
    )

    _make_dremio_source()._infer_schemas_and_update_cll(
        {upstream.dbt_name: upstream, model.dbt_name: model}
    )

    assert model.raw_sql_parsing_result is not None
    assert model.raw_sql_parsing_result.in_tables == [
        _urn("dremio.my_source_s3.materialize.orders")
    ]
    assert [
        (c.upstream_dbt_name, c.upstream_col, c.downstream_col)
        for c in model.upstream_cll
    ] == [(upstream.dbt_name, "order_id", "order_id")]


def test_dremio_cll_through_ephemeral_model() -> None:
    upstream = _node(
        "source.my_project.my_source_s3.orders",
        "my_source_s3",
        "materialize",
        "orders",
        node_type="source",
        materialization=None,
        columns=[_column("order_id")],
    )
    ephemeral = _node(
        "model.my_project.stg_orders",
        "my_space",
        "staging",
        "stg_orders",
        materialization="ephemeral",
        upstream_nodes=[upstream.dbt_name],
        compiled_code='SELECT order_id FROM "my_source_s3"."materialize"."orders"',
    )
    model = _node(
        "model.my_project.order_ids",
        "my_space",
        "marts",
        "order_ids",
        upstream_nodes=[ephemeral.dbt_name],
        compiled_code=(
            "WITH __dbt__cte__stg_orders AS (SELECT order_id FROM "
            '"my_source_s3"."materialize"."orders") '
            "SELECT order_id FROM __dbt__cte__stg_orders"
        ),
    )

    _make_dremio_source()._infer_schemas_and_update_cll(
        {n.dbt_name: n for n in (upstream, ephemeral, model)}
    )

    assert [
        (c.upstream_dbt_name, c.upstream_col, c.downstream_col)
        for c in model.upstream_cll
    ] == [(ephemeral.dbt_name, "order_id", "order_id")]
