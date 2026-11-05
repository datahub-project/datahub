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
    dbt_adapter: Optional[str] = "dremio",
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
        dbt_adapter=dbt_adapter,
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
        # A space or source named "dremio" is part of the path, as in the Dremio source.
        ("dremio", "dremio.dremio.materialize.orders"),
        (None, "dremio.materialize.orders"),
    ],
)
def test_dremio_target_urn_matches_dremio_source(
    database: Optional[str], expected_name: str
) -> None:
    node = _node("model.my_project.orders", database, "materialize", "orders")
    assert node.get_urn("dremio", "DEV", None) == _urn(expected_name)


@pytest.mark.parametrize(
    "name, expected_name",
    [
        ("orders", "dremio.my_space.orders"),
        # Only the schema position is the placeholder; a model may be called no_schema.
        ("no_schema", "dremio.my_space.no_schema"),
    ],
)
def test_dremio_target_urn_drops_dbt_dremio_no_schema(
    name: str, expected_name: str
) -> None:
    # dbt-dremio stores a model with no folder under schema "no_schema" and
    # leaves it out of the SQL it runs; the dataset is directly in the space.
    node = _node(f"model.my_project.{name}", "my_space", "no_schema", name)
    assert node.get_urn("dremio", "DEV", None) == _urn(expected_name)


def test_non_dremio_urns_unchanged() -> None:
    # "no_schema" is only dbt-dremio's placeholder; elsewhere it is a real schema.
    node = _node("model.my_project.orders", "my_source_s3", "no_schema", "orders")
    assert node.get_urn("snowflake", "DEV", None) == _urn(
        "my_source_s3.no_schema.orders", "snowflake"
    )


def _make_dremio_source(
    target_platform: str = "dremio", include_database_name: bool = True
) -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-dremio")
    return DBTCoreSource(
        DBTCoreConfig(
            manifest_path="temp/",
            catalog_path="temp/",
            target_platform=target_platform,
            include_database_name=include_database_name,
            env="DEV",
            enable_meta_mapping=False,
        ),
        ctx,
    )


def _column(name: str) -> DBTColumn:
    return DBTColumn(name=name, comment="", description="", index=0, data_type="int")


@pytest.mark.parametrize(
    "upstream_database",
    [
        "my_source_s3",
        # dbt-dremio renders full paths, so a leading "dremio" in compiled SQL is
        # a space or source of that name, not the default database.
        "dremio",
    ],
)
def test_dremio_cll_upstream_urns_carry_dremio_prefix(upstream_database: str) -> None:
    upstream = _node(
        f"source.my_project.{upstream_database}.orders",
        upstream_database,
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
        compiled_code=(
            f'SELECT order_id FROM "{upstream_database}"."materialize"."orders"'
        ),
    )

    _make_dremio_source()._infer_schemas_and_update_cll(
        {upstream.dbt_name: upstream, model.dbt_name: model}
    )

    assert model.raw_sql_parsing_result is not None
    assert model.raw_sql_parsing_result.in_tables == [
        _urn(f"dremio.{upstream_database}.materialize.orders")
    ]
    assert [
        (c.upstream_dbt_name, c.upstream_col, c.downstream_col)
        for c in model.upstream_cll
    ] == [(upstream.dbt_name, "order_id", "order_id")]


@pytest.mark.parametrize(
    "ephemeral_database, ephemeral_schema, target_platform, dbt_adapter",
    [
        ("my_space", "staging", "dremio", "dremio"),
        ("my_space", "no_schema", "dremio", "dremio"),
        # dbt Cloud passes target_platform through as dbt_adapter, as typed.
        ("my_space", "no_schema", "Dremio", "Dremio"),
        ("my_space", "no_schema", "dremio", None),
        # A nested folder is a dotted schema.
        ("my_space", "staging.sub", "dremio", "dremio"),
        # A space named "dremio" must be kept on both sides of the placeholder.
        ("dremio", "staging", "dremio", "dremio"),
        ("dremio", "no_schema", "dremio", "dremio"),
    ],
)
def test_dremio_cll_through_ephemeral_model(
    ephemeral_database: str,
    ephemeral_schema: str,
    target_platform: str,
    dbt_adapter: Optional[str],
) -> None:
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
        ephemeral_database,
        ephemeral_schema,
        "stg_orders",
        materialization="ephemeral",
        upstream_nodes=[upstream.dbt_name],
        compiled_code='SELECT order_id FROM "my_source_s3"."materialize"."orders"',
        dbt_adapter=dbt_adapter,
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

    _make_dremio_source(target_platform)._infer_schemas_and_update_cll(
        {n.dbt_name: n for n in (upstream, ephemeral, model)}
    )

    assert [
        (c.upstream_dbt_name, c.upstream_col, c.downstream_col)
        for c in model.upstream_cll
    ] == [(ephemeral.dbt_name, "order_id", "order_id")]


def test_dremio_duplicate_source_matched_by_dremio_name() -> None:
    # Different dbt casing, same Dremio dataset: the source is a duplicate.
    model = _node("model.my_project.orders", "My_Space", "marts", "orders")
    source = _node(
        "source.my_project.my_space.orders",
        "my_space",
        "marts",
        "orders",
        node_type="source",
        materialization=None,
    )
    nodes = _make_dremio_source()._drop_duplicate_sources([model, source])
    assert [n.dbt_name for n in nodes] == [model.dbt_name]


@pytest.mark.parametrize(
    "target_platform, include_database_name, expect_warning",
    [
        ("dremio", False, True),
        ("Dremio", False, True),
        ("dremio", True, False),
        ("snowflake", False, False),
    ],
)
def test_dremio_without_database_name_warns(
    target_platform: str, include_database_name: bool, expect_warning: bool
) -> None:
    # The Dremio source names datasets by their full path, so dbt URNs without
    # the database can't match them.
    source = _make_dremio_source(target_platform, include_database_name)
    titles = [w.title for w in source.report.warnings]
    assert (
        "Dremio datasets can't be matched without the database name" in titles
    ) == expect_warning
