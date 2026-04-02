"""Unit tests for LookerViewLineageResolver."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.looker_v2.looker_v2_pdt_graph_parser import (
    PDTDependencyEdge,
)
from datahub.ingestion.source.looker_v2.looker_v2_view_processor import (
    LookerViewLineageResolver,
)
from datahub.metadata.schema_classes import FineGrainedLineageClass
from datahub.metadata.urns import DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from tests.unit.looker_v2.conftest import make_ctx


def _make_resolver(**overrides: object) -> LookerViewLineageResolver:
    ctx = make_ctx(**overrides)
    return LookerViewLineageResolver(ctx)


def test_resolve_view_lineage_returns_none_when_not_in_map() -> None:
    resolver = _make_resolver()
    conn_def = MagicMock()
    view_urn_resolver = MagicMock()

    result = resolver.resolve_view_lineage_via_api(
        "unknown_view", conn_def, view_urn_resolver
    )

    assert result is None


def test_pdt_edges_merged_into_result() -> None:
    ctx = make_ctx()

    # Add a view → explore mapping so the resolver proceeds past the early-return.
    ctx.view_to_explore_map["my_view"] = ("my_model", "my_explore")

    # Provide a PDT edge that points to a database table.
    edge = PDTDependencyEdge(
        view_name="my_view",
        model_name="my_model",
        upstream_name="raw_schema.raw_table",
        upstream_model=None,
        is_database_table=True,
    )
    ctx.pdt_upstream_map["my_view"] = [edge]

    # The explore cache must return an explore with fields so chunks are produced.
    mock_explore = MagicMock()
    mock_dim = MagicMock()
    mock_dim.name = "my_view.id"
    mock_dim.view = "my_view"
    mock_dim.dimension_group = None
    mock_explore.fields.dimensions = [mock_dim]
    mock_explore.fields.measures = []
    ctx.explore_cache[("my_model", "my_explore")] = mock_explore

    # generate_sql_query returns a SQL string; parse it to an empty result.
    looker_api_mock = cast(MagicMock, ctx.looker_api)
    looker_api_mock.generate_sql_query.return_value = (
        "SELECT id FROM raw_schema.raw_table"
    )

    conn_def = MagicMock()
    conn_def.platform = "bigquery"
    conn_def.platform_instance = None
    conn_def.default_db = "mydb"
    conn_def.default_schema = "myschema"

    mock_spr = MagicMock()
    mock_spr.in_tables = set()
    mock_spr.column_lineage = []
    mock_spr.debug_info.table_error = None

    view_urn_resolver = MagicMock(return_value=None)

    resolver = LookerViewLineageResolver(ctx)

    with patch(
        "datahub.ingestion.source.looker_v2.looker_v2_view_processor.create_lineage_sql_parsed_result",
        return_value=mock_spr,
    ):
        result = resolver.resolve_view_lineage_via_api(
            "my_view", conn_def, view_urn_resolver
        )

    assert result is not None
    # The PDT edge should have contributed a dataset URN string.
    assert any(
        isinstance(item, str) and "raw_schema" in item.lower() for item in result
    )


def test_chunk_fallback_used_when_generate_sql_raises() -> None:
    ctx = make_ctx()
    ctx.view_to_explore_map["dt_view"] = ("model_a", "explore_a")

    mock_explore = MagicMock()
    mock_dim = MagicMock()
    mock_dim.name = "dt_view.col1"
    mock_dim.view = "dt_view"
    mock_dim.dimension_group = None
    mock_dim2 = MagicMock()
    mock_dim2.name = "dt_view.col2"
    mock_dim2.view = "dt_view"
    mock_dim2.dimension_group = None
    mock_explore.fields.dimensions = [mock_dim, mock_dim2]
    mock_explore.fields.measures = []
    ctx.explore_cache[("model_a", "explore_a")] = mock_explore

    # Make the chunk-level call fail, but per-field calls succeed.
    good_sql = "SELECT col1 FROM upstream_table"
    call_count = 0

    def side_effect(query: object) -> str:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("chunk too large")
        return good_sql

    cast(MagicMock, ctx.looker_api).generate_sql_query.side_effect = side_effect
    ctx.config.api_sql_lineage_individual_field_fallback = True
    # Set chunk size larger than field count so one chunk is generated.
    ctx.config.api_sql_lineage_field_chunk_size = 100

    table_urn = str(
        DatasetUrn(platform="bigquery", name="mydb.myschema.upstream_table", env="PROD")
    )
    mock_spr = MagicMock()
    mock_spr.in_tables = {table_urn}
    mock_spr.column_lineage = []
    mock_spr.debug_info.table_error = None

    conn_def = MagicMock()
    conn_def.platform = "bigquery"
    conn_def.platform_instance = None
    conn_def.default_db = "mydb"
    conn_def.default_schema = "myschema"

    resolver = LookerViewLineageResolver(ctx)

    with patch(
        "datahub.ingestion.source.looker_v2.looker_v2_view_processor.create_lineage_sql_parsed_result",
        return_value=mock_spr,
    ):
        result = resolver.resolve_view_lineage_via_api("dt_view", conn_def, MagicMock())

    # Fallback per-field calls happened (call_count > 1).
    assert call_count > 1
    assert result is not None
    assert table_urn in result


def test_cll_to_fine_grained_converts_column_lineage() -> None:
    ctx = make_ctx()
    resolver = LookerViewLineageResolver(ctx)

    upstream_urn = str(
        DatasetUrn(platform="bigquery", name="mydb.schema.upstream", env="PROD")
    )
    downstream_urn = str(
        DatasetUrn(platform="bigquery", name="mydb.schema.downstream", env="PROD")
    )

    cll = ColumnLineageInfo(
        downstream=DownstreamColumnRef(table=downstream_urn, column="col_out"),
        upstreams=[ColumnRef(table=upstream_urn, column="col_in")],
    )

    result = resolver._cll_to_fine_grained([cll])

    assert len(result) == 1
    fgl = result[0]
    assert isinstance(fgl, FineGrainedLineageClass)
    assert fgl.upstreams is not None
    assert fgl.downstreams is not None
    assert len(fgl.upstreams) == 1
    assert len(fgl.downstreams) == 1
    assert "col_in" in fgl.upstreams[0]
    assert "col_out" in fgl.downstreams[0]
