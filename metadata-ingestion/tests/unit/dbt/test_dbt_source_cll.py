from typing import Any, Dict, List
from unittest import mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.dbt.dbt_common import (
    DBTColumnLineageInfo,
    DBTNode,
    parse_semantic_view_cll,
)
from datahub.ingestion.source.dbt.dbt_core import DBTCoreConfig, DBTCoreSource
from tests.unit.dbt.test_helpers import (  # type: ignore[import-untyped]
    create_mock_dbt_node,
)


def test_parse_semantic_view_cll_with_sql_comments_after_columns() -> None:
    """
    Regression test: SQL comments (--) after column names caused DIMENSION_RE to fail.
    Also covers TABLES section with nested parentheses (PRIMARY KEY (...)).
    """
    compiled_sql = """
    TABLES (
        OrdersTable AS DB.SCHEMA.ORDERS
            PRIMARY KEY (ORDER_ID, CUSTOMER_ID),
        TransactionsTable AS DB.SCHEMA.TRANSACTIONS
            PRIMARY KEY (TRANSACTION_ID)
    )
    DIMENSIONS (
        OrdersTable.CUSTOMER_ID AS CUSTOMER_ID -- comment
            COMMENT='desc',
        OrdersTable.ORDER_ID AS ORDER_ID -- comment
            COMMENT='desc'
    )
    FACTS (
        TransactionsTable.AMOUNT AS AMOUNT -- comment
            COMMENT='desc'
    )
    METRICS (
        OrdersTable.REVENUE AS SUM(ORDER_TOTAL)
            COMMENT='desc'
    )
    """

    upstream_nodes: List[str] = [
        "source.project.shop.ORDERS",
        "source.project.shop.TRANSACTIONS",
    ]
    all_nodes_map: Dict[str, Any] = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
        "source.project.shop.TRANSACTIONS": create_mock_dbt_node("TRANSACTIONS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    downstream_cols = {cll.downstream_col for cll in cll_info}
    # Dimensions
    assert "customer_id" in downstream_cols
    assert "order_id" in downstream_cols
    # Facts - from TransactionsTable (verifies TABLES section not truncated)
    assert "amount" in downstream_cols
    # Metrics
    assert "revenue" in downstream_cols


def test_parse_semantic_view_cll_with_various_functions() -> None:
    """Test that metric regex matches any function name."""
    compiled_sql = """
    TABLES (
        OrdersTable AS DB.SCHEMA.ORDERS
    )
    METRICS (
        OrdersTable.UNIQUE_CUSTOMERS AS APPROX_COUNT_DISTINCT(CUSTOMER_ID),
        OrdersTable.MEDIAN_VALUE AS PERCENTILE_CONT(ORDER_VALUE),
        OrdersTable.VALUE_STDDEV AS STDDEV_SAMP(ORDER_VALUE),
        OrdersTable.TOTAL AS SUM(AMOUNT)
    )
    """

    upstream_nodes: List[str] = ["source.project.shop.ORDERS"]
    all_nodes_map: Dict[str, Any] = {
        "source.project.shop.ORDERS": create_mock_dbt_node("ORDERS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert "unique_customers" in downstream_cols
    assert "median_value" in downstream_cols
    assert "value_stddev" in downstream_cols
    assert "total" in downstream_cols


def _make_dbt_source() -> DBTCoreSource:
    ctx = PipelineContext(run_id="test-run-id", pipeline_name="dbt-source")
    ctx.graph = mock.MagicMock()
    return DBTCoreSource(
        DBTCoreConfig(
            manifest_path="temp/",
            catalog_path="temp/",
            target_platform="bigquery",
            enable_meta_mapping=False,
        ),
        ctx,
    )


def _make_model_node(dbt_name: str, upstream_dbt_name: str) -> DBTNode:
    return DBTNode(
        database="myproject",
        schema="mydataset",
        name="my_model",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="bigquery",
        dbt_name=dbt_name,
        dbt_file_path=None,
        dbt_package_name="mypackage",
        node_type="model",
        max_loaded_at=None,
        materialization="table",
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
        upstream_nodes=[upstream_dbt_name],
    )


def _make_upstream_node(dbt_name: str) -> DBTNode:
    return DBTNode(
        database="myproject",
        schema="internal_staging",
        name="stg_utm_campaigns",
        alias=None,
        comment="",
        description="",
        language="sql",
        raw_code=None,
        dbt_adapter="bigquery",
        dbt_name=dbt_name,
        dbt_file_path=None,
        dbt_package_name="mypackage",
        node_type="source",
        max_loaded_at=None,
        materialization=None,
        catalog_type=None,
        missing_from_catalog=False,
        owner=None,
    )


def test_empty_column_names_filtered_from_fine_grained_lineage() -> None:
    """Regression: sqlglot v30 can produce empty column names from SQL parsing.
    These must be dropped before emitting schemaField URNs or GMS rejects them with 422."""
    upstream_dbt_name = "source.mypackage.mydb.stg_utm_campaigns"
    model_dbt_name = "model.mypackage.my_model"

    node = _make_model_node(model_dbt_name, upstream_dbt_name)
    upstream_node = _make_upstream_node(upstream_dbt_name)

    node.upstream_cll = [
        # valid entry — should be kept
        DBTColumnLineageInfo(
            upstream_dbt_name=upstream_dbt_name,
            upstream_col="campaign_id",
            downstream_col="campaign_id",
        ),
        # empty upstream column — must be dropped
        DBTColumnLineageInfo(
            upstream_dbt_name=upstream_dbt_name,
            upstream_col="",
            downstream_col="campaign_name",
        ),
        # empty downstream column — must be dropped
        DBTColumnLineageInfo(
            upstream_dbt_name=upstream_dbt_name,
            upstream_col="source_col",
            downstream_col="",
        ),
    ]

    source = _make_dbt_source()
    all_nodes_map = {
        model_dbt_name: node,
        upstream_dbt_name: upstream_node,
    }

    result = source._create_lineage_aspect_for_dbt_node(node, all_nodes_map)

    assert result is not None
    assert result.fineGrainedLineages is not None
    assert len(result.fineGrainedLineages) == 1

    fgl = result.fineGrainedLineages[0]
    assert fgl.upstreams is not None
    assert fgl.downstreams is not None
    assert len(fgl.upstreams) == 1
    assert len(fgl.downstreams) == 1

    # Neither upstream nor downstream should contain a schemaField URN with an empty field name.
    for urn in fgl.upstreams + fgl.downstreams:
        assert not urn.endswith(",)"), f"Empty field name in schemaField URN: {urn}"
