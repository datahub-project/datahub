from typing import Any, Dict, List
from unittest import mock

from datahub.ingestion.source.dbt.dbt_common import parse_semantic_view_cll


def _create_mock_node(table_name: str) -> mock.Mock:
    """Helper to create a mock DBTNode with a name attribute."""
    node = mock.Mock()
    node.name = table_name
    return node


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
        "source.project.shop.ORDERS": _create_mock_node("ORDERS"),
        "source.project.shop.TRANSACTIONS": _create_mock_node("TRANSACTIONS"),
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
        "source.project.shop.ORDERS": _create_mock_node("ORDERS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert "unique_customers" in downstream_cols
    assert "median_value" in downstream_cols
    assert "value_stddev" in downstream_cols
    assert "total" in downstream_cols
