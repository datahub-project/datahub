from typing import Any, Dict, List
from unittest import mock

from datahub.ingestion.source.dbt.dbt_common import parse_semantic_view_cll


def _create_mock_node(table_name: str) -> mock.Mock:
    """Helper to create a mock DBTNode with a name attribute."""
    node = mock.Mock()
    node.name = table_name
    return node


def test_parse_semantic_view_cll_with_comments_and_whitespace() -> None:
    """
    Test that the CLL parser is robust against comments and varied whitespace.
    """
    compiled_sql = """
    -- Top-level comment
    DIMENSIONS (
        -- Dimension 1
        ORDERS.CUSTOMER_ID AS CUSTOMER_ID, -- trailing comment

        /* 
            Multi-line comment
            for dimension 2
        */
        "ORDERS"."ORDER_ID" 
        AS 
        "ORDER_ID"

    )
    METRICS (
        -- Metric 1
        TRANSACTIONS.NET_PAYMENT AS SUM(TRANSACTION_AMOUNT)
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

    assert len(cll_info) == 3

    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert downstream_cols == {"customer_id", "order_id", "net_payment"}

    # Check that ORDER_ID was parsed correctly despite newlines
    order_id = next(cll for cll in cll_info if cll.downstream_col == "order_id")
    assert order_id.upstream_col == "order_id"
    assert order_id.upstream_dbt_name == "source.project.shop.ORDERS"


def test_parse_semantic_view_cll_with_sql_comments_after_columns() -> None:
    """
    Test that SQL comments (--) after column names don't break parsing.
    This was a bug where '-- Use alias here' comments caused DIMENSION_RE to fail.
    """
    compiled_sql = """
    TABLES (
        OrdersTable AS DB.SCHEMA.ORDERS
            PRIMARY KEY (ORDER_ID, CUSTOMER_ID),
        TransactionsTable AS DB.SCHEMA.TRANSACTIONS
            PRIMARY KEY (TRANSACTION_ID)
    )
    DIMENSIONS (
        OrdersTable.CUSTOMER_ID AS CUSTOMER_ID -- Use alias here
            COMMENT='Customer identifier',
        OrdersTable.ORDER_ID AS ORDER_ID -- Another comment
            COMMENT='Order identifier'
    )
    FACTS (
        OrdersTable.ORDER_TOTAL AS ORDER_TOTAL -- Fact comment
            COMMENT='Total order amount',
        TransactionsTable.TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT -- More comments
            COMMENT='Transaction amount'
    )
    METRICS (
        OrdersTable.GROSS_REVENUE AS SUM(ORDER_TOTAL) -- Metric comment
            COMMENT='Gross revenue'
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

    # Should have: 2 dimensions + 2 facts + 1 metric = 5 entries
    assert len(cll_info) >= 5

    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert "customer_id" in downstream_cols
    assert "order_id" in downstream_cols
    assert "order_total" in downstream_cols
    assert "transaction_amount" in downstream_cols
    assert "gross_revenue" in downstream_cols


def test_parse_semantic_view_cll_with_nested_parens_in_tables() -> None:
    """
    Test that nested parentheses in TABLES section (e.g., PRIMARY KEY (...))
    don't truncate the table mapping.
    This was a bug where the TABLES regex stopped at the first ')'.
    """
    compiled_sql = """
    TABLES (
        OrdersTable AS DB.SCHEMA.ORDERS
            PRIMARY KEY (ORDER_ID, CUSTOMER_ID)
            WITH SYNONYMS=('Sales', 'Orders'),
        TransactionsTable AS DB.SCHEMA.TRANSACTIONS
            PRIMARY KEY (TRANSACTION_ID)
            UNIQUE (ORDER_ID, TRANSACTION_ID)
    )
    DIMENSIONS (
        OrdersTable.CUSTOMER_ID AS CUSTOMER_ID,
        TransactionsTable.PAYMENT_METHOD AS PAYMENT_METHOD
    )
    METRICS (
        OrdersTable.REVENUE AS SUM(ORDER_TOTAL),
        TransactionsTable.TOTAL_PAYMENTS AS SUM(AMOUNT)
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

    # Both tables should be mapped - TransactionsTable should NOT be missed
    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert "customer_id" in downstream_cols
    assert (
        "payment_method" in downstream_cols
    )  # This would fail if TABLES section truncated
    assert "revenue" in downstream_cols
    assert (
        "total_payments" in downstream_cols
    )  # This would fail if TABLES section truncated


def test_parse_semantic_view_cll_comment_keyword_without_sql_comment() -> None:
    """
    Test that COMMENT keyword (without SQL -- comment) is handled correctly.
    """
    compiled_sql = """
    TABLES (
        Orders AS DB.SCHEMA.ORDERS
    )
    DIMENSIONS (
        Orders.CUSTOMER_ID AS CUSTOMER_ID
            COMMENT='Customer identifier - links to customer master data',
        Orders.ORDER_TYPE AS ORDER_TYPE
            COMMENT='Order channel classification'
    )
    FACTS (
        Orders.ORDER_TOTAL AS ORDER_TOTAL
            COMMENT='Total order amount in USD'
    )
    METRICS (
        Orders.GROSS_REVENUE AS SUM(ORDER_TOTAL)
            COMMENT='Total gross revenue'
    )
    """

    upstream_nodes: List[str] = ["source.project.shop.ORDERS"]
    all_nodes_map: Dict[str, Any] = {
        "source.project.shop.ORDERS": _create_mock_node("ORDERS"),
    }

    cll_info = parse_semantic_view_cll(compiled_sql, upstream_nodes, all_nodes_map)

    # Should have: 2 dimensions + 1 fact + 1 metric = 4 entries
    assert len(cll_info) >= 4

    downstream_cols = {cll.downstream_col for cll in cll_info}
    assert "customer_id" in downstream_cols
    assert "order_type" in downstream_cols
    assert "order_total" in downstream_cols
    assert "gross_revenue" in downstream_cols
