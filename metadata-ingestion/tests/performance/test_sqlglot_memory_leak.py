"""
Test to reproduce sqlglot[c] memory leak when parsing BigQuery views.

This test reproduces the memory leak reported in https://github.com/tobymao/sqlglot/issues/7506
where accessing Table.name in sqlglot[c] causes a reference count leak.

The leak manifests when parsing many views leading to multi-GB memory accumulation.
"""

import gc
import sys
from typing import List

import pytest
import sqlglot

from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import sqlglot_lineage

# Sample BigQuery view definitions similar to those in production
SAMPLE_VIEW_QUERIES: List[str] = [
    # Complex view with multiple joins
    """
    CREATE VIEW `project.dataset.sales_summary` AS
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_name,
        p.product_name,
        o.order_date,
        o.total_amount
    FROM `project.dataset.orders` o
    JOIN `project.dataset.customers` c ON o.customer_id = c.customer_id
    JOIN `project.dataset.products` p ON o.product_id = p.product_id
    WHERE o.order_date >= '2026-01-01'
    """,
    # View with window functions
    """
    CREATE VIEW `project.dataset.customer_rankings` AS
    SELECT
        customer_id,
        customer_name,
        total_revenue,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) as rank
    FROM `project.dataset.customer_summary`
    """,
    # View with CTEs
    """
    CREATE VIEW `project.dataset.monthly_sales` AS
    WITH monthly_data AS (
        SELECT
            DATE_TRUNC(order_date, MONTH) as month,
            SUM(total_amount) as revenue
        FROM `project.dataset.orders`
        GROUP BY month
    )
    SELECT
        month,
        revenue,
        LAG(revenue) OVER (ORDER BY month) as prev_month_revenue
    FROM monthly_data
    """,
    # View with subquery
    """
    CREATE VIEW `project.dataset.top_customers` AS
    SELECT
        c.customer_id,
        c.customer_name,
        (SELECT COUNT(*) FROM `project.dataset.orders` o WHERE o.customer_id = c.customer_id) as order_count
    FROM `project.dataset.customers` c
    WHERE c.status = 'active'
    """,
    # View with QUALIFY clause (BigQuery specific)
    """
    CREATE VIEW `project.dataset.latest_orders` AS
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount
    FROM `project.dataset.orders`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1
    """,
]


@pytest.mark.perf
def test_sqlglot_table_name_memory_leak():
    """
    Test for memory leak when accessing Table.name in sqlglot[c].

    This reproduces the issue from https://github.com/tobymao/sqlglot/issues/7506
    in the context of DataHub's view parsing.
    """
    ITERATIONS = 100  # Parse each view 100 times

    # Use a simple query that accesses table names
    query = "SELECT * FROM project.dataset.table1 JOIN project.dataset.table2 ON table1.id = table2.id"
    parsed = sqlglot.parse_one(query, dialect="bigquery")

    # Get a reference to one of the table identifiers
    tables = list(parsed.find_all(sqlglot.exp.Table))
    if not tables:
        pytest.skip("No tables found in parsed query")

    identifier = tables[0].this
    before_refcount = sys.getrefcount(identifier)

    # Access Table.name repeatedly (simulating lineage extraction)
    for _ in range(ITERATIONS):
        for table in tables:
            _ = table.name  # This causes the leak in sqlglot[c]

    gc.collect()
    after_refcount = sys.getrefcount(identifier)
    refcount_increase = after_refcount - before_refcount

    print(
        f"\nReference count increase after {ITERATIONS} iterations: {refcount_increase}"
    )
    print(f"Expected leak size: ~{refcount_increase * len(tables)} refs")

    # In sqlglot[c] with the leak, this would be close to ITERATIONS * len(tables)
    # In pure Python or fixed version, this should be close to 0
    # Allow some variance (up to 10 refs) for normal Python reference tracking
    assert refcount_increase < 10, (
        f"Memory leak detected: refcount increased by {refcount_increase} "
        f"(expected <10). This indicates the sqlglot[c] Table.name leak "
        f"from https://github.com/tobymao/sqlglot/issues/7506"
    )


@pytest.mark.perf
def test_view_lineage_extraction_memory_usage():
    """
    Test memory usage when extracting lineage from many views.

    This simulates the BigQuery ingestion scenario where many views are parsed,
    leading to high memory usage due to the sqlglot[c] leak.
    """
    import tracemalloc

    # Create a simple schema resolver for testing
    schema_resolver = SchemaResolver(
        platform="bigquery",
        env="PROD",
    )

    # Add some schema info
    for table_name in ["orders", "customers", "products", "customer_summary"]:
        schema_resolver.add_raw_schema_info(
            urn=f"urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.{table_name},PROD)",
            schema_info={
                "customer_id": "STRING",
                "customer_name": "STRING",
                "order_id": "STRING",
                "product_id": "STRING",
                "product_name": "STRING",
                "order_date": "DATE",
                "total_amount": "NUMERIC",
                "total_revenue": "NUMERIC",
                "status": "STRING",
            },
        )

    # Start memory tracking
    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()

    # Parse multiple views (simulating 1% of the 16k views scenario)
    num_iterations = 160  # 1% of 16,443 views
    for i in range(num_iterations):
        # Cycle through sample queries
        query = SAMPLE_VIEW_QUERIES[i % len(SAMPLE_VIEW_QUERIES)]

        try:
            result = sqlglot_lineage(
                sql=query,
                schema_resolver=schema_resolver,
                default_db="project",
                default_schema="dataset",
                override_dialect="bigquery",
            )

            # Access table names (triggers the leak)
            _ = result.in_tables
            _ = result.out_tables

        except Exception as e:
            # Ignore parsing errors for this test
            print(f"Ignoring parse error for query {i}: {e}")
            continue

    # Force garbage collection
    gc.collect()

    # Take memory snapshot
    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    # Calculate memory increase
    top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
    total_increase_mb = sum(stat.size_diff for stat in top_stats) / 1024 / 1024

    print(
        f"\nMemory increase after parsing {num_iterations} views: {total_increase_mb:.2f} MB"
    )
    print(
        f"Projected for 16,443 views: {total_increase_mb * (16443 / num_iterations):.2f} MB"
    )

    # Print top memory consumers
    print("\nTop 10 memory increases:")
    for stat in top_stats[:10]:
        print(f"  {stat.size_diff / 1024:.1f} KB - {stat.traceback.format()[0]}")

    # With the leak, we'd expect significant memory growth
    # For 160 views, if we see >100MB increase, that projects to >10GB for 16k views
    # This is just a warning, not a hard assertion, as it depends on the fix status
    if total_increase_mb > 100:
        print(
            f"\n⚠️  WARNING: High memory usage detected ({total_increase_mb:.1f} MB for {num_iterations} views)"
        )
        print(
            f"   Projected for 10,000 views: {total_increase_mb * (10000 / num_iterations):.1f} MB"
        )
        print("   This may indicate the sqlglot[c] memory leak is present.")
        print("   See: https://github.com/tobymao/sqlglot/issues/7506")


@pytest.mark.perf
def test_parse_cache_memory_footprint():
    """
    Test the memory footprint of the LRU cache holding SqlParsingResult objects.

    The cache size is 1000 by default, and with complex views, each cached entry
    can be large due to column lineage information.
    """
    import sys

    from datahub.sql_parsing.sqlglot_lineage import _sqlglot_lineage_cached

    # Get cache info before
    cache_info_before = _sqlglot_lineage_cached.cache_info()
    print(f"\nCache info before: {cache_info_before}")

    # Create a schema resolver
    schema_resolver = SchemaResolver(platform="bigquery", env="PROD")

    # Add schemas for tables referenced in views
    for i in range(10):
        for j in range(100):  # 100 columns per table
            schema_resolver.add_raw_schema_info(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table{i},PROD)",
                schema_info={f"col{j}": "STRING"},
            )

    # Parse views and fill the cache
    results = []
    for i in range(100):  # Less than cache size to avoid eviction
        query = f"""
        CREATE VIEW view{i} AS
        SELECT t1.col1, t2.col2, t3.col3
        FROM project.dataset.table1 t1
        JOIN project.dataset.table2 t2 ON t1.col1 = t2.col1
        JOIN project.dataset.table3 t3 ON t2.col2 = t3.col2
        """

        try:
            result = sqlglot_lineage(
                sql=query,
                schema_resolver=schema_resolver,
                default_db="project",
                default_schema="dataset",
                override_dialect="bigquery",
            )
            results.append(result)
        except Exception:
            pass

    # Get cache info after
    cache_info_after = _sqlglot_lineage_cached.cache_info()
    print(f"Cache info after:  {cache_info_after}")
    print(f"Cache hits: {cache_info_after.hits}, misses: {cache_info_after.misses}")

    # Estimate memory per cached entry
    if cache_info_after.currsize > 0:
        # Get approximate size of one result
        result_size = sys.getsizeof(results[0]) if results else 0
        print(f"\nApproximate size per cached result: {result_size} bytes")
        print(
            f"Total cache size estimate: {result_size * cache_info_after.currsize / 1024 / 1024:.2f} MB"
        )
        print(
            f"Projected for 1000 cache entries: {result_size * 1000 / 1024 / 1024:.2f} MB"
        )
