"""Performance tests for SQL aggregator.

This module provides performance benchmarking for the SQL aggregator's
query processing capabilities.

Usage:
    # Run the benchmark test
    pytest tests/performance/test_sql_aggregator.py::test_benchmark -s --log-cli-level=INFO
"""

import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pytest

from datahub.configuration.env_vars import get_ci
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)

# Matrix parameters for performance testing
# Use smaller values in CI to keep test times reasonable, larger values for local testing
QUERY_COUNT_OPTIONS = [100, 1000] if get_ci() else [100, 1000, 10000]

# Throughput should be < 100 queries/sec (threshold to detect heavy performance regressions)
MAX_THROUGHPUT_THRESHOLD = 100.0


def generate_queries_at_scale(
    num_queries: int, seed: Optional[int] = None
) -> List[ObservedQuery]:
    """Generate test queries at different scales with varying complexity and randomness.

    Args:
        num_queries: Number of queries to generate
        seed: Optional random seed for reproducibility

    Returns:
        List of ObservedQuery objects with varying complexity and randomness
    """
    if seed is not None:
        random.seed(seed)

    queries = []
    base_timestamp = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Query templates with varying complexity
    simple_queries = [
        "SELECT * FROM table_{i}",
        "SELECT col1, col2 FROM table_{i} WHERE col1 > {val}",
        "INSERT INTO target_{i} SELECT * FROM source_{i}",
        "SELECT COUNT(*) FROM table_{i} WHERE col1 = {val}",
    ]

    medium_queries = [
        "SELECT t1.col1, t2.col2 FROM table_{i} t1 JOIN table_{i}_2 t2 ON t1.id = t2.id WHERE t1.col1 > {val}",
        "CREATE TABLE result_{i} AS SELECT col1, col2, col3 FROM table_{i} WHERE col1 > {val}",
        "INSERT INTO target_{i} (col1, col2) SELECT col1, col2 FROM source_{i} WHERE col1 IS NOT NULL AND col2 < {val}",
        "SELECT t1.*, t2.col3 FROM table_{i} t1 LEFT JOIN table_{i}_2 t2 ON t1.id = t2.id WHERE t1.col1 BETWEEN {val} AND {val2}",
    ]

    complex_queries = [
        "SELECT t1.col1, t2.col2, t3.col3 FROM table_{i} t1 "
        "LEFT JOIN table_{i}_2 t2 ON t1.id = t2.id "
        "INNER JOIN table_{i}_3 t3 ON t2.id = t3.id "
        "WHERE t1.col1 > {val} AND t2.col2 < {val2} AND t3.col3 = {val3}",
        "CREATE TABLE result_{i} AS "
        "SELECT col1, SUM(col2) as total, COUNT(*) as cnt, AVG(col3) as avg_val "
        "FROM table_{i} "
        "WHERE col1 > {val} "
        "GROUP BY col1 "
        "HAVING total > {val2}",
        "WITH cte_{i} AS (SELECT col1, col2 FROM table_{i} WHERE col1 > {val}) "
        "SELECT c.col1, c.col2, t.col3 FROM cte_{i} c JOIN table_{i}_2 t ON c.col1 = t.col1 WHERE t.col3 < {val2}",
    ]

    all_templates = simple_queries + medium_queries + complex_queries

    for i in range(num_queries):
        # Randomly select template and add randomness to values
        template = random.choice(all_templates)

        # Generate random values for placeholders
        val = random.randint(1, 1000)
        val2 = random.randint(100, 2000)
        val3 = random.randint(1, 500)

        query_text = template.format(i=i, val=val, val2=val2, val3=val3)

        # Add randomness to other fields
        user_idx = random.randint(0, 19)  # 20 different users
        timestamp_offset = random.randint(0, num_queries * 2)  # Random but increasing

        queries.append(
            ObservedQuery(
                query=query_text,
                default_db="dev",
                default_schema="public",
                timestamp=base_timestamp
                + timedelta(seconds=i + timestamp_offset % 100),  # Mostly increasing
                user=CorpUserUrn(f"user{user_idx}"),
                extra_info={"sequence": i},  # Track original sequence for validation
            )
        )

    return queries


def run_configuration(
    queries: List[ObservedQuery],
) -> float:
    """Run a given configuration and compute elapsed time.

    Args:
        queries: List of ObservedQuery objects to process

    Returns:
        Elapsed time in seconds
    """
    aggregator = SqlParsingAggregator(
        platform="redshift",
        generate_lineage=True,
        generate_usage_statistics=False,
        generate_operations=False,
    )

    # Measure processing time
    start_time = time.perf_counter()
    for query in queries:
        aggregator.add(query)
    elapsed_time = time.perf_counter() - start_time

    aggregator.close()

    return elapsed_time


@pytest.mark.integration
def test_benchmark(pytestconfig: pytest.Config) -> None:
    """Run benchmark test across a matrix of configurations.

    Tests with:
    - Query counts: [1000] (configurable via QUERY_COUNT_OPTIONS)

    Results are logged for each configuration.
    """
    # Set environment variable to skip joins for performance benchmarking
    os.environ["DATAHUB_SQL_AGG_SKIP_JOINS"] = "true"

    seed = int(
        os.getenv("SQL_AGGREGATOR_TEST_SEED", "42")
    )  # Fixed seed for reproducibility

    results: List[dict] = []
    total_combinations = len(QUERY_COUNT_OPTIONS)

    print("=" * 100)
    print(f"Running performance benchmark across {total_combinations} configurations")
    print("=" * 100)

    for combination_num, query_count in enumerate(QUERY_COUNT_OPTIONS, start=1):
        # Generate queries once per query_count
        print(f"Generating {query_count} queries...")
        all_queries = generate_queries_at_scale(query_count, seed=seed)

        print(f"[{combination_num}/{total_combinations}] Queries={query_count}")

        # Run configuration and measure time
        elapsed_time = run_configuration(all_queries)

        avg_time_per_query = elapsed_time / query_count if query_count > 0 else 0.0
        throughput = query_count / elapsed_time if elapsed_time > 0 else 0.0

        print(
            f"  Elapsed: {elapsed_time:.2f}s, "
            f"Avg: {avg_time_per_query * 1000:.2f}ms/query, "
            f"Throughput: {throughput:.2f} queries/sec"
        )

        results.append(
            {
                "query_count": query_count,
                "elapsed_time": elapsed_time,
                "avg_time_per_query": avg_time_per_query,
                "throughput": throughput,
            }
        )

    # Print results table
    print("")
    print("=" * 100)
    print("Performance Benchmark Results")
    print("=" * 100)
    print("")

    # Define column headers and widths for better alignment
    col_widths = {
        "query_count": 15,
        "elapsed_time": 18,
        "avg_time": 22,
        "throughput": 18,
    }

    # Print header row with centered text
    print(
        f"| {'Query Count':^{col_widths['query_count']}} | "
        f"{'Elapsed Time (s)':^{col_widths['elapsed_time']}} | "
        f"{'Avg Time/Query (ms)':^{col_widths['avg_time']}} | "
        f"{'Throughput (q/s)':^{col_widths['throughput']}} |"
    )

    # Print separator row
    print(
        f"| {'-' * col_widths['query_count']} | "
        f"{'-' * col_widths['elapsed_time']} | "
        f"{'-' * col_widths['avg_time']} | "
        f"{'-' * col_widths['throughput']} |"
    )

    # Print data rows with right-aligned numbers
    for res in results:
        query_count_str: str = f"{res['query_count']:,}".rjust(
            col_widths["query_count"]
        )
        elapsed_time_str: str = f"{res['elapsed_time']:.2f}".rjust(
            col_widths["elapsed_time"]
        )
        avg_time_str: str = f"{res['avg_time_per_query'] * 1000:.2f}".rjust(
            col_widths["avg_time"]
        )
        throughput_str: str = f"{res['throughput']:.2f}".rjust(col_widths["throughput"])

        print(
            f"| {query_count_str} | {elapsed_time_str} | {avg_time_str} | {throughput_str} |"
        )

    print("")
    print("=" * 100)

    # Basic assertions to ensure tests ran successfully
    assert len(results) == total_combinations, (
        f"Expected {total_combinations} results, got {len(results)}"
    )
    assert all(r["elapsed_time"] > 0 for r in results), (
        "All configurations should have positive elapsed time"
    )
    assert all(r["throughput"] < MAX_THROUGHPUT_THRESHOLD for r in results), (
        "All configurations should have throughput below the threshold"
    )
