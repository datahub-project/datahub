"""Performance tests for SQL aggregator batch processing.

This module provides performance benchmarking for the SQL aggregator's batch processing
capabilities, including parallel parsing with configurable worker counts.

Usage:
    # Run the benchmark test (runs matrix of configurations)
    pytest tests/performance/test_sql_aggregator.py::test_benchmark -s --log-cli-level=INFO
"""

import logging
import os
import time
from typing import Iterable, List

import pytest

from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from tests.unit.sql_parsing.test_sql_aggregator import (  # type: ignore[import-untyped]
    generate_queries_at_scale,
)

logger = logging.getLogger(__name__)

# Matrix parameters for performance testing
WORKERS_OPTIONS = [1, 2, 8]
QUERY_COUNT_OPTIONS = [10000]
BATCH_SIZE_OPTIONS = [500]


def run_configuration(
    workers: int,
    batch_size: int,
    queries: Iterable[ObservedQuery],
) -> float:
    """Run a given configuration and compute elapsed time.

    Args:
        workers: Number of worker threads for parallel parsing
        batch_size: Size of each batch
        queries: Iterable of ObservedQuery objects to process

    Returns:
        Elapsed time in seconds
    """
    # Set environment variable for worker count
    os.environ["SQL_AGGREGATOR_PARSING_WORKERS"] = str(workers)
    try:
        aggregator = SqlParsingAggregator(
            platform="redshift",
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
    finally:
        # Clean up environment variable
        os.environ.pop("SQL_AGGREGATOR_PARSING_WORKERS", None)

    # Convert queries to list and split into batches
    queries_list = list(queries)
    query_batches = [
        queries_list[i : i + batch_size]
        for i in range(0, len(queries_list), batch_size)
    ]

    # Measure processing time
    start_time = time.perf_counter()
    for batch in query_batches:
        aggregator.add_batch(batch)
    elapsed_time = time.perf_counter() - start_time

    aggregator.close()

    return elapsed_time


def test_benchmark(pytestconfig: pytest.Config) -> None:
    """Run benchmark test across a matrix of configurations.

    Tests with:
    - Workers: [1, 2, 4, 8, 10]
    - Query counts: [10000, 100000]
    - Batch sizes: [200, 400]

    Results are logged for each configuration.
    """
    seed = int(
        os.getenv("SQL_AGGREGATOR_TEST_SEED", "42")
    )  # Fixed seed for reproducibility

    results: List[dict] = []
    total_combinations = (
        len(WORKERS_OPTIONS) * len(QUERY_COUNT_OPTIONS) * len(BATCH_SIZE_OPTIONS)
    )

    print("=" * 100)
    print(f"Running performance benchmark across {total_combinations} configurations")
    print("=" * 100)

    combination_num = 0

    for query_count in QUERY_COUNT_OPTIONS:
        # Generate queries once per query_count (all configurations with same count use same queries)
        print(f"Generating {query_count} queries...")
        all_queries = generate_queries_at_scale(query_count, seed=seed)

        for workers in WORKERS_OPTIONS:
            for batch_size in BATCH_SIZE_OPTIONS:
                combination_num += 1
                print(
                    f"[{combination_num}/{total_combinations}] "
                    f"Workers={workers}, Queries={query_count}, Batch={batch_size}"
                )

                # Run configuration and measure time
                elapsed_time = run_configuration(workers, batch_size, all_queries)

                avg_time_per_query = (
                    elapsed_time / query_count if query_count > 0 else 0.0
                )
                throughput = query_count / elapsed_time if elapsed_time > 0 else 0.0

                print(
                    f"  Elapsed: {elapsed_time:.2f}s, "
                    f"Avg: {avg_time_per_query * 1000:.2f}ms/query, "
                    f"Throughput: {throughput:.2f} queries/sec"
                )

                results.append(
                    {
                        "workers": workers,
                        "query_count": query_count,
                        "batch_size": batch_size,
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

    headers = [
        "Workers",
        "Query Count",
        "Batch Size",
        "Elapsed Time (s)",
        "Avg Time/Query (ms)",
        "Throughput (q/s)",
    ]
    print("| " + " | ".join(headers) + " |")
    print("|" + "---|" * len(headers))

    for res in results:
        print(
            f"| {res['workers']} | {res['query_count']} | {res['batch_size']} | "
            f"{res['elapsed_time']:.2f} | {res['avg_time_per_query'] * 1000:.2f} | "
            f"{res['throughput']:.2f} |"
        )

    print("")
    print("=" * 100)

    # Basic assertions to ensure tests ran
    assert len(results) == total_combinations, (
        f"Expected {total_combinations} results, got {len(results)}"
    )
    assert all(r["elapsed_time"] > 0 for r in results), (
        "All configurations should have positive elapsed time"
    )
