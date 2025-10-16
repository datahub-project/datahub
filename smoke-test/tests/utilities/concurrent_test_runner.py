# ABOUTME: Generic utility for running test functions concurrently using ThreadPoolExecutor.
# ABOUTME: Enables parallel execution of parameterized test logic for faster smoke test runs.

import concurrent.futures
import logging
from typing import Any, Callable, Dict, List

logger = logging.getLogger(__name__)


def run_concurrent_tests(
    test_cases: List[Any],
    test_fn: Callable[[Any], None],
    num_workers: int = 3,
    test_name: str = "test",
) -> None:
    """
    Execute a test function concurrently for multiple test cases.

    Args:
        test_cases: List of test case parameters (e.g., entity types)
        test_fn: Test function that takes a single test case parameter
        num_workers: Number of concurrent workers (default: 3)
        test_name: Name of the test for logging purposes

    Raises:
        AssertionError: If any test case fails, raises with details of all failures

    Example:
        >>> def test_entity(entity_type):
        ...     result = search(entity_type)
        ...     assert result["total"] > 0
        ...
        >>> entity_types = ["dataset", "dashboard", "chart"]
        >>> run_concurrent_tests(entity_types, test_entity, test_name="test_search")
    """
    failures: Dict[Any, Exception] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all test cases
        future_to_case = {
            executor.submit(test_fn, test_case): test_case for test_case in test_cases
        }

        # Wait for completion and collect results
        for future in concurrent.futures.as_completed(future_to_case):
            test_case = future_to_case[future]
            try:
                future.result()
                logger.info(f"{test_name}[{test_case}] passed")
            except Exception as e:
                logger.error(f"{test_name}[{test_case}] failed: {e}")
                failures[test_case] = e

    # Report all failures at once
    if failures:
        failure_summary = "\n".join(
            f"  - {test_case}: {str(exc)}" for test_case, exc in failures.items()
        )
        raise AssertionError(
            f"{test_name} failed for {len(failures)}/{len(test_cases)} test cases:\n{failure_summary}"
        )


def run_concurrent_tests_with_args(
    test_cases: List[tuple],
    test_fn: Callable[..., None],
    num_workers: int = 3,
    test_name: str = "test",
) -> None:
    """
    Execute a test function concurrently for multiple test cases with multiple arguments.

    Args:
        test_cases: List of tuples containing test case arguments
        test_fn: Test function that takes unpacked arguments from each tuple
        num_workers: Number of concurrent workers (default: 3)
        test_name: Name of the test for logging purposes

    Raises:
        AssertionError: If any test case fails, raises with details of all failures

    Example:
        >>> def test_entity(entity_type, api_name):
        ...     result = search(entity_type, api_name)
        ...     assert result["total"] > 0
        ...
        >>> test_cases = [("dataset", "dataset"), ("dashboard", "dashboard")]
        >>> run_concurrent_tests_with_args(test_cases, test_entity, test_name="test_search")
    """
    failures: Dict[tuple, Exception] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all test cases
        future_to_case = {
            executor.submit(test_fn, *test_case): test_case for test_case in test_cases
        }

        # Wait for completion and collect results
        for future in concurrent.futures.as_completed(future_to_case):
            test_case = future_to_case[future]
            try:
                future.result()
                logger.info(f"{test_name}{test_case} passed")
            except Exception as e:
                logger.error(f"{test_name}{test_case} failed: {e}")
                failures[test_case] = e

    # Report all failures at once
    if failures:
        failure_summary = "\n".join(
            f"  - {test_case}: {str(exc)}" for test_case, exc in failures.items()
        )
        raise AssertionError(
            f"{test_name} failed for {len(failures)}/{len(test_cases)} test cases:\n{failure_summary}"
        )
