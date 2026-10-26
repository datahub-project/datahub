# ABOUTME: Generic utility for running test functions concurrently using ThreadPoolExecutor.
# ABOUTME: Enables parallel execution of parameterized test logic for faster smoke test runs.

import concurrent.futures
import logging
from typing import Any, Callable, Dict, List

import pytest
from _pytest.outcomes import Skipped

logger = logging.getLogger(__name__)


def run_concurrent_tests(
    test_cases: List[Any],
    test_fn: Callable[[Any], None],
    num_workers: int = 5,
    test_name: str = "test",
) -> None:
    """
    Execute a test function concurrently for multiple test cases.

    Args:
        test_cases: List of test case parameters (e.g., entity types)
        test_fn: Test function that takes a single test case parameter
        num_workers: Number of concurrent workers (default: 5)
        test_name: Name of the test for logging purposes

    Raises:
        AssertionError: If any test case fails, raises with details of all failures
        pytest.skip.Exception: If every case skipped and none passed/failed

    Example:
        >>> def test_entity(entity_type):
        ...     result = search(entity_type)
        ...     assert result["total"] > 0
        ...
        >>> entity_types = ["dataset", "dashboard", "chart"]
        >>> run_concurrent_tests(entity_types, test_entity, test_name="test_search")
    """
    failures: Dict[Any, Exception] = {}
    skipped: Dict[Any, str] = {}
    passed = 0

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
                passed += 1
                logger.info(f"{test_name}[{test_case}] passed")
            except Skipped as e:
                # pytest.skip() in a worker — not a failure; track separately so
                # mixed pass+skip stays green and all-skip is not a false pass.
                skipped[test_case] = str(e)
                logger.warning(f"{test_name}[{test_case}] skipped: {e}")
            except Exception as e:
                logger.error(f"{test_name}[{test_case}] failed: {e}")
                failures[test_case] = e

    if skipped:
        skip_summary = "\n".join(
            f"  - {test_case}: {msg}" for test_case, msg in skipped.items()
        )
        logger.info(
            f"{test_name}: skipped {len(skipped)}/{len(test_cases)} cases:\n{skip_summary}"
        )

    # Report all failures at once
    if failures:
        failure_summary = "\n".join(
            f"  - {test_case}: {str(exc)}" for test_case, exc in failures.items()
        )
        raise AssertionError(
            f"{test_name} failed for {len(failures)}/{len(test_cases)} test cases:\n{failure_summary}"
        )

    # All workers skipped (e.g. ES lag) — yellow skip, not vacuous green
    if passed == 0 and skipped:
        skip_summary = "\n".join(
            f"  - {test_case}: {msg}" for test_case, msg in skipped.items()
        )
        pytest.skip(f"{test_name}: all {len(skipped)} cases skipped:\n{skip_summary}")


def run_concurrent_tests_with_args(
    test_cases: List[tuple],
    test_fn: Callable[..., None],
    num_workers: int = 5,
    test_name: str = "test",
) -> None:
    """
    Execute a test function concurrently for multiple test cases with multiple arguments.

    Args:
        test_cases: List of tuples containing test case arguments
        test_fn: Test function that takes unpacked arguments from each tuple
        num_workers: Number of concurrent workers (default: 5)
        test_name: Name of the test for logging purposes

    Raises:
        AssertionError: If any test case fails, raises with details of all failures
        pytest.skip.Exception: If every case skipped and none passed/failed

    Example:
        >>> def test_entity(entity_type, api_name):
        ...     result = search(entity_type, api_name)
        ...     assert result["total"] > 0
        ...
        >>> test_cases = [("dataset", "dataset"), ("dashboard", "dashboard")]
        >>> run_concurrent_tests_with_args(test_cases, test_entity, test_name="test_search")
    """
    failures: Dict[tuple, Exception] = {}
    skipped: Dict[tuple, str] = {}
    passed = 0

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
                passed += 1
                logger.info(f"{test_name}{test_case} passed")
            except Skipped as e:
                skipped[test_case] = str(e)
                logger.warning(f"{test_name}{test_case} skipped: {e}")
            except Exception as e:
                logger.error(f"{test_name}{test_case} failed: {e}")
                failures[test_case] = e

    if skipped:
        skip_summary = "\n".join(
            f"  - {test_case}: {msg}" for test_case, msg in skipped.items()
        )
        logger.info(
            f"{test_name}: skipped {len(skipped)}/{len(test_cases)} cases:\n{skip_summary}"
        )

    # Report all failures at once
    if failures:
        failure_summary = "\n".join(
            f"  - {test_case}: {str(exc)}" for test_case, exc in failures.items()
        )
        raise AssertionError(
            f"{test_name} failed for {len(failures)}/{len(test_cases)} test cases:\n{failure_summary}"
        )

    if passed == 0 and skipped:
        skip_summary = "\n".join(
            f"  - {test_case}: {msg}" for test_case, msg in skipped.items()
        )
        pytest.skip(f"{test_name}: all {len(skipped)} cases skipped:\n{skip_summary}")
