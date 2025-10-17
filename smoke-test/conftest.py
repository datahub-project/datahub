import os
import json
from pathlib import Path

import pytest
from typing import Dict, List, Optional, Tuple
from _pytest.nodes import Item
import requests
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph, get_default_graph

from tests.test_result_msg import send_message
from tests.utilities import env_vars
from tests.utils import (
    TestSessionWrapper,
    get_frontend_session,
    wait_for_healthcheck_util,
    ingest_file_via_rest,
    delete_urns,
    delete_urns_from_file,
    wait_for_writes_to_sync,
)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"
# Suppress logging manager to prevent I/O errors during pytest teardown
os.environ["DATAHUB_SUPPRESS_LOGGING_MANAGER"] = "1"


def build_auth_session():
    wait_for_healthcheck_util(requests)
    return TestSessionWrapper(get_frontend_session())


@pytest.fixture(scope="session")
def auth_session():
    auth_session = build_auth_session()
    yield auth_session
    auth_session.destroy()


def build_graph_client(auth_session, openapi_ingestion=False):
    graph: DataHubGraph = DataHubGraph(
        config=DatahubClientConfig(
            server=auth_session.gms_url(), token=auth_session.gms_token(),
            openapi_ingestion=openapi_ingestion
        )
    )
    return graph


@pytest.fixture(scope="session")
def graph_client(auth_session) -> DataHubGraph:
    return build_graph_client(auth_session)


@pytest.fixture(scope="session")
def openapi_graph_client(auth_session) -> DataHubGraph:
    return build_graph_client(auth_session, openapi_ingestion=True)


@pytest.fixture(scope="function", autouse=True)
def clear_graph_cache():
    """Clear the get_default_graph LRU cache before each test.

    This ensures that tests using run_datahub_cmd() with custom environment
    variables get a fresh DataHubGraph instance instead of a cached one with
    stale credentials.
    """
    get_default_graph.cache_clear()
    yield


def _ingest_cleanup_data_impl(
    auth_session,
    graph_client,
    data_file: str,
    test_name: str,
    to_delete_urns: Optional[List[str]] = None
):
    """Helper for ingesting test data with automatic cleanup.

    Args:
        auth_session: The authenticated session
        graph_client: The DataHub graph client
        data_file: Path to the data file to ingest
        test_name: Name of the test (for logging)
        to_delete_urns: URNs to delete after cleanup

    Usage in test files:
        @pytest.fixture(scope="module", autouse=True)
        def ingest_cleanup_data(auth_session, graph_client):
            yield from _ingest_cleanup_data_impl(
                auth_session, graph_client,
                "tests/tags_and_terms/data.json",
                "tags_and_terms"
            )
    """
    print(f"deleting {test_name} test data for idempotency")
    delete_urns_from_file(graph_client, data_file)
    print(f"ingesting {test_name} test data")
    ingest_file_via_rest(auth_session, data_file)
    wait_for_writes_to_sync()
    yield
    print(f"removing {test_name} test data")
    delete_urns_from_file(graph_client, data_file)
    if to_delete_urns:
        delete_urns(graph_client, to_delete_urns)
    wait_for_writes_to_sync()



def pytest_sessionfinish(session, exitstatus):
    """whole test run finishes."""
    send_message(exitstatus)


def bin_pack_tasks(tasks, n_buckets):
    """
    Bin-pack tasks into n_buckets with roughly equal weights.

    Parameters:
    tasks (list): List of (task, weight) tuples. If only task is provided, weight defaults to 1.
    n_buckets (int): Number of buckets to distribute tasks into.

    Returns:
    list: List of buckets, where each bucket is a list of tasks.
    """
    # Normalize the tasks to ensure they're all (task, weight) tuples
    normalized_tasks = []
    for task in tasks:
        if isinstance(task, tuple) and len(task) == 2:
            normalized_tasks.append(task)
        else:
            normalized_tasks.append((task, 1))

    # Sort tasks by weight in descending order
    sorted_tasks = sorted(normalized_tasks, key=lambda x: x[1], reverse=True)

    # Initialize the buckets with zero weight
    buckets: List = [[] for _ in range(n_buckets)]
    bucket_weights: List[int] = [0] * n_buckets

    # Assign each task to the bucket with the lowest current weight
    for task, weight in sorted_tasks:
        # Find the bucket with the minimum weight
        min_bucket_idx = bucket_weights.index(min(bucket_weights))

        # Add the task to this bucket
        buckets[min_bucket_idx].append(task)
        bucket_weights[min_bucket_idx] += weight

    return buckets


def load_pytest_test_weights() -> Dict[str, float]:
    """
    Load pytest test weights from JSON file.

    Returns:
        Dictionary mapping test IDs (classname::test_name) to durations in seconds.
        Returns empty dict if weights file doesn't exist.
    """
    weights_file = Path(__file__).parent / "pytest_test_weights.json"

    if not weights_file.exists():
        return {}

    try:
        with open(weights_file) as f:
            weights_data = json.load(f)

        # Convert to dict: {"test_e2e::test_gms_get_dataset": 262.807, ...}
        return {
            item["testId"]: float(item["duration"][:-1])  # Strip 's' suffix
            for item in weights_data
        }
    except Exception as e:
        print(f"Warning: Failed to load pytest test weights: {e}")
        return {}


def aggregate_module_weights(items: List[Item], test_weights: Dict[str, float]) -> List[Tuple[str, List[Item], float]]:
    """
    Group test items by module and aggregate their weights.

    Args:
        items: List of pytest test items
        test_weights: Dictionary mapping test IDs to durations

    Returns:
        List of (module_path, items_in_module, total_weight) tuples
    """
    from collections import defaultdict

    # Group items by module (file path)
    modules: Dict[str, List[Item]] = defaultdict(list)
    for item in items:
        # Get the module path from the item's fspath
        module_path = str(item.fspath)
        modules[module_path].append(item)

    # Calculate total weight for each module
    module_data = []
    for module_path, module_items in modules.items():
        total_weight = 0.0
        for item in module_items:
            # Build test ID from nodeid
            # nodeid format: "path/to/test_file.py::TestClass::test_method" or "path/to/test_file.py::test_function"
            # We need to extract everything after the file path
            nodeid = item.nodeid
            if "::" in nodeid:
                # Get the part after the filename
                test_id = nodeid.split(".py::", 1)[1] if ".py::" in nodeid else nodeid
            else:
                test_id = item.name

            weight = test_weights.get(test_id, 1.0)  # Default to 1.0 if not found
            total_weight += weight

        module_data.append((module_path, module_items, total_weight))

    return module_data


def get_batch_start_end(num_tests: int) -> Tuple[int, int]:
    batch_count = env_vars.get_batch_count()

    batch_number = env_vars.get_batch_number()

    if batch_count == 0 or batch_count > num_tests:
        raise ValueError(
            f"Invalid batch count {batch_count}: must be >0 and <= {num_tests} (num_tests)"
        )
    if batch_number >= batch_count:
        raise ValueError(
            f"Invalid batch number: {batch_number}, must be less than {batch_count} (zer0 based index)"
        )

    batch_size = round(num_tests / batch_count)

    batch_start = batch_size * batch_number
    batch_end = batch_start + batch_size
    # We must have exactly as many batches as specified by BATCH_COUNT.
    if (
            batch_number == batch_count - 1  # this is the last batch
    ):  # If ths is last batch put any remaining tests in the last batch.
        batch_end = num_tests

    if batch_count > 0:
        print(f"Running tests for batch {batch_number} of {batch_count}")

    return batch_start, batch_end

def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[Item]
) -> None:
    if env_vars.get_test_strategy() == "cypress":
        return  # We launch cypress via pytests, but needs a different batching mechanism at cypress level.

    # Get batch configuration
    batch_count_env = os.getenv("BATCH_COUNT", "1")
    batch_count = int(batch_count_env)
    batch_number_env = os.getenv("BATCH_NUMBER", "0")
    batch_number = int(batch_number_env)

    if batch_count <= 1:
        # No batching needed
        return

    # Load test weights
    test_weights = load_pytest_test_weights()

    # Group items by module and aggregate weights
    module_data = aggregate_module_weights(items, test_weights)

    # Sort modules by path for stability
    module_data.sort(key=lambda x: x[0])

    # Create weighted tuples for bin-packing: (module_path, weight)
    # We'll also keep track of the items for each module
    module_map = {module_path: module_items for module_path, module_items, _ in module_data}
    weighted_modules = [(module_path, total_weight) for module_path, _, total_weight in module_data]

    print(f"Batching {len(items)} tests from {len(weighted_modules)} modules across {batch_count} batches")

    # Apply bin-packing to modules
    module_batches = bin_pack_tasks(weighted_modules, batch_count)

    # Get the modules for this batch
    selected_modules = module_batches[batch_number]

    # Flatten back to individual test items
    selected_items = []
    for module_path in selected_modules:
        selected_items.extend(module_map[module_path])

    # Sort items within batch by nodeid for stability
    selected_items.sort(key=lambda x: x.nodeid)

    print(f"Batch {batch_number}: Running {len(selected_items)} tests from {len(selected_modules)} modules")

    # Replace items with the filtered list
    items[:] = selected_items

