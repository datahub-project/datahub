import os

import pytest
from typing import List, Tuple
from _pytest.nodes import Item
import requests
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from tests.test_result_msg import send_message
from tests.utils import (
    TestSessionWrapper,
    get_frontend_session,
    wait_for_healthcheck_util,
)

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


def build_auth_session():
    wait_for_healthcheck_util(requests)
    return TestSessionWrapper(get_frontend_session())


@pytest.fixture(scope="session")
def auth_session():
    auth_session = build_auth_session()
    yield auth_session
    auth_session.destroy()


def build_graph_client(auth_session):
    print(auth_session.cookies)
    graph: DataHubGraph = DataHubGraph(
        config=DatahubClientConfig(
            server=auth_session.gms_url(), token=auth_session.gms_token()
        )
    )
    return graph


@pytest.fixture(scope="session")
def graph_client(auth_session) -> DataHubGraph:
    return build_graph_client(auth_session)


def pytest_sessionfinish(session, exitstatus):
    """whole test run finishes."""
    send_message(exitstatus)


def get_batch_start_end(num_tests: int) -> Tuple[int, int]:
    batch_count_env = os.getenv("BATCH_COUNT", 1)
    batch_count = int(batch_count_env)

    batch_number_env = os.getenv("BATCH_NUMBER", 0)
    batch_number = int(batch_number_env)

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
        num_tests - batch_end < batch_size
    ):  # We must have exactly as many batches as specified by BATCH_COUNT, put the remaining in the last batch.
        batch_end = num_tests

    if batch_count > 0:
        print(f"Running tests for batch {batch_number} of {batch_count}")

    return batch_start, batch_end


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: List[Item]
) -> None:
    if os.getenv("TEST_STRATEGY") == "cypress":
        return  # We launch cypress via pytests, but needs a different batching mechanism at cypress level.

    # If BATCH_COUNT and BATCH_ENV vars are set, splits the pytests to batches and runs filters only the BATCH_NUMBER
    # batch for execution. Enables multiple parallel launches. Current implementation assumes all test are of equal
    # weight for batching. TODO. A weighted batching method can help make batches more equal sized by cost.
    # this effectively is a no-op if BATCH_COUNT=1
    start_index, end_index = get_batch_start_end(num_tests=len(items))

    items.sort(key=lambda x: x.nodeid)  # we want the order to be stable across batches
    # replace items with the filtered list
    print(f"Running tests for batch {start_index}-{end_index}")
    items[:] = items[start_index:end_index]
