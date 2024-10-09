import os

import pytest
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


@pytest.fixture(scope="session")
def auth_session():
    wait_for_healthcheck_util(requests)
    auth_session = TestSessionWrapper(get_frontend_session())
    yield auth_session
    auth_session.destroy()


@pytest.fixture(scope="session")
def graph_client(auth_session) -> DataHubGraph:
    print(auth_session.cookies)
    graph: DataHubGraph = DataHubGraph(
        config=DatahubClientConfig(
            server=auth_session.gms_url(), token=auth_session.gms_token()
        )
    )
    return graph


def pytest_sessionfinish(session, exitstatus):
    """whole test run finishes."""
    send_message(exitstatus)
