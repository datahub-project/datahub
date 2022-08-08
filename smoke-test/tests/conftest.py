import os

import pytest
import requests

from tests.utils import get_frontend_url, wait_for_healthcheck_util
from tests.test_result_msg import send_message

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    session = requests.Session()

    headers = {
        "Content-Type": "application/json",
    }
    data = '{"username":"datahub", "password":"datahub"}'
    response = session.post(f"{get_frontend_url()}/logIn", headers=headers, data=data)
    response.raise_for_status()

    yield session


# TODO: Determine whether we need this or not.
@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def pytest_sessionfinish(session, exitstatus):
    """ whole test run finishes. """
    send_message(exitstatus)
