# Copied largely without modification from datahub-project/datahub/smoke-test/tests

import os

import pytest

from tests.utils import get_frontend_session, wait_for_healthcheck_util

# Disable telemetry
os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    wait_for_healthcheck_util()
    yield


@pytest.fixture(scope="session")
def frontend_session(wait_for_healthchecks):
    yield get_frontend_session()


# TODO: Determine whether we need this or not.
@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass
