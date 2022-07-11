import os

import pytest
import requests
from datahub.cli.docker import check_local_docker_containers

from tests.utils import get_frontend_url, get_gms_url, check_k8s_endpoint, is_k8s_enabled

# Disable telemetry
os.putenv("DATAHUB_TELEMETRY_ENABLED", "false")


@pytest.fixture(scope="session")
def wait_for_healthchecks():
    if is_k8s_enabled():
        # Simply assert that kubernetes endpoints are healthy, but don't wait.
        assert not check_k8s_endpoint(f"{get_frontend_url()}/admin")
        assert not check_k8s_endpoint(f"{get_gms_url()}/health")
    else:
        # Simply assert that docker is healthy, but don't wait.
        assert not check_local_docker_containers()
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
