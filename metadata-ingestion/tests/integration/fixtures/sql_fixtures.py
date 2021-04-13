import os

import pytest

from tests.test_helpers.docker_helpers import wait_for_port

wait_for_db = wait_for_port


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests/integration/", "docker-compose.yml"
    )


@pytest.fixture(scope="session")
def sql_server(docker_ip, docker_services):
    return wait_for_db(docker_services, "testsqlserver", 1433)


@pytest.fixture(scope="session")
def mysql(docker_ip, docker_services):
    return wait_for_db(docker_services, "testmysql", 3306)


@pytest.fixture(scope="session")
def ldap(docker_ip, docker_services):
    return wait_for_db(docker_services, "openldap", 3306)
