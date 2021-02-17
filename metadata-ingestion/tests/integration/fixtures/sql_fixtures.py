import os
import time

import pytest


def is_responsive(container: str, port: int):
    ret = os.system(f"docker exec {container} /setup/wait-for-it.sh localhost:{port}")
    return ret == 0


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(
        str(pytestconfig.rootdir), "tests/integration/", "docker-compose.yml"
    )


def wait_for_db(docker_services, container_name, container_port):
    port = docker_services.port_for(container_name, container_port)
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.1,
        check=lambda: is_responsive(container_name, container_port),
    )

    # TODO: this is an ugly hack.
    time.sleep(5)
    return port


@pytest.fixture(scope="session")
def sql_server(docker_ip, docker_services):
    return wait_for_db(docker_services, "testsqlserver", 1433)


@pytest.fixture(scope="session")
def mysql(docker_ip, docker_services):
    return wait_for_db(docker_services, "testmysql", 3306)
