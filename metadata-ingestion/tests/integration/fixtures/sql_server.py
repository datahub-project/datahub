import os
import pytest

def is_responsive(container: str):
    ready_string="SQL Server is now ready for client connections."
    ret = os.system(f"docker logs {container} | grep -q \"{ready_string}\"")
    return ret == 0
    

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "tests/integration/", "docker-compose.yml")

@pytest.fixture(scope="session")
def sql_server(docker_ip, docker_services):
    port = docker_services.port_for("testsqlserver", 1433)
    docker_services.wait_until_responsive(
        timeout=30.0, pause=0.1, check=lambda: is_responsive("testsqlserver"))
    return port


