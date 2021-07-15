import contextlib
import subprocess

import pytest
import pytest_docker.plugin


def is_responsive(container_name: str, port: int) -> bool:
    ret = subprocess.run(
        f"docker exec {container_name} /bin/bash -c 'echo -n > /dev/tcp/localhost/{port}'",
        shell=True,
    )
    return ret.returncode == 0


def wait_for_port(
    docker_services: pytest_docker.plugin.Services,
    container_name: str,
    container_port: int,
    timeout: float = 30.0,
) -> None:
    try:
        # port = docker_services.port_for(container_name, container_port)
        docker_services.wait_until_responsive(
            timeout=timeout,
            pause=0.5,
            check=lambda: is_responsive(container_name, container_port),
        )
    finally:
        # use check=True to raise an error if command gave bad exit code
        subprocess.run(f"docker logs {container_name}", shell=True, check=True)


@pytest.fixture
def docker_compose_runner(docker_compose_project_name, docker_cleanup):
    @contextlib.contextmanager
    def run(compose_file_path: str, key: str) -> pytest_docker.plugin.Services:
        with pytest_docker.plugin.get_docker_services(
            str(compose_file_path),
            f"{docker_compose_project_name}-{key}",
            docker_cleanup,
        ) as docker_services:
            yield docker_services

    return run
