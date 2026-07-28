import contextlib
import logging
import subprocess
from typing import Callable, Iterator, List, Optional, Union

import pytest
import pytest_docker.plugin

logger = logging.getLogger(__name__)


def is_responsive(container_name: str, port: int, hostname: Optional[str]) -> bool:
    """A cheap way to figure out if a port is responsive on a container"""
    if hostname:
        cmd = f"docker exec {container_name} /bin/bash -c 'echo -n > /dev/tcp/{hostname}/{port}'"
    else:
        # use the hostname of the container
        cmd = f"docker exec {container_name} /bin/bash -c 'c_host=`hostname`;echo -n > /dev/tcp/$c_host/{port}'"
    ret = subprocess.run(
        cmd,
        shell=True,
    )
    return ret.returncode == 0


def wait_for_port(
    docker_services: pytest_docker.plugin.Services,
    container_name: str,
    container_port: int,
    hostname: Optional[str] = None,
    timeout: float = 30.0,
    pause: float = 0.5,
    checker: Optional[Callable[[], bool]] = None,
) -> None:
    try:
        docker_services.wait_until_responsive(
            timeout=timeout,
            pause=pause,
            check=(
                checker
                if checker
                else lambda: is_responsive(container_name, container_port, hostname)
            ),
        )
        logger.info(f"Container {container_name} is ready!")
    finally:
        # use check=True to raise an error if command gave bad exit code
        subprocess.run(f"docker logs {container_name}", shell=True, check=True)


DOCKER_DEFAULT_UNLIMITED_PARALLELISM = -1


@pytest.fixture(scope="module")
def docker_compose_runner(
    docker_compose_command, docker_compose_project_name, docker_setup, docker_cleanup
):
    def _as_commands(commands: Union[List[str], str]) -> List[str]:
        return [commands] if isinstance(commands, str) else list(commands or [])

    @contextlib.contextmanager
    def run(
        compose_file_path: Union[str, List[str]],
        key: str,
        cleanup: bool = True,
        parallel: int = DOCKER_DEFAULT_UNLIMITED_PARALLELISM,
        setup_command: Optional[Union[List[str], str]] = None,
    ) -> Iterator[pytest_docker.plugin.Services]:
        # We deliberately do NOT delegate to pytest_docker.get_docker_services: it
        # runs docker_setup *before* the try/finally that owns cleanup, so a setup
        # failure — e.g. an `up --wait` healthcheck timeout on a loaded runner —
        # raises before cleanup is registered and leaks the container (with its
        # host port still bound). Running setup inside our own try keeps `down -v`
        # reachable on that path, which fixes every suite in one place.
        compose = pytest_docker.plugin.DockerComposeExecutor(
            f"{docker_compose_command} --parallel {parallel}",
            compose_file_path,
            f"{docker_compose_project_name}-{key}",
        )
        setup = setup_command if setup_command is not None else docker_setup
        cleanup_commands = _as_commands(docker_cleanup) if cleanup else []
        try:
            for command in _as_commands(setup):
                compose.execute(command)
            yield pytest_docker.plugin.Services(compose)
        finally:
            for command in cleanup_commands:
                compose.execute(command)

    return run
