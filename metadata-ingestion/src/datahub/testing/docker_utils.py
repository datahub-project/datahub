import contextlib
import logging
import os
import subprocess
from typing import Callable, Iterator, List, Optional, Union

import pytest
import pytest_docker.plugin
import yaml

logger = logging.getLogger(__name__)


def _fixed_container_names(compose_file_path: Union[str, List[str]]) -> List[str]:
    """Container names hardcoded via `container_name:` in the given compose file(s).

    Docker container names are unique host-wide, independent of the compose
    project that created them. A container left running by a killed/timed-out
    job on a reused (self-hosted) CI runner keeps its fixed name and fixed host
    port forever, so a `docker compose down` scoped to a *new* project name
    can't remove it -- it belongs to a different project label. Force-removing
    by name, regardless of project, is what actually reclaims it.
    """
    names = []
    paths = (
        [compose_file_path]
        if isinstance(compose_file_path, (str, os.PathLike))
        else compose_file_path
    )
    for path in paths:
        with open(path) as f:
            compose = yaml.safe_load(f) or {}
        for service in (compose.get("services") or {}).values():
            if service.get("container_name"):
                names.append(service["container_name"])
    return names


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
        # A container leaked by a killed/timed-out job on a reused CI runner holds its
        # fixed container_name (and thus its fixed host port) forever, and belongs to a
        # different compose project than this run, so `docker compose down` can't reach
        # it. Force-remove by name first so a stale container never fails a fresh `up`.
        # This assumes only one run of a given fixture is ever live on a runner at once
        # (true for CI today); two deliberately-concurrent runs sharing a fixed name
        # would race here. Removing that assumption needs per-run container names
        # (dropping `container_name:` from the compose files), a larger follow-up.
        stale_names = _fixed_container_names(compose_file_path)
        if stale_names:
            result = subprocess.run(
                ["docker", "rm", "-f", *stale_names],
                capture_output=True,
                text=True,
            )
            # "No such container" is the expected, benign outcome on every clean
            # run (nothing was leaked). Anything else means the removal itself
            # failed, so a genuinely stale container could still be sitting on
            # our port when `up` runs next -- surface that instead of masking
            # it as a confusing name/port-in-use error from `up`. Checked per
            # line: with multiple stale_names, one absent (benign) container's
            # message must not hide another's real removal failure.
            if result.returncode != 0:
                real_errors = [
                    line
                    for line in result.stderr.splitlines()
                    if "No such container" not in line
                ]
                if real_errors:
                    logger.warning(
                        f"Failed to remove stale container(s) {stale_names}: "
                        f"{' '.join(real_errors)}"
                    )

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
