import contextlib
import logging
import os
import subprocess
from typing import Callable, Iterator, List, Optional, Set, Union

import pytest
import pytest_docker.plugin
import yaml

from datahub.configuration.env_vars import is_ci

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


# Ceiling on the teardown image removal. Deleting several multi-GB stacks is
# genuinely slow on a loaded runner, so this is deliberately generous -- it
# exists only so a wedged daemon cannot hang the session, not to bound normal
# work.
_PRUNE_TIMEOUT = 300.0


def _project_image_ids(
    compose: pytest_docker.plugin.DockerComposeExecutor,
) -> Set[str]:
    """Image IDs backing a compose project's containers.

    Must be called while the containers still exist: `docker compose images`
    enumerates the project's *containers*, so once `down` has removed them it
    reports nothing and there is no longer any record of what to prune.
    """
    try:
        output = compose.execute("images -q", ignore_stderr=True)
    except Exception as e:
        # Never fail a module that passed just because the prune bookkeeping
        # failed -- the images are leaked instead, which is the status quo.
        logger.warning(f"Failed to list compose images for pruning: {e}")
        return set()
    return {
        line.strip() for line in output.decode("utf-8").splitlines() if line.strip()
    }


def _prune_images(image_ids: Set[str]) -> None:
    """Delete the images a module's compose suites pulled.

    `docker compose down -v` removes containers, networks and volumes but never
    images, so on CI every connector suite leaves its whole stack on disk for
    the rest of the job -- several GB each for Spark/Iceberg, Hadoop/Hive or
    Informix. That is invisible until the test-weight bot packs a few such
    suites onto one runner, at which point the job dies with ENOSPC mid-run.
    Reclaiming per module keeps peak disk proportional to the largest single
    suite rather than to the sum of everything in the batch.
    """
    if not image_ids:
        return

    if not is_ci():
        # Locally these images are a cache worth keeping: re-pulling a stack on
        # every test run costs far more than the disk it occupies.
        logger.debug("Not pruning docker images to speed up local development")
        return

    logger.info(f"Pruning {len(image_ids)} docker image(s) used by this module")
    # `-f` for two reasons: an image carrying several tags is removed rather
    # than merely untagged, and an image a previous module already pruned (the
    # same base image is shared by several suites) is reported as absent rather
    # than aborting the removal of the others. Whether docker exits nonzero for
    # a merely-absent id has differed between versions, so the handling below
    # keys off stderr rather than the exit status. Docker's per-layer "Deleted:"
    # chatter is captured, not logged -- writing it to the job log would feed
    # the very problem this fixes.
    try:
        result = subprocess.run(
            ["docker", "image", "rm", "-f", *sorted(image_ids)],
            capture_output=True,
            text=True,
            timeout=_PRUNE_TIMEOUT,
        )
    except OSError as e:
        # No docker on PATH, fork failure, etc. Pruning is an optimization, so
        # a module whose tests passed must not fail in teardown over it.
        logger.warning(f"Could not run docker image rm: {e}")
        return
    except subprocess.TimeoutExpired:
        # Without a timeout a wedged daemon blocks teardown forever and stalls
        # every module after this one. Leaking the images is the lesser cost.
        logger.warning(
            f"Timed out after {_PRUNE_TIMEOUT}s pruning docker images; "
            "leaving them on disk"
        )
        return

    if result.returncode != 0:
        # Best-effort by design. An image still held by a container outside
        # this project can't be removed, and that must not fail a green module.
        # An id that was merely absent (already pruned by an earlier module) is
        # benign noise -- drop those lines so a real conflict is not buried in
        # them, as the stale-container removal above does for "No such
        # container".
        real_errors = [
            line
            for line in result.stderr.splitlines()
            if line.strip() and "No such image" not in line
        ]
        if real_errors:
            logger.warning(f"Failed to prune docker image(s): {' '.join(real_errors)}")
        elif not result.stderr.strip():
            # Nonzero with nothing to show for it. Suppressing this would make
            # a wholly failed cleanup indistinguishable from a clean one, so
            # report the exit status itself.
            logger.warning(
                f"docker image rm exited {result.returncode} with no output; "
                "images may still be on disk"
            )


DOCKER_DEFAULT_UNLIMITED_PARALLELISM = -1


@pytest.fixture(scope="module")
def docker_compose_runner(
    docker_compose_command, docker_compose_project_name, docker_setup, docker_cleanup
):
    # Images used by every compose suite this module ran, pruned together when
    # the module finishes rather than at each teardown: several suites bring
    # the same stack up once per test (iceberg, ldap, postgres, mssql), and
    # pruning between those runs would re-pull the image each time.
    module_image_ids: Set[str] = set()

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
            if cleanup_commands:
                # Enumerate before `down` removes the containers compose reads
                # the image list from. Skipped when the caller opted out of
                # cleanup: those containers stay up, so their images must too.
                module_image_ids.update(_project_image_ids(compose))
            for command in cleanup_commands:
                compose.execute(command)

    yield run

    _prune_images(module_image_ids)
