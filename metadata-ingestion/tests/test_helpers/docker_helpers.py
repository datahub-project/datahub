import logging
import pathlib
import subprocess
import time
from typing import List, Union

import pytest

from datahub.configuration.env_vars import is_ci
from datahub.testing.docker_utils import (
    docker_compose_runner as docker_compose_runner,
    is_responsive as is_responsive,
    wait_for_port as wait_for_port,
)

logger = logging.getLogger(__name__)

# GitHub runners intermittently hit Docker Hub rate-limits / TLS handshake
# timeouts while pulling service images during `docker compose up`. pytest-docker
# runs `up --build --wait` with no retry, so a single transient pull failure
# fails the whole test module. Pre-pulling with a few retries makes those
# transients recoverable; the subsequent `up` then starts from cached images.
_DOCKER_PULL_MAX_ATTEMPTS = 3
_DOCKER_PULL_RETRY_BACKOFF_SEC = 5


def docker_compose_pull_with_retry(
    compose_file_path: Union[str, pathlib.Path, List[Union[str, pathlib.Path]]],
    max_attempts: int = _DOCKER_PULL_MAX_ATTEMPTS,
    backoff_sec: int = _DOCKER_PULL_RETRY_BACKOFF_SEC,
) -> None:
    """Pre-pull a compose file's images with retry, best-effort.

    Call this just before ``with docker_compose_runner(...)`` so a transient
    Docker Hub rate-limit / TLS handshake timeout during the pull doesn't abort
    the test. A final failure is ignored -- the subsequent ``up`` still gets a
    chance and may succeed from cache.
    """
    if isinstance(compose_file_path, (str, pathlib.Path)):
        files = [compose_file_path]
    else:
        files = list(compose_file_path)
    file_args = " ".join(f"-f {path}" for path in files)
    pull_cmd = f"docker compose {file_args} pull"

    for attempt in range(1, max_attempts + 1):
        result = subprocess.run(
            pull_cmd,
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            if attempt > 1:
                logger.info("docker compose pull succeeded on attempt %d", attempt)
            return
        logger.warning(
            "docker compose pull attempt %d/%d failed (exit %d): %s",
            attempt,
            max_attempts,
            result.returncode,
            (result.stderr or result.stdout or "").strip()[:500],
        )
        if attempt < max_attempts:
            time.sleep(backoff_sec)


@pytest.fixture(scope="session")
def docker_compose_command():
    """Docker Compose command to use, it could be either `docker-compose`
    for Docker Compose v1 or `docker compose` for Docker Compose
    v2."""

    return "docker compose"


def cleanup_image(image_name: str) -> None:
    assert ":" not in image_name, "image_name should not contain a tag"

    if not is_ci():
        logger.debug("Not cleaning up images to speed up local development")
        return

    images_proc = subprocess.run(
        f"docker image ls --filter 'reference={image_name}*' -q",
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )

    if not images_proc.stdout:
        logger.debug(f"No images to cleanup for {image_name}")
        return

    image_ids = images_proc.stdout.splitlines()
    subprocess.run(
        f"docker image rm {' '.join(image_ids)}",
        shell=True,
        check=True,
    )
