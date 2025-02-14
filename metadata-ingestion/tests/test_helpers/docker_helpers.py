import logging
import os
import subprocess

import pytest

from datahub.testing.docker_utils import (
    docker_compose_runner as docker_compose_runner,
    is_responsive as is_responsive,
    wait_for_port as wait_for_port,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def docker_compose_command():
    """Docker Compose command to use, it could be either `docker-compose`
    for Docker Compose v1 or `docker compose` for Docker Compose
    v2."""

    return "docker compose"


def cleanup_image(image_name: str) -> None:
    assert ":" not in image_name, "image_name should not contain a tag"

    if not os.environ.get("CI"):
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
