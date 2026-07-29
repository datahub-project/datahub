import logging
import subprocess

import pytest

from datahub.configuration.env_vars import is_ci
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


def is_mysql_up(container_name: str, port: int) -> bool:
    """A cheap way to figure out if mysql is responsive on a container.

    Shared by the MySQL ingest suite (tests/integration/mysql) and the MySQL
    SQLAlchemy-profiler suite (tests/integration/sqlalchemy_profiler/mysql).
    ``capture_output=True`` keeps the docker-logs dump out of the test
    output; ``shell=True`` is retained because the command is a literal
    pipeline with no interpolated user input (the port is an int).
    """
    cmd = (
        f"docker logs {container_name} 2>&1 | "
        "grep '/usr/sbin/mysqld: ready for connections.' | grep " + str(port)
    )
    ret = subprocess.run(cmd, shell=True, capture_output=True)
    return ret.returncode == 0
