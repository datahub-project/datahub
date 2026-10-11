import pytest

from datahub.testing.docker_utils import (
    docker_compose_runner as docker_compose_runner,
    is_responsive as is_responsive,
    wait_for_port as wait_for_port,
)


@pytest.fixture(scope="session")
def docker_compose_command():
    """Docker Compose command to use, it could be either `docker-compose`
    for Docker Compose v1 or `docker compose` for Docker Compose
    v2."""

    return "docker compose"
