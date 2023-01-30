import os
from contextlib import contextmanager
from typing import Iterator, List

import docker
import docker.errors

from datahub.configuration.common import DockerLowMemoryError, DockerNotRunningError

REQUIRED_CONTAINERS = [
    "elasticsearch",
    "datahub-gms",
    "datahub-frontend-react",
    "broker",
]

# We expect these containers to exit 0, while all other containers
# are expected to be running and healthy.
ENSURE_EXIT_SUCCESS = [
    "kafka-setup",
    "elasticsearch-setup",
    "mysql-setup",
]

# If present, we check that the container is running / exited properly.
CONTAINERS_TO_CHECK_IF_PRESENT = [
    "mysql",
    "mysql-setup",
    "cassandra",
    "cassandra-setup",
    "neo4j",
    "elasticsearch-setup",
    "schema-registry",
    "zookeeper",
    # "datahub-mce-consumer",
    # "datahub-mae-consumer",
]

# Docker seems to under-report memory allocated, so we also need a bit of buffer to account for it.
MIN_MEMORY_NEEDED = 3.8  # GB

DATAHUB_COMPOSE_PROJECT_FILTER = {"label": "com.docker.compose.project=datahub"}


@contextmanager
def get_docker_client() -> Iterator[docker.DockerClient]:
    # Get a reference to the Docker client.
    docker_cli = None
    try:
        docker_cli = docker.from_env()
    except docker.errors.DockerException as error:
        try:
            # Docker Desktop 4.13.0 broke the docker.sock symlink.
            # See https://github.com/docker/docker-py/issues/3059.
            maybe_sock_path = os.path.expanduser("~/.docker/run/docker.sock")
            if os.path.exists(maybe_sock_path):
                docker_cli = docker.DockerClient(base_url=f"unix://{maybe_sock_path}")
            else:
                raise error
        except docker.errors.DockerException as error:
            raise DockerNotRunningError(
                "Docker doesn't seem to be running. Did you start it?"
            ) from error
    assert docker_cli

    # Make sure that we can talk to Docker.
    try:
        docker_cli.ping()
    except docker.errors.DockerException as error:
        raise DockerNotRunningError(
            "Unable to talk to Docker. Did you start it?"
        ) from error

    # Yield the client and make sure to close it.
    try:
        yield docker_cli
    finally:
        docker_cli.close()


def memory_in_gb(mem_bytes: int) -> float:
    return mem_bytes / (1024 * 1024 * 1000)


def run_quickstart_preflight_checks(client: docker.DockerClient) -> None:
    # Check total memory.
    # TODO: add option to skip this check.
    total_mem_configured = int(client.info()["MemTotal"])
    if memory_in_gb(total_mem_configured) < MIN_MEMORY_NEEDED:
        raise DockerLowMemoryError(
            f"Total Docker memory configured {memory_in_gb(total_mem_configured):.2f}GB is below the minimum threshold {MIN_MEMORY_NEEDED}GB"
        )


def check_local_docker_containers() -> List[str]:
    issues: List[str] = []
    with get_docker_client() as client:
        containers = client.containers.list(
            all=True,
            filters=DATAHUB_COMPOSE_PROJECT_FILTER,
        )

        # Check number of containers.
        if len(containers) == 0:
            issues.append("quickstart.sh or dev.sh is not running")
        else:
            existing_containers = {container.name for container in containers}
            missing_containers = set(REQUIRED_CONTAINERS) - existing_containers
            issues.extend(
                f"{missing} container is not present" for missing in missing_containers
            )

        # Check that the containers are running and healthy.
        for container in containers:
            if container.name not in (
                REQUIRED_CONTAINERS + CONTAINERS_TO_CHECK_IF_PRESENT
            ):
                # Ignores things like "datahub-frontend" which are no longer used.
                # This way, we only check required containers like "datahub-frontend-react"
                # even if there are some old containers lying around.
                continue

            if container.name in ENSURE_EXIT_SUCCESS:
                if container.status != "exited":
                    issues.append(f"{container.name} is still running")
                elif container.attrs["State"]["ExitCode"] != 0:
                    issues.append(f"{container.name} did not exit cleanly")

            elif container.status != "running":
                issues.append(f"{container.name} is not running")
            elif "Health" in container.attrs["State"]:
                if container.attrs["State"]["Health"]["Status"] == "starting":
                    issues.append(f"{container.name} is still starting")
                elif container.attrs["State"]["Health"]["Status"] != "healthy":
                    issues.append(f"{container.name} is running but not healthy")

    return issues
