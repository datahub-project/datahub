import enum
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import docker
import docker.errors
import docker.models.containers

from datahub.configuration.common import ExceptionWithProps

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
    "datahub-upgrade",
]

# If present, we check that the container is ok. If it exists
# in ENSURE_EXIT_SUCCESS, we check that it exited 0. Otherwise,
# we check that it is running and healthy.
CONTAINERS_TO_CHECK_IF_PRESENT = [
    "mysql",
    "mysql-setup",
    "cassandra",
    "cassandra-setup",
    "neo4j",
    "elasticsearch-setup",
    "schema-registry",
    "zookeeper",
    "datahub-upgrade",
    "kafka-setup",
    # "datahub-mce-consumer",
    # "datahub-mae-consumer",
]

# Docker seems to under-report memory allocated, so we also need a bit of buffer to account for it.
MIN_MEMORY_NEEDED = 3.8  # GB

DATAHUB_COMPOSE_PROJECT_FILTER = {"label": "com.docker.compose.project=datahub"}


class DockerNotRunningError(Exception):
    SHOW_STACK_TRACE = False


class DockerLowMemoryError(Exception):
    SHOW_STACK_TRACE = False


class DockerComposeVersionError(Exception):
    SHOW_STACK_TRACE = False


class QuickstartError(Exception, ExceptionWithProps):
    SHOW_STACK_TRACE = False

    def __init__(self, message: str, container_statuses: Dict[str, Any]):
        super().__init__(message)
        self.container_statuses = container_statuses

    def get_telemetry_props(self) -> Dict[str, Any]:
        return self.container_statuses


@contextmanager
def get_docker_client() -> Iterator[docker.DockerClient]:
    # Get a reference to the Docker client.
    client = None
    try:
        client = docker.from_env()
    except docker.errors.DockerException as error:
        try:
            # Docker Desktop 4.13.0 broke the docker.sock symlink.
            # See https://github.com/docker/docker-py/issues/3059.
            maybe_sock_path = os.path.expanduser("~/.docker/run/docker.sock")
            if os.path.exists(maybe_sock_path):
                client = docker.DockerClient(base_url=f"unix://{maybe_sock_path}")
            else:
                raise error
        except docker.errors.DockerException as error:
            raise DockerNotRunningError(
                "Docker doesn't seem to be running. Did you start it?"
            ) from error
    assert client

    # Make sure that we can talk to Docker.
    try:
        client.ping()
    except docker.errors.DockerException as error:
        raise DockerNotRunningError(
            "Unable to talk to Docker. Did you start it?"
        ) from error

    # Yield the client and make sure to close it.
    try:
        yield client
    finally:
        client.close()


def memory_in_gb(mem_bytes: int) -> float:
    return mem_bytes / (1024 * 1024 * 1000)


def run_quickstart_preflight_checks(client: docker.DockerClient) -> None:
    # Check total memory.
    # TODO: add option to skip this check.
    total_mem_configured = int(client.info()["MemTotal"])
    if memory_in_gb(total_mem_configured) < MIN_MEMORY_NEEDED:
        raise DockerLowMemoryError(
            f"Total Docker memory configured {memory_in_gb(total_mem_configured):.2f}GB is below the minimum threshold {MIN_MEMORY_NEEDED}GB. "
            "You can increase the memory allocated to Docker in the Docker settings."
        )


class ContainerStatus(enum.Enum):
    OK = "is ok"

    # For containers that are expected to exit 0.
    STILL_RUNNING = "is still running"
    EXITED_WITH_FAILURE = "exited with an error"

    # For containers that are expected to be running.
    DIED = "is not running"
    MISSING = "is not present"
    STARTING = "is still starting"
    UNHEALTHY = "is running by not yet healthy"


@dataclass
class DockerContainerStatus:
    name: str
    status: ContainerStatus


@dataclass
class QuickstartStatus:
    containers: List[DockerContainerStatus]

    def errors(self) -> List[str]:
        if not self.containers:
            return ["quickstart.sh or dev.sh is not running"]

        return [
            f"{container.name} {container.status.value}"
            for container in self.containers
            if container.status != ContainerStatus.OK
        ]

    def is_ok(self) -> bool:
        return not self.errors()

    def to_exception(
        self, header: str, footer: Optional[str] = None
    ) -> QuickstartError:
        message = f"{header}\n"
        for error in self.errors():
            message += f"- {error}\n"
        if footer:
            message += f"\n{footer}"

        return QuickstartError(
            message,
            {
                "containers_all": [container.name for container in self.containers],
                "containers_errors": [
                    container.name
                    for container in self.containers
                    if container.status != ContainerStatus.OK
                ],
                **{
                    f"container_{container.name}": container.status.name
                    for container in self.containers
                },
            },
        )


def check_docker_quickstart() -> QuickstartStatus:
    container_statuses: List[DockerContainerStatus] = []

    with get_docker_client() as client:
        containers = client.containers.list(
            all=True,
            filters=DATAHUB_COMPOSE_PROJECT_FILTER,
        )

        # Check that the containers are running and healthy.
        container: docker.models.containers.Container
        for container in containers:
            name = container.name
            status = ContainerStatus.OK

            if container.name not in (
                REQUIRED_CONTAINERS + CONTAINERS_TO_CHECK_IF_PRESENT
            ):
                # Ignores things like "datahub-frontend" which are no longer used.
                # This way, we only check required containers like "datahub-frontend-react"
                # even if there are some old containers lying around.
                continue

            if container.name in ENSURE_EXIT_SUCCESS:
                if container.status != "exited":
                    status = ContainerStatus.STILL_RUNNING
                elif container.attrs["State"]["ExitCode"] != 0:
                    status = ContainerStatus.EXITED_WITH_FAILURE

            elif container.status != "running":
                status = ContainerStatus.DIED
            elif "Health" in container.attrs["State"]:
                if container.attrs["State"]["Health"]["Status"] == "starting":
                    status = ContainerStatus.STARTING
                elif container.attrs["State"]["Health"]["Status"] != "healthy":
                    status = ContainerStatus.UNHEALTHY

            container_statuses.append(DockerContainerStatus(name, status))

        # Check for missing containers.
        existing_containers = {container.name for container in containers}
        missing_containers = set(REQUIRED_CONTAINERS) - existing_containers
        for missing in missing_containers:
            container_statuses.append(
                DockerContainerStatus(missing, ContainerStatus.MISSING)
            )

    return QuickstartStatus(container_statuses)
