import enum
import os
import pathlib
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Set

import docker
import docker.errors
import docker.models.containers
import yaml

from datahub.configuration.common import ExceptionWithProps

# Docker seems to under-report memory allocated, so we also need a bit of buffer to account for it.
MIN_MEMORY_NEEDED = 3.8  # GB
MIN_DISK_SPACE_NEEDED = 12  # GB

DOCKER_COMPOSE_PROJECT_NAME = os.getenv("DATAHUB_COMPOSE_PROJECT_NAME", "datahub")
DATAHUB_COMPOSE_PROJECT_FILTER = {
    "label": f"com.docker.compose.project={DOCKER_COMPOSE_PROJECT_NAME}"
}

DATAHUB_COMPOSE_LEGACY_VOLUME_FILTERS = [
    {"name": "datahub_neo4jdata"},
    {"name": "datahub_mysqldata"},
    {"name": "datahub_zkdata"},
    {"name": "datahub_esdata"},
    {"name": "datahub_cassandradata"},
    {"name": "datahub_broker"},
]


class DockerNotRunningError(Exception):
    SHOW_STACK_TRACE = False


class DockerLowMemoryError(Exception):
    SHOW_STACK_TRACE = False


class DockerLowDiskSpaceError(Exception):
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

    result = client.containers.run(
        "alpine:latest",
        "sh -c \"df -B1 / | tail -1 | awk '{print $2, $4}'\"",  # total, available
        remove=True,
        stdout=True,
        stderr=True,
    )

    output = result.decode("utf-8").strip()
    total_bytes, available_bytes = map(int, output.split())

    available_gb = available_bytes / (1024**3)
    if available_gb < MIN_DISK_SPACE_NEEDED:
        raise DockerLowDiskSpaceError(
            f"Total Docker disk space available {available_gb:.2f}GB is below the minimum threshold {MIN_DISK_SPACE_NEEDED}GB. "
            "You can increase the disk space allocated to Docker in the Docker settings or free up disk space`"
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
    volumes: Set[str]
    # On moving to compose profiles, this CLI will no longer support running quickstart instances from earlier versions.
    # While the check command can work, upgrades or
    running_unsupported_version: bool

    def __init__(
        self,
        containers: List[DockerContainerStatus],
        volumes: List[str],
        running_unsupported_version: bool = False,
    ):
        self.containers = containers
        self.running_unsupported_version = running_unsupported_version
        self.volumes = set(volumes)

    def errors(self) -> List[str]:
        if not self.containers:
            return ["datahub is not running"]

        return [
            f"{container.name} {container.status.value}"
            for container in self.containers
            if container.status != ContainerStatus.OK
        ]

    def is_ok(self) -> bool:
        return not self.errors()

    def needs_up(self) -> bool:
        return any(
            container.status
            in {
                ContainerStatus.EXITED_WITH_FAILURE,
                ContainerStatus.DIED,
                ContainerStatus.MISSING,
            }
            for container in self.containers
        )

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

    def get_containers(self) -> Set[str]:
        if self.containers:
            return {container.name for container in self.containers}
        else:
            return set()


def detect_legacy_quickstart_compose(containers: Set[str]) -> bool:
    return "zookeeper" in containers


def _get_services_from_compose(compose_file: str) -> Set[str]:
    with open(compose_file) as config_file:
        return yaml.safe_load(config_file).get("services", {}).keys()


def _get_volumes_from_compose(compose_file: str) -> Set[str]:
    with open(compose_file) as config_file:
        return yaml.safe_load(config_file).get("volumes", {}).keys()


def check_docker_quickstart() -> QuickstartStatus:
    container_statuses: List[DockerContainerStatus] = []
    with get_docker_client() as client:
        containers = client.containers.list(
            all=True,
            filters=DATAHUB_COMPOSE_PROJECT_FILTER,
            # We can get race conditions between docker running up / recreating
            # containers and our status checks.
            ignore_removed=True,
        )
        if len(containers) == 0:
            return QuickstartStatus([], [], running_unsupported_version=False)

        # load the expected containers from the docker-compose file
        config_files = (
            containers[0]
            .labels.get("com.docker.compose.project.config_files")
            .split(",")
        )

        # If using profiles, alternative check  ##TODO: Does this really work? Check mixpanel for usage of this.
        if config_files and "/profiles/" in config_files[0]:
            return check_docker_quickstart_profiles(client)

        all_containers = set()
        for config_file in config_files:
            all_containers.update(_get_services_from_compose(config_file))

        all_volumes = set()
        for config_file in config_files:
            all_volumes.update(_get_volumes_from_compose(config_file))

        existing_containers = set()
        # Check that the containers are running and healthy.
        container: docker.models.containers.Container
        for container in containers:
            name = container.labels.get("com.docker.compose.service", container.name)
            existing_containers.add(name)
            status = ContainerStatus.OK
            if name not in all_containers:
                # Ignores containers that are not part of the datahub docker-compose
                continue
            if container.labels.get("datahub_setup_job", False):
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
        missing_containers = set(all_containers) - existing_containers
        for missing in missing_containers:
            container_statuses.append(
                DockerContainerStatus(missing, ContainerStatus.MISSING)
            )
        running_unsupported_version = detect_legacy_quickstart_compose(all_containers)
    return QuickstartStatus(
        containers=container_statuses,
        volumes=list(all_volumes),
        running_unsupported_version=running_unsupported_version,
    )


def check_docker_quickstart_profiles(client: docker.DockerClient) -> QuickstartStatus:
    container_statuses: List[DockerContainerStatus] = []
    containers = client.containers.list(
        all=True,
        filters={"label": "io.datahubproject.datahub.component=gms"},
        # We can get race conditions between docker running up / recreating
        # containers and our status checks.
        ignore_removed=True,
    )
    if len(containers) == 0:
        return QuickstartStatus([], [], running_unsupported_version=False)

    existing_containers = set()
    # Check that the containers are running and healthy.
    container: docker.models.containers.Container
    for container in containers:
        name = container.labels.get("com.docker.compose.service", container.name)
        existing_containers.add(name)
        status = ContainerStatus.OK
        if container.status != "running":
            status = ContainerStatus.DIED
        elif "Health" in container.attrs["State"]:
            if container.attrs["State"]["Health"]["Status"] == "starting":
                status = ContainerStatus.STARTING
            elif container.attrs["State"]["Health"]["Status"] != "healthy":
                status = ContainerStatus.UNHEALTHY

        container_statuses.append(DockerContainerStatus(name, status))

    # TODO: Can this be handled with older verions?
    return QuickstartStatus(
        container_statuses, volumes=[], running_unsupported_version=False
    )


def check_upgrade_supported(
    quickstart_compose_file: List[pathlib.Path], quickstart_status: QuickstartStatus
) -> bool:
    if (
        quickstart_status.running_unsupported_version
    ):  # we detected a legacy quickstart service
        return False

    if not quickstart_status.get_containers():  # no containers are running
        return True

    compose_services = set()
    compose_volumes = set()

    for compose_file in quickstart_compose_file:
        compose_services.update(_get_services_from_compose(str(compose_file)))
        compose_volumes.update(_get_volumes_from_compose(str(compose_file)))

    # if all services and volumes are not the same, the state in the volumes may not be compatible with the new services.
    # We are checking for containers and volumes per the compose file, not necessarily all of them being present
    if (
        compose_services == quickstart_status.get_containers()
        and compose_volumes == quickstart_status.volumes
    ):
        return True
    else:
        return False
