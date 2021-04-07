from contextlib import contextmanager
from typing import List

import docker

REQUIRED_CONTAINERS = [
    "elasticsearch-setup",
    "elasticsearch",
    "datahub-gms",
    "datahub-mce-consumer",
    "datahub-frontend-react",
    "datahub-mae-consumer",
    "kafka-topics-ui",
    "kafka-rest-proxy",
    "kafka-setup",
    "schema-registry-ui",
    "schema-registry",
    "broker",
    "kibana",
    "mysql",
    "neo4j",
    "zookeeper",
]

ALLOW_STOPPED = [
    "kafka-setup",
    "elasticsearch-setup",
]

MIN_MEMORY_NEEDED = 8  # GB
# docker seems to under-report memory allocated, adding a bit of buffer to account for it
MEMORY_TOLERANCE = 0.2  # GB


@contextmanager
def get_client_with_error():
    try:
        docker_cli = docker.from_env()
    except docker.errors.DockerException as error:
        yield None, error
    else:
        try:
            yield docker_cli, None
        finally:
            docker_cli.close()


def memory_in_gb(mem_bytes: int):
    return mem_bytes / (1024 * 1024 * 1000)


def check_local_docker_containers() -> List[str]:
    issues: List[str] = []
    with get_client_with_error() as (client, error):
        if error:
            issues.append("Docker doesn't seem to be running. Did you start it?")
            return issues

        # check total memory
        total_mem_configured = int(client.info()["MemTotal"])
        if memory_in_gb(total_mem_configured) + MEMORY_TOLERANCE < MIN_MEMORY_NEEDED:
            issues.append(
                f"Total Docker memory configured {memory_in_gb(total_mem_configured):.2f}GB is below the minimum threshold {MIN_MEMORY_NEEDED}GB"
            )

        containers = client.containers.list(
            all=True,
            filters={
                "label": "com.docker.compose.project=datahub",
            },
        )

        # Check number of containers.
        if len(containers) == 0:
            issues.append("quickstart.sh or dev.sh is not running")
        else:
            existing_containers = set(container.name for container in containers)
            missing_containers = set(REQUIRED_CONTAINERS) - existing_containers
            for missing in missing_containers:
                issues.append(f"{missing} container is not present")

        # Check that the containers are running and healthy.
        for container in containers:
            if container.name not in REQUIRED_CONTAINERS:
                # Ignores things like "datahub-frontend" which are no longer used.
                # This way, we only check required containers like "datahub-frontend-react"
                # even if there are some old containers lying around.
                continue
            if container.name in ALLOW_STOPPED:
                continue
            elif container.status != "running":
                issues.append(f"{container.name} is not running")
            elif "Health" in container.attrs["State"]:
                if container.attrs["State"]["Health"]["Status"] == "starting":
                    issues.append(f"{container.name} is still starting")
                elif container.attrs["State"]["Health"]["Status"] != "healthy":
                    issues.append(f"{container.name} is running but not healthy")

    return issues
