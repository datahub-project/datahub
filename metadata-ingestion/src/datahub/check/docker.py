from contextlib import contextmanager
from typing import List

import docker

ENSURE_EXIT_SUCCESS = [
    "kafka-setup",
    "elasticsearch-setup",
]

REQUIRED_CONTAINERS = [
    "elasticsearch-setup",
    "elasticsearch",
    "datahub-gms",
    "datahub-mce-consumer",
    "datahub-frontend-react",
    "datahub-mae-consumer",
    "kafka-setup",
    "schema-registry",
    "broker",
    "mysql",
    "neo4j",
    "zookeeper",
    # These two containers are not necessary - only helpful in debugging.
    # "kafka-topics-ui",
    # "schema-registry-ui",
    # "kibana",
    # "kafka-rest-proxy",
]

# Docker seems to under-report memory allocated, so we also need a bit of buffer to account for it.
MIN_MEMORY_NEEDED = 6.75  # GB


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


def memory_in_gb(mem_bytes: int) -> float:
    return mem_bytes / (1024 * 1024 * 1000)


def check_local_docker_containers() -> List[str]:
    issues: List[str] = []
    with get_client_with_error() as (client, error):
        if error:
            issues.append("Docker doesn't seem to be running. Did you start it?")
            return issues

        # Check total memory.
        total_mem_configured = int(client.info()["MemTotal"])
        if memory_in_gb(total_mem_configured) < MIN_MEMORY_NEEDED:
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
