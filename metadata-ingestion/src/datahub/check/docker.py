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


def check_local_docker_containers() -> List[str]:
    issues: List[str] = []
    client = docker.from_env()

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
