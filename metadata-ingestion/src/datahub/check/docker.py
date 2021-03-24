from typing import List

import docker


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
    elif len(containers) < 15:
        issues.append("not enough containers are running")

    # Check that the containers are running and healthy.
    ALLOW_STOPPED = [
        "kafka-setup",
        "elasticsearch-setup",
    ]
    for container in containers:
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
