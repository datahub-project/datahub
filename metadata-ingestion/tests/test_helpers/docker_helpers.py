import os
import time


def is_responsive(container: str, port: int):
    ret = os.system(f"docker exec {container} nc -z localhost {port}")
    return ret == 0


def wait_for_db(docker_services, container_name, container_port):
    port = docker_services.port_for(container_name, container_port)
    docker_services.wait_until_responsive(
        timeout=30.0,
        pause=0.5,
        check=lambda: is_responsive(container_name, container_port),
    )

    # TODO: this is an ugly hack.
    time.sleep(5)
    return port
