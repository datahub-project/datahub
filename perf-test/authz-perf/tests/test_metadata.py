from pathlib import Path
from unittest.mock import patch

from lib.metadata import capture_deployment_metadata, capture_git_metadata


def test_capture_git_metadata() -> None:
    meta = capture_git_metadata(Path(__file__).resolve().parent.parent.parent.parent)
    assert meta.commit != ""


def test_capture_deployment_metadata() -> None:
    dep = capture_deployment_metadata(
        "http://localhost:8080",
        "http://localhost:9002",
        gms_host="localhost:8080",
        gms_version="v0.14.0",
    )
    assert dep.gms_url == "http://localhost:8080"
    assert dep.gms_host == "localhost:8080"
    assert dep.gms_version == "v0.14.0"


@patch.dict("os.environ", {"DATAHUB_VERSION": "v0.15.0"})
def test_deployment_docker_tag() -> None:
    dep = capture_deployment_metadata(
        "http://localhost:8080",
        "http://localhost:9002",
        gms_host="localhost:8080",
        docker_tag="v0.15.0",
    )
    assert dep.mode == "docker_tag"
    assert dep.docker_tag == "v0.15.0"
