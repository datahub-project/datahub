import pathlib
from unittest.mock import MagicMock

import pytest

from datahub.cli.docker_check import (
    MIN_DISK_SPACE_NEEDED,
    MIN_MEMORY_NEEDED,
    ContainerStatus,
    DockerContainerStatus,
    DockerLowDiskSpaceError,
    DockerLowMemoryError,
    QuickstartStatus,
    check_upgrade_supported,
    run_quickstart_preflight_checks,
)


@pytest.fixture
def mock_docker_client():
    client = MagicMock()
    return client


def test_run_quickstart_preflight_checks_success(mock_docker_client):
    # Mock Docker info to return sufficient memory
    mock_docker_client.info.return_value = {
        "MemTotal": int(MIN_MEMORY_NEEDED * 1024 * 1024 * 1000)  # Convert GB to bytes
    }

    # Mock container run to return sufficient disk space
    mock_container = MagicMock()
    mock_container.decode.return_value = f"{MIN_DISK_SPACE_NEEDED * 1024**3} {MIN_DISK_SPACE_NEEDED * 1024**3}"  # total, available in bytes
    mock_docker_client.containers.run.return_value = mock_container

    # Test should pass without raising any exceptions
    run_quickstart_preflight_checks(mock_docker_client)


def test_run_quickstart_preflight_checks_insufficient_memory(mock_docker_client):
    # Mock Docker info to return insufficient memory
    mock_docker_client.info.return_value = {
        "MemTotal": int(
            (MIN_MEMORY_NEEDED - 1) * 1024 * 1024 * 1000
        )  # 1GB less than required
    }

    # Test should raise DockerLowMemoryError
    with pytest.raises(DockerLowMemoryError) as exc_info:
        run_quickstart_preflight_checks(mock_docker_client)

    assert "Total Docker memory configured" in str(exc_info.value)


def test_run_quickstart_preflight_checks_insufficient_disk_space(mock_docker_client):
    # Mock Docker info to return sufficient memory
    mock_docker_client.info.return_value = {
        "MemTotal": int(MIN_MEMORY_NEEDED * 1024 * 1024 * 1000)
    }

    # Mock container run to return insufficient disk space
    mock_container = MagicMock()
    mock_container.decode.return_value = f"{MIN_DISK_SPACE_NEEDED * 1024**3} {(MIN_DISK_SPACE_NEEDED - 1) * 1024**3}"  # total, available in bytes
    mock_docker_client.containers.run.return_value = mock_container

    # Test should raise DockerLowDiskSpaceError
    with pytest.raises(DockerLowDiskSpaceError) as exc_info:
        run_quickstart_preflight_checks(mock_docker_client)

    assert "Total Docker disk space available" in str(exc_info.value)


@pytest.fixture
def mock_compose_file(tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_file.write_text("""
services:
  datahub-gms:
    image: datahub-gms
  datahub-frontend:
    image: datahub-frontend
volumes:
  datahub_volume1:
  datahub_volume2:
    """)
    return compose_file


def test_check_upgrade_supported_no_containers(mock_compose_file):
    # Test when no containers are running
    quickstart_status = QuickstartStatus([], [], running_unsupported_version=False)
    assert check_upgrade_supported([mock_compose_file], quickstart_status) is True


def test_check_upgrade_supported_legacy_version():
    # Test when running legacy version
    quickstart_status = QuickstartStatus(
        [DockerContainerStatus("zookeeper", ContainerStatus.OK)],
        [],
        running_unsupported_version=True,
    )
    assert (
        check_upgrade_supported([pathlib.Path("dummy.yml")], quickstart_status) is False
    )


def test_check_upgrade_supported_matching_services(mock_compose_file):
    # Test when services and volumes match
    quickstart_status = QuickstartStatus(
        [
            DockerContainerStatus("datahub-gms", ContainerStatus.OK),
            DockerContainerStatus("datahub-frontend", ContainerStatus.OK),
        ],
        ["datahub_volume1", "datahub_volume2"],
        running_unsupported_version=False,
    )
    assert check_upgrade_supported([mock_compose_file], quickstart_status) is True


def test_check_upgrade_supported_mismatched_services(mock_compose_file):
    # Test when services don't match
    quickstart_status = QuickstartStatus(
        [
            DockerContainerStatus("datahub-gms", ContainerStatus.OK),
            DockerContainerStatus("datahub-frontend", ContainerStatus.OK),
            DockerContainerStatus(
                "zookeeper", ContainerStatus.OK
            ),  # Extra service not in compose
        ],
        ["datahub_volume1", "datahub_volume2"],
        running_unsupported_version=False,
    )
    assert check_upgrade_supported([mock_compose_file], quickstart_status) is False


def test_check_upgrade_supported_mismatched_volumes(mock_compose_file):
    # Test when volumes don't match
    quickstart_status = QuickstartStatus(
        [
            DockerContainerStatus("datahub-gms", ContainerStatus.OK),
            DockerContainerStatus("datahub-frontend", ContainerStatus.OK),
        ],
        [
            "datahub_volume1",
            "datahub_volume2",
            "zookeeper_volume",
        ],  # Extra volume not in compose
        running_unsupported_version=False,
    )
    assert check_upgrade_supported([mock_compose_file], quickstart_status) is False
