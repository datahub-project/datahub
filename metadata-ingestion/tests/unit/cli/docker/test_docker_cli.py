from unittest.mock import MagicMock, patch

import click
import pytest
from docker import DockerClient

from datahub.cli.docker_cli import check


@pytest.fixture
def mock_click():
    with patch("datahub.cli.docker_cli.click") as mock:
        yield mock


@pytest.fixture
def mock_docker_client():
    with patch("datahub.cli.docker_check.get_docker_client") as mock:
        client = MagicMock(spec=DockerClient)
        mock.return_value.__enter__.return_value = client
        yield client


@pytest.fixture
def click_context():
    """Create a Click context for testing commands"""
    ctx = click.Context(check)
    ctx.obj = {}  # Initialize empty context object
    return ctx


def test_check_healthy(mock_click, mock_docker_client, click_context):
    # Setup mock Docker client responses
    # Mock containers
    container1 = MagicMock()
    container1.name = "datahub-mysql-1"
    container1.status = "running"
    container1.attrs = {"State": {"Health": {"Status": "healthy"}}}
    container1.labels = {
        "com.docker.compose.service": "mysql",
        "com.docker.compose.project.config_files": "docker-compose.yml",
    }

    container2 = MagicMock()
    container2.name = "datahub-frontend-1"
    container2.status = "running"
    container2.attrs = {"State": {"Health": {"Status": "healthy"}}}
    container2.labels = {
        "com.docker.compose.service": "frontend",
        "com.docker.compose.project.config_files": "docker-compose.yml",
    }

    mock_docker_client.containers.list.return_value = [container1, container2]
    mock_docker_client.containers.get.return_value = container1
    mock_docker_client.containers.get.side_effect = lambda name: next(
        (c for c in [container1, container2] if c.name == name), None
    )

    # Mock networks
    network = MagicMock()
    network.name = "datahub_network"
    mock_docker_client.networks.list.return_value = [network]

    # Mock volumes
    volume = MagicMock()
    volume.name = "datahub_mysqldata"
    mock_docker_client.volumes.list.return_value = [volume]

    # Mock compose file
    with patch(
        "datahub.cli.docker_check._get_services_from_compose",
        return_value={"mysql", "frontend"},
    ), patch(
        "datahub.cli.docker_check._get_volumes_from_compose",
        return_value={"datahub_mysqldata"},
    ):
        # Execute
        click_context.invoke(check)

        # Verify
        mock_click.secho.assert_called_once_with("✔ No issues detected", fg="green")


def test_check_unhealthy(mock_click, mock_docker_client, click_context):
    # Setup mock Docker client responses
    # Mock containers with unhealthy status
    container1 = MagicMock()
    container1.name = "datahub-mysql-1"
    container1.status = "running"
    container1.attrs = {"State": {"Health": {"Status": "unhealthy"}}}
    container1.labels = {
        "com.docker.compose.service": "mysql",
        "com.docker.compose.project.config_files": "docker-compose.yml",
    }

    mock_docker_client.containers.list.return_value = [container1]
    mock_docker_client.containers.get.return_value = container1

    # Mock networks
    network = MagicMock()
    network.name = "datahub_network"
    mock_docker_client.networks.list.return_value = [network]

    # Mock volumes
    volume = MagicMock()
    volume.name = "datahub_mysqldata"
    mock_docker_client.volumes.list.return_value = [volume]

    # Mock compose file
    with patch(
        "datahub.cli.docker_check._get_services_from_compose", return_value={"mysql"}
    ), patch(
        "datahub.cli.docker_check._get_volumes_from_compose",
        return_value={"datahub_mysqldata"},
    ):
        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            click_context.invoke(check)

        assert "issues were detected" in str(exc_info.value)


def test_check_missing_containers(mock_click, mock_docker_client, click_context):
    # Setup mock Docker client responses
    # Return empty container list to simulate missing containers
    mock_docker_client.containers.list.return_value = []

    # Mock networks
    network = MagicMock()
    network.name = "datahub_network"
    mock_docker_client.networks.list.return_value = [network]

    # Mock volumes
    volume = MagicMock()
    volume.name = "datahub_mysqldata"
    mock_docker_client.volumes.list.return_value = [volume]

    # Mock compose file
    with patch(
        "datahub.cli.docker_check._get_services_from_compose",
        return_value={"mysql", "frontend"},
    ), patch(
        "datahub.cli.docker_check._get_volumes_from_compose",
        return_value={"datahub_mysqldata"},
    ):
        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            click_context.invoke(check)

        assert "issues were detected" in str(exc_info.value)
