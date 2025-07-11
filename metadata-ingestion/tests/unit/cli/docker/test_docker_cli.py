from unittest.mock import MagicMock, patch

import click
import pytest
from docker import DockerClient

from datahub.cli.docker_cli import (
    _check_upgrade_and_show_instructions,
    check,
    download_compose_files,
    get_github_file_url,
)


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
    with (
        patch(
            "datahub.cli.docker_check._get_services_from_compose",
            return_value={"mysql", "frontend"},
        ),
        patch(
            "datahub.cli.docker_check._get_volumes_from_compose",
            return_value={"datahub_mysqldata"},
        ),
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
    with (
        patch(
            "datahub.cli.docker_check._get_services_from_compose",
            return_value={"mysql"},
        ),
        patch(
            "datahub.cli.docker_check._get_volumes_from_compose",
            return_value={"datahub_mysqldata"},
        ),
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
    with (
        patch(
            "datahub.cli.docker_check._get_services_from_compose",
            return_value={"mysql", "frontend"},
        ),
        patch(
            "datahub.cli.docker_check._get_volumes_from_compose",
            return_value={"datahub_mysqldata"},
        ),
    ):
        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            click_context.invoke(check)

        assert "issues were detected" in str(exc_info.value)


def test_get_github_file_url_default():
    """Test get_github_file_url with default environment (no DOCKER_COMPOSE_BASE set)"""
    with patch.dict("os.environ", {}, clear=True):
        result = get_github_file_url("v1.0.0")
        expected = "https://raw.githubusercontent.com/datahub-project/datahub/v1.0.0/docker/quickstart/docker-compose.quickstart-profile.yml"
        assert result == expected


def test_get_github_file_url_custom_base():
    """Test get_github_file_url with custom DOCKER_COMPOSE_BASE environment variable"""
    custom_base = "https://github.com/my-fork/datahub/my-branch"
    with patch.dict("os.environ", {"DOCKER_COMPOSE_BASE": custom_base}):
        result = get_github_file_url("v2.0.0")
        expected = (
            f"{custom_base}/docker/quickstart/docker-compose.quickstart-profile.yml"
        )
        assert result == expected


def test_download_compose_files_404_error():
    """Test download_compose_files when quickstart_download_response.status_code is 404"""
    # Mock the requests.Session and its get method
    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_session = MagicMock()
    mock_session.get.return_value = mock_response

    with (
        patch("datahub.cli.docker_cli.requests.Session", return_value=mock_session),
        patch("datahub.cli.docker_cli.tempfile.NamedTemporaryFile") as mock_tempfile,
    ):
        # Setup tempfile mock
        mock_tempfile_instance = MagicMock()
        mock_tempfile_instance.__enter__.return_value = mock_tempfile_instance
        mock_tempfile_instance.__exit__.return_value = None
        mock_tempfile_instance.name = "/tmp/test.yml"
        mock_tempfile.return_value = mock_tempfile_instance

        # Test that ClickException is raised with the correct message
        with pytest.raises(click.ClickException) as exc_info:
            download_compose_files(None, [], "v1.0.0")

        expected_message = "Could not find quickstart compose file for version v1.0.0. Please try a different version or check the version exists at https://github.com/datahub-project/datahub/releases"
        assert str(exc_info.value) == expected_message


def test_check_upgrade_and_show_instructions_upgrade_not_supported():
    """Test _check_upgrade_and_show_instructions when check_upgrade_supported returns False"""
    # Mock the dependencies
    mock_status = MagicMock()
    mock_status.is_ok.return_value = (
        True  # Status is OK, so migration instructions will be shown
    )

    with (
        patch(
            "datahub.cli.docker_cli.check_docker_quickstart", return_value=mock_status
        ),
        patch("datahub.cli.docker_cli.check_upgrade_supported", return_value=False),
        patch(
            "datahub.cli.docker_cli.show_migration_instructions"
        ) as mock_show_migration,
    ):
        # Call the function
        result = _check_upgrade_and_show_instructions([])

        # Verify that show_migration_instructions was called (since status.is_ok() returns True)
        mock_show_migration.assert_called_once()

        # Verify that the function returns False
        assert result is False


def test_check_upgrade_and_show_instructions_upgrade_not_supported_repair():
    """Test _check_upgrade_and_show_instructions when check_upgrade_supported returns False and status is not OK"""
    # Mock the dependencies
    mock_status = MagicMock()
    mock_status.is_ok.return_value = (
        False  # Status is not OK, so repair instructions will be shown
    )

    with (
        patch(
            "datahub.cli.docker_cli.check_docker_quickstart", return_value=mock_status
        ),
        patch("datahub.cli.docker_cli.check_upgrade_supported", return_value=False),
        patch("datahub.cli.docker_cli.show_repair_instructions") as mock_show_repair,
    ):
        # Call the function
        result = _check_upgrade_and_show_instructions([])

        # Verify that show_repair_instructions was called (since status.is_ok() returns False)
        mock_show_repair.assert_called_once()

        # Verify that the function returns False
        assert result is False
