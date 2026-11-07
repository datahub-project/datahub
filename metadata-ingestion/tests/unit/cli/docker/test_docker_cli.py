import os
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner
from docker import DockerClient
from docker.errors import DockerException, NotFound

from datahub.cli.docker_check import (
    OPENSEARCH2_VOLUME,
    OPENSEARCH3_VOLUME,
    DockerComposeVersionError,
    volume_has_named_entry,
)
from datahub.cli.docker_cli import (
    _check_upgrade_and_show_instructions,
    _docker_compose_v2,
    _keep_existing_opensearch2_install,
    _opensearch2_overrides,
    _quickstart_honors_search_env,
    _resolve_token_service_secrets,
    check,
    download_compose_files,
    get_github_file_url,
    ingest_sample_data,
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


def test_docker_compose_v2_plugin_v2():
    """Test _docker_compose_v2 with Docker Compose v2 plugin"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.return_value = b"2.24.0\n"
        result = _docker_compose_v2()
        assert result == ["docker", "compose"]


def test_docker_compose_v2_plugin_v5():
    """Test _docker_compose_v2 with Docker Compose v5 plugin"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.return_value = b"5.0.1\n"
        result = _docker_compose_v2()
        assert result == ["docker", "compose"]


def test_docker_compose_v2_plugin_v2_with_v_prefix():
    """Test _docker_compose_v2 with Docker Compose v2 plugin (with v prefix)"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.return_value = b"v2.20.0\n"
        result = _docker_compose_v2()
        assert result == ["docker", "compose"]


def test_docker_compose_v2_standalone_v2():
    """Test _docker_compose_v2 with Docker Compose v2 standalone binary"""
    with patch("subprocess.check_output") as mock_check_output:
        # First call for docker compose plugin fails, second succeeds for standalone
        mock_check_output.side_effect = [
            FileNotFoundError("docker compose not found"),
            b"2.24.0\n",
        ]
        result = _docker_compose_v2()
        assert result == ["docker-compose"]


def test_docker_compose_v2_reject_v1_plugin():
    """Test _docker_compose_v2 rejects v1.x as plugin (should never happen but test defensive logic)"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.return_value = b"1.29.2\n"
        with pytest.raises(DockerComposeVersionError) as exc_info:
            _docker_compose_v2()
        assert "require Docker Compose v2 or later" in str(exc_info.value)


def test_docker_compose_v2_reject_v1_standalone():
    """Test _docker_compose_v2 rejects v1.x standalone"""
    with patch("subprocess.check_output") as mock_check_output:
        # First call for plugin fails, second returns v1.x for standalone
        mock_check_output.side_effect = [
            FileNotFoundError("docker compose not found"),
            b"1.29.2\n",
        ]
        with pytest.raises(DockerComposeVersionError) as exc_info:
            _docker_compose_v2()
        assert "require Docker Compose v2 or later" in str(exc_info.value)
        assert "1.29.2" in str(exc_info.value)


def test_docker_compose_v2_reject_v1_with_v_prefix():
    """Test _docker_compose_v2 rejects v1.x with v prefix"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.side_effect = [
            FileNotFoundError("docker compose not found"),
            b"v1.29.2\n",
        ]
        with pytest.raises(DockerComposeVersionError) as exc_info:
            _docker_compose_v2()
        assert "require Docker Compose v2 or later" in str(exc_info.value)


def test_docker_compose_v2_not_installed():
    """Test _docker_compose_v2 when Docker Compose is not installed at all"""
    with patch("subprocess.check_output") as mock_check_output:
        mock_check_output.side_effect = [
            FileNotFoundError("docker compose not found"),
            FileNotFoundError("docker-compose not found"),
        ]
        with pytest.raises(DockerComposeVersionError) as exc_info:
            _docker_compose_v2()
        assert "don't have Docker Compose installed" in str(exc_info.value)


# Tests for _resolve_token_service_secrets


def test_resolve_secrets_generates_and_persists(tmp_path: Path) -> None:
    """When no env vars or secrets file exist, values are generated and written to disk."""
    secrets_file = tmp_path / "quickstart" / ".local-secrets.env"

    with (
        patch("datahub.cli.docker_cli.DATAHUB_ROOT_FOLDER", str(tmp_path)),
        patch.dict(
            os.environ,
            {},
            clear=False,
        ),
    ):
        os.environ.pop("DATAHUB_TOKEN_SERVICE_SIGNING_KEY", None)
        os.environ.pop("DATAHUB_TOKEN_SERVICE_SALT", None)

        _resolve_token_service_secrets()

        assert "DATAHUB_TOKEN_SERVICE_SIGNING_KEY" in os.environ
        assert "DATAHUB_TOKEN_SERVICE_SALT" in os.environ
        assert len(os.environ["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"]) > 0
        assert len(os.environ["DATAHUB_TOKEN_SERVICE_SALT"]) > 0
        assert secrets_file.exists()
        content = secrets_file.read_text()
        assert "DATAHUB_TOKEN_SERVICE_SIGNING_KEY=" in content
        assert "DATAHUB_TOKEN_SERVICE_SALT=" in content


def test_resolve_secrets_env_vars_take_priority(tmp_path: Path) -> None:
    """Env vars override both persisted file values and generated values."""
    secrets_file = tmp_path / "quickstart" / ".local-secrets.env"
    secrets_file.parent.mkdir(parents=True)
    secrets_file.write_text(
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY=from_file_key\n"
        "DATAHUB_TOKEN_SERVICE_SALT=from_file_salt\n"
    )

    with (
        patch("datahub.cli.docker_cli.DATAHUB_ROOT_FOLDER", str(tmp_path)),
        patch.dict(
            os.environ,
            {
                "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "env_key",
                "DATAHUB_TOKEN_SERVICE_SALT": "env_salt",
            },
        ),
    ):
        _resolve_token_service_secrets()

        assert os.environ["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "env_key"
        assert os.environ["DATAHUB_TOKEN_SERVICE_SALT"] == "env_salt"


def test_resolve_secrets_persisted_file_used_when_no_env(tmp_path: Path) -> None:
    """Values from the secrets file are used when env vars are absent."""
    secrets_file = tmp_path / "quickstart" / ".local-secrets.env"
    secrets_file.parent.mkdir(parents=True)
    secrets_file.write_text(
        "DATAHUB_TOKEN_SERVICE_SIGNING_KEY=persisted_key\n"
        "DATAHUB_TOKEN_SERVICE_SALT=persisted_salt\n"
    )

    with (
        patch("datahub.cli.docker_cli.DATAHUB_ROOT_FOLDER", str(tmp_path)),
        patch.dict(os.environ, {}, clear=False),
    ):
        os.environ.pop("DATAHUB_TOKEN_SERVICE_SIGNING_KEY", None)
        os.environ.pop("DATAHUB_TOKEN_SERVICE_SALT", None)

        _resolve_token_service_secrets()

        assert os.environ["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"] == "persisted_key"
        assert os.environ["DATAHUB_TOKEN_SERVICE_SALT"] == "persisted_salt"


def test_resolve_secrets_no_file_written_when_already_set(tmp_path: Path) -> None:
    """No secrets file is created when all values come from env vars."""
    secrets_file = tmp_path / "quickstart" / ".local-secrets.env"

    with (
        patch("datahub.cli.docker_cli.DATAHUB_ROOT_FOLDER", str(tmp_path)),
        patch.dict(
            os.environ,
            {
                "DATAHUB_TOKEN_SERVICE_SIGNING_KEY": "env_key",
                "DATAHUB_TOKEN_SERVICE_SALT": "env_salt",
            },
        ),
    ):
        _resolve_token_service_secrets()

        assert not secrets_file.exists()


def test_resolve_secrets_generated_values_are_unique(tmp_path: Path) -> None:
    """Each call without existing secrets generates distinct random values."""
    results: list[tuple[str, str]] = []
    for _ in range(2):
        with (
            patch(
                "datahub.cli.docker_cli.DATAHUB_ROOT_FOLDER",
                str(tmp_path / str(len(results))),
            ),
            patch.dict(os.environ, {}, clear=False),
        ):
            os.environ.pop("DATAHUB_TOKEN_SERVICE_SIGNING_KEY", None)
            os.environ.pop("DATAHUB_TOKEN_SERVICE_SALT", None)
            _resolve_token_service_secrets()
            results.append(
                (
                    os.environ["DATAHUB_TOKEN_SERVICE_SIGNING_KEY"],
                    os.environ["DATAHUB_TOKEN_SERVICE_SALT"],
                )
            )

    assert results[0][0] != results[1][0]
    assert results[0][1] != results[1][1]


@patch("datahub.cli.docker_cli.Pipeline.create")
@patch("datahub.cli.docker_cli.check_docker_quickstart")
@patch("datahub.upgrade.upgrade.check_upgrade_post")
def test_ingest_sample_data_puts_server_and_token_on_source(
    mock_upgrade: MagicMock,
    mock_check: MagicMock,
    mock_pipeline_create: MagicMock,
) -> None:
    status = MagicMock()
    status.is_ok.return_value = True
    mock_check.return_value = status
    pipeline = MagicMock()
    pipeline.pretty_print_summary.return_value = 0
    mock_pipeline_create.return_value = pipeline

    result = CliRunner().invoke(ingest_sample_data, ["--token", "sample-token"])

    assert result.exit_code == 0, result.output
    recipe = mock_pipeline_create.call_args.args[0]
    assert recipe["source"]["config"]["server"] == "http://localhost:8080"
    assert recipe["source"]["config"]["token"] == "sample-token"
    assert recipe["sink"]["config"]["server"] == "http://localhost:8080"
    assert recipe["sink"]["config"]["token"] == "sample-token"


@pytest.mark.parametrize(
    "has_os2, has_os3, environ, expect_pin",
    [
        (True, False, {}, True),
        (False, False, {}, False),
        (True, True, {}, False),
        (True, False, {"DATAHUB_SEARCH_VOLUME": "os3data"}, False),
    ],
    ids=["existing-os2", "fresh-install", "already-on-os3", "user-override"],
)
def test_opensearch2_overrides(
    has_os2: bool, has_os3: bool, environ: Dict[str, str], expect_pin: bool
) -> None:
    overrides = _opensearch2_overrides(
        has_opensearch2_data=has_os2,
        opensearch3_initialized=has_os3,
        environ=environ,
    )

    if expect_pin:
        assert overrides == {
            "DATAHUB_SEARCH_TAG": "2.19.3",
            "DATAHUB_SEARCH_VOLUME": "osdata",
            "ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED": "false",
        }
    else:
        assert overrides == {}


def test_keep_existing_opensearch2_install_sets_env(mock_click: MagicMock) -> None:
    client = MagicMock(spec=DockerClient)
    client.containers.list.return_value = []
    client.volumes = MagicMock()
    client.volumes.get.side_effect = lambda name: (
        MagicMock() if name == OPENSEARCH2_VOLUME else _raise(NotFound(name))
    )
    client.containers.run.return_value = b"nodes\n"

    with patch.dict(os.environ, {}, clear=False):
        for key in (
            "DATAHUB_SEARCH_TAG",
            "DATAHUB_SEARCH_VOLUME",
            "ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED",
        ):
            os.environ.pop(key, None)

        _keep_existing_opensearch2_install(client)

        assert os.environ["DATAHUB_SEARCH_TAG"] == "2.19.3"
        assert os.environ["DATAHUB_SEARCH_VOLUME"] == "osdata"
    mock_click.secho.assert_called_once()


def _clear_opensearch_env() -> None:
    for key in (
        "DATAHUB_SEARCH_TAG",
        "DATAHUB_SEARCH_VOLUME",
        "ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED",
    ):
        os.environ.pop(key, None)


@pytest.mark.parametrize(
    "listing, expect_pin",
    [(b"", True), (b"nodes\n", False)],
    ids=["empty-os3", "os3-has-nodes"],
)
def test_keep_existing_opensearch2_ignores_empty_os3_volume(
    mock_click: MagicMock, listing: bytes, expect_pin: bool
) -> None:
    client = MagicMock(spec=DockerClient)
    client.containers.list.return_value = []
    client.volumes.get.return_value = MagicMock()

    client.containers.run.side_effect = _os3_volume_listing(listing)

    with patch.dict(os.environ, {}, clear=False):
        _clear_opensearch_env()
        _keep_existing_opensearch2_install(client)

        if expect_pin:
            assert os.environ["DATAHUB_SEARCH_TAG"] == "2.19.3"
            assert os.environ["DATAHUB_SEARCH_VOLUME"] == "osdata"
            mock_click.secho.assert_called_once()
        else:
            assert "DATAHUB_SEARCH_TAG" not in os.environ
            mock_click.secho.assert_not_called()


def _raise(error: Exception) -> None:
    raise error


@pytest.mark.parametrize(
    "version, expected",
    [
        (None, False),
        ("default", False),
        ("head", True),
        ("quickstart", True),
        ("HEAD", True),
        ("stable", False),
        ("v1.7.0", False),
        ("v0.13.0", False),
    ],
)
def test_quickstart_honors_search_env(version: Optional[str], expected: bool) -> None:
    assert _quickstart_honors_search_env(version) is expected


def test_volume_has_named_entry_treats_docker_failure_as_unknown() -> None:
    client = MagicMock(spec=DockerClient)
    client.volumes.get.return_value = MagicMock()
    client.containers.run.side_effect = DockerException("pull failed")

    assert volume_has_named_entry(client, OPENSEARCH3_VOLUME, "nodes") is None


def test_volume_has_named_entry_treats_volume_lookup_failure_as_unknown() -> None:
    client = MagicMock(spec=DockerClient)
    client.volumes.get.side_effect = DockerException("daemon unavailable")

    assert volume_has_named_entry(client, OPENSEARCH3_VOLUME, "nodes") is None
    client.containers.run.assert_not_called()


def test_volume_has_named_entry_is_false_for_missing_volume() -> None:
    client = MagicMock(spec=DockerClient)
    client.volumes.get.side_effect = NotFound("missing")

    assert volume_has_named_entry(client, OPENSEARCH3_VOLUME, "nodes") is False
    client.containers.run.assert_not_called()


def _search_container(image_ref: str, service: str = "opensearch") -> MagicMock:
    container = MagicMock()
    container.labels = {"com.docker.compose.service": service}
    container.image.tags = [image_ref]
    container.attrs = {}
    return container


def _os3_volume_listing(os3_listing: bytes) -> Callable[..., bytes]:
    def _listing(*_args: object, **kwargs: object) -> bytes:
        volumes = kwargs["volumes"]
        assert isinstance(volumes, dict)
        if next(iter(volumes)) == OPENSEARCH3_VOLUME:
            return os3_listing
        return b"nodes\n"

    return _listing


@pytest.mark.parametrize(
    "containers, os3_listing, expect_pin",
    [
        ([("opensearch", "opensearchproject/opensearch:2.19.3")], b"", True),
        (
            [
                ("opensearch", "opensearchproject/opensearch:2.19.3"),
                ("opensearch", "opensearchproject/opensearch:3.7.0"),
            ],
            b"",
            False,
        ),
        (
            [
                ("opensearch", "opensearchproject/opensearch:3.7.0"),
                ("opensearch", "opensearchproject/opensearch:2.19.3"),
            ],
            b"",
            False,
        ),
        (
            [
                ("opensearch2", "opensearchproject/opensearch:2.19.3"),
                ("opensearch3", "opensearchproject/opensearch:3.7.0"),
            ],
            b"",
            False,
        ),
        ([("opensearch", "opensearchproject/opensearch:2.19.3")], b"nodes\n", False),
    ],
    ids=[
        "2x-only-os3-empty",
        "2x-listed-before-3x",
        "3x-listed-before-2x",
        "3x-service-recognized",
        "2x-only-os3-has-nodes",
    ],
)
def test_keep_existing_opensearch2_follows_container_image(
    mock_click: MagicMock,
    containers: List[Tuple[str, str]],
    os3_listing: bytes,
    expect_pin: bool,
) -> None:
    client = MagicMock(spec=DockerClient)
    client.volumes.get.return_value = MagicMock()
    client.containers.run.side_effect = _os3_volume_listing(os3_listing)
    client.containers.list.return_value = [
        _search_container(image, service=service) for service, image in containers
    ]

    with patch.dict(os.environ, {}, clear=False):
        _clear_opensearch_env()
        _keep_existing_opensearch2_install(client)

        if expect_pin:
            assert os.environ["DATAHUB_SEARCH_TAG"] == "2.19.3"
            assert os.environ["DATAHUB_SEARCH_VOLUME"] == "osdata"
        else:
            assert "DATAHUB_SEARCH_TAG" not in os.environ


def test_keep_existing_opensearch2_skips_volume_checks_when_3x_is_running(
    mock_click: MagicMock,
) -> None:
    client = MagicMock(spec=DockerClient)
    client.containers.list.return_value = [
        _search_container("opensearchproject/opensearch:3.7.0")
    ]

    with patch.dict(os.environ, {}, clear=False):
        _clear_opensearch_env()
        _keep_existing_opensearch2_install(client)
        assert "DATAHUB_SEARCH_TAG" not in os.environ
        client.containers.run.assert_not_called()


def test_keep_existing_opensearch2_pins_when_2x_container_and_os3_listing_fails(
    mock_click: MagicMock,
) -> None:
    client = MagicMock(spec=DockerClient)
    client.containers.list.return_value = [
        _search_container("opensearchproject/opensearch:2.19.3")
    ]
    client.volumes.get.return_value = MagicMock()
    client.containers.run.side_effect = DockerException("pull failed")

    with patch.dict(os.environ, {}, clear=False):
        _clear_opensearch_env()
        _keep_existing_opensearch2_install(client)
        assert os.environ["DATAHUB_SEARCH_TAG"] == "2.19.3"
        assert os.environ["DATAHUB_SEARCH_VOLUME"] == "osdata"
        mock_click.secho.assert_called_once()


def test_keep_existing_opensearch2_does_not_pin_when_listing_fails(
    mock_click: MagicMock,
) -> None:
    client = MagicMock(spec=DockerClient)
    client.containers.list.return_value = []
    client.volumes.get.return_value = MagicMock()
    client.containers.run.side_effect = DockerException("pull failed")

    with patch.dict(os.environ, {}, clear=False):
        _clear_opensearch_env()
        _keep_existing_opensearch2_install(client)
        assert "DATAHUB_SEARCH_TAG" not in os.environ
        mock_click.secho.assert_not_called()
