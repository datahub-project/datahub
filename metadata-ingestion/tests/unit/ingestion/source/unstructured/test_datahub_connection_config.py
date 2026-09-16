"""Unit tests for DataHubConnectionConfig and default value loading."""

from unittest.mock import MagicMock, patch

from datahub.ingestion.source.unstructured.chunking_config import (
    DataHubConnectionConfig,
    _get_default_gms_server,
    _get_default_gms_token,
)


class TestGetDefaultGmsServer:
    """Tests for _get_default_gms_server() helper function."""

    def test_returns_env_var_when_set(self):
        """Test that environment variable takes priority."""
        with patch(
            "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
        ) as mock_env:
            mock_env.return_value = "http://env-server:8080"
            result = _get_default_gms_server()
            assert result == "http://env-server:8080"

    def test_returns_datahubenv_when_no_env_var(self):
        """Test that ~/.datahubenv is used when env var not set."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False

            # Mock config file loading
            mock_config = MagicMock()
            mock_config.server = "http://datahubenv-server:8080"
            mock_load.return_value = mock_config

            result = _get_default_gms_server()
            assert result == "http://datahubenv-server:8080"

    def test_returns_default_when_no_config_file(self):
        """Test that default is used when ~/.datahubenv doesn't exist."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False

            # Simulate MissingConfigError
            from datahub.cli.config_utils import MissingConfigError

            mock_load.side_effect = MissingConfigError("Config file not found")

            result = _get_default_gms_server()
            assert result == "http://localhost:8080"

    def test_returns_default_when_skip_config_set(self):
        """Test that default is used when DATAHUB_SKIP_CONFIG is set."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
        ):
            mock_env.return_value = None
            mock_skip.return_value = True

            result = _get_default_gms_server()
            assert result == "http://localhost:8080"

    def test_returns_default_on_exception(self):
        """Test that default is used when loading config raises unexpected exception."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False
            mock_load.side_effect = Exception("Unexpected error")

            result = _get_default_gms_server()
            assert result == "http://localhost:8080"


class TestGetDefaultGmsToken:
    """Tests for _get_default_gms_token() helper function."""

    def test_returns_env_var_when_set(self):
        """Test that environment variable takes priority."""
        with patch(
            "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
        ) as mock_env:
            mock_env.return_value = "env-token-12345"
            result = _get_default_gms_token()
            assert result is not None
            assert result.get_secret_value() == "env-token-12345"

    def test_returns_datahubenv_when_no_env_var(self):
        """Test that ~/.datahubenv is used when env var not set."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False

            # Mock config file loading
            mock_config = MagicMock()
            mock_config.token = "datahubenv-token-67890"
            mock_load.return_value = mock_config

            result = _get_default_gms_token()
            assert result is not None
            assert result.get_secret_value() == "datahubenv-token-67890"

    def test_returns_none_when_no_config_file(self):
        """Test that None is returned when ~/.datahubenv doesn't exist."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False

            # Simulate MissingConfigError
            from datahub.cli.config_utils import MissingConfigError

            mock_load.side_effect = MissingConfigError("Config file not found")

            result = _get_default_gms_token()
            assert result is None

    def test_returns_none_when_skip_config_set(self):
        """Test that None is returned when DATAHUB_SKIP_CONFIG is set."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
        ):
            mock_env.return_value = None
            mock_skip.return_value = True

            result = _get_default_gms_token()
            assert result is None

    def test_returns_none_when_config_has_no_token(self):
        """Test that None is returned when ~/.datahubenv exists but has no token."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False

            # Mock config file with no token
            mock_config = MagicMock()
            mock_config.token = None
            mock_load.return_value = mock_config

            result = _get_default_gms_token()
            assert result is None

    def test_returns_none_on_exception(self):
        """Test that None is returned when loading config raises unexpected exception."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_skip.return_value = False
            mock_load.side_effect = Exception("Unexpected error")

            result = _get_default_gms_token()
            assert result is None


class TestDataHubConnectionConfig:
    """Tests for DataHubConnectionConfig class."""

    def test_defaults_to_env_var_server(self):
        """Test that server defaults to environment variable."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_token,
        ):
            mock_env.return_value = "http://test-server:8080"
            mock_token.return_value = None

            config = DataHubConnectionConfig()
            assert config.server == "http://test-server:8080"

    def test_defaults_to_localhost_when_no_env(self):
        """Test that server defaults to localhost when no env var or config file."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_token,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_token.return_value = None
            mock_skip.return_value = False

            from datahub.cli.config_utils import MissingConfigError

            mock_load.side_effect = MissingConfigError("Config file not found")

            config = DataHubConnectionConfig()
            assert config.server == "http://localhost:8080"

    def test_defaults_to_none_token_when_no_env(self):
        """Test that token defaults to None when no env var or config file."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_token,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_token.return_value = None
            mock_skip.return_value = False

            from datahub.cli.config_utils import MissingConfigError

            mock_load.side_effect = MissingConfigError("Config file not found")

            config = DataHubConnectionConfig()
            assert config.token is None

    def test_explicit_values_override_defaults(self):
        """Test that explicitly provided values override defaults."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_token,
        ):
            mock_env.return_value = "http://env-server:8080"
            mock_token.return_value = "env-token"

            # Explicit values should override
            config = DataHubConnectionConfig(
                server="http://explicit-server:8080", token="explicit-token"
            )
            assert config.server == "http://explicit-server:8080"
            assert config.token is not None
            assert config.token.get_secret_value() == "explicit-token"

    def test_loads_from_datahubenv_file(self):
        """Test that values are loaded from ~/.datahubenv when no env vars set."""
        with (
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_url"
            ) as mock_env,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_gms_token"
            ) as mock_token,
            patch(
                "datahub.ingestion.source.unstructured.chunking_config.env_vars.get_skip_config"
            ) as mock_skip,
            patch("datahub.cli.config_utils.load_client_config") as mock_load,
        ):
            mock_env.return_value = None
            mock_token.return_value = None
            mock_skip.return_value = False

            # Mock config file loading
            mock_config = MagicMock()
            mock_config.server = "http://file-server:8080"
            mock_config.token = "file-token-abc"
            mock_load.return_value = mock_config

            config = DataHubConnectionConfig()
            assert config.server == "http://file-server:8080"
            assert config.token is not None
            assert config.token.get_secret_value() == "file-token-abc"
