import os
from unittest.mock import mock_open, patch

import pytest
import yaml

from datahub.cli import config_utils
from datahub.cli.config_utils import (
    DATAHUB_CONFIG_PATH,
    MissingConfigError,
    get_raw_client_config,
    get_system_auth,
    load_client_config,
    persist_raw_datahub_config,
    require_config_from_env,
    write_gms_config,
)


class TestConfigUtils:
    def test_get_system_auth(self):
        """Test the get_system_auth function with and without environment variables."""
        # Test when environment variables are not set
        with patch.dict(os.environ, {}, clear=True):
            assert get_system_auth() is None

        # Test when both environment variables are set
        with patch.dict(
            os.environ,
            {
                "DATAHUB_SYSTEM_CLIENT_ID": "test-id",
                "DATAHUB_SYSTEM_CLIENT_SECRET": "test-secret",
            },
        ):
            assert get_system_auth() == "Basic test-id:test-secret"

        # Test when only one environment variable is set
        with patch.dict(
            os.environ, {"DATAHUB_SYSTEM_CLIENT_ID": "test-id"}, clear=True
        ):
            assert get_system_auth() is None

    def test_persist_raw_datahub_config(self):
        """Test persisting config to file."""
        test_config = {
            "gms": {"server": "http://localhost:8080", "token": "test-token"}
        }

        mock_file = mock_open()
        with patch("builtins.open", mock_file):
            persist_raw_datahub_config(test_config)

        mock_file.assert_called_once_with(DATAHUB_CONFIG_PATH, "w+")
        mock_file().write.assert_called()  # Check that write was called (yaml.dump writes to the file)

    def test_get_raw_client_config(self):
        """Test loading config from file."""
        test_config = {
            "gms": {"server": "http://localhost:8080", "token": "test-token"}
        }
        yaml_content = yaml.dump(test_config)

        # Mock the file open and yaml load
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            config = get_raw_client_config()

        assert config == test_config

        # Test with malformed YAML
        with patch(
            "builtins.open", mock_open(read_data="invalid: yaml: content:")
        ), patch("click.secho") as mock_secho:
            config = get_raw_client_config()
            mock_secho.assert_called_once()
            assert config is None

    def test_get_config_from_env(self):
        """Test retrieving config from environment variables."""
        # Test with complete URL
        with patch.dict(
            os.environ,
            {
                "DATAHUB_GMS_URL": "http://test-url:8080",
                "DATAHUB_GMS_TOKEN": "test-token",
            },
            clear=True,
        ):
            url, token = config_utils._get_config_from_env()
            assert url == "http://test-url:8080"
            assert token == "test-token"

        # Test with host and port
        with patch.dict(
            os.environ,
            {"DATAHUB_GMS_HOST": "test-host", "DATAHUB_GMS_PORT": "8080"},
            clear=True,
        ):
            url, token = config_utils._get_config_from_env()
            assert url == "http://test-host:8080"
            assert token is None

        # Test with host only (backward compatibility)
        with patch.dict(
            os.environ, {"DATAHUB_GMS_HOST": "test-host"}, clear=True
        ), patch.object(config_utils.logger, "warning") as mock_warning:
            url, token = config_utils._get_config_from_env()
            assert url == "test-host"
            assert token is None
            mock_warning.assert_called_once()  # Warning about using host as URL

        # Test with empty environment
        with patch.dict(os.environ, {}, clear=True):
            url, token = config_utils._get_config_from_env()
            assert url is None
            assert token is None

    def test_require_config_from_env(self):
        """Test requiring config from environment variables."""
        # Test with host set
        with patch.dict(
            os.environ, {"DATAHUB_GMS_URL": "http://test-url:8080"}, clear=True
        ):
            url, token = require_config_from_env()
            assert url == "http://test-url:8080"
            assert token is None

        # Test with no host (should raise error)
        with patch.dict(os.environ, {}, clear=True), pytest.raises(
            MissingConfigError, match="No GMS host was provided in env variables."
        ):
            require_config_from_env()

    def test_load_client_config_from_env(self):
        """Test loading client config from environment variables."""
        with patch.dict(
            os.environ,
            {
                "DATAHUB_GMS_URL": "http://test-url:8080",
                "DATAHUB_GMS_TOKEN": "test-token",
            },
            clear=True,
        ):
            config = load_client_config()
            assert config.server == "http://test-url:8080"
            assert config.token == "test-token"

    def test_load_client_config_from_file(self):
        """Test loading client config from file."""
        test_config = {
            "gms": {"server": "http://localhost:8080", "token": "test-token"}
        }

        # Mock both environment check and file loading
        with patch.dict(os.environ, {}, clear=True), patch.object(
            config_utils, "_should_skip_config", return_value=False
        ), patch.object(config_utils, "_ensure_datahub_config"), patch.object(
            config_utils, "get_raw_client_config", return_value=test_config
        ):
            config = load_client_config()
            assert config.server == "http://localhost:8080"
            assert config.token == "test-token"

    def test_load_client_config_missing(self):
        """Test loading client config when missing."""
        # When skip config is true and no env variables
        with patch.dict(os.environ, {}, clear=True), patch.object(
            config_utils, "_should_skip_config", return_value=True
        ), pytest.raises(MissingConfigError, match="You have set the skip config flag"):
            load_client_config()

        # When config file is missing
        with patch.dict(os.environ, {}, clear=True), patch.object(
            config_utils, "_should_skip_config", return_value=False
        ), patch.object(
            config_utils,
            "_ensure_datahub_config",
            side_effect=MissingConfigError("No config"),
        ), pytest.raises(MissingConfigError, match="No config"):
            load_client_config()

    def test_ensure_datahub_config(self):
        """Test ensuring datahub config exists."""
        # Test when file doesn't exist
        with patch.object(os.path, "isfile", return_value=False), pytest.raises(
            MissingConfigError, match="No ~/.datahubenv file found"
        ):
            config_utils._ensure_datahub_config()

        # Test when file exists
        with patch.object(os.path, "isfile", return_value=True):
            # Should not raise an exception
            config_utils._ensure_datahub_config()

    def test_write_gms_config(self):
        """Test writing GMS config."""
        # Test with merge=True but no previous config
        with patch.object(
            config_utils, "get_raw_client_config", side_effect=Exception("No config")
        ), patch.object(
            config_utils, "persist_raw_datahub_config"
        ) as mock_persist, patch.object(config_utils.logger, "debug") as mock_debug:
            write_gms_config("http://test-host:8080", "test-token")
            mock_persist.assert_called_once()
            mock_debug.assert_called_once()  # Should log debug message about failure

            # Check the config that would be persisted
            expected_config = {
                "gms": {
                    "server": "http://test-host:8080",
                    "token": "test-token",
                    "client_mode": None,
                    "datahub_component": None,
                    "ca_certificate_path": None,
                    "client_certificate_path": None,
                    "disable_ssl_verification": False,
                    "extra_headers": None,
                    "openapi_ingestion": None,
                    "retry_max_times": None,
                    "retry_status_codes": None,
                    "timeout_sec": None,
                }
            }
            assert mock_persist.call_args[0][0] == expected_config

        # Test with merge=True and existing config
        previous_config = {
            "gms": {
                "server": "http://old-host:8080",
                "token": "old-token",
                "client_mode": None,
                "datahub_component": None,
                "ca_certificate_path": None,
                "client_certificate_path": None,
                "disable_ssl_verification": False,
                "extra_headers": None,
                "openapi_ingestion": None,
                "retry_max_times": None,
                "retry_status_codes": None,
                "timeout_sec": None,
            },
            "other": {"setting": "value"},
        }
        with patch.object(
            config_utils, "get_raw_client_config", return_value=previous_config
        ), patch.object(config_utils, "persist_raw_datahub_config") as mock_persist:
            write_gms_config("http://test-host:8080", "test-token")
            mock_persist.assert_called_once()

            # Check the config that would be persisted (merged)
            expected_config = {
                "gms": {
                    "server": "http://test-host:8080",
                    "token": "test-token",
                    "client_mode": None,
                    "datahub_component": None,
                    "ca_certificate_path": None,
                    "client_certificate_path": None,
                    "disable_ssl_verification": False,
                    "extra_headers": None,
                    "openapi_ingestion": None,
                    "retry_max_times": None,
                    "retry_status_codes": None,
                    "timeout_sec": None,
                },
                "other": {"setting": "value"},
            }
            assert mock_persist.call_args[0][0] == expected_config

        # Test with merge=False
        with patch.object(config_utils, "persist_raw_datahub_config") as mock_persist:
            write_gms_config(
                "http://test-host:8080", "test-token", merge_with_previous=False
            )
            mock_persist.assert_called_once()

            # Check the config that would be persisted (no merge)
            expected_config = {
                "gms": {
                    "server": "http://test-host:8080",
                    "token": "test-token",
                    "client_mode": None,
                    "datahub_component": None,
                    "ca_certificate_path": None,
                    "client_certificate_path": None,
                    "disable_ssl_verification": False,
                    "extra_headers": None,
                    "openapi_ingestion": None,
                    "retry_max_times": None,
                    "retry_status_codes": None,
                    "timeout_sec": None,
                }
            }
            assert mock_persist.call_args[0][0] == expected_config
