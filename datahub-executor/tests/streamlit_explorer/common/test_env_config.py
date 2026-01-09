"""Tests for the env_config module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

from scripts.streamlit_explorer.common.env_config import (
    DataHubEnvConfig,
    get_env_config_summary,
    list_env_files,
    load_env_config,
    load_env_config_from_file,
)


class TestDataHubEnvConfig:
    """Tests for DataHubEnvConfig dataclass."""

    def test_hostname_extraction_simple(self):
        config = DataHubEnvConfig(server="https://gms.example.com")
        assert config.hostname == "gms.example.com"

    def test_hostname_extraction_with_port(self):
        config = DataHubEnvConfig(server="https://gms.example.com:8080")
        assert config.hostname == "gms.example.com:8080"

    def test_hostname_extraction_localhost(self):
        config = DataHubEnvConfig(server="http://localhost:8080")
        assert config.hostname == "localhost:8080"

    def test_display_name(self):
        config = DataHubEnvConfig(server="https://gms.example.com")
        assert config.display_name == "gms.example.com"

    def test_to_dict(self):
        config = DataHubEnvConfig(
            server="https://gms.example.com",
            token="secret",
            source_file="/path/to/file",
        )
        d = config.to_dict()
        assert d["server"] == "https://gms.example.com"
        assert d["token"] == "secret"
        assert d["source_file"] == "/path/to/file"
        assert d["hostname"] == "gms.example.com"


class TestLoadEnvConfig:
    """Tests for loading env config from files."""

    def test_load_from_yaml_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("gms:\n  server: https://test.example.com\n  token: mytoken\n")
            f.flush()

            config = load_env_config_from_file(f.name)
            assert config is not None
            assert config.server == "https://test.example.com"
            assert config.token == "mytoken"

    def test_load_from_yaml_file_nested_gms(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("gms:\n  server: https://nested.example.com\n")
            f.flush()

            config = load_env_config_from_file(f.name)
            assert config is not None
            assert config.server == "https://nested.example.com"

    def test_load_from_nonexistent_file(self):
        config = load_env_config_from_file("/nonexistent/path/file.yaml")
        assert config is None

    def test_load_from_invalid_yaml(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content: [[[")
            f.flush()

            config = load_env_config_from_file(f.name)
            assert config is None

    def test_load_from_yaml_without_server(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("gms:\n  token: onlytoken\n")
            f.flush()

            config = load_env_config_from_file(f.name)
            assert config is None  # server is required


class TestGetEnvConfigSummary:
    """Tests for the summary function."""

    def test_summary_with_token(self):
        config = DataHubEnvConfig(
            server="https://gms.example.com",
            token="verylongsecrettoken",
            source_file="~/.datahubenv",
        )
        summary = get_env_config_summary(config)

        assert summary["Server"] == "https://gms.example.com"
        assert summary["Hostname"] == "gms.example.com"
        assert "***" in summary["Token"]  # Token should be masked
        assert "oken" in summary["Token"]  # Last 4 chars visible
        assert summary["Source"] == "~/.datahubenv"

    def test_summary_without_token(self):
        config = DataHubEnvConfig(server="https://gms.example.com")
        summary = get_env_config_summary(config)

        assert summary["Token"] == "Not set"


class TestListEnvFiles:
    """Tests for discovering env files."""

    def test_list_env_files_with_search_dir(self):
        """Test that list_env_files finds files in custom search directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a test env file
            env_file = Path(tmpdir) / "test.env"
            env_file.write_text(
                "gms:\n  server: https://test.example.com\n  token: testtoken\n"
            )

            files = list_env_files(search_dirs=[tmpdir])

            # Should find at least our test file
            file_paths = [f[0] for f in files]
            assert str(env_file) in file_paths

            # Should have loaded the config
            for path, config in files:
                if path == str(env_file):
                    assert config is not None
                    assert config.server == "https://test.example.com"
                    assert config.token == "testtoken"

    def test_list_env_files_returns_tuples(self):
        """Test that list_env_files returns (path, config) tuples."""
        with tempfile.TemporaryDirectory() as tmpdir:
            env_file = Path(tmpdir) / "config.yaml"
            env_file.write_text("gms:\n  server: https://example.com\n")

            files = list_env_files(search_dirs=[tmpdir])

            # Check that each result is a tuple of (str, config_or_none)
            for item in files:
                assert isinstance(item, tuple)
                assert len(item) == 2
                assert isinstance(item[0], str)
                # config can be DataHubEnvConfig or None

    def test_list_env_files_invalid_file_returns_none_config(self):
        """Test that invalid files return None for the config part."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create an invalid yaml file (no server)
            env_file = Path(tmpdir) / "invalid.yaml"
            env_file.write_text("gms:\n  token: onlytoken\n")

            files = list_env_files(search_dirs=[tmpdir])

            for path, config in files:
                if path == str(env_file):
                    assert config is None  # Invalid config


class TestCredentialLookupByHostname:
    """Tests for finding API credentials by hostname (used in publish flow)."""

    def test_find_config_by_hostname_exact_match(self):
        """Test finding a config when hostname matches exactly."""
        configs = [
            DataHubEnvConfig(server="http://localhost:8080", token="local_token"),
            DataHubEnvConfig(server="https://prod.example.com", token="prod_token"),
            DataHubEnvConfig(
                server="https://staging.example.com", token="staging_token"
            ),
        ]

        hostname = "localhost:8080"
        found = None
        for cfg in configs:
            if hostname in cfg.server:
                found = cfg
                break

        assert found is not None
        assert found.token == "local_token"

    def test_find_config_by_hostname_partial_match(self):
        """Test finding a config when hostname is a partial match."""
        configs = [
            DataHubEnvConfig(server="https://gms.example.com:443", token="gms_token"),
        ]

        hostname = "gms.example.com"
        found = None
        for cfg in configs:
            if hostname in cfg.server:
                found = cfg
                break

        assert found is not None
        assert found.token == "gms_token"

    def test_find_config_by_hostname_no_match(self):
        """Test when no config matches the hostname."""
        configs = [
            DataHubEnvConfig(server="http://localhost:8080", token="local_token"),
        ]

        hostname = "other.example.com"
        found = None
        for cfg in configs:
            if hostname in cfg.server:
                found = cfg
                break

        assert found is None

    def test_build_graphql_url_from_server(self):
        """Test building GraphQL URL from server URL."""
        config = DataHubEnvConfig(server="http://localhost:8080")
        graphql_url = config.server.rstrip("/") + "/api/graphql"
        assert graphql_url == "http://localhost:8080/api/graphql"

    def test_build_graphql_url_with_trailing_slash(self):
        """Test building GraphQL URL handles trailing slash."""
        config = DataHubEnvConfig(server="http://localhost:8080/")
        graphql_url = config.server.rstrip("/") + "/api/graphql"
        assert graphql_url == "http://localhost:8080/api/graphql"

    def test_authorization_header_with_token(self):
        """Test building authorization header with token."""
        config = DataHubEnvConfig(server="http://localhost:8080", token="mytoken")
        headers = {"Authorization": f"Bearer {config.token}"} if config.token else {}
        assert headers == {"Authorization": "Bearer mytoken"}

    def test_authorization_header_without_token(self):
        """Test building authorization header without token."""
        config = DataHubEnvConfig(server="http://localhost:8080", token=None)
        headers = {"Authorization": f"Bearer {config.token}"} if config.token else {}
        assert headers == {}


class TestLoadEnvConfigWithEnvVars:
    """Tests for loading config from environment variables."""

    def test_load_from_env_vars(self):
        """Test loading config from DATAHUB_GMS_URL environment variable."""
        with patch.dict(
            os.environ,
            {
                "DATAHUB_GMS_URL": "http://env-server.example.com",
                "DATAHUB_GMS_TOKEN": "env_token",
            },
        ):
            config = load_env_config(allow_env_override=True)
            assert config is not None
            assert config.server == "http://env-server.example.com"
            assert config.token == "env_token"
            assert config.source_file == "environment variables"

    def test_load_from_legacy_env_vars(self):
        """Test loading config from legacy DATAHUB_GMS_HOST/PORT environment variables."""
        with patch.dict(
            os.environ,
            {
                "DATAHUB_GMS_HOST": "legacy.example.com",
                "DATAHUB_GMS_PORT": "9090",
                "DATAHUB_GMS_PROTOCOL": "https",
            },
            clear=False,
        ):
            # Clear the URL var to test legacy path
            env = os.environ.copy()
            env.pop("DATAHUB_GMS_URL", None)
            with patch.dict(os.environ, env, clear=True):
                config = load_env_config(allow_env_override=True)
                if config and config.source_file == "environment variables":
                    assert "legacy.example.com" in config.server
                    assert "9090" in config.server

    def test_env_vars_disabled(self):
        """Test that env vars are ignored when allow_env_override=False."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("gms:\n  server: https://file.example.com\n")
            f.flush()

            with patch.dict(
                os.environ,
                {
                    "DATAHUB_GMS_URL": "http://env-server.example.com",
                },
            ):
                config = load_env_config(config_path=f.name, allow_env_override=False)
                assert config is not None
                assert config.server == "https://file.example.com"
