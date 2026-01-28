"""Unit tests for profile CLI commands."""

import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from datahub.cli.profile_cli import profile
from datahub.configuration.config import DataHubConfig, ProfileConfig


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def temp_config_dir(monkeypatch: pytest.MonkeyPatch) -> Generator[Path, None, None]:
    """Create a temporary config directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir)
        config_dir = temp_path / ".datahub"
        config_dir.mkdir()

        # Patch the config paths
        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_CONFIG_DIR", str(config_dir)
        )
        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_NEW_CONFIG_PATH",
            str(config_dir / "config.yaml"),
        )
        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_CONFIG_PATH",
            str(temp_path / ".datahubenv"),
        )

        yield config_dir


@pytest.fixture
def sample_config(temp_config_dir: Path) -> DataHubConfig:
    """Create and save a sample configuration."""
    config = DataHubConfig(
        version="1.0",
        current_profile="dev",
        profiles={
            "dev": ProfileConfig(
                server="http://localhost:8080",
                token="dev_token",
                description="Local development",
            ),
            "prod": ProfileConfig(
                server="https://prod.datahub.com",
                token="prod_token",
                require_confirmation=True,
                description="Production",
            ),
        },
    )

    # Save it
    config_file = temp_config_dir / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config.model_dump(exclude_none=True), f)

    return config


class TestProfileList:
    """Test 'datahub profile list' command."""

    def test_list_empty(self, cli_runner: CliRunner, temp_config_dir: Path) -> None:
        """Test listing when no profiles exist."""
        result = cli_runner.invoke(profile, ["list"])

        assert result.exit_code == 0
        assert "No profiles configured" in result.output

    def test_list_with_profiles(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test listing existing profiles."""
        result = cli_runner.invoke(profile, ["list"])

        assert result.exit_code == 0
        assert "dev" in result.output
        assert "prod" in result.output
        assert "http://localhost:8080" in result.output
        assert "https://prod.datahub.com" in result.output

    def test_list_shows_current_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test that current profile is highlighted."""
        result = cli_runner.invoke(profile, ["list"])

        assert result.exit_code == 0
        assert (
            "Current profile: dev" in result.output or "dev (current)" in result.output
        )


class TestProfileCurrent:
    """Test 'datahub profile current' command."""

    def test_current_no_profile_set(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test when no current profile is set."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
            }
        )
        config_file = temp_config_dir / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config.model_dump(exclude_none=True), f)

        result = cli_runner.invoke(profile, ["current"])

        assert result.exit_code == 0
        assert (
            "No current profile set" in result.output
            or "Profile precedence" in result.output
        )

    def test_current_with_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test showing current profile."""
        result = cli_runner.invoke(profile, ["current"])

        assert result.exit_code == 0
        assert "dev" in result.output
        assert "http://localhost:8080" in result.output


class TestProfileShow:
    """Test 'datahub profile show' command."""

    def test_show_specific_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test showing a specific profile."""
        result = cli_runner.invoke(profile, ["show", "prod"])

        assert result.exit_code == 0
        assert "prod" in result.output
        assert "https://prod.datahub.com" in result.output
        assert "Production" in result.output

    def test_show_redacts_token(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test that tokens are redacted in output."""
        result = cli_runner.invoke(profile, ["show", "dev"])

        assert result.exit_code == 0
        # Token should be redacted
        assert "dev_token" not in result.output or "****" in result.output

    def test_show_nonexistent_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test showing non-existent profile."""
        result = cli_runner.invoke(profile, ["show", "missing"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_show_current_profile_when_not_specified(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test showing current profile when no name provided."""
        result = cli_runner.invoke(profile, ["show"])

        assert result.exit_code == 0
        # Should show dev (the current profile)
        assert "dev" in result.output or "localhost" in result.output


class TestProfileUse:
    """Test 'datahub profile use' command."""

    def test_use_profile(
        self,
        cli_runner: CliRunner,
        sample_config: DataHubConfig,
        temp_config_dir: Path,
    ) -> None:
        """Test switching to a different profile."""
        result = cli_runner.invoke(profile, ["use", "prod"])

        assert result.exit_code == 0
        assert "Switched to profile 'prod'" in result.output or "prod" in result.output

        # Verify config was updated
        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["current_profile"] == "prod"

    def test_use_nonexistent_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test switching to non-existent profile."""
        result = cli_runner.invoke(profile, ["use", "missing"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestProfileAdd:
    """Test 'datahub profile add' command."""

    def test_add_profile_basic(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test adding a new profile."""
        result = cli_runner.invoke(
            profile,
            [
                "add",
                "new_profile",
                "--server",
                "http://new:8080",
                "--token",
                "new_token",
            ],
        )

        assert result.exit_code == 0
        assert (
            "Added profile 'new_profile'" in result.output
            or "new_profile" in result.output
        )

        # Verify it was saved
        config_file = temp_config_dir / "config.yaml"
        assert config_file.exists()
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert "new_profile" in data["profiles"]

    def test_add_profile_with_env_var(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test adding profile with environment variable for token."""
        result = cli_runner.invoke(
            profile,
            [
                "add",
                "staging",
                "--server",
                "http://staging:8080",
                "--token-env",
                "STAGING_TOKEN",
            ],
        )

        assert result.exit_code == 0

        # Verify token is stored as env var reference
        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["profiles"]["staging"]["token"] == "${STAGING_TOKEN}"

    def test_add_profile_with_confirmation(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test adding profile with require_confirmation flag."""
        result = cli_runner.invoke(
            profile,
            [
                "add",
                "prod",
                "--server",
                "http://prod:8080",
                "--require-confirmation",
            ],
        )

        assert result.exit_code == 0

        # Verify flag was saved
        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data["profiles"]["prod"]["require_confirmation"] is True

    def test_add_profile_set_current(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test adding profile and setting it as current."""
        result = cli_runner.invoke(
            profile,
            [
                "add",
                "new_profile",
                "--server",
                "http://new:8080",
                "--set-current",
            ],
        )

        assert result.exit_code == 0

        # Verify it's set as current
        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert data.get("current_profile") == "new_profile"

    def test_add_profile_overwrite_prompts(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test that overwriting existing profile prompts for confirmation."""
        # Try to add profile that already exists, answer 'n' to overwrite
        result = cli_runner.invoke(
            profile,
            [
                "add",
                "dev",
                "--server",
                "http://new:8080",
            ],
            input="n\n",
        )

        # Should abort
        assert (
            "already exists" in result.output.lower()
            or "overwrite" in result.output.lower()
        )


class TestProfileRemove:
    """Test 'datahub profile remove' command."""

    def test_remove_profile(
        self,
        cli_runner: CliRunner,
        sample_config: DataHubConfig,
        temp_config_dir: Path,
    ) -> None:
        """Test removing a profile."""
        result = cli_runner.invoke(
            profile,
            ["remove", "prod", "--force"],
        )

        assert result.exit_code == 0
        assert (
            "Removed profile 'prod'" in result.output
            or "removed" in result.output.lower()
        )

        # Verify it was removed
        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)
        assert "prod" not in data["profiles"]

    def test_remove_profile_prompts_confirmation(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test that removing profile prompts for confirmation."""
        # Answer 'y' to confirmation
        result = cli_runner.invoke(
            profile,
            ["remove", "prod"],
            input="y\n",
        )

        assert result.exit_code == 0

    def test_remove_nonexistent_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test removing non-existent profile."""
        result = cli_runner.invoke(
            profile,
            ["remove", "missing", "--force"],
        )

        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestProfileTest:
    """Test 'datahub profile test' command."""

    def test_test_connection_success(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test successful connection test."""
        # Mock get_default_graph to avoid actual connection
        mock_graph = MagicMock()
        mock_graph.config.server = "http://localhost:8080"

        with patch(
            "datahub.ingestion.graph.client.get_default_graph",
            return_value=mock_graph,
        ):
            result = cli_runner.invoke(profile, ["test", "dev"])

        assert result.exit_code == 0
        assert "successful" in result.output.lower() or "✓" in result.output

    def test_test_connection_failure(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test failed connection test."""
        # Mock get_default_graph to raise exception
        with patch(
            "datahub.ingestion.graph.client.get_default_graph",
            side_effect=Exception("Connection failed"),
        ):
            result = cli_runner.invoke(profile, ["test", "dev"])

        assert result.exit_code != 0
        assert "failed" in result.output.lower() or "✗" in result.output

    def test_test_current_profile_when_not_specified(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test testing current profile when no name provided."""
        mock_graph = MagicMock()
        mock_graph.config.server = "http://localhost:8080"

        with patch(
            "datahub.ingestion.graph.client.get_default_graph",
            return_value=mock_graph,
        ):
            result = cli_runner.invoke(profile, ["test"])

        # Should test the current profile (dev)
        assert result.exit_code == 0


class TestProfileValidate:
    """Test 'datahub profile validate' command."""

    def test_validate_valid_config(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test validating a valid configuration."""
        result = cli_runner.invoke(profile, ["validate"])

        assert result.exit_code == 0
        assert "valid" in result.output.lower() or "✓" in result.output
        assert "Found 2 profile" in result.output

    def test_validate_warns_missing_fields(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test validation warns about missing required fields."""
        config = DataHubConfig(
            profiles={
                "incomplete": ProfileConfig(
                    server="http://localhost:8080",
                    # No token
                ),
            }
        )
        config_file = temp_config_dir / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config.model_dump(exclude_none=True), f)

        result = cli_runner.invoke(profile, ["validate"])

        assert result.exit_code == 0
        # Should warn about missing token
        assert "no token" in result.output.lower() or "⚠" in result.output

    def test_validate_no_config(
        self, cli_runner: CliRunner, temp_config_dir: Path
    ) -> None:
        """Test validation when no config exists."""
        result = cli_runner.invoke(profile, ["validate"])

        assert result.exit_code == 0
        assert "No new-format config file found" in result.output


class TestProfileExport:
    """Test 'datahub profile export' command."""

    def test_export_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test exporting a profile."""
        result = cli_runner.invoke(profile, ["export", "dev"])

        assert result.exit_code == 0
        assert "profile: dev" in result.output.lower() or "dev" in result.output
        assert "server:" in result.output.lower()

    def test_export_redacts_token(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test that export redacts token."""
        result = cli_runner.invoke(profile, ["export", "prod"])

        assert result.exit_code == 0
        # Token should be redacted
        assert "prod_token" not in result.output
        assert "****" in result.output or "token" in result.output.lower()

    def test_export_nonexistent_profile(
        self, cli_runner: CliRunner, sample_config: DataHubConfig
    ) -> None:
        """Test exporting non-existent profile."""
        result = cli_runner.invoke(profile, ["export", "missing"])

        assert result.exit_code != 0
        assert "not found" in result.output.lower()
