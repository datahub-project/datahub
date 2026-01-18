"""Unit tests for profile management in config_utils."""

import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
import yaml

from datahub.cli.config_utils import (
    MissingConfigError,
    add_profile,
    confirm_destructive_operation,
    ensure_datahub_dir,
    get_current_profile,
    get_profile,
    list_profiles,
    load_profile_config,
    migrate_legacy_config,
    profile_exists,
    remove_profile,
    requires_confirmation,
    save_profile_config,
    set_current_profile,
)
from datahub.configuration.config import DataHubConfig, ProfileConfig


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
def sample_config() -> DataHubConfig:
    """Create a sample configuration for testing."""
    return DataHubConfig(
        version="1.0",
        current_profile="dev",
        profiles={
            "dev": ProfileConfig(
                server="http://localhost:8080",
                token="dev_token",
                description="Local development",
            ),
            "staging": ProfileConfig(
                server="https://staging.datahub.com",
                token="${STAGING_TOKEN}",
                description="Staging environment",
            ),
            "prod": ProfileConfig(
                server="https://prod.datahub.com",
                token="${PROD_TOKEN}",
                require_confirmation=True,
                description="Production",
            ),
        },
    )


class TestEnsureDatahubDir:
    """Test ensure_datahub_dir function."""

    def test_creates_directory_if_not_exists(
        self, temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that directory is created if it doesn't exist."""
        new_dir = temp_config_dir.parent / "new_datahub"
        monkeypatch.setattr("datahub.cli.config_utils.DATAHUB_CONFIG_DIR", str(new_dir))

        assert not new_dir.exists()
        ensure_datahub_dir()
        assert new_dir.exists()

    def test_does_not_error_if_exists(self, temp_config_dir: Path) -> None:
        """Test that no error occurs if directory already exists."""
        assert temp_config_dir.exists()
        ensure_datahub_dir()  # Should not raise
        assert temp_config_dir.exists()


class TestSaveAndLoadProfileConfig:
    """Test saving and loading profile configurations."""

    def test_save_and_load_config(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test saving and loading a configuration."""
        save_profile_config(sample_config)

        config_file = temp_config_dir / "config.yaml"
        assert config_file.exists()

        # Verify YAML structure
        with open(config_file) as f:
            data = yaml.safe_load(f)

        assert data["version"] == "1.0"
        assert data["current_profile"] == "dev"
        assert "profiles" in data
        assert "dev" in data["profiles"]

    def test_save_preserves_all_fields(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test that all profile fields are preserved."""
        save_profile_config(sample_config)

        config_file = temp_config_dir / "config.yaml"
        with open(config_file) as f:
            data = yaml.safe_load(f)

        prod_profile = data["profiles"]["prod"]
        assert prod_profile["server"] == "https://prod.datahub.com"
        assert prod_profile["require_confirmation"] is True
        assert prod_profile["description"] == "Production"


class TestListProfiles:
    """Test list_profiles function."""

    def test_list_profiles_empty(self, temp_config_dir: Path) -> None:
        """Test listing profiles when config doesn't exist."""
        profiles = list_profiles()
        assert profiles == []

    def test_list_profiles_with_data(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test listing profiles from config."""
        save_profile_config(sample_config)

        profiles = list_profiles()
        assert len(profiles) == 3
        assert set(profiles) == {"dev", "staging", "prod"}


class TestGetCurrentProfile:
    """Test get_current_profile function."""

    def test_get_current_profile_from_config(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test getting current profile from config file."""
        save_profile_config(sample_config)

        current = get_current_profile()
        assert current == "dev"

    def test_get_current_profile_none(self, temp_config_dir: Path) -> None:
        """Test when no current profile is set."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
            }
        )
        save_profile_config(config)

        current = get_current_profile()
        assert current is None

    def test_get_current_profile_env_var_takes_precedence(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that DATAHUB_PROFILE env var takes precedence."""
        save_profile_config(sample_config)
        monkeypatch.setenv("DATAHUB_PROFILE", "staging")

        current = get_current_profile()
        assert current == "staging"

    def test_get_current_profile_fallback_to_default(
        self, temp_config_dir: Path
    ) -> None:
        """Test fallback to 'default' profile."""
        config = DataHubConfig(
            profiles={
                "default": ProfileConfig(server="http://localhost:8080"),
                "dev": ProfileConfig(server="http://dev:8080"),
            }
        )
        save_profile_config(config)

        current = get_current_profile()
        assert current == "default"


class TestSetCurrentProfile:
    """Test set_current_profile function."""

    def test_set_current_profile(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test setting current profile."""
        save_profile_config(sample_config)

        set_current_profile("staging")

        # Verify it was updated
        current = get_current_profile()
        assert current == "staging"

    def test_set_current_profile_nonexistent(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test setting non-existent profile raises error."""
        save_profile_config(sample_config)

        with pytest.raises(KeyError, match="Profile 'missing' not found"):
            set_current_profile("missing")

    def test_set_current_profile_no_config(self, temp_config_dir: Path) -> None:
        """Test setting profile when config doesn't exist."""
        with pytest.raises(MissingConfigError, match="No configuration file found"):
            set_current_profile("dev")


class TestProfileExists:
    """Test profile_exists function."""

    def test_profile_exists_true(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test checking if profile exists."""
        save_profile_config(sample_config)

        assert profile_exists("dev") is True
        assert profile_exists("staging") is True
        assert profile_exists("prod") is True

    def test_profile_exists_false(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test checking non-existent profile."""
        save_profile_config(sample_config)

        assert profile_exists("missing") is False

    def test_profile_exists_no_config(self, temp_config_dir: Path) -> None:
        """Test when config doesn't exist."""
        assert profile_exists("dev") is False


class TestGetProfile:
    """Test get_profile function."""

    def test_get_profile_success(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test getting an existing profile."""
        save_profile_config(sample_config)

        profile = get_profile("dev")
        assert profile.server == "http://localhost:8080"
        assert profile.description == "Local development"

    def test_get_profile_not_found(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test getting non-existent profile."""
        save_profile_config(sample_config)

        with pytest.raises(KeyError, match="Profile 'missing' not found"):
            get_profile("missing")

    def test_get_profile_no_config(self, temp_config_dir: Path) -> None:
        """Test getting profile when config doesn't exist."""
        with pytest.raises(MissingConfigError, match="No configuration file found"):
            get_profile("dev")


class TestAddProfile:
    """Test add_profile function."""

    def test_add_profile_to_empty_config(self, temp_config_dir: Path) -> None:
        """Test adding profile when config doesn't exist."""
        new_profile = ProfileConfig(
            server="http://localhost:8080",
            token="test_token",
        )

        add_profile("dev", new_profile)

        # Verify it was created
        profiles = list_profiles()
        assert "dev" in profiles

        loaded_profile = get_profile("dev")
        assert loaded_profile.server == "http://localhost:8080"

    def test_add_profile_to_existing_config(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test adding profile to existing config."""
        save_profile_config(sample_config)

        new_profile = ProfileConfig(
            server="http://new:8080",
            description="New environment",
        )

        add_profile("new", new_profile)

        profiles = list_profiles()
        assert "new" in profiles
        assert len(profiles) == 4

    def test_add_profile_overwrites_existing(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test adding profile with existing name overwrites it."""
        save_profile_config(sample_config)

        updated_profile = ProfileConfig(
            server="http://updated:8080",
            description="Updated",
        )

        add_profile("dev", updated_profile)

        profile = get_profile("dev")
        assert profile.server == "http://updated:8080"
        assert profile.description == "Updated"


class TestRemoveProfile:
    """Test remove_profile function."""

    def test_remove_profile_success(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test removing a profile."""
        save_profile_config(sample_config)

        remove_profile("dev")

        assert not profile_exists("dev")
        profiles = list_profiles()
        assert len(profiles) == 2

    def test_remove_current_profile(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test removing the current profile clears current_profile."""
        save_profile_config(sample_config)
        assert get_current_profile() == "dev"

        remove_profile("dev")

        current = get_current_profile()
        # Should fall back to 'default' or None, not 'dev'
        assert current != "dev"

    def test_remove_nonexistent_profile(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test removing non-existent profile raises error."""
        save_profile_config(sample_config)

        with pytest.raises(KeyError, match="Profile 'missing' not found"):
            remove_profile("missing")

    def test_remove_profile_no_config(self, temp_config_dir: Path) -> None:
        """Test removing profile when config doesn't exist."""
        with pytest.raises(MissingConfigError, match="No configuration file found"):
            remove_profile("dev")


class TestLoadProfileConfig:
    """Test load_profile_config function."""

    def test_load_explicit_profile(
        self,
        temp_config_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test loading a specific profile."""
        # Set env var BEFORE creating config so interpolation works
        monkeypatch.setenv("STAGING_TOKEN", "staging_secret")

        # Create config with env var reference
        config = DataHubConfig(
            version="1.0",
            current_profile="dev",
            profiles={
                "dev": ProfileConfig(
                    server="http://localhost:8080",
                ),
                "staging": ProfileConfig(
                    server="https://staging.datahub.com",
                    token="${STAGING_TOKEN}",
                ),
            },
        )
        save_profile_config(config)

        loaded_config = load_profile_config("staging")

        assert loaded_config.server == "https://staging.datahub.com"
        assert loaded_config.token == "staging_secret"  # Interpolated

    def test_load_current_profile(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test loading current profile when no profile specified."""
        save_profile_config(sample_config)

        config = load_profile_config()

        assert config.server == "http://localhost:8080"

    def test_load_profile_env_override(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that DATAHUB_PROFILE env var overrides config."""
        save_profile_config(sample_config)
        monkeypatch.setenv("DATAHUB_PROFILE", "prod")
        monkeypatch.setenv("PROD_TOKEN", "prod_secret")

        config = load_profile_config()

        assert config.server == "https://prod.datahub.com"

    def test_load_profile_env_var_overrides_server(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that DATAHUB_GMS_URL overrides profile server."""
        save_profile_config(sample_config)
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://override:9090")

        config = load_profile_config("dev")

        assert config.server == "http://override:9090"

    def test_load_profile_env_var_overrides_token(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that DATAHUB_GMS_TOKEN overrides profile token."""
        save_profile_config(sample_config)
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "override_token")

        config = load_profile_config("dev")

        assert config.token == "override_token"

    def test_load_profile_not_found(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test loading non-existent profile raises error."""
        save_profile_config(sample_config)

        with pytest.raises(MissingConfigError, match="Profile 'missing' not found"):
            load_profile_config("missing")

    def test_load_profile_no_config(
        self, temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test loading profile when no config exists."""
        # No env vars set
        monkeypatch.delenv("DATAHUB_GMS_URL", raising=False)
        monkeypatch.delenv("DATAHUB_GMS_HOST", raising=False)

        with pytest.raises(MissingConfigError, match="No configuration found"):
            load_profile_config()


class TestMigrateLegacyConfig:
    """Test migrate_legacy_config function."""

    def test_migrate_creates_new_config(
        self, temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test migrating legacy config creates new config."""
        # Create legacy config
        legacy_path = temp_config_dir.parent / ".datahubenv"
        legacy_data = {
            "gms": {
                "server": "http://localhost:8080",
                "token": "legacy_token",
                "timeout_sec": 30,
            }
        }
        with open(legacy_path, "w") as f:
            yaml.dump(legacy_data, f)

        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_CONFIG_PATH", str(legacy_path)
        )

        result = migrate_legacy_config()

        assert result is True
        assert profile_exists("default")

        profile = get_profile("default")
        assert profile.server == "http://localhost:8080"
        assert profile.token == "legacy_token"
        assert profile.description == "Migrated from legacy config"

    def test_migrate_skips_if_new_config_exists(
        self, temp_config_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test migration skips if new config already exists."""
        # Create both configs
        legacy_path = temp_config_dir.parent / ".datahubenv"
        with open(legacy_path, "w") as f:
            yaml.dump({"gms": {"server": "http://old:8080"}}, f)

        existing_config = DataHubConfig(
            profiles={"dev": ProfileConfig(server="http://existing:8080")}
        )
        save_profile_config(existing_config)

        monkeypatch.setattr(
            "datahub.cli.config_utils.DATAHUB_CONFIG_PATH", str(legacy_path)
        )

        result = migrate_legacy_config()

        assert result is False
        # Existing config should be unchanged
        assert get_profile("dev").server == "http://existing:8080"

    def test_migrate_no_legacy_config(self, temp_config_dir: Path) -> None:
        """Test migration fails when no legacy config exists."""
        with pytest.raises(MissingConfigError, match="No legacy config found"):
            migrate_legacy_config()


class TestRequiresConfirmation:
    """Test requires_confirmation function."""

    def test_requires_confirmation_true(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test profile with require_confirmation=True."""
        save_profile_config(sample_config)

        assert requires_confirmation("prod") is True

    def test_requires_confirmation_false(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test profile with require_confirmation=False."""
        save_profile_config(sample_config)

        assert requires_confirmation("dev") is False
        assert requires_confirmation("staging") is False

    def test_requires_confirmation_no_profile(self, temp_config_dir: Path) -> None:
        """Test when no profile exists."""
        assert requires_confirmation("missing") is False


class TestConfirmDestructiveOperation:
    """Test confirm_destructive_operation function."""

    def test_confirm_with_force_flag(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test that force flag bypasses confirmation."""
        save_profile_config(sample_config)

        # Should return True without prompting
        result = confirm_destructive_operation(
            operation="delete",
            profile_name="prod",
            force=True,
        )

        assert result is True

    def test_confirm_no_confirmation_required(
        self, temp_config_dir: Path, sample_config: DataHubConfig
    ) -> None:
        """Test that profiles without require_confirmation don't prompt."""
        save_profile_config(sample_config)

        result = confirm_destructive_operation(
            operation="delete",
            profile_name="dev",
            force=False,
        )

        assert result is True

    def test_confirm_with_correct_input(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test confirmation with correct profile name."""
        save_profile_config(sample_config)

        # Mock click.prompt to return the correct confirmation
        with patch("datahub.cli.config_utils.click.prompt", return_value="PROD"):
            result = confirm_destructive_operation(
                operation="delete",
                profile_name="prod",
                force=False,
            )

        assert result is True

    def test_confirm_with_incorrect_input(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test confirmation with incorrect input."""
        save_profile_config(sample_config)

        # Mock click.prompt to return incorrect confirmation
        with patch("datahub.cli.config_utils.click.prompt", return_value="wrong"):
            result = confirm_destructive_operation(
                operation="delete",
                profile_name="prod",
                force=False,
            )

        assert result is False

    def test_confirm_shows_extra_info(
        self,
        temp_config_dir: Path,
        sample_config: DataHubConfig,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that extra_info is displayed."""
        save_profile_config(sample_config)

        mock_echo = MagicMock()
        with (
            patch("datahub.cli.config_utils.click.echo", mock_echo),
            patch("datahub.cli.config_utils.click.prompt", return_value="PROD"),
        ):
            confirm_destructive_operation(
                operation="delete",
                profile_name="prod",
                force=False,
                extra_info="URN: urn:li:dataset:...",
            )

        # Check that extra_info was echoed
        call_args = [str(call) for call in mock_echo.call_args_list]
        assert any("URN: urn:li:dataset" in str(call) for call in call_args)
