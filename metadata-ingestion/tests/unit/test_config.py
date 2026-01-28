"""Unit tests for profile configuration models."""

import pytest

from datahub.configuration.config import (
    DataHubConfig,
    ProfileConfig,
    interpolate_env_vars,
)


class TestEnvironmentVariableInterpolation:
    """Test environment variable interpolation in config values."""

    def test_simple_interpolation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test basic ${VAR} interpolation."""
        monkeypatch.setenv("TEST_VAR", "test_value")
        result = interpolate_env_vars("${TEST_VAR}")
        assert result == "test_value"

    def test_interpolation_with_text(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test interpolation within text."""
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "8080")
        result = interpolate_env_vars("http://${HOST}:${PORT}")
        assert result == "http://localhost:8080"

    def test_interpolation_with_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test ${VAR:-default} syntax."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        result = interpolate_env_vars("${MISSING_VAR:-default_value}")
        assert result == "default_value"

    def test_interpolation_with_default_uses_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that ${VAR:-default} uses env var when set."""
        monkeypatch.setenv("PRESENT_VAR", "actual_value")
        result = interpolate_env_vars("${PRESENT_VAR:-default_value}")
        assert result == "actual_value"

    def test_interpolation_required_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test ${VAR:?error} raises error when missing."""
        monkeypatch.delenv("REQUIRED_VAR", raising=False)
        with pytest.raises(ValueError, match="Required environment variable not set"):
            interpolate_env_vars("${REQUIRED_VAR:?This var is required}")

    def test_interpolation_required_present(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test ${VAR:?error} uses value when present."""
        monkeypatch.setenv("REQUIRED_VAR", "present_value")
        result = interpolate_env_vars("${REQUIRED_VAR:?This var is required}")
        assert result == "present_value"

    def test_interpolation_missing_without_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test ${VAR} returns empty string when missing."""
        monkeypatch.delenv("MISSING_VAR", raising=False)
        result = interpolate_env_vars("${MISSING_VAR}")
        assert result == ""

    def test_no_interpolation_for_non_string(self) -> None:
        """Test that non-strings are returned as-is."""
        assert interpolate_env_vars(123) == 123  # type: ignore
        assert interpolate_env_vars(None) is None  # type: ignore

    def test_multiple_interpolations(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test multiple variables in one string."""
        monkeypatch.setenv("VAR1", "value1")
        monkeypatch.setenv("VAR2", "value2")
        result = interpolate_env_vars("${VAR1}_${VAR2}")
        assert result == "value1_value2"


class TestProfileConfig:
    """Test ProfileConfig model."""

    def test_create_basic_profile(self) -> None:
        """Test creating a basic profile."""
        profile = ProfileConfig(
            server="http://localhost:8080",
            token="test_token",
        )
        assert profile.server == "http://localhost:8080"
        assert profile.token == "test_token"
        assert profile.require_confirmation is False

    def test_profile_with_confirmation(self) -> None:
        """Test profile with require_confirmation flag."""
        profile = ProfileConfig(
            server="https://prod.datahub.com",
            token="prod_token",
            require_confirmation=True,
            description="Production",
        )
        assert profile.require_confirmation is True
        assert profile.description == "Production"

    def test_profile_interpolates_env_vars(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that ProfileConfig interpolates environment variables."""
        monkeypatch.setenv("MY_TOKEN", "secret_token")
        monkeypatch.setenv("MY_HOST", "datahub.example.com")

        profile = ProfileConfig(
            server="https://${MY_HOST}",
            token="${MY_TOKEN}",
        )

        assert profile.server == "https://datahub.example.com"
        assert profile.token == "secret_token"

    def test_profile_with_timeout(self) -> None:
        """Test profile with custom timeout."""
        profile = ProfileConfig(
            server="http://localhost:8080",
            timeout_sec=60,
        )
        assert profile.timeout_sec == 60


class TestDataHubConfig:
    """Test DataHubConfig model."""

    def test_create_empty_config(self) -> None:
        """Test creating config with no profiles."""
        config = DataHubConfig(version="1.0", profiles={})
        assert config.version == "1.0"
        assert len(config.profiles) == 0
        assert config.current_profile is None

    def test_create_config_with_profiles(self) -> None:
        """Test creating config with multiple profiles."""
        config = DataHubConfig(
            version="1.0",
            current_profile="dev",
            profiles={
                "dev": ProfileConfig(
                    server="http://localhost:8080",
                    description="Local dev",
                ),
                "prod": ProfileConfig(
                    server="https://datahub.company.com",
                    require_confirmation=True,
                    description="Production",
                ),
            },
        )

        assert len(config.profiles) == 2
        assert config.current_profile == "dev"
        assert "dev" in config.profiles
        assert "prod" in config.profiles
        assert config.profiles["prod"].require_confirmation is True

    def test_get_profile_success(self) -> None:
        """Test getting an existing profile."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
            }
        )

        profile = config.get_profile("dev")
        assert profile.server == "http://localhost:8080"

    def test_get_profile_not_found(self) -> None:
        """Test getting a non-existent profile raises KeyError."""
        config = DataHubConfig(profiles={})

        with pytest.raises(KeyError, match="Profile 'missing' not found"):
            config.get_profile("missing")

    def test_get_profile_shows_available_profiles(self) -> None:
        """Test error message includes available profiles."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
                "staging": ProfileConfig(server="http://staging:8080"),
            }
        )

        with pytest.raises(KeyError) as exc_info:
            config.get_profile("missing")

        error_msg = str(exc_info.value)
        assert "dev" in error_msg or "staging" in error_msg

    def test_add_profile(self) -> None:
        """Test adding a new profile."""
        config = DataHubConfig(profiles={})

        new_profile = ProfileConfig(server="http://localhost:8080")
        config.add_profile("dev", new_profile)

        assert "dev" in config.profiles
        assert config.profiles["dev"].server == "http://localhost:8080"

    def test_add_profile_overwrites_existing(self) -> None:
        """Test that adding a profile with existing name overwrites it."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://old:8080"),
            }
        )

        new_profile = ProfileConfig(server="http://new:8080")
        config.add_profile("dev", new_profile)

        assert config.profiles["dev"].server == "http://new:8080"

    def test_remove_profile(self) -> None:
        """Test removing a profile."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
                "prod": ProfileConfig(server="http://prod:8080"),
            }
        )

        config.remove_profile("dev")

        assert "dev" not in config.profiles
        assert "prod" in config.profiles

    def test_remove_nonexistent_profile(self) -> None:
        """Test removing a non-existent profile raises KeyError."""
        config = DataHubConfig(profiles={})

        with pytest.raises(KeyError, match="Profile 'missing' not found"):
            config.remove_profile("missing")

    def test_list_profiles(self) -> None:
        """Test listing all profile names."""
        config = DataHubConfig(
            profiles={
                "dev": ProfileConfig(server="http://localhost:8080"),
                "staging": ProfileConfig(server="http://staging:8080"),
                "prod": ProfileConfig(server="http://prod:8080"),
            }
        )

        profiles = config.list_profiles()
        assert len(profiles) == 3
        assert set(profiles) == {"dev", "staging", "prod"}

    def test_validate_profiles_dict_with_plain_dicts(self) -> None:
        """Test that validator converts plain dicts to ProfileConfig."""
        config = DataHubConfig(
            profiles={
                "dev": {
                    "server": "http://localhost:8080",
                    "token": "test",
                }
            }
        )

        assert isinstance(config.profiles["dev"], ProfileConfig)
        assert config.profiles["dev"].server == "http://localhost:8080"

    def test_validate_profiles_dict_invalid_type(self) -> None:
        """Test that invalid profile config raises error."""
        with pytest.raises(ValueError, match="Invalid profile config"):
            DataHubConfig(
                profiles={
                    "dev": "invalid"  # type: ignore
                }
            )
