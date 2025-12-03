"""Tests for recording configuration models."""

import os
from unittest.mock import patch

import pytest
from pydantic import SecretStr, ValidationError

from datahub.ingestion.recording.config import (
    REPLAY_DUMMY_MARKER,
    REPLAY_DUMMY_VALUE,
    RecordingConfig,
    ReplayConfig,
    get_recording_password_from_env,
)


class TestRecordingConfig:
    """Tests for RecordingConfig model."""

    def test_default_disabled(self) -> None:
        """Test that recording is disabled by default."""
        config = RecordingConfig()
        assert config.enabled is False

    def test_enabled_requires_password(self) -> None:
        """Test that password is required when enabled."""
        with pytest.raises(ValidationError) as exc_info:
            RecordingConfig(enabled=True)
        assert "password is required" in str(exc_info.value)

    def test_enabled_with_password(self) -> None:
        """Test valid config with password."""
        config = RecordingConfig(
            enabled=True,
            password=SecretStr("mysecret"),
        )
        assert config.enabled is True
        assert config.password is not None
        assert config.password.get_secret_value() == "mysecret"
        assert config.s3_upload is True

    def test_disabled_s3_requires_output_path(self) -> None:
        """Test that output_path is required when s3_upload is disabled."""
        with pytest.raises(ValidationError) as exc_info:
            RecordingConfig(
                enabled=True,
                password=SecretStr("mysecret"),
                s3_upload=False,
            )
        assert "output_path is required" in str(exc_info.value)

    def test_disabled_s3_with_output_path(self) -> None:
        """Test valid config with disabled S3 and output path."""
        config = RecordingConfig(
            enabled=True,
            password=SecretStr("mysecret"),
            s3_upload=False,
            output_path="/tmp/recording.zip",
        )
        assert config.s3_upload is False
        assert config.output_path is not None


class TestReplayConfig:
    """Tests for ReplayConfig model."""

    def test_required_fields(self) -> None:
        """Test that archive_path and password are required."""
        config = ReplayConfig(
            archive_path="/tmp/recording.zip",
            password=SecretStr("mysecret"),
        )
        assert config.archive_path == "/tmp/recording.zip"
        assert config.live_sink is False

    def test_live_sink_mode(self) -> None:
        """Test live sink mode configuration."""
        config = ReplayConfig(
            archive_path="/tmp/recording.zip",
            password=SecretStr("mysecret"),
            live_sink=True,
            gms_server="http://localhost:8080",
        )
        assert config.live_sink is True
        assert config.gms_server == "http://localhost:8080"


class TestGetRecordingPasswordFromEnv:
    """Tests for get_recording_password_from_env function."""

    def test_no_env_vars(self) -> None:
        """Test when no environment variables are set."""
        with patch.dict(os.environ, {}, clear=True):
            assert get_recording_password_from_env() is None

    def test_datahub_recording_password(self) -> None:
        """Test DATAHUB_RECORDING_PASSWORD environment variable."""
        with patch.dict(
            os.environ,
            {"DATAHUB_RECORDING_PASSWORD": "env_password"},
            clear=True,
        ):
            assert get_recording_password_from_env() == "env_password"

    def test_admin_password_fallback(self) -> None:
        """Test fallback to ADMIN_PASSWORD."""
        with patch.dict(
            os.environ,
            {"ADMIN_PASSWORD": "admin_password"},
            clear=True,
        ):
            assert get_recording_password_from_env() == "admin_password"

    def test_datahub_password_takes_precedence(self) -> None:
        """Test that DATAHUB_RECORDING_PASSWORD takes precedence."""
        with patch.dict(
            os.environ,
            {
                "DATAHUB_RECORDING_PASSWORD": "datahub_password",
                "ADMIN_PASSWORD": "admin_password",
            },
            clear=True,
        ):
            assert get_recording_password_from_env() == "datahub_password"


def test_replay_dummy_constants() -> None:
    """Test that replay dummy constants are defined correctly."""
    assert REPLAY_DUMMY_MARKER == "__REPLAY_DUMMY__"
    assert REPLAY_DUMMY_VALUE == "replay-mode-no-secret-needed"
