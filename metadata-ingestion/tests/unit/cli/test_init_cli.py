from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from datahub.entrypoints import init


@pytest.fixture
def temp_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create temporary config file path and set it as DATAHUB_CONFIG_PATH."""
    config_path = tmp_path / ".datahubenv"
    monkeypatch.setattr("datahub.entrypoints.DATAHUB_CONFIG_PATH", str(config_path))
    monkeypatch.setattr(
        "datahub.cli.config_utils.DATAHUB_CONFIG_PATH", str(config_path)
    )
    return config_path


@pytest.fixture
def mock_generate_token():
    """Mock generate_access_token to avoid actual API calls."""
    with patch("datahub.entrypoints.generate_access_token") as mock:
        mock.return_value = ("dummy_id", "generated-token-123")
        yield mock


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all DATAHUB env vars to ensure clean test environment."""
    env_vars = [
        "DATAHUB_GMS_URL",
        "DATAHUB_GMS_TOKEN",
        "DATAHUB_USERNAME",
        "DATAHUB_PASSWORD",
    ]
    for var in env_vars:
        monkeypatch.delenv(var, raising=False)


class TestNonInteractiveCLIFlags:
    """Tests for non-interactive init with CLI flags."""

    def test_init_with_token_args(self, temp_config: Path, clean_env: None) -> None:
        """Test non-interactive init with --host and --token."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--token",
                "test-token-123",
            ],
        )

        assert result.exit_code == 0
        assert temp_config.exists()
        assert "Configuration written" in result.output

        # Verify config contents
        config_content = temp_config.read_text()
        assert "localhost:8080" in config_content
        assert "test-token-123" in config_content

    def test_init_with_password_args(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test non-interactive init with --use-password, --host, --username, --password."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--use-password",
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret123",
            ],
        )

        assert result.exit_code == 0
        assert temp_config.exists()
        assert "Configuration written" in result.output

        # Verify generate_access_token was called correctly
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret123",
            gms_url="http://localhost:8080",
            validity="ONE_HOUR",
        )

        # Verify generated token was written
        config_content = temp_config.read_text()
        assert "generated-token-123" in config_content

    def test_init_force_overwrite(self, temp_config: Path, clean_env: None) -> None:
        """Test --force skips overwrite confirmation."""
        # Create existing config
        temp_config.write_text("existing config")

        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--token",
                "new-token",
                "--force",
            ],
        )

        # Should not prompt for confirmation with --force
        assert result.exit_code == 0
        assert "Overwrite?" not in result.output
        assert "Configuration written" in result.output

        # Verify config was overwritten
        config_content = temp_config.read_text()
        assert "new-token" in config_content
        assert "existing config" not in config_content

    def test_init_without_force_prompts(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test without --force prompts for confirmation when config exists."""
        # Create existing config
        temp_config.write_text("existing config")

        runner = CliRunner()
        # Provide 'n' to abort the confirmation
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--token",
                "new-token",
            ],
            input="n\n",
        )

        assert result.exit_code == 1
        assert "Aborted" in result.output


class TestNonInteractiveEnvVars:
    """Tests for non-interactive init with environment variables."""

    def test_init_with_env_vars(
        self, temp_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test non-interactive init with environment variables."""
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token-456")

        runner = CliRunner()
        result = runner.invoke(init, ["--force"])

        assert result.exit_code == 0
        assert temp_config.exists()

        # Verify config contents
        config_content = temp_config.read_text()
        assert "localhost:8080" in config_content
        assert "env-token-456" in config_content

    def test_init_with_password_env_vars(
        self,
        temp_config: Path,
        monkeypatch: pytest.MonkeyPatch,
        mock_generate_token: Any,
    ) -> None:
        """Test password flow with environment variables."""
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
        monkeypatch.setenv("DATAHUB_USERNAME", "bob")
        monkeypatch.setenv("DATAHUB_PASSWORD", "secret456")

        runner = CliRunner()
        result = runner.invoke(init, ["--use-password", "--force"])

        assert result.exit_code == 0
        assert temp_config.exists()

        # Verify generate_access_token was called with env var values
        mock_generate_token.assert_called_once_with(
            username="bob",
            password="secret456",
            gms_url="http://localhost:8080",
            validity="ONE_HOUR",
        )

    def test_cli_args_override_env_vars(
        self, temp_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test CLI arguments take precedence over environment variables."""
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://env-host:8080")
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "env-token")

        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://cli-host:8080",
                "--token",
                "cli-token",
            ],
        )

        assert result.exit_code == 0

        # Verify CLI values were used, not env values
        config_content = temp_config.read_text()
        assert "cli-host" in config_content
        assert "cli-token" in config_content
        assert "env-host" not in config_content
        assert "env-token" not in config_content


class TestValidation:
    """Tests for input validation."""

    def test_validation_token_and_password(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test error when using both --token and --use-password."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--token",
                "my-token",
                "--use-password",
                "--username",
                "alice",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code != 0
        assert "Cannot use both --token and username/password" in result.output

    def test_validation_token_and_username_password(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test error when providing both token and credentials."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--token",
                "my-token",
                "--username",
                "alice",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code != 0
        assert "Cannot use both --token and username/password" in result.output

    def test_validation_username_without_password(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test error when providing username without password."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
            ],
        )

        assert result.exit_code != 0
        assert "Both --username and --password required" in result.output

    def test_validation_password_without_username(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test error when providing password without username."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code != 0
        assert "Both --username and --password required" in result.output

    def test_validation_token_duration_without_credentials(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test error when using --token-duration with --token."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--token",
                "my-token",
                "--token-duration",
                "ONE_MONTH",
            ],
        )

        assert result.exit_code != 0
        assert "--token-duration only applies when generating token" in result.output


class TestBackwardCompatibility:
    """Tests for backward compatibility with interactive mode."""

    def test_interactive_mode_token_flow(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test interactive prompts work when no args provided (token flow)."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            input="http://localhost:8080\nmy-interactive-token\n",
        )

        assert result.exit_code == 0
        assert temp_config.exists()

        # Verify prompted values were used
        config_content = temp_config.read_text()
        assert "localhost:8080" in config_content
        assert "my-interactive-token" in config_content

    def test_interactive_mode_password_flow(
        self,
        temp_config: Path,
        clean_env: None,
        mock_generate_token: Any,
    ) -> None:
        """Test interactive prompts work with --use-password (password flow)."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            ["--use-password"],
            input="http://localhost:8080\nalice\nsecret123\n",
        )

        assert result.exit_code == 0
        assert temp_config.exists()

        # Verify generate_access_token was called with prompted values
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret123",
            gms_url="http://localhost:8080",
            validity="ONE_HOUR",
        )

    def test_partial_args_prompts_for_missing(
        self, temp_config: Path, clean_env: None
    ) -> None:
        """Test providing only some args prompts for the rest."""
        runner = CliRunner()
        # Provide host via CLI, token via prompt
        result = runner.invoke(
            init,
            ["--host", "http://localhost:8080"],
            input="my-prompted-token\n",
        )

        assert result.exit_code == 0
        assert temp_config.exists()

        config_content = temp_config.read_text()
        assert "localhost:8080" in config_content
        assert "my-prompted-token" in config_content


class TestURLHandling:
    """Tests for URL fixup and validation."""

    def test_gms_url_fixup(self, temp_config: Path, clean_env: None) -> None:
        """Test that GMS URL fixup is applied correctly."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "https://my-instance.acryl.io/gms",
                "--token",
                "test-token",
            ],
        )

        assert result.exit_code == 0

        # URL should be fixed up by fixup_gms_url
        config_content = temp_config.read_text()
        assert "acryl.io" in config_content


class TestRealWorldScenarios:
    """Tests simulating real-world usage patterns."""

    def test_agent_scenario_env_vars(
        self, temp_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simulate an agent using environment variables."""
        # Agent sets up environment
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "agent-token-xyz")

        runner = CliRunner()
        # Agent runs init with --force (non-interactive)
        result = runner.invoke(init, ["--force"])

        assert result.exit_code == 0
        assert temp_config.exists()

    def test_agent_scenario_cli_args(self, temp_config: Path, clean_env: None) -> None:
        """Simulate an agent using CLI arguments."""
        runner = CliRunner()
        # Agent runs init with all args in one command
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--token",
                "agent-token-abc",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert temp_config.exists()
        assert "Configuration written" in result.output

    def test_ci_cd_scenario(
        self, temp_config: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simulate CI/CD pipeline usage."""
        # CI/CD typically uses env vars and force flag
        monkeypatch.setenv("DATAHUB_GMS_URL", "https://prod.example.com/gms")
        monkeypatch.setenv("DATAHUB_GMS_TOKEN", "ci-token-secret")

        runner = CliRunner()
        result = runner.invoke(init, ["--force"])

        assert result.exit_code == 0
        assert temp_config.exists()
        # Should complete without any user interaction
        assert "Enter" not in result.output


class TestAutoDetectionAndDeprecation:
    """Tests for auto-detection of token generation and --use-password deprecation."""

    def test_init_auto_detect_token_generation(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test auto-detection of token generation mode without --use-password."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code == 0
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="ONE_HOUR",
        )
        assert "Generated token (expires: ONE_HOUR)" in result.output

    def test_init_use_password_deprecated(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test deprecation warning for --use-password."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--use-password",
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code == 0
        assert "Warning: --use-password is deprecated" in result.output
        assert (
            "Token generation is now auto-detected when --username and --password are provided"
            in result.output
        )


class TestTokenDuration:
    """Tests for configurable token duration."""

    def test_init_with_custom_token_duration(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test token generation with custom duration."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
                "--token-duration",
                "ONE_MONTH",
            ],
        )

        assert result.exit_code == 0
        assert "Generated token (expires: ONE_MONTH)" in result.output
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="ONE_MONTH",
        )

    def test_init_with_no_expiry_duration(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test token generation with NO_EXPIRY duration."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
                "--token-duration",
                "NO_EXPIRY",
            ],
        )

        assert result.exit_code == 0
        assert "Generated token (expires: NO_EXPIRY)" in result.output
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="NO_EXPIRY",
        )

    def test_init_case_insensitive_duration(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test token duration is case-insensitive."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
                "--token-duration",
                "one_week",
            ],
        )

        assert result.exit_code == 0
        assert "Generated token (expires: ONE_WEEK)" in result.output
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="ONE_WEEK",
        )

    def test_init_default_duration_one_hour(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test default token duration is ONE_HOUR."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
            ],
        )

        assert result.exit_code == 0
        assert "Generated token (expires: ONE_HOUR)" in result.output
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="ONE_HOUR",
        )

    def test_init_with_password_flag_and_duration(
        self, temp_config: Path, clean_env: None, mock_generate_token: Any
    ) -> None:
        """Test custom duration works with deprecated --use-password flag."""
        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--use-password",
                "--host",
                "http://localhost:8080",
                "--username",
                "alice",
                "--password",
                "secret",
                "--token-duration",
                "ONE_YEAR",
            ],
        )

        assert result.exit_code == 0
        assert "Warning: --use-password is deprecated" in result.output
        assert "Generated token (expires: ONE_YEAR)" in result.output
        mock_generate_token.assert_called_once_with(
            username="alice",
            password="secret",
            gms_url="http://localhost:8080",
            validity="ONE_YEAR",
        )

    def test_init_env_vars_with_duration(
        self,
        temp_config: Path,
        monkeypatch: pytest.MonkeyPatch,
        mock_generate_token: Any,
    ) -> None:
        """Test token duration with environment variables."""
        monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8080")
        monkeypatch.setenv("DATAHUB_USERNAME", "bob")
        monkeypatch.setenv("DATAHUB_PASSWORD", "secret456")

        runner = CliRunner()
        result = runner.invoke(
            init,
            [
                "--token-duration",
                "THREE_MONTHS",
                "--force",
            ],
        )

        assert result.exit_code == 0
        assert "Generated token (expires: THREE_MONTHS)" in result.output
        mock_generate_token.assert_called_once_with(
            username="bob",
            password="secret456",
            gms_url="http://localhost:8080",
            validity="THREE_MONTHS",
        )
