import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Generator
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from datahub_actions.cli.actions import actions, pipeline_config_to_pipeline
from datahub_actions.pipeline.pipeline import Pipeline
from datahub_actions.pipeline.pipeline_manager import PipelineManager


@contextmanager
def local_monkeypatch(monkeypatch, target, replacement):
    """Apply monkeypatch temporarily within a context."""
    monkeypatch.setattr(target, replacement)
    try:
        yield
    finally:
        monkeypatch.undo()


@pytest.fixture
def capture_logger(
    caplog: pytest.LogCaptureFixture,
) -> Generator[pytest.LogCaptureFixture, None, None]:
    """Fixture to capture logger output with the proper level."""
    caplog.set_level(logging.INFO)
    yield caplog


@pytest.fixture
def mock_pipeline() -> Mock:
    """Create a mock pipeline object."""
    mock = Mock(spec=Pipeline)
    mock.name = "test_pipeline"
    return mock


@pytest.fixture
def mock_pipeline_manager() -> Mock:
    """Create a mock pipeline manager."""
    mock = Mock(spec=PipelineManager)
    mock.start_pipeline = Mock(return_value=None)
    mock.stop_all = Mock(return_value=None)
    return mock


@pytest.fixture
def temp_config_file() -> Generator[str, None, None]:
    """Creates a temporary YAML config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
name: "test_pipeline"
datahub:
    server: "https://test.datahub.io"
    token: "test-token"
source:
    type: "datahub-cloud"
action:
    type: "hello_world"
"""
        )
        config_path = f.name

    yield config_path
    # Cleanup
    os.unlink(config_path)


@pytest.fixture
def disabled_config_file() -> Generator[str, None, None]:
    """Creates a temporary YAML config file with disabled pipeline."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
name: "disabled_pipeline"
enabled: false
datahub:
    server: "https://test.datahub.io"
    token: "test-token"
source:
    type: "datahub-cloud"
action:
    type: "hello_world"
"""
        )
        config_path = f.name

    yield config_path
    # Cleanup
    os.unlink(config_path)


@pytest.fixture
def invalid_config_file() -> Generator[str, None, None]:
    """Creates a temporary YAML config file with unbound variables."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
name: "invalid_pipeline"
datahub:
    server: "https://test.datahub.io"
    token: ${UNDEFINED_VAR}
source:
    type: "datahub-cloud"
action:
    type: "hello_world"
"""
        )
        config_path = f.name

    yield config_path
    # Cleanup
    os.unlink(config_path)


def test_version_command() -> None:
    """Test the version command outputs version information."""
    runner = CliRunner()
    result = runner.invoke(actions, ["version"])
    assert result.exit_code == 0
    assert "DataHub Actions version:" in result.output
    assert "Python version:" in result.output


def test_disabled_pipeline_exits(
    disabled_config_file: str, capture_logger: pytest.LogCaptureFixture
) -> None:
    """Test that disabled pipelines cause program exit."""
    runner = CliRunner()
    result = runner.invoke(actions, ["run", "-c", disabled_config_file])

    assert result.exit_code == 1
    assert any(
        "Skipping pipeline disabled_pipeline as it is not enabled" in record.message
        for record in capture_logger.records
    )


def test_invalid_config_single(invalid_config_file: str) -> None:
    """Test handling of single config with unbound variables."""
    runner = CliRunner()
    result = runner.invoke(actions, ["run", "-c", invalid_config_file])
    assert result.exit_code != 0
    assert "Unbound variable(s) provided in config YAML" in str(result.exception)


def test_all_configs_invalid_or_disabled(
    invalid_config_file: str,
    disabled_config_file: str,
) -> None:
    """Test that program exits when all configs are invalid or disabled."""
    runner = CliRunner()
    result = runner.invoke(
        actions, ["run", "-c", invalid_config_file, "-c", disabled_config_file]
    )
    assert result.exit_code == 1
    assert (
        "Failed to load action configuration. Unbound variable(s) provided in config YAML."
        in str(result.exception)
    )


def test_all_configs_multiple_disabled(
    disabled_config_file: str,
    capture_logger: pytest.LogCaptureFixture,
) -> None:
    """Test that program exits when all configs are invalid or disabled."""
    runner = CliRunner()
    result = runner.invoke(
        actions, ["run", "-c", disabled_config_file, "-c", disabled_config_file]
    )
    assert result.exit_code == 1
    assert any(
        "No valid pipelines were started from 2 config(s). Check that at least one pipeline is enabled and all required environment variables are set."
        in record.message
        for record in capture_logger.records
    )


def test_mixed_valid_and_invalid_configs(
    temp_config_file: str,
    disabled_config_file: str,
    capture_logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    mock_pipeline: Mock,
    mock_pipeline_manager: Mock,
) -> None:
    """Test handling mix of valid and invalid configs."""

    def mock_create_pipeline(config: dict) -> Pipeline:
        """Mock implementation of pipeline creation."""
        return mock_pipeline

    sleep_count = 0

    def mock_sleep(seconds: int) -> None:
        """Mock sleep that raises KeyboardInterrupt after first call."""
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count > 1:  # Allow one sleep to ensure logs are captured
            raise KeyboardInterrupt()

    runner = CliRunner()

    # Use local_monkeypatch for tighter control

    with local_monkeypatch(
        monkeypatch,
        "datahub_actions.pipeline.pipeline.Pipeline.create",
        mock_create_pipeline,
    ), local_monkeypatch(monkeypatch, "time.sleep", mock_sleep), local_monkeypatch(
        monkeypatch,
        "datahub_actions.cli.actions.pipeline_manager",
        mock_pipeline_manager,
    ):
        result = runner.invoke(
            actions, ["run", "-c", temp_config_file, "-c", disabled_config_file]
        )

    assert result.exit_code == 1
    assert any(
        "Skipping pipeline disabled_pipeline as it is not enabled" in record.message
        for record in capture_logger.records
    )
    assert any(
        "Action Pipeline with name 'test_pipeline' is now running." in record.message
        for record in capture_logger.records
    )


def test_debug_mode_with_valid_config(
    temp_config_file: str,
    capture_logger: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    mock_pipeline: Mock,
    mock_pipeline_manager: Mock,
) -> None:
    """Test debug mode with valid pipeline config."""

    def mock_create_pipeline(config: dict) -> Pipeline:
        """Mock implementation of pipeline creation."""
        return mock_pipeline

    sleep_count = 0

    def mock_sleep(seconds: int) -> None:
        """Mock sleep that raises KeyboardInterrupt after first call."""
        nonlocal sleep_count
        sleep_count += 1
        if sleep_count > 1:  # Allow one sleep to ensure logs are captured
            raise KeyboardInterrupt()

    runner = CliRunner()

    # Use local_monkeypatch for tighter control
    with local_monkeypatch(
        monkeypatch,
        "datahub_actions.pipeline.pipeline.Pipeline.create",
        mock_create_pipeline,
    ), local_monkeypatch(monkeypatch, "time.sleep", mock_sleep), local_monkeypatch(
        monkeypatch,
        "datahub_actions.cli.actions.pipeline_manager",
        mock_pipeline_manager,
    ):
        result = runner.invoke(
            actions,
            ["run", "-c", temp_config_file, "--debug"],
        )

    assert result.exit_code == 1
    assert any(
        "Action Pipeline with name 'test_pipeline' is now running." in record.message
        for record in capture_logger.records
    )


def test_pipeline_config_to_pipeline_error() -> None:
    """Test error handling in pipeline creation."""
    invalid_config = {
        "name": "invalid_pipeline",
        # Missing required fields
    }

    with pytest.raises(Exception) as exc_info:
        pipeline_config_to_pipeline(invalid_config)
    assert "Failed to instantiate Actions Pipeline" in str(exc_info.value)


def test_multiple_disabled_configs(
    disabled_config_file: str,
    capture_logger: pytest.LogCaptureFixture,
) -> None:
    """Test handling of multiple disabled configs."""
    runner = CliRunner()
    result = runner.invoke(
        actions, ["run", "-c", disabled_config_file, "-c", disabled_config_file]
    )
    assert result.exit_code == 1
    assert any(
        "No valid pipelines were started from 2 config(s). Check that at least one pipeline is enabled and all required environment variables are set."
        in record.message
        for record in capture_logger.records
    )
    assert any(
        "Skipping pipeline disabled_pipeline as it is not enabled" in record.message
        for record in capture_logger.records
    )


# Type checking with mypy annotations
def test_type_annotations() -> None:
    """Verify type hints are correct (this is a compile-time check)."""
    # These assignments will be checked by mypy
    runner: CliRunner = CliRunner()
    config_dict: dict = {
        "name": "test_pipeline",
        "datahub": {"server": "https://test.datahub.io", "token": "test-token"},
        "source": {"type": "datahub-cloud"},
        "action": {"type": "hello_world"},
    }

    # These assertions serve as runtime checks
    assert isinstance(runner, CliRunner)
    assert isinstance(config_dict, dict)
