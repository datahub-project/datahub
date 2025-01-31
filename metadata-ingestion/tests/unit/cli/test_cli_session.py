import json
import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from datahub.cli.session_cli import session
from datahub.ingestion.graph.config import DatahubClientConfig


@pytest.fixture
def mock_sessions_file(tmp_path):
    # Create a temporary sessions file for testing
    test_sessions_file = tmp_path / "sessions.json"
    with patch(
        "datahub.cli.session_cli.DATAHUB_SESSIONS_PATH", str(test_sessions_file)
    ):
        yield test_sessions_file
    # Cleanup
    if test_sessions_file.exists():
        test_sessions_file.unlink()


@pytest.fixture
def sample_config():
    return DatahubClientConfig(server="http://localhost:8080", token="test-token")


def test_create_session(mock_sessions_file):
    runner = CliRunner()
    with runner.isolated_filesystem():
        # Test creating a new profile
        result = runner.invoke(
            session,
            ["create"],
            input="http://localhost:8080\ntest-token\ntest-profile\n",
        )
        assert result.exit_code == 0
        assert "Created profile: test-profile" in result.output

        # Verify the session was saved
        with open(mock_sessions_file) as f:
            saved_sessions = json.load(f)
            assert "test-profile" in saved_sessions
            assert saved_sessions["test-profile"]["server"] == "http://localhost:8080"
            assert saved_sessions["test-profile"]["token"] == "test-token"


def test_list_sessions_empty_data(mock_sessions_file):
    runner = CliRunner()
    # Create an empty sessions file
    os.makedirs(os.path.dirname(mock_sessions_file), exist_ok=True)
    with open(mock_sessions_file, "w") as f:
        json.dump({}, f)

    result = runner.invoke(session, ["list"])
    assert result.exit_code == 0
    assert "No profiles found" in result.output


def test_list_sessions(mock_sessions_file, sample_config):
    runner = CliRunner()
    # Setup test data
    sessions_data = {"test-profile": sample_config.dict()}
    os.makedirs(os.path.dirname(mock_sessions_file), exist_ok=True)
    with open(mock_sessions_file, "w") as f:
        json.dump(sessions_data, f)

    result = runner.invoke(session, ["list"])
    assert result.exit_code == 0
    assert "test-profile" in result.output
    assert "http://localhost:8080" in result.output


def test_delete_session(mock_sessions_file, sample_config):
    runner = CliRunner()
    # Setup test data
    sessions_data = {"test-profile": sample_config.dict()}
    os.makedirs(os.path.dirname(mock_sessions_file), exist_ok=True)
    with open(mock_sessions_file, "w") as f:
        json.dump(sessions_data, f)

    # Test deleting existing profile
    result = runner.invoke(session, ["delete", "test-profile"])
    assert result.exit_code == 0
    assert "Deleted profile: test-profile" in result.output

    # Verify profile was deleted
    with open(mock_sessions_file) as f:
        saved_sessions = json.load(f)
        assert "test-profile" not in saved_sessions


def test_delete_unknown_session(mock_sessions_file, sample_config):
    runner = CliRunner()
    # Setup test data with a known profile
    sessions_data = {"test-profile": sample_config.dict()}
    os.makedirs(os.path.dirname(mock_sessions_file), exist_ok=True)
    with open(mock_sessions_file, "w") as f:
        json.dump(sessions_data, f)

    # Test deleting non-existent profile
    result = runner.invoke(session, ["delete", "unknown-profile"])
    assert result.exit_code == 0
    assert "Profile unknown-profile not found" in result.output

    # Verify original profile still exists
    with open(mock_sessions_file) as f:
        saved_sessions = json.load(f)
        assert "test-profile" in saved_sessions


def test_use_session(mock_sessions_file, sample_config):
    runner = CliRunner()
    # Setup test data
    sessions_data = {"test-profile": sample_config.dict()}
    os.makedirs(os.path.dirname(mock_sessions_file), exist_ok=True)
    with open(mock_sessions_file, "w") as f:
        json.dump(sessions_data, f)

    with patch("datahub.cli.session_cli.persist_raw_datahub_config") as mock_persist:
        result = runner.invoke(session, ["use", "test-profile"])
        assert result.exit_code == 0
        assert "Using profile test-profile" in result.output
        mock_persist.assert_called_once_with(sample_config.dict())


def test_save_session(mock_sessions_file, sample_config):
    runner = CliRunner()
    with patch(
        "datahub.cli.session_cli.load_client_config", return_value=sample_config
    ):
        result = runner.invoke(session, ["save", "--profile", "new-profile"])
        assert result.exit_code == 0
        assert "Saved current datahubenv as profile: new-profile" in result.output

        # Verify the session was saved
        with open(mock_sessions_file) as f:
            saved_sessions = json.load(f)
            assert "new-profile" in saved_sessions
            assert saved_sessions["new-profile"]["server"] == "http://localhost:8080"
            assert saved_sessions["new-profile"]["token"] == "test-token"


def test_create_session_with_password(mock_sessions_file):
    runner = CliRunner()
    with runner.isolated_filesystem(), patch(
        "datahub.cli.session_cli.generate_access_token",
        return_value=("username", "generated-token"),
    ):
        result = runner.invoke(
            session,
            ["create", "--use-password"],
            input="http://localhost:8080\nusername\npassword\ntest-profile\n",
        )
        assert result.exit_code == 0
        assert "Created profile: test-profile" in result.output

        # Verify the session was saved with generated token
        with open(mock_sessions_file) as f:
            saved_sessions = json.load(f)
            assert saved_sessions["test-profile"]["token"] == "generated-token"
