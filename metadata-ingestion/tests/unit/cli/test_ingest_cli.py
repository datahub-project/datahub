"""Tests for ingest_cli module."""

from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from datahub.cli.ingest_cli import _setup_recording
from datahub.ingestion.recording.recorder import IngestionRecorder


class TestSetupRecording:
    """Tests for _setup_recording function."""

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_basic_setup_with_cli_password(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test basic setup with CLI password."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "postgres"},
            "sink": {"type": "datahub-rest"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="cli_password",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert isinstance(recorder, IngestionRecorder)
        assert recorder.run_id is not None
        assert recorder.password == "cli_password"
        assert recorder.source_type == "postgres"
        assert recorder.sink_type == "datahub-rest"
        assert recorder.s3_upload is False
        assert recorder.redact_secrets is True
        mock_check_deps.assert_called_once()

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_env_var_password_fallback(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that env var password is used when CLI password not provided."""
        mock_get_password.return_value = "env_password"

        pipeline_config = {
            "source": {"type": "snowflake"},
            "sink": {"type": "file"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password=None,
            record_output_path=None,
            no_s3_upload=False,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.password == "env_password"
        mock_get_password.assert_called_once()

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_recipe_password_when_no_cli_or_env(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that recipe password is used when CLI and env not provided."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "bigquery"},
            "sink": {"type": "datahub-rest"},
            "recording": {
                "password": "recipe_password",
            },
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password=None,
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.password == "recipe_password"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_cli_password_takes_precedence_over_recipe(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that CLI password takes precedence over recipe password."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "mysql"},
            "recording": {"password": "recipe_password"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="cli_password",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.password == "cli_password"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_cli_output_path_override(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that CLI --record-output-path overrides recipe."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "mysql"},
            "recording": {"output_path": "/recipe/path.zip"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path="/cli/path.zip",
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.output_path == "/cli/path.zip"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_no_s3_upload_flag(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that --no-s3-upload flag disables S3 upload."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "redshift"},
            "recording": {"s3_upload": True},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.s3_upload is False

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_no_secret_redaction_flag(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that --no-secret-redaction flag disables redaction."""
        mock_get_password.return_value = None

        pipeline_config = {"source": {"type": "mssql"}}
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=True,
            raw_config=raw_config,
        )

        assert recorder.redact_secrets is False

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_run_id_from_config(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that run_id from config is used."""
        mock_get_password.return_value = None

        pipeline_config = {
            "run_id": "custom-run-id",
            "source": {"type": "sqlite"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.run_id == "custom-run-id"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    @patch("datahub.ingestion.run.pipeline_config._generate_run_id")
    def test_run_id_generation(
        self,
        mock_generate_run_id: MagicMock,
        mock_get_password: MagicMock,
        mock_check_deps: MagicMock,
    ) -> None:
        """Test that run_id is generated when not in config."""
        mock_get_password.return_value = None
        mock_generate_run_id.return_value = "generated-run-id"

        pipeline_config = {"source": {"type": "hive"}}
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.run_id == "generated-run-id"
        mock_generate_run_id.assert_called_once_with("hive")

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    @patch("datahub.ingestion.run.pipeline_config._generate_run_id")
    def test_run_id_generation_with_unknown_source(
        self,
        mock_generate_run_id: MagicMock,
        mock_get_password: MagicMock,
        mock_check_deps: MagicMock,
    ) -> None:
        """Test that run_id generation uses 'unknown' when source type not specified."""
        mock_get_password.return_value = None
        mock_generate_run_id.return_value = "generated-run-id"

        pipeline_config: Dict[str, Any] = {}
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.run_id == "generated-run-id"
        mock_generate_run_id.assert_called_once_with("unknown")

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_missing_password_error(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that missing password causes sys.exit."""
        mock_get_password.return_value = None

        pipeline_config = {"source": {"type": "postgres"}}
        raw_config = pipeline_config.copy()

        with pytest.raises(SystemExit):
            _setup_recording(
                pipeline_config=pipeline_config,
                record_password=None,
                record_output_path=None,
                no_s3_upload=True,
                no_secret_redaction=False,
                raw_config=raw_config,
            )

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_invalid_config_validation_error(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that invalid config causes sys.exit."""
        mock_get_password.return_value = None

        # Invalid: s3_upload=True but no output_path
        pipeline_config = {
            "source": {"type": "postgres"},
            "recording": {
                "password": "test",
                "s3_upload": True,
                # Missing output_path
            },
        }
        raw_config = pipeline_config.copy()

        with pytest.raises(SystemExit):
            _setup_recording(
                pipeline_config=pipeline_config,
                record_password=None,
                record_output_path=None,
                no_s3_upload=False,
                no_secret_redaction=False,
                raw_config=raw_config,
            )

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_default_sink_type(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that default sink type is 'datahub-rest' when not specified."""
        mock_get_password.return_value = None

        pipeline_config = {"source": {"type": "postgres"}}
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password="test",
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.sink_type == "datahub-rest"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_enabled_always_set_to_true(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that enabled is always set to True regardless of recipe."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "postgres"},
            "recording": {"enabled": False, "password": "test"},
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password=None,
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        # The function should still work (enabled is set internally)
        assert isinstance(recorder, IngestionRecorder)

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_s3_upload_with_output_path(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that S3 upload works when output_path is provided."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "postgres"},
            "recording": {
                "password": "test",
                "s3_upload": True,
                "output_path": "s3://bucket/recording.zip",
            },
        }
        raw_config = pipeline_config.copy()

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password=None,
            record_output_path=None,
            no_s3_upload=False,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.s3_upload is True
        assert recorder.output_path == "s3://bucket/recording.zip"

    @patch("datahub.ingestion.recording.config.check_recording_dependencies")
    @patch("datahub.ingestion.recording.config.get_recording_password_from_env")
    def test_recipe_config_preserved_in_raw_config(
        self, mock_get_password: MagicMock, mock_check_deps: MagicMock
    ) -> None:
        """Test that raw_config is passed through to IngestionRecorder."""
        mock_get_password.return_value = None

        pipeline_config = {
            "source": {"type": "postgres"},
            "recording": {"password": "test"},
        }
        raw_config = {"source": {"type": "postgres"}, "custom": "value"}

        recorder = _setup_recording(
            pipeline_config=pipeline_config,
            record_password=None,
            record_output_path=None,
            no_s3_upload=True,
            no_secret_redaction=False,
            raw_config=raw_config,
        )

        assert recorder.recipe == raw_config
        assert recorder.recipe.get("custom") == "value"
