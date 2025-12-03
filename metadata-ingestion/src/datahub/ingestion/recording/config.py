"""Recording configuration models."""

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, SecretStr, model_validator

from datahub.configuration.common import ConfigModel

# Fixed S3 prefix for all recordings - enables automation to find them
S3_RECORDING_PREFIX = "dh-ingestion-debug-recordings"

# Marker used to replace secrets in stored recipes
REPLAY_DUMMY_MARKER = "__REPLAY_DUMMY__"

# Dummy value used during replay to pass Pydantic validation
REPLAY_DUMMY_VALUE = "replay-mode-no-secret-needed"


class RecordingConfig(ConfigModel):
    """Configuration for recording ingestion runs."""

    enabled: bool = Field(
        default=False,
        description="Enable recording of ingestion run for debugging.",
    )

    password: Optional[SecretStr] = Field(
        default=None,
        description="Password for encrypting the recording archive. "
        "Required when enabled=true. Can be supplied via UI secrets.",
    )

    s3_upload: bool = Field(
        default=True,
        description="Upload recording to S3 after completion. "
        "Set to false for local testing.",
    )

    output_path: Optional[Path] = Field(
        default=None,
        description="Local path to save the recording archive. "
        "Required when s3_upload=false. Optional when s3_upload=true "
        "(will keep a local copy in addition to S3 upload).",
    )

    @model_validator(mode="after")
    def validate_recording_config(self) -> "RecordingConfig":
        """Validate recording configuration requirements."""
        if self.enabled and not self.password:
            raise ValueError("password is required when recording is enabled")
        if self.enabled and not self.s3_upload and not self.output_path:
            raise ValueError("output_path is required when s3_upload is disabled")
        return self


class ReplayConfig(ConfigModel):
    """Configuration for replaying recorded ingestion runs."""

    archive_path: str = Field(
        description="Path to the recording archive. Can be a local file path "
        "or an S3 URL (s3://bucket/path/to/recording.zip).",
    )

    password: SecretStr = Field(
        description="Password for decrypting the recording archive.",
    )

    live_sink: bool = Field(
        default=False,
        description="If true, replay sources from recording but emit to real GMS. "
        "If false (default), fully air-gapped replay with mocked GMS responses.",
    )

    gms_server: Optional[str] = Field(
        default=None,
        description="GMS server URL when live_sink=true. "
        "If not specified, uses the server from the recorded recipe.",
    )

    @model_validator(mode="after")
    def validate_replay_config(self) -> "ReplayConfig":
        """Validate replay configuration."""
        # gms_server without live_sink is allowed but will be ignored
        return self


def get_recording_password_from_env() -> Optional[str]:
    """Get recording password from environment variable.

    Checks DATAHUB_RECORDING_PASSWORD first, then falls back to ADMIN_PASSWORD.
    Returns None if neither is set.
    """
    return os.getenv("DATAHUB_RECORDING_PASSWORD") or os.getenv("ADMIN_PASSWORD")


def check_recording_dependencies() -> None:
    """Check that recording dependencies are installed.

    Raises ImportError with helpful message if dependencies are missing.
    """
    missing = []

    try:
        import vcr  # noqa: F401
    except ImportError:
        missing.append("vcrpy")

    try:
        import pyzipper  # noqa: F401
    except ImportError:
        missing.append("pyzipper")

    if missing:
        raise ImportError(
            f"Recording dependencies not installed: {', '.join(missing)}. "
            "Install with: pip install 'acryl-datahub[debug-recording]'"
        )
